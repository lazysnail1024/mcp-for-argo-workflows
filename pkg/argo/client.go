package argo

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/argoproj/argo-workflows/v4/pkg/apiclient"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/clusterworkflowtemplate"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/cronworkflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/info"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowarchive"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowtemplate"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/pipekit/mcp-for-argo-workflows/pkg/compat"
)

const (
	maxCachedArgoClients = 64
	modeArgoServer       = "argo-server"
	modeKubernetesDirect = "kubernetes"
)

var (
	// ErrArchivedWorkflowsNotSupported is returned when accessing archived workflows in direct K8s mode.
	ErrArchivedWorkflowsNotSupported = errors.New("archived workflows are only available in Argo Server mode")

	// ErrMultiContextUnavailable is returned when kubeconfig context switching is not enabled.
	ErrMultiContextUnavailable = errors.New("per-call kubeconfig context selection is not available in this mode")

	// ErrArgoBaseURLUnavailable is returned when argoBaseUrl override is requested in direct K8s mode.
	ErrArgoBaseURLUnavailable = errors.New("argoBaseUrl override is only supported in Argo Server mode")
)

// TargetDescriptor summarizes a configured Argo Server instance or Kubeconfig context.
type TargetDescriptor struct {
	Name             string `json:"name"`
	Mode             string `json:"mode"`
	Default          bool   `json:"default"`
	DefaultNamespace string `json:"defaultNamespace,omitempty"`
	ArchiveSupported bool   `json:"archiveSupported"`
	Version          string `json:"version,omitempty"`
	Protocol         string `json:"protocol,omitempty"`
}

// ClusterInfo reports runtime metadata probed from a target cluster.
type ClusterInfo struct {
	Mode             string `json:"mode"`
	Target           string `json:"target"`
	ArgoVersion      string `json:"argoVersion,omitempty"`
	ManagedNamespace string `json:"managedNamespace,omitempty"`
	ArchiveSupported bool   `json:"archiveSupported"`
}

// ClientInterface abstracts Argo Workflows operations symmetrically across pure
// Argo Server mode and pure Direct Kubernetes API mode (never mixed in one instance).
type ClientInterface interface {
	WorkflowService() workflow.WorkflowServiceClient
	CronWorkflowService() (cronworkflow.CronWorkflowServiceClient, error)
	WorkflowTemplateService() (workflowtemplate.WorkflowTemplateServiceClient, error)
	ClusterWorkflowTemplateService() (clusterworkflowtemplate.ClusterWorkflowTemplateServiceClient, error)
	ArchivedWorkflowService() (workflowarchive.ArchivedWorkflowServiceClient, error)
	InfoService() (info.InfoServiceClient, error)

	// ListWorkflowEvents symmetrically fetches Kubernetes Events for a workflow and its pods:
	// - Argo Server mode: bounded WatchEvents stream snapshot (no K8s SA required).
	// - Direct K8s mode: CoreV1().Events().List().
	ListWorkflowEvents(ctx context.Context, namespace, workflowName string, podNames []string, onlyWarnings bool) ([]corev1.Event, error)

	// GetPodStatuses returns Pod status objects in Direct K8s mode, or nil/nil in Argo Server mode.
	GetPodStatuses(ctx context.Context, namespace string, podNames []string) (map[string]*corev1.Pod, error)

	// ProbeClusterInfo inspects version and capabilities for the active target.
	ProbeClusterInfo(ctx context.Context) (*ClusterInfo, error)

	IsArgoServerMode() bool
	DefaultNamespace() string
	TargetID() string
	Context() context.Context

	// ResolveTarget resolves an optional per-call argoBaseUrl (Argo Server mode) or
	// kubeconfig context (Direct K8s mode), returning the bound client and merged context.
	ResolveTarget(ctx context.Context, argoBaseURL, kubeContext string) (context.Context, ClientInterface, error)

	// ListTargets lists all reachable clusters/instances and their default flags.
	ListTargets(ctx context.Context) ([]TargetDescriptor, error)

	// MultiContextEnabled reports whether per-call kubeconfig context selection is active.
	MultiContextEnabled() bool
}

// Client wraps a single Argo Server or Direct K8s connection and optional target pool.
type Client struct {
	config         *Config
	apiClient      apiclient.Client
	kubeClient     kubernetes.Interface
	tokenRegistry  *TokenRegistry
	defaultBaseURL string
	targetID       string
	defaultNS      string
	// ctx carries the Argo SDK logger and (in Direct K8s mode) gatekeeper K8s client metadata.
	ctx context.Context //nolint:containedctx // Required by Argo SDK apiclient design

	// Multi-instance cache for Argo Server TokenRegistry overrides.
	mu             sync.Mutex
	instanceCache  map[string]*Client
	cachedOrder    []string
	cachedInfoOnce sync.Once
	cachedInfo     *ClusterInfo
}

var _ ClientInterface = (*Client)(nil)

// NewClient initializes an Argo Workflows client strictly in either Argo Server mode
// (when config.ArgoServer != "") or Direct Kubernetes API mode (when config.ArgoServer == "").
func NewClient(ctx context.Context, cfg *Config) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}
	clone := *cfg
	if strings.TrimSpace(clone.Namespace) == "" {
		clone.Namespace = defaultNamespace
	}

	if strings.TrimSpace(clone.ArgoServer) != "" {
		registry, err := LoadTokenRegistryFromFile(clone.TokenRegistryPath, WithDefaultSecure(clone.Secure))
		if err != nil {
			return nil, err
		}
		return newArgoServerClient(ctx, &clone, registry)
	}

	return newDirectKubeClient(ctx, &clone)
}

func newArgoServerClient(ctx context.Context, cfg *Config, registry *TokenRegistry) (*Client, error) {
	parsed, err := ParseArgoServerURL(cfg.ArgoServer, cfg.Secure)
	if err != nil {
		return nil, err
	}

	token := NormalizeBearerToken(cfg.ArgoToken)
	if token == "" && registry != nil {
		if regTok, ok := registry.GetToken(parsed.NormalizedBaseURL); ok {
			token = regTok
		}
	}

	opts := apiclient.Opts{
		ArgoServerOpts: apiclient.ArgoServerOpts{
			URL:                parsed.HostPort,
			Path:               parsed.Path,
			Secure:             parsed.Secure,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
			HTTP1:              cfg.HTTP1,
			CACert:             cfg.CACert,
		},
		// Always provide AuthSupplier even if empty (supports unauthenticated --auth-mode=server).
		AuthSupplier: func() string {
			return token
		},
	}

	if cfg.HTTP1 {
		httpClient, tlsErr := buildArgoHTTPClient(cfg.CACert, cfg.InsecureSkipVerify)
		if tlsErr != nil {
			return nil, fmt.Errorf("failed to build Argo HTTP client: %w", tlsErr)
		}
		opts.ArgoServerOpts.HTTP1Client = httpClient
	}

	clientCtx, apiClient, err := apiclient.NewClientFromOptsWithContext(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create Argo Server client for %s: %w", parsed.NormalizedBaseURL, err)
	}

	slog.Info("initialized Argo Server client",
		"target", parsed.NormalizedBaseURL,
		"http1", cfg.HTTP1,
		"secure", parsed.Secure,
		"namespace", cfg.Namespace,
		"registryEntries", registry.Size(),
	)

	return &Client{
		config:         cfg,
		apiClient:      apiClient,
		tokenRegistry:  registry,
		defaultBaseURL: parsed.NormalizedBaseURL,
		targetID:       parsed.NormalizedBaseURL,
		defaultNS:      cfg.Namespace,
		ctx:            clientCtx,
		instanceCache:  make(map[string]*Client),
	}, nil
}

func buildArgoHTTPClient(caCertPath string, insecureSkipVerify bool) (*http.Client, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify, //nolint:gosec // Explicitly operator-configured flag
	}
	if strings.TrimSpace(caCertPath) != "" {
		pemBytes, err := os.ReadFile(strings.TrimSpace(caCertPath)) //nolint:gosec // Operator-configured CA path
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert %q: %w", caCertPath, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("CA cert %q contains no valid PEM certificates", caCertPath)
		}
		tlsConfig.RootCAs = pool
	}

	baseTransport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		TLSClientConfig:       tlsConfig,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &http.Client{
		Transport: compat.NewNormalizingRoundTripper(baseTransport),
	}, nil
}

// compatClientConfig wraps clientcmd.ClientConfig to force JSON wire encoding and mount
// compat.NormalizingRoundTripper on Direct Kubernetes API requests so v3.6 legacy fields
// (spec.schedule, mutex, semaphore) are losslessly preserved before v4 struct unmarshaling.
type compatClientConfig struct {
	inner clientcmd.ClientConfig
}

func (c compatClientConfig) RawConfig() (clientcmdapi.Config, error) {
	return c.inner.RawConfig()
}

func (c compatClientConfig) ClientConfig() (*restclient.Config, error) {
	rc, err := c.inner.ClientConfig()
	if err != nil {
		return nil, err
	}
	copied := restclient.CopyConfig(rc)
	copied.ContentType = "application/json"
	copied.Wrap(compat.WrapRoundTripper)
	return copied, nil
}

func (c compatClientConfig) Namespace() (string, bool, error) {
	return c.inner.Namespace()
}

func (c compatClientConfig) ConfigAccess() clientcmd.ConfigAccess {
	return c.inner.ConfigAccess()
}

func newDirectKubeClient(ctx context.Context, cfg *Config) (*Client, error) {
	loadingRules := buildLoadingRules(cfg.Kubeconfig)
	overrides := &clientcmd.ConfigOverrides{CurrentContext: cfg.Context}

	baseConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	wrappedConfig := compatClientConfig{inner: baseConfig}

	resolvedNS := cfg.Namespace
	if ctxNS, overridden, nsErr := baseConfig.Namespace(); nsErr == nil && overridden && strings.TrimSpace(ctxNS) != "" {
		// Respect the kubeconfig context's explicit namespace when global default wasn't overridden,
		// or store it cleanly per context.
		if cfg.Namespace == defaultNamespace {
			resolvedNS = ctxNS
		}
	}

	contextName := resolveContextName(loadingRules, overrides)

	restCfg, err := wrappedConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build Kubernetes rest config: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes clientset: %w", err)
	}

	opts := apiclient.Opts{
		ClientConfigSupplier: func() clientcmd.ClientConfig {
			return wrappedConfig
		},
	}

	clientCtx, apiClient, err := apiclient.NewClientFromOptsWithContext(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create Direct Kubernetes Argo client: %w", err)
	}

	slog.Info("initialized Direct Kubernetes Argo client",
		"context", contextName,
		"namespace", resolvedNS,
	)

	return &Client{
		config:     cfg,
		apiClient:  apiClient,
		kubeClient: kubeClient,
		targetID:   contextName,
		defaultNS:  resolvedNS,
		ctx:        clientCtx,
	}, nil
}

func (c *Client) WorkflowService() workflow.WorkflowServiceClient {
	return c.apiClient.NewWorkflowServiceClient(c.ctx)
}

func (c *Client) CronWorkflowService() (cronworkflow.CronWorkflowServiceClient, error) {
	svc, err := c.apiClient.NewCronWorkflowServiceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create cron workflow service client: %w", err)
	}
	return svc, nil
}

func (c *Client) WorkflowTemplateService() (workflowtemplate.WorkflowTemplateServiceClient, error) {
	svc, err := c.apiClient.NewWorkflowTemplateServiceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow template service client: %w", err)
	}
	return svc, nil
}

func (c *Client) ClusterWorkflowTemplateService() (clusterworkflowtemplate.ClusterWorkflowTemplateServiceClient, error) {
	svc, err := c.apiClient.NewClusterWorkflowTemplateServiceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster workflow template service client: %w", err)
	}
	return svc, nil
}

func (c *Client) ArchivedWorkflowService() (workflowarchive.ArchivedWorkflowServiceClient, error) {
	if !c.IsArgoServerMode() {
		return nil, ErrArchivedWorkflowsNotSupported
	}
	svc, err := c.apiClient.NewArchivedWorkflowServiceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create archived workflow service client: %w", err)
	}
	return svc, nil
}

func (c *Client) InfoService() (info.InfoServiceClient, error) {
	svc, err := c.apiClient.NewInfoServiceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create info service client: %w", err)
	}
	return svc, nil
}

func (c *Client) ListWorkflowEvents(
	ctx context.Context,
	namespace string,
	workflowName string,
	podNames []string,
	onlyWarnings bool,
) ([]corev1.Event, error) {
	if c.IsArgoServerMode() {
		return collectEventsViaWatch(ctx, c.WorkflowService(), namespace, workflowName, podNames, onlyWarnings)
	}
	if c.kubeClient == nil {
		return nil, nil
	}

	evList, err := c.kubeClient.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list Kubernetes events in namespace %q: %w", namespace, err)
	}

	podSet := make(map[string]struct{}, len(podNames))
	for _, p := range podNames {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			podSet[trimmed] = struct{}{}
		}
	}

	dedup := make(map[string]corev1.Event)
	for i := range evList.Items {
		ev := &evList.Items[i]
		if matchesWorkflowEvent(ev, workflowName, podSet, onlyWarnings) {
			dedup[eventDedupKey(ev)] = *ev
		}
	}
	return sortEventsNewestFirst(dedup), nil
}

func (c *Client) GetPodStatuses(ctx context.Context, namespace string, podNames []string) (map[string]*corev1.Pod, error) {
	// Strict non-mixing invariant: Argo Server mode never accesses K8s API directly.
	if c.IsArgoServerMode() || c.kubeClient == nil || len(podNames) == 0 {
		return nil, nil
	}

	result := make(map[string]*corev1.Pod, len(podNames))
	for _, podName := range podNames {
		trimmed := strings.TrimSpace(podName)
		if trimmed == "" {
			continue
		}
		pod, err := c.kubeClient.CoreV1().Pods(namespace).Get(ctx, trimmed, metav1.GetOptions{})
		if err != nil {
			continue
		}
		result[trimmed] = pod
	}
	return result, nil
}

func (c *Client) ProbeClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	c.cachedInfoOnce.Do(func() {
		ci := &ClusterInfo{
			Target:           c.targetID,
			ArchiveSupported: c.IsArgoServerMode(),
		}
		if c.IsArgoServerMode() {
			ci.Mode = modeArgoServer
			if infoSvc, err := c.InfoService(); err == nil {
				probeCtx, cancel := context.WithTimeout(MergeContext(ctx, c.ctx), 5*time.Second)
				defer cancel()
				if ver, verErr := infoSvc.GetVersion(probeCtx, &info.GetVersionRequest{}); verErr == nil && ver != nil {
					ci.ArgoVersion = ver.Version
				}
				if inf, infoErr := infoSvc.GetInfo(probeCtx, &info.GetInfoRequest{}); infoErr == nil && inf != nil {
					ci.ManagedNamespace = inf.ManagedNamespace
				}
			}
		} else {
			ci.Mode = modeKubernetesDirect
			if c.kubeClient != nil {
				if sv, err := c.kubeClient.Discovery().ServerVersion(); err == nil && sv != nil {
					ci.ArgoVersion = "k8s:" + sv.GitVersion
				}
			}
		}
		c.cachedInfo = ci
	})
	copied := *c.cachedInfo
	return &copied, nil
}

func (c *Client) IsArgoServerMode() bool {
	return c.defaultBaseURL != ""
}

func (c *Client) DefaultNamespace() string {
	return c.defaultNS
}

func (c *Client) TargetID() string {
	return c.targetID
}

func (c *Client) Context() context.Context {
	return c.ctx
}

func (c *Client) MultiContextEnabled() bool {
	return false
}

func (c *Client) ResolveTarget(ctx context.Context, argoBaseURL, kubeContext string) (context.Context, ClientInterface, error) {
	argoBaseURL = strings.TrimSpace(argoBaseURL)
	kubeContext = strings.TrimSpace(kubeContext)

	if c.IsArgoServerMode() {
		if kubeContext != "" {
			return nil, nil, ErrMultiContextUnavailable
		}
		if argoBaseURL == "" {
			return MergeContext(ctx, c.ctx), c, nil
		}
		targetClient, err := c.forArgoBaseURL(argoBaseURL)
		if err != nil {
			return nil, nil, err
		}
		return MergeContext(ctx, targetClient.Context()), targetClient, nil
	}

	if argoBaseURL != "" {
		return nil, nil, ErrArgoBaseURLUnavailable
	}
	if kubeContext != "" {
		return nil, nil, ErrMultiContextUnavailable
	}
	return MergeContext(ctx, c.ctx), c, nil
}

// forArgoBaseURL resolves an Argo Server client enforcing the host-bound credential invariant:
// default ARGO_TOKEN is ONLY ever sent to default ARGO_SERVER; any non-default base URL
// must match an entry in ARGO_TOKEN_REGISTRY_PATH.
func (c *Client) forArgoBaseURL(rawURL string) (*Client, error) {
	parsed, err := ParseArgoServerURL(rawURL, c.config.Secure)
	if err != nil {
		return nil, err
	}
	if parsed.NormalizedBaseURL == c.defaultBaseURL {
		return c, nil
	}

	token, ok := c.tokenRegistry.GetToken(parsed.NormalizedBaseURL)
	if !ok {
		return nil, fmt.Errorf(
			"Argo Server instance %q is not registered in ARGO_TOKEN_REGISTRY_PATH; call list_clusters to see available targets",
			parsed.NormalizedBaseURL,
		)
	}

	cacheKey := parsed.NormalizedBaseURL + "\x00" + token

	c.mu.Lock()
	defer c.mu.Unlock()

	if cached, exists := c.instanceCache[cacheKey]; exists {
		return cached, nil
	}

	if len(c.cachedOrder) >= maxCachedArgoClients {
		oldest := c.cachedOrder[0]
		c.cachedOrder = c.cachedOrder[1:]
		delete(c.instanceCache, oldest)
	}

	clonedCfg := *c.config
	clonedCfg.ArgoServer = parsed.NormalizedBaseURL
	clonedCfg.ArgoToken = token
	clonedCfg.Secure = parsed.Secure

	inst, err := newArgoServerClient(context.WithoutCancel(c.ctx), &clonedCfg, c.tokenRegistry)
	if err != nil {
		return nil, err
	}
	c.instanceCache[cacheKey] = inst
	c.cachedOrder = append(c.cachedOrder, cacheKey)
	return inst, nil
}

func (c *Client) ListTargets(ctx context.Context) ([]TargetDescriptor, error) {
	if c.IsArgoServerMode() {
		proto := "grpc"
		if c.config.HTTP1 {
			proto = "http1-rest"
		}
		info, _ := c.ProbeClusterInfo(ctx)
		ver := ""
		if info != nil {
			ver = info.ArgoVersion
		}

		seen := map[string]bool{c.defaultBaseURL: true}
		targets := []TargetDescriptor{
			{
				Name:             c.defaultBaseURL,
				Mode:             modeArgoServer,
				Default:          true,
				DefaultNamespace: c.defaultNS,
				ArchiveSupported: true,
				Version:          ver,
				Protocol:         proto,
			},
		}
		for _, regURL := range c.tokenRegistry.ListBaseURLs() {
			if seen[regURL] {
				continue
			}
			seen[regURL] = true
			targets = append(targets, TargetDescriptor{
				Name:             regURL,
				Mode:             modeArgoServer,
				Default:          false,
				DefaultNamespace: c.defaultNS,
				ArchiveSupported: true,
				Protocol:         proto,
			})
		}
		sort.SliceStable(targets, func(i, j int) bool {
			if targets[i].Default != targets[j].Default {
				return targets[i].Default
			}
			return targets[i].Name < targets[j].Name
		})
		return targets, nil
	}

	info, _ := c.ProbeClusterInfo(ctx)
	ver := ""
	if info != nil {
		ver = info.ArgoVersion
	}
	return []TargetDescriptor{
		{
			Name:             c.targetID,
			Mode:             modeKubernetesDirect,
			Default:          true,
			DefaultNamespace: c.defaultNS,
			ArchiveSupported: false,
			Version:          ver,
			Protocol:         "k8s-json",
		},
	}, nil
}

func buildLoadingRules(kubeconfig string) *clientcmd.ClientConfigLoadingRules {
	if kubeconfig == "" {
		return clientcmd.NewDefaultClientConfigLoadingRules()
	}
	return &clientcmd.ClientConfigLoadingRules{Precedence: filepath.SplitList(kubeconfig)}
}

func resolveContextName(loadingRules *clientcmd.ClientConfigLoadingRules, overrides *clientcmd.ConfigOverrides) string {
	rawConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).RawConfig()
	if err != nil {
		return "in-cluster"
	}
	if overrides.CurrentContext != "" {
		return overrides.CurrentContext
	}
	if rawConfig.CurrentContext != "" {
		return rawConfig.CurrentContext
	}
	return "in-cluster"
}
