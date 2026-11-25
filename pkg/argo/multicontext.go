package argo

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"k8s.io/client-go/tools/clientcmd"
)

// MultiContextClient serves the default kubeconfig context and lazily resolves
// additional allowlisted kubeconfig contexts per call via ResolveTarget.
type MultiContextClient struct {
	baseCtx context.Context //nolint:containedctx // Matches Argo SDK client lifetime model

	*Client

	config            *Config
	available         map[string]struct{}
	contextNamespaces map[string]string
	newClient         func(context.Context, *Config) (*Client, error)
	cache             map[string]*Client
	defaultContext    string
	names             []string
	mu                sync.Mutex
}

var _ ClientInterface = (*MultiContextClient)(nil)

// NewMultiContextClient creates a Direct K8s multi-context client bounded by allowedContexts.
func NewMultiContextClient(ctx context.Context, cfg *Config, allowedContexts []string) (*MultiContextClient, error) {
	base, err := NewClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return newMultiContextClient(ctx, base, cfg, allowedContexts, NewClient)
}

func newMultiContextClient(
	ctx context.Context,
	base *Client,
	cfg *Config,
	allowedContexts []string,
	newClient func(context.Context, *Config) (*Client, error),
) (*MultiContextClient, error) {
	rawConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		buildLoadingRules(cfg.Kubeconfig),
		&clientcmd.ConfigOverrides{},
	).RawConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to read kubeconfig contexts: %w", err)
	}

	defaultContext := cfg.Context
	if defaultContext == "" {
		defaultContext = rawConfig.CurrentContext
	}

	allowed := make(map[string]struct{}, len(allowedContexts))
	for _, name := range allowedContexts {
		if name = strings.TrimSpace(name); name != "" {
			allowed[name] = struct{}{}
		}
	}
	if len(allowed) > 0 {
		if _, ok := allowed[defaultContext]; !ok {
			return nil, fmt.Errorf("default context %q is not in the allowed contexts list", defaultContext)
		}
	}

	available := make(map[string]struct{}, len(rawConfig.Contexts))
	contextNamespaces := make(map[string]string, len(rawConfig.Contexts))
	names := make([]string, 0, len(rawConfig.Contexts))
	for name, kctx := range rawConfig.Contexts {
		if len(allowed) > 0 {
			if _, ok := allowed[name]; !ok {
				continue
			}
		}
		available[name] = struct{}{}
		names = append(names, name)
		if kctx != nil && strings.TrimSpace(kctx.Namespace) != "" {
			contextNamespaces[name] = strings.TrimSpace(kctx.Namespace)
		}
	}
	sort.Strings(names)

	return &MultiContextClient{
		Client:            base,
		baseCtx:           ctx,
		config:            cfg,
		defaultContext:    defaultContext,
		available:         available,
		contextNamespaces: contextNamespaces,
		names:             names,
		newClient:         newClient,
		cache:             make(map[string]*Client),
	}, nil
}

// MultiContextEnabled reports true: per-call kubeconfig context selection is active.
func (m *MultiContextClient) MultiContextEnabled() bool {
	return true
}

// ResolveTarget selects the allowlisted kubeconfig context for this call.
func (m *MultiContextClient) ResolveTarget(ctx context.Context, argoBaseURL, kubeContext string) (context.Context, ClientInterface, error) {
	if strings.TrimSpace(argoBaseURL) != "" {
		return nil, nil, ErrArgoBaseURLUnavailable
	}
	name := strings.TrimSpace(kubeContext)
	if name == "" || name == m.defaultContext {
		return MergeContext(ctx, m.ctx), m, nil
	}

	if _, ok := m.available[name]; !ok {
		return nil, nil, fmt.Errorf("context %q is not available (call list_clusters to see selectable contexts)", name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if client, ok := m.cache[name]; ok {
		return MergeContext(ctx, client.Context()), client, nil
	}

	contextCfg := *m.config
	contextCfg.Context = name
	if ns := m.contextNamespaces[name]; ns != "" && contextCfg.Namespace == defaultNamespace {
		contextCfg.Namespace = ns
	}
	client, err := m.newClient(context.WithoutCancel(m.baseCtx), &contextCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create client for context %q: %w", name, err)
	}
	m.cache[name] = client
	slog.Info("created client for kubeconfig context", "context", name, "namespace", client.DefaultNamespace())
	return MergeContext(ctx, client.Context()), client, nil
}

// ListTargets lists all selectable kubeconfig contexts along with their default namespace.
func (m *MultiContextClient) ListTargets(ctx context.Context) ([]TargetDescriptor, error) {
	defaultInfo, _ := m.ProbeClusterInfo(ctx)
	defaultVer := ""
	if defaultInfo != nil {
		defaultVer = defaultInfo.ArgoVersion
	}

	targets := make([]TargetDescriptor, 0, len(m.names))
	for _, name := range m.names {
		ns := m.contextNamespaces[name]
		if ns == "" {
			ns = m.config.Namespace
		}
		isDefault := name == m.defaultContext
		ver := ""
		if isDefault {
			ver = defaultVer
		}
		targets = append(targets, TargetDescriptor{
			Name:             name,
			Mode:             modeKubernetesDirect,
			Default:          isDefault,
			DefaultNamespace: ns,
			ArchiveSupported: false,
			Version:          ver,
			Protocol:         "k8s-json",
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

// MergeContext draws cancellation and deadlines from request while resolving Argo SDK
// metadata values (logger, K8s gatekeeper clients) from clientValues first.
func MergeContext(request, clientValues context.Context) context.Context {
	if request == nil {
		return clientValues
	}
	if clientValues == nil {
		return request
	}
	return mergedContext{Context: request, values: clientValues}
}

//nolint:containedctx // Intentionally combines request cancellation with Argo client values.
type mergedContext struct {
	context.Context
	values context.Context
}

func (m mergedContext) Value(key any) any {
	if v := m.values.Value(key); v != nil {
		return v
	}
	return m.Context.Value(key)
}
