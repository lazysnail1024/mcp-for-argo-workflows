// Package main is the entry point for mcp-for-argo-workflows.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pipekit/mcp-for-argo-workflows/internal/config"
	"github.com/pipekit/mcp-for-argo-workflows/internal/server"
	"github.com/pipekit/mcp-for-argo-workflows/internal/version"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/argo"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/toolsets"
)

const serverName = "mcp-for-argo-workflows"

func main() {
	// Structured single-line JSON logs to stderr (stdout is reserved for stdio MCP JSON-RPC).
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx)
	cancel()

	if err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.NewFromFlags()
	if err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	var argoClient argo.ClientInterface
	if cfg.MultiContextEnabled() {
		mc, clientErr := argo.NewMultiContextClient(ctx, cfg.ToArgoConfig(), cfg.AllowedContexts)
		if clientErr != nil {
			return fmt.Errorf("failed to create Argo multi-context client: %w", clientErr)
		}
		argoClient = mc
	} else {
		c, clientErr := argo.NewClient(ctx, cfg.ToArgoConfig())
		if clientErr != nil {
			return fmt.Errorf("failed to create Argo client: %w", clientErr)
		}
		argoClient = c
	}

	// Propagate Argo SDK client context (logger + Direct K8s gatekeeper clients).
	//nolint:contextcheck // Required by Argo SDK apiclient architecture
	baseCtx := argoClient.Context()

	deps := toolsets.Dependencies{
		Client:           argoClient,
		ScopeFence:       cfg.ToScopeFence(),
		MRTRGate:         governance.NewMRTRGate(cfg.RequestStateSecret, 300*time.Second),
		MaxResponseBytes: cfg.MaxResponseBytes,
	}
	sel := toolsets.SelectionConfig{
		Toolsets: cfg.Toolsets,
		Tools:    cfg.Tools,
		ReadOnly: cfg.ReadOnly,
	}

	srv, err := server.NewServer(serverName, version.Version, deps, sel)
	if err != nil {
		return fmt.Errorf("failed to initialize MCP server: %w", err)
	}

	mode := "kubernetes"
	if argoClient.IsArgoServerMode() {
		mode = "argo-server"
	}

	slog.Info("mcp-for-argo-workflows initialized",
		"name", serverName,
		"version", version.Version,
		"transport", cfg.Transport,
		"mode", mode,
		"namespace", cfg.Namespace,
		"readOnly", cfg.ReadOnly,
		"toolsets", cfg.Toolsets,
		"toolCount", len(srv.RegisteredTools()),
		"tools", srv.RegisteredTools(),
		"multiContext", cfg.MultiContextEnabled(),
	)

	if cfg.UsesHTTPListener() {
		listenerSec, secErr := cfg.ResolveListenerSecurity()
		if secErr != nil {
			return secErr
		}
		return srv.RunStreamableHTTP(baseCtx, server.HTTPOptions{ //nolint:contextcheck // baseCtx carries Argo SDK context
			Security:     listenerSec,
			LegacyMode:   cfg.LegacyMode,
			JSONResponse: cfg.JSONResponse,
		})
	}

	return srv.RunStdio(baseCtx) //nolint:contextcheck // baseCtx carries Argo SDK context
}
