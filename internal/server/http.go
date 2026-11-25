package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/pipekit/mcp-for-argo-workflows/internal/security"
)

const (
	// MCPPath is the single stateless Streamable HTTP MCP endpoint.
	MCPPath = "/mcp"
	// HealthPath is the unauthenticated liveness/readiness probe endpoint.
	HealthPath = "/healthz"
)

// HTTPOptions configures the stateless Dual-Era Streamable HTTP listener.
type HTTPOptions struct {
	Security     *security.ListenerSecurity
	LegacyMode   string
	JSONResponse bool
}

// BuildHTTPHandler constructs the HTTP mux with strict security ordering:
//   - GET /healthz (unauthenticated probe)
//   - /mcp -> BearerAuth -> HostValidation -> OriginValidation -> HeaderGuard -> Stateless StreamableHTTPHandler
func (s *Server) BuildHTTPHandler(opts HTTPOptions) http.Handler {
	streamable := mcp.NewStreamableHTTPHandler(
		func(_ *http.Request) *mcp.Server {
			return s.mcp
		},
		&mcp.StreamableHTTPOptions{
			Stateless:           true,
			JSONResponse:        opts.JSONResponse,
			Logger:              slog.Default(),
			MaxRequestBodyBytes: security.MaxGuardBodyBytes,
			// ListenerSecurity enforces BearerAuth before Host/Origin checks so
			// unauthenticated callers cannot enumerate allowed hosts or origins.
			DisableLocalhostProtection: true,
		},
	)

	guarded := security.HeaderGuard(opts.LegacyMode, streamable)
	if opts.Security != nil {
		guarded = opts.Security.WrapHandler(guarded)
	}

	mux := http.NewServeMux()
	mux.HandleFunc(HealthPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status":  "ok",
				"version": s.version,
			})
		}
	})
	mux.Handle(MCPPath, guarded)
	return mux
}

func newHTTPServer(base context.Context, addr string, handler http.Handler) *http.Server {
	baseCtx := context.WithoutCancel(base)
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
	}
}

// RunStreamableHTTP runs the stateless Dual-Era Streamable HTTP server until SIGINT/SIGTERM.
func (s *Server) RunStreamableHTTP(ctx context.Context, opts HTTPOptions) error {
	addr := "127.0.0.1:8080"
	if opts.Security != nil && opts.Security.ListenAddr != "" {
		addr = opts.Security.ListenAddr
	}

	handler := s.BuildHTTPHandler(opts)
	httpServer := newHTTPServer(ctx, addr, handler)

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("starting stateless MCP HTTP server",
		"addr", addr,
		"path", MCPPath,
		"healthPath", HealthPath,
		"jsonResponse", opts.JSONResponse,
		"legacyMode", opts.LegacyMode,
	)

	errChan := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-sigCtx.Done():
		slog.Info("shutting down stateless MCP HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}
