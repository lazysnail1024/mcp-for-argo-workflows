# syntax=docker/dockerfile:1

# Build stage: compile statically on build host native architecture (zero QEMU/CGO overhead)
FROM --platform=$BUILDPLATFORM golang:1.27.1-bookworm AS builder

WORKDIR /app

ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY pkg/ ./pkg/

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=0.6.0
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags="-s -w \
        -X 'github.com/pipekit/mcp-for-argo-workflows/internal/version.Version=${VERSION}' \
        -X 'github.com/pipekit/mcp-for-argo-workflows/internal/version.Commit=${COMMIT}' \
        -X 'github.com/pipekit/mcp-for-argo-workflows/internal/version.BuildTime=${BUILD_TIME}'" \
    -o /out/mcp-for-argo-workflows \
    ./cmd/mcp-for-argo-workflows

# Runtime stage: minimal static distroless image (root CA bundle included, zero shell/libc)
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="mcp-for-argo-workflows" \
      org.opencontainers.image.description="Stateless, Dual-Era MCP server for Argo Workflows (v3.6+ and v4.x)" \
      org.opencontainers.image.source="https://github.com/lazysnail1024/mcp-for-argo-workflows" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /out/mcp-for-argo-workflows /mcp-for-argo-workflows

EXPOSE 8080
USER 1000:1000

ENTRYPOINT ["/mcp-for-argo-workflows"]
CMD ["http"]
