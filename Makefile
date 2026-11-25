BINARY_NAME := mcp-for-argo-workflows
MODULE := github.com/pipekit/mcp-for-argo-workflows
DIST_DIR := dist
IMAGE_NAME ?= ghcr.io/lazysnail1024/mcp-for-argo-workflows
IMAGE_TAG ?= 0.6.0
IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)
DOCKER_BUILD_FLAGS ?= --network=host
GOPROXY ?= https://proxy.golang.org,direct
KIND_CLUSTER ?= kind
ARGO_VERSION ?= v3.6.5

VERSION ?= $(IMAGE_TAG)
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS := -s -w \
	-X $(MODULE)/internal/version.Version=$(VERSION) \
	-X $(MODULE)/internal/version.Commit=$(COMMIT) \
	-X $(MODULE)/internal/version.BuildTime=$(BUILD_TIME)

GO ?= go
GOFMT := gofmt
GOLANGCI_LINT := golangci-lint
HELM ?= helm

.DEFAULT_GOAL := help
.PHONY: help fmt vet lint test build build-all clean docker-build docker-build-multiarch docker-push test-image helm-lint kind-load test-e2e-kind clean-e2e-kind

help: ## Show available make targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

fmt: ## Format Go source files
	$(GOFMT) -s -w cmd internal pkg

vet: ## Run go vet
	$(GO) vet ./...

lint: ## Run golangci-lint (if installed) and go vet
	$(GO) vet ./...
	@if command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then \
		$(GOLANGCI_LINT) run ./...; \
	else \
		echo "golangci-lint not found in PATH; go vet passed"; \
	fi

test: ## Run Tier 1 unit and Tier 2 Dual-Era protocol integration tests
	$(GO) test -race -coverprofile=coverage.out -covermode=atomic ./internal/... ./pkg/...

build: ## Build static binary for current platform
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME) ./cmd/$(BINARY_NAME)

build-all: ## Cross-compile static binaries for linux/darwin/windows
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/$(BINARY_NAME)
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/$(BINARY_NAME)
	CGO_ENABLED=0 GOOS=darwin  GOARCH=amd64 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-darwin-amd64 ./cmd/$(BINARY_NAME)
	CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-darwin-arm64 ./cmd/$(BINARY_NAME)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-windows-amd64.exe ./cmd/$(BINARY_NAME)

clean: ## Remove local build and coverage artifacts
	rm -rf $(DIST_DIR) coverage.out coverage.html

docker-build: ## Build production static distroless container image ($(IMAGE))
	docker build $(DOCKER_BUILD_FLAGS) \
		--build-arg GOPROXY=$(GOPROXY) \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(IMAGE) \
		.

docker-build-multiarch: ## Build multi-architecture Docker image (linux/amd64, linux/arm64)
	docker buildx build $(DOCKER_BUILD_FLAGS) \
		--platform linux/amd64,linux/arm64 \
		--build-arg GOPROXY=$(GOPROXY) \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(IMAGE) \
		.

docker-push: ## Build and push multi-architecture Docker image to registry
	docker buildx build $(DOCKER_BUILD_FLAGS) \
		--platform linux/amd64,linux/arm64 \
		--build-arg GOPROXY=$(GOPROXY) \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(IMAGE) \
		--push \
		.

helm-lint: ## Lint and template-validate the Helm Chart
	$(HELM) lint deploy/charts/mcp-for-argo-workflows
	$(HELM) lint deploy/charts/mcp-for-argo-workflows -f deploy/charts/mcp-for-argo-workflows/examples/values-cce-istio.yaml
	$(HELM) lint deploy/charts/mcp-for-argo-workflows -f deploy/charts/mcp-for-argo-workflows/examples/values-kubernetes-direct.yaml

test-image: docker-build ## Run Tier 3 black-box container smoke tests against $(IMAGE)
	IMAGE=$(IMAGE) bash scripts/test-image.sh

kind-load: docker-build ## Load $(IMAGE) into local kind cluster ($(KIND_CLUSTER))
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)

test-e2e-kind: kind-load ## Run Tier 4 live kind 2-replica E2E acceptance suite (ARGO_VERSION=$(ARGO_VERSION))
	IMAGE=$(IMAGE) KIND_CLUSTER=$(KIND_CLUSTER) ARGO_VERSION=$(ARGO_VERSION) bash scripts/run-e2e-kind.sh

clean-e2e-kind: ## Remove isolated E2E test namespaces and ClusterRBAC from kind cluster
	kubectl delete ns mcp-wf-e2e-system mcp-wf-e2e-work mcp-wf-e2e-denied --ignore-not-found
	kubectl delete clusterrole,clusterrolebinding e2e-mcp-argo-cwt-reader mcp-wf-e2e-k8s-mcp-for-argo-workflows --ignore-not-found
