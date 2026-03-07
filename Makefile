# NovaStor Build System

# Get the currently used golang install path
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# All binaries
BINARIES = controller agent meta csi filer s3gw scheduler webhook cli scheduler

.PHONY: all
all: build-all

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: generate
generate: controller-gen ## Generate deepcopy methods for CRD types.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: manifests
manifests: controller-gen ## Generate CRD manifests.
	$(CONTROLLER_GEN) rbac:roleName=novastor-controller-role crd:allowDangerousTypes=true webhook paths="./..." output:crd:artifacts:config=config/crd

.PHONY: generate-proto
generate-proto: protoc-gen-go protoc-gen-go-grpc ## Generate Go code from protobuf definitions.
	PATH=$(LOCALBIN):$$PATH protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		--proto_path=. \
		api/proto/chunk/chunk.proto \
		api/proto/metadata/metadata.proto \
		api/proto/nvme/nvme.proto

.PHONY: generate-crds
generate-crds: manifests ## Generate CRD manifests from kubebuilder markers (runs controller-gen; copy output from config/crd to deploy/helm/novastor/crds/ manually).
	@echo "CRD manifests written to config/crd/ — copy to deploy/helm/novastor/crds/ as needed."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter.
	$(GOLANGCI_LINT) run

.PHONY: check
check: fmt vet lint ## Run all code quality checks.

.PHONY: test
test: fmt vet ## Run tests with race detection.
	go test -v -race -coverprofile=coverage.out ./...

.PHONY: test-coverage
test-coverage: test ## Run tests and open coverage report.
	go tool cover -html=coverage.out -o coverage.html

.PHONY: test-integration
test-integration: ## Run integration tests with race detection.
	go test -tags integration -race -count=1 ./test/integration/...

.PHONY: test-bench
test-bench: ## Run benchmarks (chunk and placement).
	go test -bench=. -benchmem ./internal/chunk/... ./internal/placement/...

.PHONY: test-bench-all
test-bench-all: ## Run all benchmarks (chunk, placement, and test/benchmark).
	go test -bench=. -benchmem ./internal/chunk/... ./internal/placement/... ./test/benchmark/...

##@ Build

.PHONY: build-all
build-all: fmt vet ## Build all binaries.
	$(foreach bin,$(BINARIES),go build -o bin/novastor-$(bin) ./cmd/$(bin)/;)

.PHONY: build-controller
build-controller: fmt vet ## Build controller binary.
	go build -o bin/novastor-controller ./cmd/controller/

.PHONY: build-agent
build-agent: fmt vet ## Build agent binary.
	go build -o bin/novastor-agent ./cmd/agent/

.PHONY: build-meta
build-meta: fmt vet ## Build metadata service binary.
	go build -o bin/novastor-meta ./cmd/meta/

.PHONY: build-csi
build-csi: fmt vet ## Build CSI driver binary.
	go build -o bin/novastor-csi ./cmd/csi/

.PHONY: build-filer
build-filer: fmt vet ## Build file gateway binary.
	go build -o bin/novastor-filer ./cmd/filer/

.PHONY: build-s3gw
build-s3gw: fmt vet ## Build S3 gateway binary.
	go build -o bin/novastor-s3gw ./cmd/s3gw/

.PHONY: build-cli
build-cli: fmt vet ## Build novastorctl CLI tool.
	go build -o bin/novastorctl ./cmd/cli/

.PHONY: build-webhook
build-webhook: fmt vet ## Build webhook server binary.
	go build -o bin/novastor-webhook ./cmd/webhook/

##@ Data Plane (Rust/SPDK)

.PHONY: build-dataplane
build-dataplane: ## Build the Rust SPDK data-plane binary (stub mode).
	cd dataplane && cargo build --release

.PHONY: build-dataplane-spdk
build-dataplane-spdk: ## Build the Rust SPDK data-plane binary (with SPDK).
	cd dataplane && cargo build --release --features spdk-sys

.PHONY: test-dataplane
test-dataplane: ## Run data-plane Rust tests.
	cd dataplane && cargo test

.PHONY: lint-dataplane
lint-dataplane: ## Run clippy on data-plane.
	cd dataplane && cargo clippy -- -D warnings

.PHONY: fmt-dataplane
fmt-dataplane: ## Format data-plane Rust code.
	cd dataplane && cargo fmt

.PHONY: check-dataplane
check-dataplane: fmt-dataplane lint-dataplane test-dataplane ## Run all data-plane checks.

.PHONY: clean-dataplane
clean-dataplane: ## Clean data-plane build artifacts.
	cd dataplane && cargo clean

##@ Docker

DOCKER ?= docker
REGISTRY ?= ghcr.io/azrtydxb
IMAGE_TAG ?= latest

.PHONY: docker-build
docker-build: ## Build all docker images.
	$(foreach comp,controller agent meta csi filer s3gw scheduler webhook,$(DOCKER) build -t novastor-$(comp):latest -f build/Dockerfile.$(comp) .;)

.PHONY: docker-build-dataplane
docker-build-dataplane: ## Build the data-plane docker image (stub mode).
	$(DOCKER) build -t novastor-dataplane:latest -f deploy/docker/Dockerfile.dataplane .

.PHONY: docker-push
docker-push: ## Push all docker images to the registry.
	$(foreach comp,controller agent meta csi filer s3gw scheduler webhook,$(DOCKER) tag novastor-$(comp):latest $(REGISTRY)/novastor-$(comp):$(IMAGE_TAG) && $(DOCKER) push $(REGISTRY)/novastor-$(comp):$(IMAGE_TAG);)

##@ Deployment

.PHONY: install-crds
install-crds: manifests ## Install CRDs into the K8s cluster.
	kubectl apply -f config/crd/

.PHONY: uninstall-crds
uninstall-crds: ## Uninstall CRDs from the K8s cluster.
	kubectl delete -f config/crd/

##@ Helm

.PHONY: helm-lint
helm-lint: ## Lint Helm chart.
	helm lint deploy/helm/novastor/

.PHONY: helm-install
helm-install: ## Install NovaStor using Helm.
	helm install novastor deploy/helm/novastor/ -n novastor-system --create-namespace

.PHONY: helm-upgrade
helm-upgrade: ## Upgrade NovaStor using Helm.
	helm upgrade novastor deploy/helm/novastor/ -n novastor-system

.PHONY: helm-uninstall
helm-uninstall: ## Uninstall NovaStor using Helm.
	helm uninstall novastor -n novastor-system

.PHONY: helm-template
helm-template: ## Generate Helm templates for debugging.
	helm template novastor deploy/helm/novastor/ -n novastor-system

.PHONY: helm-package
helm-package: ## Package the Helm chart into a versioned archive.
	mkdir -p dist/helm
	helm package deploy/helm/novastor/ --destination dist/helm/

##@ Documentation

.PHONY: docs-build
docs-build: ## Build documentation (strict mode).
	mkdocs build --strict

.PHONY: docs-serve
docs-serve: ## Serve documentation locally.
	mkdocs serve

##@ Clean

.PHONY: clean
clean: ## Clean build artifacts.
	rm -rf bin/
	rm -rf $(LOCALBIN)/
	rm -f coverage.out coverage.html

##@ Build Dependencies

LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint
PROTOC_GEN_GO ?= $(LOCALBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC ?= $(LOCALBIN)/protoc-gen-go-grpc

## Tool Versions
CONTROLLER_TOOLS_VERSION ?= v0.16.5
GOLANGCI_LINT_VERSION ?= v2.10.1
PROTOC_GEN_GO_VERSION ?= v1.35.1
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: protoc-gen-go
protoc-gen-go: $(PROTOC_GEN_GO) ## Download protoc-gen-go locally if necessary.
$(PROTOC_GEN_GO): $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

.PHONY: protoc-gen-go-grpc
protoc-gen-go-grpc: $(PROTOC_GEN_GO_GRPC) ## Download protoc-gen-go-grpc locally if necessary.
$(PROTOC_GEN_GO_GRPC): $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: build-scheduler
build-scheduler: fmt vet ## Build scheduler plugin binary.
	go build -o bin/novastor-scheduler ./cmd/scheduler/
