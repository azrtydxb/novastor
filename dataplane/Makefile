# NovaStor SPDK Dataplane — Build targets
#
# The dataplane must be compiled on Linux with SPDK headers and libraries.
# On macOS, use the Docker-based build targets.

IMAGE_NAME  ?= novastor-dataplane
IMAGE_TAG   ?= dev
DOCKER      ?= docker

# ---------------------------------------------------------------------------
# Docker build (works on macOS and Linux)
# ---------------------------------------------------------------------------

.PHONY: build
build: ## Build the dataplane image (multi-stage: SPDK + Rust)
	$(DOCKER) build -f Dockerfile.build -t $(IMAGE_NAME):$(IMAGE_TAG) .

.PHONY: build-no-cache
build-no-cache: ## Build without Docker layer cache
	$(DOCKER) build -f Dockerfile.build --no-cache -t $(IMAGE_NAME):$(IMAGE_TAG) .

# ---------------------------------------------------------------------------
# Extract the compiled binary from the image
# ---------------------------------------------------------------------------

.PHONY: extract-binary
extract-binary: build ## Copy the binary out of the Docker image
	$(DOCKER) create --name novastor-extract $(IMAGE_NAME):$(IMAGE_TAG) true
	$(DOCKER) cp novastor-extract:/usr/local/bin/novastor-dataplane ./novastor-dataplane
	$(DOCKER) rm novastor-extract
	@echo "Binary extracted: ./novastor-dataplane"

# ---------------------------------------------------------------------------
# Run (requires hugepages on the host or Docker Desktop VM)
# ---------------------------------------------------------------------------

.PHONY: run
run: build ## Run the dataplane container with hugepage support
	$(DOCKER) run --rm -it --privileged \
		-v /dev/hugepages:/dev/hugepages \
		-v /dev/shm:/dev/shm \
		$(IMAGE_NAME):$(IMAGE_TAG) \
		--reactor-mask 0x1 --mem-size 512

# ---------------------------------------------------------------------------
# Test (runs unit tests inside Docker — no SPDK runtime needed)
# ---------------------------------------------------------------------------

.PHONY: test
test: ## Run unit tests (erasure, replica) inside Docker
	$(DOCKER) build -f Dockerfile.build --target test -t $(IMAGE_NAME)-test:$(IMAGE_TAG) .

# ---------------------------------------------------------------------------
# Integration test (requires hugepages — tests all 3 backends via JSON-RPC)
# ---------------------------------------------------------------------------

.PHONY: test-backends
test-backends: build ## Run backend integration tests (requires hugepages + privileged)
	$(DOCKER) run --rm --privileged \
		-v /dev/hugepages:/dev/hugepages \
		-v /dev/shm:/dev/shm \
		--entrypoint /bin/bash \
		$(IMAGE_NAME):$(IMAGE_TAG) \
		-c '/usr/local/bin/entrypoint.sh --reactor-mask 0x1 --mem-size 512 & sleep 5 && /usr/local/bin/test_backends.sh; rc=$$?; kill %1 2>/dev/null; exit $$rc'

# ---------------------------------------------------------------------------
# NVMe-oF integration test (kernel nvme-tcp + mkfs + mount)
# ---------------------------------------------------------------------------

.PHONY: test-nvmeof
test-nvmeof: build ## Run NVMe-oF export tests (requires hugepages + privileged + /dev access)
	$(DOCKER) run --rm --privileged \
		-v /dev/hugepages:/dev/hugepages \
		-v /dev/shm:/dev/shm \
		-v /dev:/dev \
		-v /lib/modules:/lib/modules:ro \
		--entrypoint /bin/bash \
		$(IMAGE_NAME):$(IMAGE_TAG) \
		-c '/usr/local/bin/entrypoint.sh --reactor-mask 0x1 --mem-size 512 & sleep 5 && /usr/local/bin/test_nvmeof.sh; rc=$$?; kill %1 2>/dev/null; exit $$rc'

# ---------------------------------------------------------------------------
# Real NVMe integration test (requires physical NVMe drive + hugepages)
# ---------------------------------------------------------------------------

.PHONY: test-real-nvme
test-real-nvme: build ## Run real NVMe tests (requires hugepages + privileged + NVMe device)
	$(DOCKER) run --rm --privileged \
		-v /dev/hugepages:/dev/hugepages \
		-v /dev/shm:/dev/shm \
		-v /dev:/dev \
		-v /lib/modules:/lib/modules:ro \
		--entrypoint /bin/bash \
		$(IMAGE_NAME):$(IMAGE_TAG) \
		-c '/usr/local/bin/entrypoint.sh --reactor-mask 0x1 --mem-size 512 & sleep 5 && /usr/local/bin/test_real_nvme.sh; rc=$$?; kill %1 2>/dev/null; exit $$rc'

# ---------------------------------------------------------------------------
# Development helpers
# ---------------------------------------------------------------------------

.PHONY: shell
shell: build ## Start a shell in the build image (for debugging)
	$(DOCKER) run --rm -it --privileged \
		-v /dev/hugepages:/dev/hugepages \
		-v /dev/shm:/dev/shm \
		--entrypoint /bin/bash \
		$(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: clean
clean: ## Remove build artifacts and Docker image
	rm -f novastor-dataplane
	$(DOCKER) rmi $(IMAGE_NAME):$(IMAGE_TAG) 2>/dev/null || true

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-20s %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
