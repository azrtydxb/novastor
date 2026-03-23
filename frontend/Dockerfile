# syntax=docker/dockerfile:1
#
# Multi-stage build for the NovaStor Frontend Controller.
# Pure userspace NVMe-oF TCP target — no kernel modules required.

# ---------------------------------------------------------------------------
# Stage 1 — Build
# ---------------------------------------------------------------------------
FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy dependency crates first for layer caching.
COPY dataplane/ndp/ dataplane/ndp/
COPY dataplane/nvmeof-tcp/ dataplane/nvmeof-tcp/

# Copy frontend source.
COPY frontend/ frontend/

# Build the frontend binary.
WORKDIR /build/frontend
RUN cargo build --release

# ---------------------------------------------------------------------------
# Stage 2 — Runtime (minimal — no kernel tools needed)
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

COPY --from=builder /build/frontend/target/release/novastor-frontend /usr/local/bin/novastor-frontend

ENTRYPOINT ["novastor-frontend"]
