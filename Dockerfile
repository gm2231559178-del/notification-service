# ── Stage 1: chef prepare ─────────────────────────────────────────────────────
# cargo-chef computes the exact set of dependencies from Cargo.toml/Cargo.lock
# and produces a recipe.json that is stable as long as deps don't change.
FROM rust:1.94-slim-bookworm AS chef
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 2: build deps ───────────────────────────────────────────────────────
# This layer is only invalidated when recipe.json changes (i.e. a dep version
# changes in Cargo.toml/Cargo.lock).  A source-only change skips straight to
# Stage 3 and saves the full dep compilation time.
FROM chef AS builder

ENV SQLX_OFFLINE=true

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# ── Stage 3: build workspace ──────────────────────────────────────────────────
# Deps are already compiled above. Only workspace crates recompile here.
COPY . .
RUN cargo build --release

# ── Stage 4: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# curl is required by the docker-compose healthcheck
# (test: curl -sf http://localhost:8080/ready || exit 1).
# Without it the container is never marked healthy and dependent
# services cannot start.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/notification-service ./notification-service
COPY --from=builder /app/migrations  ./migrations
COPY --from=builder /app/config      ./config

# Drop privileges
RUN useradd -m -u 1001 appuser
USER appuser

EXPOSE 8080

ENTRYPOINT ["./notification-service"]