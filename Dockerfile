# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV SQLX_OFFLINE=true

# Cache dependencies first (layer-friendly).
# ALL workspace member Cargo.tomls must be present here so that
# `cargo build` can resolve the full workspace and actually compile
# (and cache) the dependency graph.  Missing any member causes the
# workspace resolver to fail, the build to exit via `|| true`, and
# zero dependencies to be cached — defeating the whole layer.
COPY Cargo.toml Cargo.lock ./
COPY .sqlx/        .sqlx/



# Now copy real source and build
COPY src/           src/
COPY crates/        crates/
COPY migrations/    migrations/
COPY config/        config/
RUN cargo build --release

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
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
