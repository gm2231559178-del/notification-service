# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies first (layer-friendly).
# ALL workspace member Cargo.tomls must be present here so that
# `cargo build` can resolve the full workspace and actually compile
# (and cache) the dependency graph.  Missing any member causes the
# workspace resolver to fail, the build to exit via `|| true`, and
# zero dependencies to be cached — defeating the whole layer.
COPY Cargo.toml Cargo.lock ./
COPY src/           src/
COPY crates/        crates/
COPY migrations/    migrations/
COPY config/        config/

RUN cargo build --release

# Drop privileges
RUN useradd -m -u 1001 appuser
USER appuser

EXPOSE 8080

ENTRYPOINT ["./target/release/notification-service"]
