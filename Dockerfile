# syntax=docker/dockerfile:1

# One image for every role. Pick the role with the subcommand:
#   scout coordinator | worker | serve | crawl | index build|activate|verify ...
# The default command serves /data on :8080. No configuration, credentials or
# corpus data are baked in: pass SCOUT_DATABASE_URL and flags at run time and
# mount the data directory at /data.
#
# The default build is keyword-only friendly. The embedding runtime is linked
# in (the fastembed crate is a dependency), but the BGE Small model is only
# downloaded on first semantic or hybrid use, into /data/models.

# Pinned toolchain, matching RUST_TOOLCHAIN in .github/workflows/ci.yml. The
# full (non-slim) image already carries g++/make, which the usearch and bundled
# SQLite builds need, so the build runs no apt-get step.
ARG BUILDER_IMAGE=rust:1.97-trixie
ARG RUNTIME_IMAGE=debian:trixie-slim

# --- cargo-chef: compile dependencies in their own cacheable layer ----------
FROM ${BUILDER_IMAGE} AS chef
ARG CARGO_CHEF_VERSION=0.1.78
RUN cargo install cargo-chef --locked --version "${CARGO_CHEF_VERSION}"
WORKDIR /src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# Strip symbols through the environment instead of Cargo.toml so local
# `cargo build --release` keeps its symbols for profiling.
ENV CARGO_PROFILE_RELEASE_STRIP=symbols
COPY --from=planner /src/recipe.json recipe.json
# Dependencies only: this layer is reused until Cargo.toml/Cargo.lock change.
# It also downloads the ONNX Runtime binaries that fastembed links against.
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bin scout

# --- runtime: slim, non-root -------------------------------------------------
FROM ${RUNTIME_IMAGE} AS runtime
# No CA bundle is needed: reqwest and hf-hub use rustls with the bundled
# webpki-roots, so the runtime needs no apt layer at all.
# Fixed UID/GID so volume ownership is predictable. /data/models is the model
# cache scout derives from --data-dir; it exists here so a named volume mounted
# there inherits this ownership.
RUN groupadd --system --gid 10001 scout \
 && useradd --system --uid 10001 --gid scout --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin scout \
 && mkdir -p /data/models \
 && chown -R scout:scout /data
COPY --from=builder /src/target/release/scout /usr/local/bin/scout

USER scout:scout
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["scout"]
CMD ["serve", "--data-dir", "/data", "--bind", "0.0.0.0:8080"]
