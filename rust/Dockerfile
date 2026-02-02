# Build stage
FROM rust:1.91-bookworm AS builder

WORKDIR /build

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY rustfmt.toml ./

# Copy all workspace members
COPY server ./server
COPY api ./api
COPY impls ./impls
COPY auth-impls ./auth-impls

# Build the application in release mode
RUN cargo build --release --bin vss-server

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from builder
COPY --from=builder /build/target/release/vss-server /app/vss-server

# Copy default configuration file
COPY server/vss-server-config.toml /app/vss-server-config.toml

# Environment variables for PostgreSQL connection
#ENV VSS_POSTGRESQL_USERNAME=postgres
#ENV VSS_POSTGRESQL_PASSWORD=YOU_MUST_CHANGE_THIS_PASSWORD
#ENV VSS_POSTGRESQL_HOST=postgres
#ENV VSS_POSTGRESQL_PORT=5432
#ENV VSS_POSTGRESQL_DATABASE=postgres

# Datadog APM Tracing Configuration
# The application uses the dd-trace-rs library for APM tracing.
# Traces are sent to the Datadog Agent which should run as a sidecar container.
# Configure the following environment variables:
#ENV DD_TRACE_ENABLED=true
#ENV DD_SERVICE=vss-server
#ENV DD_ENV=production
#ENV DD_VERSION=0.1.0
#ENV DD_AGENT_HOST=datadog-agent
#ENV DD_TRACE_AGENT_PORT=8126

EXPOSE 8080

# Run the server with the config file
CMD ["/app/vss-server", "/app/vss-server-config.toml"]