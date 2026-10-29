# syntax=docker/dockerfile:1.4

# ==============================================================================
# Stage 1: Build stage (compile wheels and install dependencies)
# ==============================================================================
FROM python:3.11-slim-bookworm AS builder

WORKDIR /build

# Install build dependencies for standard C extensions if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy package specifications and source directory
COPY pyproject.toml README.md /build/
COPY src /build/src

# Install application dependencies into virtualenv
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir '.[kafka]'

# ==============================================================================
# Stage 2: Runtime stage (minimal, secure, non-root)
# ==============================================================================
FROM python:3.11-slim-bookworm AS runtime

WORKDIR /app

# Ensure security, standard output flushing, and isolated execution
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app/src"

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create dedicated non-root user and group (UID 10001)
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /bin/bash -m appuser

# Copy application source code, scripts, and baseline reference data (<1MB total)
COPY --chown=appuser:appgroup pyproject.toml README.md /app/
COPY --chown=appuser:appgroup config /app/config
COPY --chown=appuser:appgroup src /app/src
COPY --chown=appuser:appgroup scripts /app/scripts
COPY --chown=appuser:appgroup data/ /data/

# Make entrypoint script executable and ensure appuser owns /app and /data directories
RUN chmod +x /app/scripts/entrypoint.sh && \
    chown -R appuser:appgroup /app /data

# Default environment for baseline reference data directory
ENV BASELINE_DATA_DIR=/data

# Switch to non-root user
USER 10001:10001

# Expose Transaction REST API port (8000)
EXPOSE 8000

# Container healthcheck querying application health probe
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["api"]
