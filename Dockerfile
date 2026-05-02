# Multi-stage build keeps the runtime image small. Builder layer pulls in
# the full Python toolchain to compile any wheels that need it (rdflib has
# C deps); runtime layer copies just the installed site-packages plus the
# app source so the final image doesn't carry build-time tooling.

# ── Builder stage ────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

# System deps needed to BUILD some wheels (lxml etc.). Removed from the
# runtime layer — final image stays small.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy requirements first so the pip install layer caches across code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Non-root user — defence-in-depth. Even if the API is compromised, the
# attacker can't poke at root-owned host paths via a bind-mounted volume.
RUN useradd --create-home --shell /bin/bash --uid 1000 kf

# Bring the installed packages over from the builder. /install was the prefix
# above so we copy it into /usr/local where pip-installed packages normally live.
COPY --from=builder /install /usr/local

# App source last (most-volatile layer = least cacheable).
WORKDIR /app
COPY --chown=kf:kf . .

# use_cases/ holds bundles + .active marker + .llm_usage.json — these are
# runtime state, not code. Mark the directory writable by the kf user so a
# bind-mount or the seeded files inside the image are usable.
RUN chown -R kf:kf /app/use_cases

USER kf

# Healthcheck hits /health which intentionally bypasses auth. Returns 200
# without touching Neo4j so it stays fast even when the DB is overloaded.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; \
                   sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health',timeout=2).status==200 else 1)"

EXPOSE 8000

# --workers 1 by default — the in-process locks (pipeline_lock, curation_lock,
# active_lock) and rate-limit bucket aren't shared across workers. Bump
# workers only after migrating those to Redis.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
