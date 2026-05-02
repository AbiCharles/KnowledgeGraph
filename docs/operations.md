# Operations guide

Everything you need to run the platform in production: hardening config,
running with Docker, backup + restore, observability, common incidents.

## Production env vars

The Python app reads its configuration from environment variables (or a
`.env` file in the working directory). The full set:

| Variable | Required | Default | Notes |
|---|---|---|---|
| `NEO4J_URI` | yes | — | `bolt://host:7687`. For docker-compose: `bolt://neo4j:7687`. |
| `NEO4J_USERNAME` | no | `neo4j` | Standard Neo4j auth. |
| `NEO4J_PASSWORD` | yes | — | |
| `OPENAI_API_KEY` | yes | — | Even with no agents, `/nl` needs it. |
| `OPENAI_MODEL` | no | `gpt-4o-mini` | Override per deployment. |
| `OPENAI_TIMEOUT_SECONDS` | no | `60` | Per-call timeout. Don't go above 120. |
| `LLM_DAILY_USD_CAP` | no | `5.0` | Soft cap. Set `0` to disable. |
| `API_KEY` | recommended | empty | Empty = auth disabled (local dev only). Set to a long random string in prod. |
| `RATE_LIMIT_PER_MINUTE` | no | `120` | `0` to disable. Per-IP token bucket. |
| `CORS_ORIGINS` | no | `http://localhost:8000,http://127.0.0.1:8000` | Comma-separated allowlist. |
| `LOG_FORMAT` | no | `text` | `json` for structured logs. |
| `LOG_LEVEL` | no | `INFO` | DEBUG / INFO / WARNING / ERROR. |
| `UPLOAD_MAX_BYTES` | no | `5242880` | Per-file cap on bundle upload (5 MiB). |
| `<custom>_PG_DSN` | yes if used | — | Per-bundle Postgres DSN env vars. The exact name comes from each datasource's `dsn_env:` in the manifest. Format: `postgresql://user:pass@host:5432/db`. Never put credentials in YAML. |

### Generating a strong API key

```bash
python -c 'import secrets; print(secrets.token_urlsafe(48))'
# 64-char URL-safe key — paste into API_KEY=
```

## Docker deployment

The `docker-compose.yml` brings up Neo4j + the API in one command:

```bash
# In the repo root, create .env with at least:
#   NEO4J_PASSWORD=<your-pw>
#   OPENAI_API_KEY=sk-...
#   API_KEY=<output of secrets.token_urlsafe(48)>
docker compose up -d
```

Then visit [http://localhost:8000](http://localhost:8000) — first request
to a protected endpoint prompts for the API key. Paste it once; it stays
in sessionStorage for the tab.

### Behind a reverse proxy

For anything not on `localhost`, terminate TLS at a reverse proxy
(Caddy / nginx / Traefik) and point it at the API on port 8000. The
backend honours `X-Forwarded-For` for the rate limiter — make sure your
proxy is the **only** thing setting that header (otherwise a hostile
client can spoof the source IP and bypass the per-IP limiter).

Caddy snippet (proxies + sets X-Forwarded-For):

```caddyfile
kf.example.com {
    reverse_proxy localhost:8000
}
```

## Observability

### Structured logs

Set `LOG_FORMAT=json` and pipe stdout to your aggregator. Each line is a
JSON object with keys: `ts`, `level`, `logger`, `msg`, `rid`, plus any
`extra=` kwargs. The `rid` is a per-request UUID that's also returned in
the `X-Request-Id` response header — use it to grep one request's full
trace.

Example query (jq):

```bash
docker compose logs api -f | jq 'select(.level == "ERROR")'
```

### Prometheus metrics

`GET /metrics` exposes:

- `kf_requests_total{method,route,status}` — counter
- `kf_request_duration_seconds{method,route}` — histogram with standard
  Prometheus buckets

Routes are normalised to their template (e.g. `/use_cases/{slug}`) so
high-cardinality slugs don't blow up the counter set.

Public by default — same as `/health` so a Prometheus scraper can hit
it without an API key. If you need it gated, add `/metrics` to
`api/security._API_PREFIXES`.

### Health probes

- `GET /health` — fast 200, no DB hit. Use for readiness + liveness
  probes.
- `GET /capabilities` — returns `{multi_database, active_database}`.
  Useful for confirming the active database after a restart.

## Backups

### What needs backing up

`use_cases/` has everything stateful that lives outside Neo4j:
- Each `<slug>/` directory (manifest + ontology + data)
- Each `<slug>.versions/` directory (archived prior uploads)
- `.active` (which slug is the current active selection)
- `.llm_usage.json` (rolling 60-day spend tracker)

Neo4j data backs up via Neo4j's own tooling
(`neo4j-admin database dump` or AuraDB's built-in snapshot). Make sure
both are scheduled — restoring `use_cases/` to a different Neo4j cluster
without the corresponding graph data leaves the dashboard pointing at
empty databases.

### Backup script

```bash
scripts/backup_bundles.sh                      # writes ./backups/use_cases-<stamp>.tar.gz
scripts/backup_bundles.sh /var/lib/kf/backups  # custom destination
```

The script:
- Excludes `.lock`, `*.staging/` (transient state).
- Verifies the archive after writing — if `tar -t` fails, the bad file
  is removed and the script exits non-zero.
- Prints `OK <path> (<size>)` on success.

### Cron example

```
# Daily 02:30 UTC, keep 14 days.
30 2 * * * /opt/kf/scripts/backup_bundles.sh /var/lib/kf/backups \
            >> /var/log/kf-backup.log 2>&1
0  3 * * * find /var/lib/kf/backups -name 'use_cases-*.tar.gz' -mtime +14 -delete
```

### Restore

```bash
scripts/restore_bundles.sh /var/lib/kf/backups/use_cases-20260502T023000Z.tar.gz
# Refuses if use_cases/ is non-empty. Pass --force to override.

scripts/restore_bundles.sh ... --force         # clobbers the existing set
```

After a restore, restart uvicorn so the registry re-discovers the
bundles. With docker-compose:

```bash
docker compose restart api
```

### Disaster-recovery rehearsal

Once a quarter, **actually do** a restore into a non-prod environment
and verify the dashboard comes up. A backup that's never been tested is
a story, not a backup.

## Common incidents

### "DB: n/a" red chip in the header

`/capabilities` is failing. Check:

1. Is uvicorn running? `docker compose ps`
2. Is Neo4j reachable from the API container? `docker compose exec api python -c "from db import supports_multi_db; print(supports_multi_db())"`
3. Did you restart after upgrading? Old uvicorn may not have the new route.

### 401 on every request after restart

The `API_KEY` env var changed but the dashboard still has the old key in
sessionStorage. Open the browser DevTools → Application → Storage →
sessionStorage → delete `kf_api_key`. Reload, paste the new key.

### Pipeline stage 1 says "Variable type not defined"

Old bug fixed in commit e4457c0 — make sure you're running the current
build. `docker compose build api && docker compose up -d`.

### Daily LLM cap hit, agents 429ing

Either:
- Raise `LLM_DAILY_USD_CAP` (be careful — soft cap exists for a reason).
- Look at `use_cases/.llm_usage.json` for the per-call breakdown by `kind`
  (nl / agent). If one agent is dominating, edit its system prompt to be
  more parsimonious, or add a query filter to its `cypher_hint` so it
  pulls less context.

### Ontology curation fails with SHACL violation

The data violates a constraint declared in the ontology. See
[docs/ontology-curation.md](ontology-curation.md) — fix either the data
(deduplicate / add missing properties) or relax the constraint via the
inline ontology editor.

### Pipeline succeeds but the graph is empty

Check the active database name (`/capabilities`). On Neo4j Enterprise
with multi-DB, switching active without re-running the pipeline against
the new bundle's database leaves it empty. Solution: run pipeline.

## Scaling

Single-process deployment is the supported topology today. Several
in-process structures don't share across workers:

- `pipeline_lock` / `curation_lock` / `active_lock`
- Rate-limit token bucket
- Schema cache
- LLM usage in-process Lock (the file lock IS shared, but the in-process
  Lock isn't)

Before bumping `--workers > 1` you'd need:

1. Move the route locks to Redis (e.g. `redis-py`'s `RedisLock`).
2. Move the rate limiter to a Redis-backed library (`slowapi` + Redis).
3. Either hash-route requests for the same active bundle to the same
   worker (sticky sessions) or rely on the file lock alone.

For pilot scale (a handful of operators, < 100 req/sec), single worker
is plenty.

## Updating CDN-pinned assets

The dashboard loads CodeMirror + marked from a CDN with SHA-384
integrity hashes. To upgrade a version:

1. Pick the new version (e.g. CodeMirror 5.65.17).
2. Compute the hash:
   ```bash
   curl -sL https://cdn.jsdelivr.net/npm/codemirror@5.65.17/lib/codemirror.min.js \
     | openssl dgst -sha384 -binary | openssl base64 -A
   ```
3. Update both the `src=` URL **and** the `integrity=` attribute in
   `frontend/index.html`. They must match.

## Security checklist before exposing the API

- [ ] `API_KEY` set to a 48+ char random string
- [ ] `RATE_LIMIT_PER_MINUTE` reasonable (default 120 is fine for most
      pilots)
- [ ] `CORS_ORIGINS` restricted to actual frontend origins (not `*`)
- [ ] TLS terminated at a reverse proxy
- [ ] `LOG_FORMAT=json`, logs shipped to a central aggregator
- [ ] `/metrics` scraped by Prometheus or equivalent
- [ ] `scripts/backup_bundles.sh` scheduled in cron
- [ ] Neo4j-side backups also scheduled (separately)
- [ ] Backup restore rehearsed at least once
