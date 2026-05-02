# Datasources in production

Operator-facing guide for hardening a Postgres datasource before pointing
it at real data. Two things matter most:

1. **TLS** — never send the DSN's password (or any payload) over a
   plaintext connection.
2. **A least-privilege read-only role** — even with the platform's SQL
   safety filter, the database role itself should be incapable of
   writing. Defence in depth.

For first-time setup of any datasource, see
[Datasources quick-start](datasources-quickstart.md). For adding a new
database engine (MySQL, SQLite, etc.), see
[Extending datasources](extending-datasources.md).

---

## TLS / `sslmode=require`

### Why

Postgres connections without TLS are plaintext over TCP. Anyone on the
network path between the API container and the database can read the
DSN's password during connection negotiation, plus every row of every
SELECT. Required for any cloud-managed Postgres (RDS, Aura, Cloud SQL,
Neon, Supabase) and any internal Postgres exposed beyond `localhost`.

### How

Append `?sslmode=require` (or stricter) to the DSN env var. The
underlying `libpq` driver respects the parameter automatically — no
code change needed in the platform.

```bash
# Minimum acceptable for production:
export ORDERS_PG_DSN='postgresql://reader:secret@db.internal:5432/orders?sslmode=require'
```

### `sslmode` levels — pick the strictest your environment allows

| Mode | What it checks | When to use |
|---|---|---|
| `disable` | Nothing — plaintext. | Never. |
| `allow` | Try TLS, fall back to plaintext. | Never (silent downgrade). |
| `prefer` | Try TLS, fall back to plaintext. | **Default if you set nothing — also never acceptable in prod.** |
| `require` | Encryption mandatory; **no certificate validation**. | Minimum for production. Stops passive eavesdroppers, doesn't catch a MITM with a fake cert. |
| `verify-ca` | Encryption + the server's cert was signed by a CA you trust. | Better. Requires you to have a `sslrootcert` file. |
| `verify-full` | Encryption + CA validation + the cert's hostname matches. | Best. Stops both eavesdroppers and MITM. Cloud providers (AWS RDS, etc.) ship a CA bundle for this. |

### Verifying CA bundle paths (for `verify-ca` / `verify-full`)

The CA bundle is a file on the host where uvicorn runs. Reference it via
DSN parameters:

```bash
export ORDERS_PG_DSN='postgresql://reader:secret@db.region.rds.amazonaws.com:5432/orders?sslmode=verify-full&sslrootcert=/etc/ssl/certs/rds-ca-bundle.pem'
```

Cloud-provider links to the right CA bundle:
- **AWS RDS / Aurora** — [global-bundle.pem](https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem)
- **Google Cloud SQL** — provided per-instance via the Cloud Console
- **Azure Database for Postgres** — [BaltimoreCyberTrustRoot.crt.pem](https://www.digicert.com/CACerts/BaltimoreCyberTrustRoot.crt.pem)
- **Aura (Neo4j-managed)** — N/A (Aura is for Neo4j; Postgres is on you)
- **Neon / Supabase / Crunchy** — see their docs; usually `sslmode=require` only

### Confirming TLS is actually in use

After setting the DSN and restarting uvicorn, click **Test** on the
datasource. If it succeeds, TLS is up — `libpq` would have failed
loud rather than silently downgraded with `sslmode=require`.

For belt-and-braces verification, query the live connection from the
Postgres side:

```sql
-- Run this as your DBA in your existing tooling (psql, DBeaver, etc.)
SELECT
  pid,
  ssl,
  ssl_version,
  ssl_cipher,
  application_name,
  client_addr
FROM pg_stat_ssl
JOIN pg_stat_activity USING (pid)
WHERE usename = 'reader';
```

`ssl` should be `t` (true). If it's `f`, the connection isn't using
TLS — check the DSN.

### Handling cert rotation

When the cloud provider rotates their CA root (RDS does this on a
fixed schedule), download the new bundle and replace the file at
`sslrootcert`. No platform changes needed.

---

## Read-only Postgres role

### Why

The platform refuses any SQL that contains `INSERT/UPDATE/DELETE/DROP/
ALTER/etc.` and sets `default_transaction_read_only = on` on the
session. Belt and braces, but the strongest single layer is on the
database side: connect with a role that *cannot* write, regardless of
what SQL gets through.

If the role can only SELECT, an attacker who somehow injected SQL still
can't change anything. If the role is the application's main user that
also runs migrations, a SQL-injection vulnerability becomes a data-loss
incident.

### Setting it up

Run these as the database owner (or any role with `CREATE ROLE` and
`GRANT` privileges). Replace `orders`, `reader`, and the password with
your own values.

```sql
-- 1. Create the read-only role.
CREATE ROLE reader WITH LOGIN PASSWORD 'long-random-string-from-secrets-manager';

-- 2. Allow connection to the database. Without this you get
--    "permission denied for database orders".
GRANT CONNECT ON DATABASE orders TO reader;

-- 3. Allow USAGE on every schema the role should be able to query.
--    Adjust schema names to your layout.
GRANT USAGE ON SCHEMA public TO reader;

-- 4. SELECT on every existing table in those schemas.
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;

-- 5. Make new tables also default-readable. Without this, any table
--    created AFTER you set this up wouldn't be visible to `reader`
--    until someone re-ran step 4.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO reader;

-- 6. Optional: SELECT on sequences if your queries reference them.
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON SEQUENCES TO reader;
```

### Verifying the role really is read-only

Connect AS the new user from your DBA tool (NOT through the Knowledge
Graph platform yet) and confirm it can read but not write:

```sql
-- Should succeed:
SELECT count(*) FROM orders;

-- Should ALL fail with:
--   ERROR: permission denied for table orders
INSERT INTO orders (order_id, customer, status) VALUES (999, 'TEST', 'OPEN');
UPDATE orders SET status = 'X' WHERE order_id = 1;
DELETE FROM orders WHERE order_id = 1;
DROP TABLE orders;

-- Should also fail (would let you escalate):
CREATE TABLE attacker (x int);
ALTER ROLE reader SUPERUSER;
```

If any write succeeds, the role still has too much. Revoke until it
doesn't:

```sql
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
  ON ALL TABLES IN SCHEMA public FROM reader;
```

### Restricting visibility further

By default the role can SELECT every table in every granted schema.
For sensitive deployments, restrict to specific tables:

```sql
-- Only allow reading orders + customers, nothing else
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM reader;
GRANT SELECT ON public.orders, public.customers TO reader;
```

Or use Postgres row-level security if the same table should expose
different rows to different roles:

```sql
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;

CREATE POLICY reader_open_orders_only ON orders
  FOR SELECT TO reader
  USING (status IN ('OPEN', 'PROCESSING'));
```

The Knowledge Graph platform sees only the rows the policy permits.

### Combining with the platform's SQL filter

The defence-in-depth stack for any pull is now:

1. **Manifest validator** — refuses pull adapter SQL containing
   `INSERT/UPDATE/etc.` at parse time. Catches typos and obvious
   misuse.
2. **`assert_read_only_sql()`** — same regex check at runtime, before
   the connection opens.
3. **`SET default_transaction_read_only = on`** — Postgres session
   refuses writes server-side, even if both filters above were bypassed.
4. **Postgres role privileges** — the connecting role *cannot* write,
   regardless of session settings.
5. **(Optional) Row-level security** — the role can read, but only
   the rows it's allowed to see.

A real attacker would need to defeat all five. The first three are in
the platform's code (audited; tested); 4 and 5 are yours to set up.

---

## Putting it together — production DSN example

```bash
# AWS RDS Postgres with verify-full TLS + read-only role:
export ORDERS_PG_DSN='postgresql://reader:gT7c3UcGwKP4dPqTV8vNmZ3X@orders.xxxxx.us-east-1.rds.amazonaws.com:5432/orders?sslmode=verify-full&sslrootcert=/etc/ssl/certs/rds-global-bundle.pem&application_name=kf-knowledgegraph'
```

Two extras worth setting:

- `application_name=kf-knowledgegraph` — so DBAs grepping `pg_stat_activity`
  can see at a glance which connections come from this platform.
- `connect_timeout=10` — so a hung Postgres doesn't make every pull
  hang for the platform's 60s OpenAI timeout. (The connector already
  passes `connect_timeout=10` to `psycopg.connect`, but explicit in
  the DSN documents intent.)

---

## Operational checklist

Before pointing the platform at production data:

- [ ] DSN env var uses `sslmode=require` at minimum (`verify-full`
      preferred).
- [ ] If using `verify-ca` / `verify-full`, the CA bundle is on disk
      where uvicorn runs, with a clear path in the DSN.
- [ ] The connecting Postgres role is a dedicated read-only role, not
      the database owner.
- [ ] Verified the read-only role using actual write attempts from
      `psql` — they all returned `permission denied`.
- [ ] Sensitive tables are either excluded from the role's privileges
      or behind row-level security.
- [ ] DSN env var is set in your secrets-management tool (Vault, AWS
      Secrets Manager, etc.) — not in shell history, not in `.env`
      committed to git, not in YAML manifests.
- [ ] Postgres-side audit logging captures the platform's connections
      so you can correlate pull-runs with DB activity.
- [ ] (If applicable) Network ACLs / VPC security groups restrict
      database access to the API host's IP only.
