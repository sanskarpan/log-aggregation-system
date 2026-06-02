# Security

## Isolation Model

- every request is tenant-scoped
- policy enforcement happens before write acceptance or query planning
- tenants cannot access another tenant's streams, fields, or cached results

## Authentication

- current runtime supports static bearer-token auth or OIDC JWT auth behind `AUTH_REQUIRED=true`
- `AUTH_MODE=static` uses `AUTH_BEARER_TOKEN` plus `X-Scopes` for scope assertions
- `AUTH_MODE=oidc` verifies RS256 JWTs from `OIDC_ISSUER_URL` and `OIDC_JWKS_URL` and derives scopes from token claims
- user and agent traffic: OIDC-issued JWTs are now supported for the enterprise path
- service-to-service traffic: mTLS is supported through `TLS_ENABLED`, `TLS_CERT_FILE`, `TLS_KEY_FILE`, `TLS_CLIENT_CA_FILE`, and client-side TLS settings
- dev mode may allow static tokens, but only outside production profiles

## Authorization

- the runtime enforces scopes via `X-Scopes` using `ingest:write`, `query:read`, `admin:read`, and `admin:write`
- separate scopes for ingest, query, and admin mutations
- tenant-scoped role bindings
- audit every control-plane mutation and privileged query; writes now land in the local audit log in the control-plane runtime
- privileged requests can also be appended to an audit log via `AUDIT_LOG_PATH`

## Metrics Exposure

- every HTTP service now exposes `/metrics` in Prometheus text format
- metrics should be treated as operational data and placed behind network policy or cluster access controls where needed

## Secret Handling

- static bearer tokens and OIDC client or issuer material should be delivered through Kubernetes Secrets or host-level secret stores
- never place bearer tokens or signed JWTs in logs, config snapshots, or audit payloads

## Data Protection

- TLS in transit
- object-store encryption where available
- secret delivery through Kubernetes Secrets or equivalent host-level secret management
- avoid writing raw auth material to logs

## Abuse Controls

- request size limits
- label cardinality budgets
- ingest and query rate limits
- bounded regex and text query safeguards
