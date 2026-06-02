# Secret Management

## Kubernetes

- Store bearer tokens, OIDC issuer material, TLS certificates, and database DSNs in Kubernetes Secrets.
- Mount secrets read-only and inject them as environment variables only when the process actually needs them.
- Rotate OIDC signing material and TLS certificates with overlapping validity windows.
- Use separate secrets for server TLS, client TLS, and database credentials.

## Bare Metal

- Keep secrets in a host-level secret manager or a root-owned file store with restricted permissions.
- Avoid embedding secrets into systemd unit files or shell history.
- Prefer per-service secret files over shared environment files for anything that contains keys or tokens.

## Practical Rules

- Never log bearer tokens, JWTs, private keys, or database passwords.
- Use distinct credentials for control plane, data plane, and observability backends.
- Rotate static bearer tokens only for dev or bootstrap flows; use OIDC for real user and agent auth.
