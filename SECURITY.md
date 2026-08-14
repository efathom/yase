# Security Policy

## Reporting a Vulnerability

We take security issues seriously. If you believe you have found a security vulnerability in YASE, please report it responsibly rather than opening a public issue.

**Do not** open a GitHub issue or PR with the details. Instead, report it privately via one of:

- **GitHub**: use the repository's *Private vulnerability reporting* feature (Security → Report a vulnerability), or
- **Email**: `security@efathom.com`

Please include:

- A clear description of the vulnerability and its impact
- Steps to reproduce, or a proof-of-concept if available
- The affected version(s) / commit(s)
- Any suggested remediation

## What to expect

- You will receive an acknowledgment within a few business days.
- We will validate the report and keep you informed of our assessment and fix timeline.
- Once a fix is available we will publish a security advisory and credit the reporter (unless you prefer to remain anonymous).

## Supported versions

| Version | Supported |
|---------|-----------|
| latest `main` | :white_check_mark: |
| tagged releases | :white_check_mark: (latest release) |

## Security best practices for deployments

When running YASE in production, enable the security controls described in
[`docs/configuration.md`](docs/configuration.md):

- Enable authentication (`auth.enabled: true`) with API keys or JWT.
- Enable TLS for HTTP and gRPC traffic (`tls.enabled: true`).
- Keep tenant isolation enabled (the `_tenant` filter is injected server-side).
- Never commit real credentials; use environment variables or a secret manager for `configs/connectors.json` and the Helm chart's `secrets`.
