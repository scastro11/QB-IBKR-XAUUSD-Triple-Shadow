# Security policy

This is a public research repository. Never submit broker credentials, account identifiers, API tokens, personal data, private endpoints, private IP addresses, `.env` files, or telemetry queues.

The telemetry endpoint and token are intentionally supplied only through local environment variables. They are not included in the repository.

If a secret is committed, revoke or rotate it immediately. Removing it in a later commit is not sufficient because Git history can retain prior content.

Report a suspected exposure privately to the repository owner rather than opening a public issue containing the secret.
