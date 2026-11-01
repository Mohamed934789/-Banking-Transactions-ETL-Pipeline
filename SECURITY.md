# Credential remediation

A Google key in another project and the Snowflake password previously committed here require
owner-controlled revocation/rotation if genuine. This branch removes current literals but does
not invalidate credentials or erase Git history. No discovered credential was tested.

Copy `.env.example` to ignored `.env.local`, set private values, and use
`docker compose --env-file .env.local up --build`. Never restore secrets to tracked `.env`.
Generate a Fernet key with the cryptography package and choose distinct local service passwords
and a Jupyter token. Existing database volumes retain old credentials; changing Compose variables
is not password rotation. Do not delete volumes to perform a security change.

Use a least-privilege Snowflake role with only the required warehouse/schema/table grants.
Ports bind to localhost, Jupyter requires a token and the unused Docker socket mount is removed.
Do not expose this learning stack to an untrusted network.
