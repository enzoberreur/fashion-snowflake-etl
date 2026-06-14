# Security note - credential exposure incident

## What happened

In early commits of this repository, the file `.env` was tracked by git and contained:

- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`
- `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`
- A raw `PRIVATE_KEY` PEM block

The private key file `keys/rsa_key.p8` was also tracked for a period.

## Why this is no longer a live risk

The Snowflake account used in this project was a **90-day free trial** that has since expired and been deactivated. The credentials in git history are therefore no longer usable against any live Snowflake environment.

## What was changed to prevent recurrence

1. `.env` was removed from the git index (`git rm --cached .env`) and is now ignored via `.gitignore`.
2. `.env.example` was added as the credential template — only placeholders.
3. `.gitignore` now excludes `.env`, `*.pem`, `*.key`, `credentials.json`, and similar.
4. The 70 MB of generated `data/*.json` was also untracked (regenerable via `python data_generator.py`).

## What would be done for a production rotation

If this were a live account, the response would be:

1. **Immediately rotate** the Snowflake user's RSA key pair (`ALTER USER ... SET RSA_PUBLIC_KEY=...`).
2. **Revoke and reissue** the passphrase.
3. **Audit** Snowflake `QUERY_HISTORY` and `LOGIN_HISTORY` for unauthorized access during the exposure window.
4. **Purge** `.env` from full git history with `git filter-repo` or BFG Repo-Cleaner, then force-push to all remotes.
5. **Notify** any downstream consumers of the rotated credentials.

Because the trial account is dead, steps 1-3 are moot.

## History purge (completed 2026-06-10)

Step 4 was carried out before publication: `git filter-repo` removed `.env` and `keys/rsa_key.p8` from the entire git history, and the repository was repacked. A mirror backup of the pre-purge history is kept offline. The next push to the GitHub remote must be a force-push (`git push --force origin main`) so the cleaned history replaces the old one. This note is kept so the incident and the full remediation remain visible as a learning artifact for the thesis jury.

## Lessons applied across the rest of this project

- `requirements.txt`, `.env.example`, and `.gitignore` are now in place from the start.
- Snowflake key loading is centralized in one module (`python/monogram_etl/config/snowflake.py`) so rotation only touches one file.
- All future secrets are documented in `.env.example` with placeholder values only.
