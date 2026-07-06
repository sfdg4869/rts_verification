# QA Portal Server

Flask-based QA portal for RTS checks, repo DB checks, target SQL checks, and MongoDB-backed DB configuration management.

## Repository Layout

```text
qa-portal-server-main/
|-- app/
|   |-- routes/            # Flask blueprints
|   |-- services/          # DB, SSH, repo-check, and WS logic
|   |-- templates/         # UI templates served by Flask
|   |-- sql_templates/     # Runtime SQL templates for repo checks
|   `-- resource/          # Runtime state and generated JSONL logs
|-- docs/                  # Design notes and workflow documents
|-- scripts/               # One-off helper scripts
|-- tests/                 # Python tests only
|-- app.py                 # Local development entrypoint
|-- wsgi.py                # WSGI entrypoint for gunicorn
|-- Dockerfile             # Container build
`-- docker-compose*.yml    # Deployment variants
```

## Runtime Files Kept At Repo Root

- `db_setup.json`
- `db_config.json`
- `.env`
- `DGServer.jar`
- `instantclient.placeholder`

These stay at the root because the current app and container setup read them from there.

## Notes

- `/rts-check` renders `app/templates/rts_check.html`.
- Repo-check SQL templates live in `app/sql_templates/` instead of `tests/`.
- Generated artifacts such as Playwright logs and chart export folders are ignored via `.gitignore`.
