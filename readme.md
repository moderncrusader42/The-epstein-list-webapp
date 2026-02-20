# The List Webapp

Web application for organizing and reviewing Epstein-related case material in one place, including:
- people records,
- evidence sources,
- unsorted incoming files,
- theories,
- role-based proposal and review workflows.


## Tech Stack

- Python 3.12
- FastAPI + Gradio
- SQLAlchemy + PostgreSQL (local or Cloud SQL)
- Google OAuth (Authlib)
- Google Cloud Storage / Secret Manager integrations

## Required Runtime Configuration

Set these in `secrets/env.dev` or `secrets/env.prod`:

- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `SESSION_SECRET` (recommended; default fallback exists for development)
- Database config using one of:
  - `DATABASE_URL`
  - `INSTANCE_CONNECTION_NAME` + `DB_USER` + `DB_NAME` + (`DB_PASS` or `DB_PASSWORD`)
  - `PGHOST` + `PGPORT` + `PGUSER` + `PGPASSWORD` + `PGDATABASE`
  - `BUCKET_NAME`
