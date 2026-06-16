# QA Portal Semi-Auto CI/CD

This repository now uses two main Docker Compose entry points:

- `docker-compose.yml`
  Production deploy. Pulls a prebuilt image from a registry.
- `docker-compose.dev.yml`
  Local development. Builds from the local `Dockerfile`.

For old Oracle servers that require Thick mode, production deploy can also add:

- `docker-compose.oracle-zip-build.yml`
  Production-only override that switches the `web` service to a local Docker build with a staged Oracle Instant Client zip.

## 1. Local development

```bash
docker compose -f docker-compose.dev.yml up --build
```

## 2. Server bootstrap

Clone this repository on the target server and prepare these files:

- `.env`
- `db_setup.json`
- `db_config.json`
- `app/resource/`

Manual deploy from the server:

```bash
bash ./deploy.sh
```

`deploy.sh` loads values from `.env`, so keep `QA_PORTAL_IMAGE` there for normal image-pull deploys.

If you prefer to override the image for one run:

```bash
QA_PORTAL_IMAGE=ghcr.io/OWNER_OR_ORG/qa-portal-server:latest bash ./deploy.sh
```

## 3. GitHub Actions flow

- `push` to `main`
  Builds the Docker image and pushes it to GHCR.
- `workflow_dispatch`
  Builds the image and then runs the remote deploy over SSH.

The workflow pushes two tags:

- `ghcr.io/<owner>/qa-portal-server:latest`
- `ghcr.io/<owner>/qa-portal-server:sha-<12-char-sha>`

Remote deploy uses the SHA tag so the deployed image matches the selected commit.

## 4. Required GitHub secrets

Add these repository secrets before running the deploy workflow:

- `APP_PORT`
- `DEPLOY_HOST`
- `DEPLOY_PASSWORD`
- `DEPLOY_PORT`
- `DEPLOY_USER`
- `DEPLOY_PATH`
- `GHCR_USERNAME`
- `GHCR_TOKEN`

Notes:

- `APP_PORT` is optional. If set, the workflow passes it to `deploy.sh` during deploy.
- `DEPLOY_PATH` can be either the repository root or the `qa-portal-server-main` directory on the server.
- `DEPLOY_PASSWORD` is the SSH password for `DEPLOY_USER`.
- `GHCR_TOKEN` should be a token that can push packages in GitHub Actions and pull packages on the target server.
- The server checkout must be able to run `git pull origin <branch>`.

## 5. Oracle Thick mode for old Oracle servers

If Oracle connections fail with `DPY-3010` in Docker, the app is running in thin mode and the target Oracle server is too old for thin mode.

Prepare Oracle Instant Client on the server and set these values in `.env`:

```bash
ORACLE_CLIENT_ZIP_PATH=/home/maxgauge/oracle/instantclient-basic-linuxx64.zip
ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient
```

When `ORACLE_CLIENT_ZIP_PATH` is set, `deploy.sh` switches from registry pull mode to a local Docker build:

1. It copies the server-local zip into the repo root as `instantclient.zip`.
2. It adds `docker-compose.oracle-zip-build.yml`.
3. It builds the `web` image with Oracle Instant Client bundled into the image.
4. It removes the staged `instantclient.zip` after the deploy ends.

Keep the zip file outside Git. The staged `instantclient.zip` is temporary and is ignored by Git.

## 6. Recommended first run

1. Push the CI/CD files to `main`.
2. Run the `Build and Deploy` workflow with `workflow_dispatch`.
3. Confirm the server updated with `docker compose ps`.
