# Conduit — Installation and Deployment Guide

End-to-end guide for building the engine image, packaging a per-flow image,
running the container, and verifying the environment with `conduit doctor`.

---

## Table of Contents

1. [Get the engine image](#1-get-the-engine-image)
2. [Build a per-flow image](#2-build-a-per-flow-image)
3. [Run a single-container engine](#3-run-a-single-container-engine)
4. [Run the dev stack (compose)](#4-run-the-dev-stack-compose)
5. [Run the operator stack (compose)](#5-run-the-operator-stack-compose)
6. [Verify with `conduit doctor`](#6-verify-with-conduit-doctor)
7. [Bump the Bun version](#7-bump-the-bun-version)
8. [End-to-end walkthrough](#8-end-to-end-walkthrough)

---

## 1. Get the engine image

The supported release image is published to GitHub Container Registry for
Linux on both amd64 and arm64. Pin the immutable version in deployments, then
optionally add the local name used by the Compose examples:

```bash
docker pull ghcr.io/theaiteam-dev/conduit-engine:1.0.0
docker tag ghcr.io/theaiteam-dev/conduit-engine:1.0.0 conduit-engine:1.0.0
docker tag ghcr.io/theaiteam-dev/conduit-engine:1.0.0 conduit-engine:latest
```

`ghcr.io/theaiteam-dev/conduit-engine:latest` tracks the newest stable release.
Use `:1.0.0` when reproducibility matters and `:main` only to test unreleased
changes.

### Build from source

The engine image is defined by the `Dockerfile` at the repository root. It pins
Bun to `oven/bun:1.3.11-slim`, creates a non-root system user (`conduit`), and
sets the ENTRYPOINT to the TypeScript CLI so every `docker run` argument is
forwarded verbatim as a `conduit` subcommand.

```bash
# Tag the image as conduit-engine (the name docker-compose.*.yml files expect).
docker build -t conduit-engine .
```

To tag a source build with the release version:

```bash
docker build -t conduit-engine:1.0.0 .
```

> **What the Dockerfile does**
>
> | Step | Purpose |
> |------|---------|
> | `FROM oven/bun:1.3.11-slim` | Pinned Bun runtime — see [§7](#7-bump-the-bun-version) to change it |
> | Create `conduit` user/group | Least-privilege execution — the container never runs as root |
> | `WORKDIR /app` | Relative paths such as `examples/branching/flow.yaml` resolve here |
> | `COPY package.json bun.lock ./` + `bun install` | Dependency layer cached independently of source changes |
> | `COPY . .` | Full source tree — `.dockerignore` strips secrets and state (see [§3](#3-run-a-single-container-engine)) |
> | `RUN mkdir -p /data && chown conduit:conduit /data` | Mount point for `conduit.sqlite`; must be writable by the non-root user |
> | `USER conduit` | Drop privileges before the ENTRYPOINT |
> | `ENTRYPOINT ["bun", "src/cli/main.ts"]` | Every `docker run <img> <args>` becomes `conduit <args>` |

---

## 2. Build a per-flow image

`conduit build` reads a `flow.yaml`, generates a Dockerfile that layers the
flow on top of the engine image, and prints it to stdout. It never calls
`docker build` itself; you redirect the output and build it with Docker.

```bash
# Generate the per-flow Dockerfile and write it to a file.
docker run --rm conduit-engine build examples/branching/flow.yaml > Dockerfile.branching

# Build the per-flow image from the examples/ directory so Docker's build
# context includes the branching/ subdirectory that COPY references.
docker build -t branching-flow -f Dockerfile.branching examples/
```

To publish the per-flow image to a registry, tag it with your registry path and
push it like any other Docker image:

```bash
docker tag branching-flow ghcr.io/my-org/branching-flow:0.1.0
docker push ghcr.io/my-org/branching-flow:0.1.0
```

### What `conduit build` generates

The generated Dockerfile:

1. **Starts `FROM conduit-engine:latest`** — the per-flow image extends the engine.
2. **Temporarily switches to `USER root`** for image-build steps, then drops
   back to `USER conduit` before runtime.
3. **COPYs the flow directory** into `/flow/` using JSON-exec form and
   `--chown=conduit:conduit` so paths with spaces are handled correctly and the
   non-root runtime user can write flow outputs.
4. **Installs `flow.prerequisites`** via a single `apt-get install` step — listing
   exactly the packages declared in the flow's `prerequisites:` field. If the
   field is absent or empty, no `apt-get install` step is emitted.

Example flow with prerequisites:

```yaml
# flow.yaml (excerpt)
prerequisites: [jq, ffmpeg]
```

Generated Dockerfile fragment:

```dockerfile
USER root
COPY --chown=conduit:conduit ["branching/", "/flow/"]
ENV CONDUIT_PROJECT_ROOT=/flow

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    jq \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

USER conduit
```

### Error paths

| Situation | Exit code | Output |
|-----------|-----------|--------|
| No flow argument given | 1 | `usage: conduit build <flow.yaml>` on stderr |
| Flow file not found | 1 | `FILE_READ_ERROR` validation error on stderr |
| Flow fails validation (e.g. `prerequisites` is not a list) | 1 | `INVALID_PREREQUISITES` on stderr; no Dockerfile emitted |

---

## 3. Run a single-container engine

The canonical `docker run` pattern mounts a named volume at `/data` for state
persistence and injects secrets at runtime via `-e`.

```bash
docker run --rm \
  -v conduit_data:/data \
  -e CONDUIT_API_KEY="$CONDUIT_API_KEY" \
  -e CONDUIT_BASE_URL="$CONDUIT_BASE_URL" \
  conduit-engine \
  run examples/branching/flow.yaml
```

### State persistence (`/data`)

The default state database path is `/data/conduit.sqlite`
(`DEFAULT_STATE_DB` in `src/cli/main.ts`). The WAL and journal files
(`conduit.sqlite-shm`, `conduit.sqlite-wal`, `conduit.journal.sqlite`) live in
the same directory. **Always mount a named Docker volume at `/data`** — the
ephemeral container filesystem is wiped on `docker rm`.

```bash
# Create the named volume once (idempotent).
docker volume create conduit_data
```

### Secret injection (FR-5)

Secrets are **never** baked into an image layer. The `.dockerignore` at the
repository root explicitly excludes:

| Excluded path | Reason |
|---------------|--------|
| `.env` | Runtime secrets file — never enters any layer |
| `conduit.sqlite` / `conduit.sqlite-shm` / `conduit.sqlite-wal` | Local state — mounted at runtime on a volume |
| `conduit.journal.sqlite` | Journal state — same |
| `node_modules` | Rebuilt inside the container |
| `.git` | Version-control metadata |

Supply secrets at runtime using one of three methods:

**Method A — shell environment variables (simplest):**

```bash
export CONDUIT_API_KEY="sk-..."
export CONDUIT_BASE_URL="https://api.openai.com/v1"
docker run --rm -v conduit_data:/data \
  -e CONDUIT_API_KEY -e CONDUIT_BASE_URL \
  conduit-engine run flow.yaml
```

**Method B — `.env` file (excluded from images by `.dockerignore`):**

```bash
# .env (never committed)
CONDUIT_API_KEY=sk-...
CONDUIT_BASE_URL=https://api.openai.com/v1

docker run --rm -v conduit_data:/data \
  --env-file .env \
  conduit-engine run flow.yaml
```

**Method C — Docker secrets (production/Swarm):**

```bash
docker secret create conduit_api_key <(echo -n "sk-...")
# Reference in docker-compose as: secrets: [conduit_api_key]
```

### Non-root bind-mount note (D3)

The container runs as the `conduit` system user (non-root). When you bind-mount
a **host directory** at `/data` instead of using a named volume, the host
directory must be writable by the container's UID:

```bash
# Identify the container UID.
docker run --rm --entrypoint id conduit-engine -u
# → e.g. 999

# Grant write permission on the host directory.
sudo chown 999 /path/to/host/data
docker run --rm -v /path/to/host/data:/data conduit-engine doctor
```

Named volumes (the default pattern) handle ownership automatically and are
recommended for all deployments.

---

## 4. Run the dev stack (compose)

`docker-compose.dev.yml` provides a two-service dev iteration stack:

| Service | Image | Purpose |
|---------|-------|---------|
| `engine` | `conduit-engine` | Engine container with project bind-mounted at `/app` — edit source, re-run without rebuilding |
| `model` | `ollama/ollama` | Local model sidecar; engine reaches it via `CONDUIT_BASE_URL: http://model:11434/v1` |

Named volumes:

| Volume | Mounted at | Contents |
|--------|------------|---------|
| `conduit_data` | `/data` (engine) | `conduit.sqlite` and WAL files |
| `ollama_models` | `/root/.ollama` (model) | Downloaded Ollama model weights |

```bash
# Start the local model sidecar. The engine is run as a one-off tool container.
export CONDUIT_API_KEY="$CONDUIT_API_KEY"
docker compose -f docker-compose.dev.yml up -d model
```

The project root is bind-mounted at `/app`, so source edits are visible
immediately — no image rebuild needed. Run a one-off command against the
engine service:

```bash
docker compose -f docker-compose.dev.yml run --rm engine run examples/branching/flow.yaml
```

---

## 5. Run the operator stack (compose)

`docker-compose.operator.yml` defines the production listener + kernel topology.
It expects a per-flow image such as `branching-flow` from [§2](#2-build-a-per-flow-image);
override the image with `CONDUIT_FLOW_IMAGE`.

| Service | Image | Command | Purpose |
|---------|-------|---------|---------|
| `listener` | `${CONDUIT_FLOW_IMAGE:-branching-flow}` | `listen --flows branching=/flow/flow.yaml --port 8080` | Long-lived process; receives webhook events on port `8080`, enqueues cards into the shared state DB |
| `kernel` | `${CONDUIT_FLOW_IMAGE:-branching-flow}` | `run /flow/flow.yaml` | Per-run process; executes the baked flow against the shared state DB and exits when the flow completes |
| `model` | `ollama/ollama` | — | Local model sidecar; both engine services reach it via `CONDUIT_BASE_URL: http://model:11434/v1` |

Named volumes:

| Volume | Mounted at | Services | Contents |
|--------|------------|---------|---------|
| `conduit_data` | `/data` | `listener` and `kernel` | Shared `conduit.sqlite` — both services read/write the same state |
| `ollama_models` | `/root/.ollama` | `model` | Ollama model weights |

```bash
# Export required secrets.
export CONDUIT_API_KEY="..."
export CONDUIT_FLOW_IMAGE="branching-flow"
export CONDUIT_SLACK_SIGNING_SECRET="..."             # required if a Slack channel is declared

# Start the long-lived listener + model sidecar. The kernel profile is run on demand.
docker compose -f docker-compose.operator.yml up -d listener model
```

The `listener` service has `restart: unless-stopped` so it survives container
restarts. The `kernel` service is started on demand:

```bash
# One-shot run against the shared state DB.
docker compose -f docker-compose.operator.yml run --rm kernel
```

> **Port mapping** — the `listener` service binds `8080:8080`. Put a reverse
> proxy or cloud load-balancer in front for TLS termination; the container
> serves plain HTTP.

---

## 6. Verify with `conduit doctor`

`conduit doctor` runs a set of named probes and reports each one individually.
The process exits non-zero if any probe fails.

```
conduit doctor [flow.yaml]
```

### Base probes (always run)

These probes run whether or not a flow argument is given:

| Probe name | What it checks |
|------------|---------------|
| `state_db_volume` | Writes and deletes a temp file in the state-DB directory. Paths under `/data` must also be backed by an actual Docker mount; no-volume containers report FAIL instead of silently using ephemeral storage |
| `project-root-present` | Checks `CONDUIT_PROJECT_ROOT` on disk. Reports FAIL only when the env var is set to a non-existent path. Reports ok (no-op) when the env var is unset or empty — the engine image sets it to an empty string so the probe is a no-op there. Per-flow images set `CONDUIT_PROJECT_ROOT=/flow` so the baked-in directory is verified |
| `model_api_key` | `CONDUIT_API_KEY` or `OPENAI_API_KEY` is set |
| `gateway_base_url` | `CONDUIT_BASE_URL` is set |

### Flow-aware probes (run only with a flow argument)

| Probe name | What it checks |
|------------|---------------|
| `flow-prereqs-present` | Every package in `flow.prerequisites` is present on PATH; reports all missing packages, not just the first |
| `model-endpoint-reachable` | HTTP HEAD/GET to `${CONDUIT_BASE_URL}/models` succeeds; FAIL names the configured endpoint |

### Example: passing run (no flow arg)

```
$ docker run --rm -v conduit_data:/data \
    -e CONDUIT_API_KEY="$CONDUIT_API_KEY" \
    -e CONDUIT_BASE_URL="$CONDUIT_BASE_URL" \
    conduit-engine doctor

  state_db_volume: ok
  project-root-present: ok
  model_api_key: ok
  gateway_base_url: ok — CONDUIT_BASE_URL is configured
```

### Example: passing run (with flow arg)

```
$ docker run --rm -v conduit_data:/data \
    -e CONDUIT_API_KEY="$CONDUIT_API_KEY" \
    -e CONDUIT_BASE_URL="$CONDUIT_BASE_URL" \
    branching-flow doctor /flow/flow.yaml

  state_db_volume: ok
  project-root-present: ok
  model_api_key: ok
  gateway_base_url: ok — CONDUIT_BASE_URL is configured
  flow-prereqs-present: ok
  model-endpoint-reachable: ok
```

### Diagnosing failures

A FAIL line includes a remedy hint:

```
  state_db_volume: FAIL — state directory is writable but is not a mounted volume — mount a Docker volume at /data
  model_api_key: FAIL — CONDUIT_API_KEY / OPENAI_API_KEY not set
  gateway_base_url: FAIL — CONDUIT_BASE_URL not set (required for model gateway)
  model-endpoint-reachable: FAIL — model endpoint unreachable: http://model:11434/v1
```

`conduit run` and `conduit listen` both invoke `doctor` as a pre-flight gate.
If any probe fails, the command aborts before dispatch or boot.

---

## 7. Bump the Bun version

The single authoritative Bun pin is the `FROM` line in the engine Dockerfile:

```dockerfile
# Dockerfile (repo root), line 1
FROM oven/bun:1.3.11-slim
```

To upgrade:

1. Edit `Dockerfile` line 1 to the desired `oven/bun:<version>-slim` tag.
2. Rebuild the engine image:
   ```bash
   docker build -t conduit-engine .
   ```
3. Re-run the test suite:
   ```bash
   bun run test
   ```
   (`bun run test` scopes discovery to `src/`; add `bun run test:blackbox` to also
   run the black-box suite against the shipped binary — see `blackbox/README.md`.)
4. Rebuild any per-flow images — they inherit `FROM conduit-engine:latest`, so
   a new engine image is picked up automatically on the next per-flow build.

There is no other Bun version pin in the repository. Do not update
`package.json` `engines` or any other file independently — the Dockerfile is
the single source of truth.

---

## 8. End-to-end walkthrough

Complete copy-pasteable walkthrough from a clean machine using the branching
example flow.

### Prerequisites

- Docker installed and running
- An OpenAI-compatible model endpoint accessible as `CONDUIT_BASE_URL`
- `CONDUIT_API_KEY` set to your API key

```bash
export CONDUIT_API_KEY="sk-..."
export CONDUIT_BASE_URL="https://api.openai.com/v1"
```

### Step 1 — Clone and build the engine image

```bash
git clone https://github.com/theaiteam-dev/conduit
cd conduit

docker build -t conduit-engine .
```

### Step 2 — Generate the per-flow Dockerfile with `conduit build`

```bash
# Generate the per-flow Dockerfile to stdout and save it.
docker run --rm conduit-engine build examples/branching/flow.yaml > Dockerfile.branching

# Inspect the generated file.
cat Dockerfile.branching
# FROM conduit-engine:latest
# ...
# COPY --chown=conduit:conduit ["branching/", "/flow/"]
#
# # Install flow-declared prerequisites (from flow.yaml prerequisites field).
# RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
#     jq \
#     && rm -rf /var/lib/apt/lists/*
```

Build the per-flow image (build context is `examples/` so the `branching/`
subdirectory is included):

```bash
docker build -t branching-flow -f Dockerfile.branching examples/
```

Optional: publish the per-flow image to your registry for deployment from
another host or compose environment:

```bash
docker tag branching-flow ghcr.io/my-org/branching-flow:0.1.0
docker push ghcr.io/my-org/branching-flow:0.1.0
```

### Step 3 — Create the state volume

```bash
docker volume create conduit_data
```

### Step 4 — Verify the environment with `conduit doctor`

```bash
docker run --rm \
  -v conduit_data:/data \
  -e CONDUIT_API_KEY \
  -e CONDUIT_BASE_URL \
  branching-flow doctor /flow/flow.yaml
```

All probes must report `ok` before running the flow. Fix any `FAIL` lines
using the remedy hints in the output (see [§6](#6-verify-with-conduit-doctor)).

### Step 5 — Run the flow with `conduit run`

```bash
docker run --rm \
  -v conduit_data:/data \
  -e CONDUIT_API_KEY \
  -e CONDUIT_BASE_URL \
  branching-flow run /flow/flow.yaml
```

The engine drives the flow to a terminal lane (`done`, `scrap`, or `hold`).
It exits 0 when the work reached `done` and 1 when it did not. State is
persisted in `conduit_data:/data/conduit.sqlite` and survives container
restarts.

Exit 1 is not always a failure. A run that stopped because every unfinished
card is waiting out a provider rate limit is recorded as **parked**: nothing was
scrapped, and the run stays resumable. It writes the gate time and the exact
command that continues it to **stderr** (so a pipeline that captures only
stdout will not see it):

```text
run "job-1" parked behind a provider rate limit until 2026-01-01T00:10:00.000Z
  — nothing was scrapped; resume with: conduit resume /flow/flow.yaml --run job-1
```

Run that command once the gate has passed to pick the run up where it stopped:

```bash
docker run --rm \
  -v conduit_data:/data \
  -e CONDUIT_API_KEY \
  -e CONDUIT_BASE_URL \
  branching-flow resume /flow/flow.yaml --run job-1
```

`conduit resume` exits 0 whenever it drove the engine at all, including when
the run halts or parks again — non-zero is reserved for the reasons it could
not start (an unknown run, a lease conflict, an unreadable flow). Check the
run's own state with `conduit run status --run job-1` rather than the resume
exit code.

Under the operator stack the listener does this for you — it recognises a
parked child, tells the channel once, and resumes the run itself when the gate
passes (see [`ingress-listener.md`](./ingress-listener.md)).

A cap that never clears does not park forever: after enough consecutive parks
with no progress the card is hard-paused to the `hold` lane, so the run stops
being auto-resumed and surfaces as a halt for a human to look at.

### Alternative: run from the engine image during development

```bash
docker run --rm \
  -v conduit_data:/data \
  -v "$PWD":/app \
  -e CONDUIT_API_KEY \
  -e CONDUIT_BASE_URL \
  conduit-engine run examples/branching/flow.yaml
```

The per-flow image is the operator path because it installs `flow.prerequisites`
and bakes the flow into `/flow/`. The engine-image pattern is useful while
iterating, but the mounted project must already have any system prerequisites
available inside the container.

---

## Environment variables reference

| Variable | Required | Description |
|----------|----------|-------------|
| `CONDUIT_API_KEY` | Yes (or `OPENAI_API_KEY`) | API key for the model gateway |
| `OPENAI_API_KEY` | Yes (or `CONDUIT_API_KEY`) | Alternative API key name |
| `CONDUIT_BASE_URL` | Yes | Base URL of the model gateway (e.g. `https://api.openai.com/v1` or `http://model:11434`) |
| `CONDUIT_VISION_UNSUPPORTED_ERROR_CODES` | No | Comma-separated local gateway error codes/types that should be treated as unsupported image input for multimodal calls; see [`local-openai-vlms.md`](./local-openai-vlms.md) |
| `CONDUIT_SLACK_SIGNING_SECRET` | Only for Slack ingress | Slack signing secret for webhook verification |
| `CONDUIT_STATE_DB` | No | Override the default state DB path (`/data/conduit.sqlite`) |
| `HTTPS_PROXY` / `HTTP_PROXY` | No | Route outbound traffic through an egress proxy. Honoured by model/API calls **and** by the Slack Socket Mode connection. |
| `NO_PROXY` | No | Comma-separated hosts to reach directly. Matches an exact host, a domain suffix (`.slack.com`), or `*` for everything; an entry may pin a port (`slack.com:443`). |

### Egress proxies

Setting `HTTPS_PROXY` routes both the engine's HTTP calls and the Socket Mode
websocket through the proxy, so a deployment can run a default-deny egress
policy with a domain allowlist (deployment-hardening rule 9) without the
listener needing a direct hole punched for it.

`wss:`/`https:` targets prefer `HTTPS_PROXY` and fall back to `HTTP_PROXY`;
`ws:`/`http:` use `HTTP_PROXY` only. Lowercase variable names take precedence
over uppercase, matching curl. A malformed or non-`http(s)` setting (e.g.
`socks5://`, which the websocket transport cannot use) is ignored and the
connection is made directly, rather than failing the listener at startup.
