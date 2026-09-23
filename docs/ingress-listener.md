# Ingress Listener

The ingress listener is a long-lived process that accepts external events (webhook POST requests, Slack messages) and spawns deterministic conduit runs. It is separate from the per-run kernel—no model calls, no tick loop, just reception and forwarding.

## Overview

### What it solves

Conduit flows run in response to external triggers (a GitHub webhook, a Slack message, a scheduled event from a queue). The listener:

1. **Receives events** from multiple sources (webhook, Slack)
2. **Authenticates them** (signature verification, shared secret)
3. **Deduplicates them** exactly-once—a retry of the same event never spawns two runs
4. **Recovers from crashes** between acceptance and spawn—an unspawned event is not lost
5. **Bounds recovery** by attempt cap—no unbounded retry loops
6. **Fails loudly at boot**—invalid flow config, route collisions, missing bindings abort startup

### Key guarantees

- **Deterministic dedup**: The same event_id always produces the same run (or none if duplicate)
- **Bounded recovery**: Crashed-but-unspawned events are recovered on restart, capped by attempt limit
- **Fail-closed auth**: Invalid signature/secret rejected before any database write
- **No model calls at rest**: Zero cost while idle
- **One spawned run per distinct event**: No accidental double-billing

## Architecture

```
External Event (webhook/Slack)
    ↓
[Adapter: verify auth, resolve route]
    ↓
[Core accept-spawn path]
  - Atomic: acceptIngressEvent (spawn_state='accepted')
  - Derive: event_id (per binding rules)
  - Build: substrate envelope (deterministic, secret-filtered)
  - Launch: conduit run via Bun.spawn
  - Mark: spawn_state='spawned' (launched) or 'failed'
  - Log: outcome to ingress_log
  - Ack: respond to the caller HERE — the run is still executing
    ↓
Conduit run launched OR logged as rejected/duplicate
    ↓
[Child exit, asynchronously]
  - Non-zero: spawn_state='failed' + alert + 'spawn_failed' log entry — unless
    the run PARKED behind a provider rate limit (`runs.outcome='parked'`, exit
    code 1): then the row stays `'spawned'`, a `'parked'` log entry is written,
    and the listener resumes the run once its gate passes (see Re-drive logic)
  - Clean: nothing further; the row stays 'spawned'
  - Either way: release the run slot
```

### Ack on accept

The webhook response is produced as soon as the event is accepted and its
`conduit run` child is **launched** — not when the run finishes. A response is:

| Outcome | Status | Body |
| --- | --- | --- |
| `accepted` — a run is now executing | `202` | `{"outcome":"accepted","run_id":"…"}` |
| `queued` — accepted, waiting on a free run slot | `202` | `{"outcome":"queued","run_id":"…"}` |
| `duplicate` — already handled | `200` | `{"outcome":"duplicate"}` |
| `spawn_failed` — the launch failed; the sweep owns the retry | `200` | `{"outcome":"spawn_failed"}` |

Before this, the response waited on the child's **exit**, so a multi-hour render
held the HTTP socket open for hours: every real client timed out and marked
accepted work as failed. Generic providers (GitHub, Stripe) time out after
~10–30s, so they would re-deliver or mark the delivery failed as well.

### The listener process

```
[Boot]
  1. Load flow allowlist (explicit, not auto-discovery)
  2. Read channels.ingress from each flow
  3. Validate all bindings (malformed?, route collision?)
  4. Resolve alert channels per flow (flow.egress || listener-global)
  5. Re-drive recoverable events (bounded by cap)
  6. Start HTTP server (webhook adapter)
  7. Register Slack event handler
    ↓
[Idle]
  - Accept events
  - Authenticate
  - Spawn runs
  - Log outcomes
    ↓
[Crash → Restart]
  - Re-drive unspawned events (bounded)
  - Resume from last clean state
```

## Configuration

### Flow allowlist

Explicit, not auto-discovery. Only flows named in the allowlist are triggerable:

```bash
conduit listen --flows flow1.yaml,flow2.yaml \
  --global-alert-channel #conduit-alerts
```

Or via config file:

```json
{
  "flows": ["flow1.yaml", "flow2.yaml"],
  "globalAlertChannel": "#conduit-alerts"
}
```

### Run backpressure (`max_concurrent_runs`)

By default the listener spawns a `conduit run` for every accepted event immediately — N
simultaneous events means N concurrent runs. Against a serial model endpoint that is a
thundering herd: every transform queues behind every other, hits its station timeout, and
the burst grinds into redrive-cap exhaustion.

`max_concurrent_runs` caps how many spawned runs are in flight at once, listener-wide
(across all hosted flows — the model box is the shared bottleneck):

```bash
conduit listen --manifest engine.yaml --max-concurrent-runs 1
```

or in the engine manifest:

```yaml
flows:
  pic-edit: ./flows/pic-edit/flow.yaml
max_concurrent_runs: 1
```

Precedence: `--max-concurrent-runs` flag > manifest `max_concurrent_runs` >
`CONDUIT_MAX_CONCURRENT_RUNS` env. Omitted → unlimited (the original behavior).

Events beyond the cap are still accepted and recorded (`ingress_events` row in
`'accepted'`, ingress_log outcome `'queued'`) — nothing is dropped, and the provider gets
its ack. Queued events launch **in arrival order** as slots free: a completed run kicks
the re-drive sweep immediately, so a `max_concurrent_runs: 1` burst of 8 photos processes
serially back-to-back with no interval wait. Because queued rows are ordinary `'accepted'`
rows, a listener crash loses nothing — boot re-drive picks the queue back up.

Queueing is not a spawn attempt: a queued event's `spawn_attempts` is untouched until it
actually launches, so the redrive cap still bounds real launches only.

**HITL resumes participate opportunistically — and deliberately bypass the cap under
saturation.** A reply-triggered `conduit resume` claims a run slot when one is
free, so accounting stays honest in the common case. When every slot is busy it proceeds
anyway: a pick resumes work that was already admitted once, arrives at human latency, and
a human's selection queueing behind a photo backlog would read as a broken reply loop. So
under saturation the true process count can exceed `max_concurrent_runs` by the number of
in-flight resumes. A second concurrent resume for the *same* run is suppressed (logged
`'duplicate'`) — the double-driver guard; the run lease remains the backstop for resumes
that bypassed the gate.

### Per-flow binding declaration

In `flow.yaml`, declare `channels.ingress`:

```yaml
channels:
  ingress:
    - type: webhook
      route: /webhook/github
      auth:
        shared_secret: env:GITHUB_WEBHOOK_SECRET
      event_id:
        from: header
        name: X-GitHub-Delivery

    - type: slack
      event_id:
        from: json_path
        path: $.event_id

    # Slack over Socket Mode — no public HTTPS endpoint required.
    # The listener opens an OUTBOUND wss connection; auth is the app-level
    # token, so no `auth` block (request signing never runs on this transport).
    - type: slack
      transport: socket            # 'events' (default, webhook) | 'socket'
      channel: C-STUDIO
      app_token_env: SLACK_APP_TOKEN   # xapp-…, scope connections:write
      event_id:
        from: json_path
        path: $.event_id

  egress: []  # optional; if omitted, uses global alert channel

```

### Event ID derivation

How does the listener derive a stable `event_id` (used for dedup)?

| Source | Mode | Example |
|--------|------|---------|
| **header** | Named HTTP header | `X-Idempotency-Key: abc123` → event_id = `abc123` |
| **json_path** | Path in JSON body | body `{"event_id": "evt_789"}` → event_id = `evt_789` |
| **content_hash** | SHA256 of body | deterministic hash of request body |
| **require** | Strict mode | Event rejected if ID cannot be derived |

### Smart defaults

- **Slack**: Defaults to `body.event_id` (Slack's native event ID)
- **Webhook** (no explicit source): Probes well-known headers in order:
  1. `X-Conduit-Delivery-Id`
  2. `Idempotency-Key`
  3. `X-GitHub-Delivery`
  4. `X-Shopify-Webhook-Id`
  5. `X-Request-Id`
  6. `body.id` (Stripe-style)
  7. **Fallback**: Content-hash with one-time degraded-dedup warning

### Auth

#### Webhook (mandatory shared secret)

```yaml
auth:
  shared_secret: env:WEBHOOK_SECRET  # or literal: abc123
```

Verification: HMAC request body signature (algorithm depends on provider; webhook.ts verifies the request matches expected signature).

#### Slack (request signing)

```yaml
auth:
  signing_secret: env:SLACK_SIGNING_SECRET
```

Verification: HMAC-SHA256 of `timestamp + ':' + raw_body`, compared against `X-Slack-Request-Timestamp` and `X-Slack-Signature`. Replay window: ±5 minutes (configurable).

#### Authentication failures

- **Missing/invalid signature**: Reject with 401, log `rejected_auth`, **never** spawn or accept
- **Fail-closed**: The request is denied before any database write

#### Slack Socket Mode (`transport: socket`)

No per-request verification exists on this transport — the outbound wss
connection itself is the auth, established by `apps.connections.open` with the
app-level token named in `app_token_env`. Guard that token accordingly.

Boot is fail-loud, not fail-closed-at-runtime (a socket client has no request
to reject — an unset token would just retry-loop `invalid_auth` forever):

- `MISSING_APP_TOKEN` — the binding declares `transport: socket` without `app_token_env` (validation)
- `MISSING_APP_TOKEN_SECRET` — `app_token_env` names an env var that is not set (listener boot)
- `SOCKET_SEAM_UNAVAILABLE` — a socket flow is configured in an environment with no Socket Mode I/O

Everything after delivery is shared with the webhook transport: the same
channel→flow routing, event-id derivation, dedup ledger, and accept/spawn path.
Envelopes are acked (`envelope_id` echoed) *before* processing — the Socket
Mode equivalent of the fast HTTP 2xx; unacked envelopes are redelivered by
Slack and absorbed by dedup. Connection lifecycle: Slack recycles connections
(~hourly, `disconnect: refresh_requested`) — the replacement is opened before
the old socket closes; unexpected drops reconnect with 1s-doubling backoff
(30s cap); `disconnect: link_disabled` (app disabled) is terminal and logged,
never retried. Flows sharing one app token share one connection.

### Substrate mapping (optional)

Optional JSON-path projection—transform the event into a custom substrate shape:

```yaml
event_id:
  from: header
  name: X-Delivery-Id
substrate:
  message: $.text
  user_id: $.user.id
  timestamp: $.ts
```

Result: The spawned run's substrate carries only the fields named in the mapping (extracted via JSON path from the event).

Numeric path segments index into arrays, which is how array-shaped webhook
payloads (Slack `files[]`, GitHub `commits[]`, Stripe `lines.data[]`) are
projected:

```yaml
substrate:
  file_url: $.body.event.files.0.url_private_download
  thread_ts: $.body.event.ts
```

Indices must be canonical non-negative integers (`0`, `1`, `12` — no leading
zeros). An out-of-range index, like any other unresolvable path, yields the
field present with value `null`. The same dialect applies to
`event_id: { from: json_path }` paths — but note those resolve against the
request **body** directly, not the envelope, so drop the `$.body` prefix
(`$.event.files.0.id`, not `$.body.event.files.0.id`).

## The durability ledger

### ingress_events table

Records every event ever seen. Columns:

- `event_id` (TEXT, PRIMARY KEY): Stable event identifier
- `received_at` (INTEGER): Timestamp of arrival
- `spawn_state` (TEXT): One of `accepted | spawned | failed`
- `spawn_attempts` (INTEGER): Number of spawn attempts (bounded by cap)

`spawned` means **launched**, not "ran to a clean exit." A run that
dies after launch moves the row back to `failed` from the exit watcher, so the
bounded re-drive still picks it up; a run that is still executing sits in
`spawned` with its run slot held. A run that **parks** behind a provider rate
limit (its own row says `status='halted', outcome='parked'`) also exits
non-zero but is not a failure: it stays `spawned` and is **resumed**, never
re-driven — a park is a run-level fact on the `runs` row, not a launch state.

### State machine transitions

```
         Initial receive
              ↓
      acceptIngressEvent
              ↓
       spawn_state='accepted'
         spawn_attempts=0
              ↓
    [Spawn attempt] (incrementSpawnAttempts → 1)
        /        \
   Launched    Launch failed
      ↓            ↓
   'spawned'     'failed'
      ↓            ↓
 [child exits] (try again, bounded)
   /      \
 code 0   code≠0 → 'failed' (+ alert, bounded re-drive)
   ↓          ↳ run parked (runs.outcome='parked') → stays 'spawned'
 (done)         (+ 'parked' log entry, one informational alert,
                 `conduit resume` from the sweep once the gate passes)
```

### Re-drive logic

On listener startup, `redriveOnBoot`:

1. List all rows where `spawn_state IN ('accepted', 'failed')` AND `spawn_attempts < CAP`
2. For each row:
   - Increment `spawn_attempts` (before the spawn)
   - Re-invoke the core spawn path
   - Mark `'spawned'` on success, or `'failed'` on failure
   - On failure, fire the alert seam — same payload and channel resolution as
     the hot path (the flow's first egress target, else the listener-global
     channel); a pre-v9 row with no flow attribution alerts on the global
     channel as `flow=unknown`
   - Log outcome as `'redriven'` to ingress_log

   A re-driven child that later exits non-zero gets the same treatment from its
   exit watcher: `'failed'` + alert (`re-driven run exited with code N`) +
   a `'spawn_failed'` log entry. Alerting is best effort and fire-and-forget —
   the alert is started but never awaited, so neither a throwing alert seam nor
   one whose promise never settles blocks the mark, the log entry, or the
   run-slot release.

3. Rows at-cap or with permanent failures are **not** re-driven. A permanent
   failure is the loudest case: the row is excluded from every future sweep, so
   its alert is the only thing that will surface the event again.

**Parked runs are resumed, not re-driven.** A run that halted behind a provider
rate limit exits 1, but its `runs` row reads `status='halted', outcome='parked'`
and its cards are `ready` behind a future `release_at`. Both exit watchers
check that (the runs row, confirmed against the cards) before marking failed:
a parked run keeps its row `spawned`, writes a `'parked'` log entry naming the
run id and the gate (ISO-8601 UTC), and fires one informational alert per
event (the first park only — a long reset is chunked into several park cycles
and the channel should not hear each one; a cap that never clears ends loudly
instead, see below). It is never re-driven — `conduit run --run-id <existing>` only prints
the run's state — so `spawn_attempts` is untouched.

After the boot re-drive, and again after every periodic sweep (the same
schedule and the same slot-release kick — no second timer), the listener
resumes every parked ingress run whose gate has passed with
`conduit resume <flow.yaml> --run <runId>`, soonest gate first. Resumes draw
from the run-slot pool **without** the HITL bypass: a rate limit is exactly
the wrong moment to let N parked runs stampede a saturated model box, so the
first run to find no free slot ends the sweep and the rest wait for the kick.
A resume already in flight for the same run is suppressed (`'duplicate'`).
When the resumed child exits the run is re-inspected: complete → an
`'accepted'` entry; parked again (the per-park cap chunks a long reset into
several cycles) → another `'parked'` entry, no new alert, and the next due
sweep picks it up; neither → **now** it is a failure: `'failed'` + alert +
`'spawn_failed'`, exactly as a dead re-driven child — and the parked sweep then
leaves it to the attempt-capped re-drive sweep, since only `spawned` events are
resume candidates. That hand-off bounds the loop: a resume that fails before it
drives the run (lease conflict, preflight, missing flow file) leaves the run
parked with its gate in the past, and re-resuming it every tick would never
end; instead the re-drive sweep's `conduit run --run-id` no-op flips the row
back to `spawned` while counting an attempt, so the cycle is bounded by the
cap. A parked run with no ingress attribution (a CLI-triggered run) is not the
listener's to resume.

A resume is also skipped for any event whose launch is already in flight in
this process: the boot re-drive resolves on **launch** and marks the row
`spawned` before the parked sweep runs, so a row it just re-drove is otherwise
immediately a resume candidate and the two drivers race for the run lease.
The re-drive sweep guards the same way, but registers its slot under the event
id while a resume registers under `parked-resume:<runId>`, so the parked sweep
checks `inFlight(eventId)` itself.

**The park loop terminates.** A park spends no execution attempt, so none of
the four rework guards bounds it, and this sweep would otherwise resume a
permanently-capped run forever on one alert. The kernel bounds it instead: a
card that parks `MAX_CONSECUTIVE_RATE_LIMIT_PARKS` times in a row without
making progress hard-pauses to `hold`. A held card is not parked, so the run
drops off this sweep and its next exit takes the ordinary failure path —
`'failed'` + alert + `'spawn_failed'`.

### Attempt cap semantics

Default cap: 3 attempts (configurable).

- Attempt 1: Initial spawn (WI-406)
- Attempt 2–3: Re-drives on restart (WI-407)
- Attempt 4+: At-cap, not re-driven (surfaced as alert)

A row at `spawn_attempts=2` is re-drivable once; after re-drive it becomes `spawn_attempts=3` and is no longer re-drivable.

## Observability

### ingress_log table

Append-only journal of all event outcomes. Columns:

- `id` (INTEGER, PRIMARY KEY AUTOINCREMENT)
- `source` (TEXT): `'webhook' | 'slack' | ...`
- `event_id` (TEXT, nullable)
- `outcome` (TEXT): One of:
  - `'accepted'` — Event accepted, spawned run
  - `'duplicate'` — Same event_id already processed
  - `'rejected_auth'` — Auth failed
  - `'rejected_unknown_flow'` — Route does not match any flow
  - `'rejected_malformed'` — Body unparseable
  - `'spawn_failed'` — Spawn succeeded, conduit run had an error
  - `'redriven'` — Re-drive attempt on restart
  - `'parked'` — The launched run halted parked behind a provider rate limit (`runs.outcome='parked'`); the row stays `spawned` and the sweep resumes it once its gate passes. Written on every park; the reason names the run id and the gate as ISO-8601 UTC
  - `'queued'` — Accepted, but all run slots busy (`max_concurrent_runs`); launches via the re-drive sweep as slots free
- `reason` (TEXT, nullable): Human-readable reason
- `attributes_json` (TEXT, nullable): Event attributes (secret-filtered)

Secret filtering: Keys like `authorization`, `*_token` are dropped before storage.

### Alerts

When an event cannot be spawned, an alert is sent:

```
Flow: order-processing
Event ID: evt_abc123
Outcome: spawn_failed
Reason: Conduit run exited with non-zero status
```

Alert destination: Per-flow `channels.egress[0].target` (if declared), else listener-global fallback channel.

A **parked** run sends one informational alert per park sequence — the reason
starts with `parked` and names the run id and the gate time — so the channel
learns the run is waiting and will resume on its own. It is keyed on the
event's existing `'parked'` log entries, so a run that re-parks across several
resume/park cycles does not repeat it, and it never marks the event failed. A
resume that ends with the run neither complete nor parked alerts as a failure.

## Examples

### GitHub webhook → conduit run

1. GitHub sends POST to `/webhook/github` with X-GitHub-Delivery header
2. Listener verifies HMAC signature
3. Event ID derived from X-GitHub-Delivery
4. Substrate: GitHub event JSON (secret-filtered)
5. Conduit run spawned with substrate as input
6. Run processes the GitHub event deterministically

### Slack message → conduit run

1. Slack sends POST event to listener
2. Listener verifies HMAC-SHA256 signature
3. Event ID derived from event_id field
4. Substrate: Message text + attachments (secret-filtered)
5. Listener acks Slack within 3s
6. Conduit run spawned asynchronously
7. Slack retries (same event_id) are deduped (logged, not spawned)

### Crash recovery

1. Listener accepted event, marked `spawn_state='accepted'`, was about to spawn
2. Listener crashes
3. Event remains in DB with `spawn_state='accepted'`, `spawn_attempts=0`
4. Listener restarts
5. `redriveOnBoot` finds the row, increments attempts to 1, re-spawns
6. Run now launches; row marked `'spawned'`
7. No double-billing, no lost events

**What ingress re-drive does not cover:** a listener that dies
*after* a child was launched leaves the row `'spawned'`, so the sweep will not
re-drive it. Recovery from that point belongs to the run's own resume/journal
machinery (`conduit resume --run <id>`), which is the only layer that knows how
far the run got. This was already effectively true — a re-driven completed run
deduplicates at the run layer — but it is now explicit in the state.

## Testing

The ingress listener is tested at three levels:

1. **Unit tests** (per module):
   - `binding.test.ts`: Route collision, malformed bindings
   - `event-id.test.ts`: Header/json_path/content_hash derivation
   - `envelope.test.ts`: Deterministic envelope building, secret filtering
   - `spawn.test.ts`: Accept-dedup-spawn orchestration
   - `recovery.test.ts`: Bounded re-drive, attempt caps
   - `webhook.test.ts`: Signature verification, fail-closed auth
   - `slack-events.test.ts`: Request signing, 3s ack window

2. **Integration tests** (adapters + listener):
   - Boot validation (route collisions, missing flows)
   - Full webhook→spawn path
   - Full Slack→spawn path
   - Concurrent dedup (multiple identical events)

3. **Crash-recovery tests** (crash oracle):
   - Crash mid-accept (before spawn): event recovered on restart
   - Crash mid-spawn: attempt counting correct
   - Crash mid-re-drive: bounded by cap

## Migration notes

### From manual CLI to ingress listener

Before step 8.2, flows were triggered via CLI:

```bash
conduit run --flow flow.yaml --input-file event.json
```

With step 8.2, flows can be triggered by external events:

```bash
conduit listen --flows flow.yaml --global-alert-channel '#alerts'
```

The listener is **optional**—CLI seeding still works. A flow can be triggered either way.

### Database migrations

The ingress listener adds:

- `ingress_events` table (new columns: `spawn_state`, `spawn_attempts`)
- `ingress_log` table (new, journal connection)

Migrations are idempotent and additive:

- Shipped v3 databases are migrated v3→v4 on first open
- Fresh databases (v0) create both tables at init
- Existing databases retain all prior state (no destructive ALTER)

## Performance

- **Boot time**: O(flows * bindings) — validation + re-drive snapshot
- **Per-event**: O(1) atomic accept, O(log n) dedup lookup, ~10ms spawn invocation
- **Memory**: Minimal — no cached flow state, single DB connection
- **Disk I/O**: Write-ahead logging (WAL mode) for atomic accept + spawn_state transitions

## Limitations and future work

- **No async retry queue**: Permanent failures surface as alerts but don't auto-retry indefinitely (design: bounded cap prevents runaway)
- **No flow hot-reload**: Listener must restart to pick up flow changes (design: explicit allowlist prevents surprise triggering)
- **No rate limiting**: Event arrival rate is unbounded (future: add per-flow or per-route rate limits). Run *concurrency* is bounded via `max_concurrent_runs`; per-endpoint model-call queueing across runs and richer burst UX ("N queued, #k processing") remain future work
- **No event transformation**: Substrate is raw event + optional JSON-path projection (future: add full transformation stations if needed)

## See also

- **WI-401**: Ingress event durability and spawn state machine
- **WI-402**: Ingress binding schema and boot validation
- **WI-403**: Deterministic substrate envelope
- **WI-404**: Append-only ingress_log observability
- **WI-405**: Stable event_id derivation
- **WI-406**: Core accept-dedup-spawn path
- **WI-407**: Bounded boot re-drive and escalation
- **WI-408**: Webhook adapter with signature auth
- **WI-409**: Slack-events adapter with request signing
- **WI-410**: Listener process assembly and validation
- **WI-411**: Secret-filter export for ingress
- **WI-412**: 'conduit listen' CLI command
