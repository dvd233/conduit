# Conduit — The A(i)-Team Engine

**A deterministic, model-independent flow-shop for LLM labor.**

> **Rev 2.** Incorporates the adversarial spec review (state machine, replay
> soundness, fan-in, liveness, claim atomicity, tool-bridge envelope, injection
> threat model) and is modeled on battle-tested patterns from the **A(i)-Team
> reference implementation**: the stage transition matrix, the deterministic
> tick/action-plan controller, dependency waves with cycle detection, the atomic
> instance pool, generation-keyed idempotency, and the enforcement-hook "Law."
> Where this spec says *"(A(i)-Team: …)"* the pattern is proven in production,
> not theoretical.

Conduit runs *flows* for knowledge work. A flow ingests a raw substrate
and excretes a finished product: a PRD becomes tested code; an idea becomes
campaign images; raw footage becomes a polished video. The structure underneath
all three is identical — which is the whole point.

> **The metabolism framing.** Every flow is a digestive tract: stations transform
> the work as it flows; quality gates keep defects out; a deterministic conveyor
> moves pieces between stations. The intelligence is *labor at the stations*, not a
> brain in the loop. Conduit is the anatomy; a flow is a metabolism configured on it.

---

## 0. Why this exists (the constraints that shaped it)

- **Cost.** A reference flow moved from a frontier-agent harness to a deterministic,
  cheap-model state machine and dropped roughly an order of magnitude in cost. (See
  §12 for the honest, apples-to-apples accounting — the headline figure is a
  *clean-path single run*, not the expected cost with rework.)
- **Model independence.** Frontier-only is a dead end for high-volume work. Conduit
  drives *any* model — frontier where judgment lives, cheap/local where it doesn't —
  chosen **per station**.
- **No host lock-in.** Conduit is **not** hosted on Claude Code or any single agent
  harness (CLI usage now bills as API; plans don't cover it; plugin formats don't
  port across harnesses). Conduit is the portable layer *above* model providers.
- **Determinism over vibes.** A long-lived LLM driving an orchestration loop is a
  liability — one such loop ran away for ~40 hours without converging. The flow is
  run by a deterministic kernel; LLMs are workers, never the conveyor.

---

## 1. Overview

Conduit pairs a **Deterministic Kernel** (Bun/SQLite) with LLM **labor** at stations.
The kernel owns all control flow — routing, WIP, transitions, recovery, budgets. The
LLMs only do work and judge work. This is the lean-manufacturing split applied to
agentic systems: the *flow* is deterministic; the *hands* are not.

### The Power Trinity

- **The Reasoning** — subagents (planners, workers, critics) that produce and judge.
- **The Muscle** — restricted tool-use (Read/Write/Bash) over a shared filesystem.
- **The Law** — the Kernel + Hooks that enforce state transitions, permissions,
  budgets, and integrity. The Law is *load-bearing*: it is the only guardrail, because
  Conduit runs its own runtime with no provider safety net behind it.
  *(A(i)-Team: the `block-*` enforcement hooks once being disabled let an agent
  `rm -rf` a directory — the Law is not optional.)*

### Runtime — why Bun (the kernel's foundation)

Bun is a **deliberate, load-bearing choice**, not an implementation detail: it is the
single binary that provides the whole foundational layer of The Law. Node adds
architectural friction (heavy `child_process`, native-binding compiles, separate build
tooling); Go/Rust slow prompt iteration. Bun hits the sweet spot for four reasons:

1. **Process spawning (`Bun.spawn`).** Flows lean on The Muscle — external CLIs (`git`,
   `gh`, `ffmpeg`) and worker harnesses. `Bun.spawn` sits on native OS calls (vfork),
   spawning subprocesses almost instantly and memory-efficiently with low-latency live
   stdout/stderr piping to the War Room. This is what makes dozens of concurrent lanes
   cheap to run.
2. **Native, zero-config IPC.** Bun-spawned worker harnesses talk to the Kernel directly
   via `process.send()` (optimized binary serialization) for `START_WORK`/`MARK_DONE`
   and peer handoffs — no sockets, no broker, none of the "proxy every message" tax that
   an LLM-in-the-loop orchestrator imposed.
3. **`bun:sqlite` baked into the binary.** The Journal of Truth is queried on every tick,
   transition, and heartbeat. Bun's in-process, synchronous SQLite (no native-binding
   compile, faster than `better-sqlite3`) gives the kernel a near-zero-overhead store.
4. **Native TypeScript + fast file I/O.** Hooks/adapters run as `.ts` with no compile
   step (instant Work-Bench iteration); `Bun.write`/`Bun.file` use native OS fast paths
   for the shared PROJECT_ROOT.

The payoff: the entire foundational layer — process control, IPC, state store, file I/O,
TS execution — lives in **one fast binary**, with no external DB driver, build tool, or
message broker.

**Two boundaries keep "Bun at the heart" from leaking:**

- **Bun runs the kernel and the worker *harness* — it does not constrain the *models or
  tools*.** A worker is a Bun subprocess (native IPC to the kernel), but inside it calls
  whatever it needs: a local model over HTTP, a Python tool, `ffmpeg`. Bun-native IPC is
  for kernel↔harness; external tools communicate over stdio/exit-code through the
  Tool-Bridge (§7). This is exactly what preserves **model independence** — Bun is the
  conveyor, not the labor.
- **`bun:sqlite` is fast, but SQLite is still single-writer.** "Query on every tick"
  scales on one host *because* §11 splits the transactional state DB from the hot
  append-only journal and uses WAL + `busy_timeout`. Bun's speed reduces lock-hold time;
  it does not remove the single-writer constraint (rev-1 M1). Multi-host scale needs a
  different store (§17 F1).

### Lean glossary (term → component)

| Lean / shop-floor term | Conduit component |
|---|---|
| Production line / routing | a **flow** defined in `flow.yaml` (a "workflow") |
| Station / work-cell | a **lane** with a worker and optional QC |
| Kanban card | a **card** (epic = parent, task = child) |
| WIP limit | per-station concurrency cap (never a global sum) |
| Pull system | a station pulls work only when it has capacity |
| Quality gate (QC) | a **check** station (LLM critic) with a back-edge |
| Rework loop | reject-to-an-earlier-station, bounded |
| Scrap bin | the `scrap` terminal lane (A(i)-Team: `blocked`) |
| Andon cord | the global run budget + liveness watchdog → halt + alert |
| Poka-yoke (mistake-proofing) | Hooks, schema validation, coercive parsing, path ownership |
| Jidoka (stop-the-flow) | forward-only across waves + hard-pause on defect; escalate, don't guess |
| Heijunka (load leveling) | global token bucket / rate limiting across lanes |
| Just-in-time | materialize expensive artifacts only on upstream approval |
| Standard work | a crystallized **skill** (a named, reusable unit) |
| Kaizen | the continuous-improvement loop (mutations) |
| Genba (the floor) | the **War Room** — live stream of the journal |

### Validated instances (same engine, three configs)

| Flow | Eats | Excretes | Notes |
|---|---|---|---|
| **A(i)-Team** | a PRD | tested code | multi-piece WIP; heavy checks; hard scrap cap |
| **Studio** (ad factory) | an idea | campaign assets + Meta CSV | batch fan-out; light checks; market-graded |
| **Autocut** | raw video | polished YouTube video | single-piece flow; medium checks; plan-first QC |

Built independently, they converged on the same kernel. Conduit extracts that kernel
so the flow becomes config. See Appendix A for the per-flow config mapping and
Appendix B for exactly which A(i)-Team mechanisms are adopted.

---

## 2. Design Principles

1. **Deterministic flow, non-deterministic labor.** The Kernel decides *what is legal
   next* (a finite state machine, §3). LLMs do the work and the judging. No LLM sits in
   the steady-state dispatch loop.
2. **The check is the quality engine.** Value comes from `work → check → bounded
   rework`. Separate the *maker* from the *inspector* (different prompt, often different
   model). The back-edge is what makes output *converge*.
3. **Check rigor ∝ 1 / (cheapness · speed · safety of your external signal).** Cheap,
   fast, safe ground truth (ad clicks) → light internal checks. Slow, expensive, risky
   (a prod bug) → heavy checks. This predicts the three flows' check depths.
4. **QC the cheapest faithful proxy; materialize expensive artifacts just-in-time.**
   Check the plan/prompt before rendering the image/video.
5. **Output-checkpoint is cost recovery — but only with a binding stamp (§5).** A
   completed station is skipped on resume *only if its inputs are unchanged*. Replaying
   a stale output across a model/prompt/config change is corruption, not recovery.
6. **Effectful stations are not pure (§5).** Publishing an ad, committing code, or
   calling a billed API has side effects a return-value checkpoint does not capture.
   Effectful work needs an idempotency key + intent log, not best-effort retry.
7. **Cost is input-context + expensive-output, not reasoning.** Critics emit tiny
   verdicts; they're expensive because they *read*. Levers: context diet, prefix
   caching, per-station model — far more than "use a smarter model."
8. **The Law is load-bearing.** Own runtime = no provider guardrails. Hooks +
   per-card path ownership + a positive Bash allowlist are the only thing between a
   cheap, less-aligned worker (processing untrusted substrate) and the filesystem.
9. **Escalate ambiguity; never guess.** On contradictory/unrecoverable state,
   hard-pause to `hold` and surface to a human. Don't auto-reverse, don't auto-mutate.
   *(A(i)-Team: the controller emits a bounded `needsJudgment` payload instead of
   guessing.)*
10. **Config is validated, not trusted.** `flow.yaml` and the Architect's generated
    child cards are checked by the Kernel at load/expansion (disjoint path ownership,
    legal transitions, acyclic deps) before anything runs. *(A(i)-Team: a cross-language
    fixture test fails CI if the Go and TS transition matrices ever diverge.)*

---

## 3. The State Machine (the centerpiece)

Routing is governed by **two orthogonal fields per card**, with a clear authority split
(this resolves the rev-1 ambiguity):

- **`lane`** — *where* the card is. Authoritative for **routing**. A lane is either a
  station defined in `flow.yaml` (work or QC) or a kernel terminal/control lane
  (`intake`, `done`, `scrap`, `hold`).
- **`status`** — the card's *execution sub-state* at that lane. Authoritative for
  **scheduling and recovery**. Universal across all flows (kernel-owned).

A card is **dispatchable** iff: `status = ready` **AND** `lane` is a work/QC station
**AND** the station is under its WIP cap **AND** a worker slot is free (§7). These are
ANDed at a single atomic claim point (§7).

### The universal `status` lifecycle (kernel-owned)

```
            deps unmet / no capacity
   intake ───────────────► waiting ──(deps met & WIP room)──► ready
   (transient; first tick                                      │ claim (atomic)
    moves card to first station)                               ▼
                              ┌──────────────► claimed ──START_WORK──► working
                              │                                          │ MARK_DONE
        re-hydrate (resume)   │                                          ▼
                              └── interrupted ◄── worker died ──   done_pending_ack
                                                                          │ Summary Hook
                            ┌─────── pass, mid-flow ───────────────────────┤
                            │                         pass, last station   │ fail (integrity)
                            ▼                                ▼             ▼
                   advance to next lane          (lane=done, status=complete)  back to working
                   (status → waiting/ready)                                   (bounded, §6)

   any ──needsJudgment / HITL──► status=held    ← STATUS only; lane is unchanged; card
                                                  resumes on HITL reply (status → ready)
   any ──dep-scrap escalation / escalate-timeout──► lane=hold  ← LANE change; kernel terminal
   any ──cap reached / unrecoverable──► scrapped
   parent fanned-out ──► awaiting_children ──(fan-in policy met)──► ready (assembler)
```

Statuses: `waiting`, `ready`, `claimed`, `working`, `done_pending_ack`, `interrupted`,
`held`, `awaiting_children`, `complete` (lane=done), `scrapped` (lane=scrap).

> **`held` vs `hold`:** `status=held` is a pause-in-place — the card stays in its current
> lane waiting for a HITL response; no budget consumed, no worker slot held. `lane=hold` is
> the kernel terminal lane for cards moved there administratively (dep-scrap escalation,
> `escalate` on_timeout). They are orthogonal: a card in `status=held` never changes lane;
> a card in `lane=hold` has `status=held` but arrived there via an explicit lane transition.

> **Why this split matters (rev-1 bug C1):** reconcile keys off `status`+heartbeat
> (§10A/§11), *not* lane. A card that is `lane=review, status=working` is a critic
> running; the same card `lane=review, status=ready` is awaiting a critic slot. The two
> were indistinguishable before.

### The lane graph (per-flow, config) + the worked example

The lane graph is **config** (`flow.yaml` stations + their `on_reject` back-edges), but
every flow gets the same kernel terminal/control lanes and the same `status` FSM above.
The lane graph must be **validated at load** (Principle 10): all `on_reject` targets
exist, no orphan lanes, terminal lanes reachable.

The A(i)-Team flow is the canonical worked example — its transition matrix serves as the
reference model for how a real lane graph looks (note the **back-edges** that make it
cyclic, and the *earliest-flagged-station* rework routing):

```
ALL LANES: briefings → ready → testing → implementing → review → probing → done
                                                                   (+ blocked = scrap)

TRANSITION MATRIX (from → allowed to):
  briefings    → ready, blocked
  ready        → testing, implementing, probing, blocked, briefings
  testing      → implementing, blocked
  implementing → review, blocked
  review       → testing, implementing, probing, blocked     ← QC back-edges (rework)
  probing      → ready, done, blocked
  done         → (terminal)
  blocked      → ready                                        ← manual recovery from scrap

PIPELINE STATIONS (lane → worker, happy-path next):
  testing      → murdock  → implementing   (writes the tests = the executable proxy)
  implementing → ba       → review
  review       → lynch    → probing         (QC: reviews tests+impl together)
  probing      → amy      → done            (QC: probes for bugs beyond tests)
```

**Earliest-flagged-station rework (A(i)-Team, adopt):** when a QC station rejects, the
back-edge target is the *earliest* station implicated by the findings — a test-coverage
gap routes to `testing` (re-do the proxy) before `implementing`, not just "one step
back." Encode this as the critic's structured verdict naming `return_to`, validated
against the matrix.

### Edge-case transitions (every one must be defined)

| Event | Transition |
|---|---|
| Worker crash mid-`working` | → `interrupted`; reconcile re-hydrates (§9). Effectful side effects guarded by intent log (§5). |
| Provider **rate limit** mid-`working` (agentic/harness) | → back to `ready` at the SAME lane, `cards.release_at` stamped with the provider's reported reset. Consumes **neither** the rework cap nor the execution-attempt cap: the work never ran and nothing was billed. This holds on a rework invocation exactly as on the first — a cap that lands after a QC reject parks the card, it does not scrap it. The release gate (§8) keeps the card undispatchable until then, and the liveness watchdog does not read a gated card as a stall — but the **consumption andon still applies**, so a cap the run cannot afford to wait out halts it rather than idling. That halt is **parked and resumable**, not a failure: nothing is scrapped, the card stays `ready` behind its gate, the run is recorded `status='halted', outcome='parked'`, and the operator is told the soonest `release_at` and the `conduit resume` command. A gate counts as a cap only when the card_log attributes it to one — the fan-out stagger (§8) stamps the same `release_at` column, and a run holding only stagger gates was halted for some other reason, so it must read as a plain halt. Under the ingress listener (§4A) that resume is automatic: the exit watchers keep the event `spawned` and the sweep issues `conduit resume` once the gate passes. An **effectful** station is the exception to all of this: its invoke is the billed or irreversible act, so a cap says only how the invocation ended, not whether its side effects landed first. A pending outbox intent (§5) therefore escalates to `hold` rather than parking, since a park spends no attempt and would re-dispatch at the same idempotency key, which reconcile can only ever answer `escalate_hold`. Because a park consumes none of the four guards, the parks themselves are bounded: a card that hits the cap `MAX_CONSECUTIVE_RATE_LIMIT_PARKS` times in a row without the station running escalates to `hold` rather than parking again, so a failure misreported as a cap — or a cap that never clears — ends in front of a human instead of looping. |
| `MARK_DONE` fails the Summary Hook (integrity) | → back to `working`, counts against the **execution-attempt** cap (§7), not the rework cap. |
| QC reject, under cap | → `on_reject` lane, `attempt++`, `status=waiting`. |
| QC reject, cap reached, `cap_policy=scrap` | → `scrap`. |
| QC reject, cap reached, `cap_policy=proceed_with_findings` | → forward, findings attached to the card payload. |
| A card's dependency lands in `scrap` | dependent → `scrap` or `hold` per `on_dep_scrap` policy (default `hold` — escalate). |
| Station has no idle worker indefinitely | stays `ready`; the **liveness watchdog** (§8) alerts on no-progress, distinct from the consumption andon. |
| Card created (parent or child) | Starts as `(lane=intake, status=waiting)`. On the first tick the kernel evaluates deps; if satisfied, transitions to `(lane=<first_station>, status=ready)`; otherwise stays `status=waiting` until deps clear. `intake` is transient — no card lingers there. |
| `hold` with no human | does **not** consume budget; if `hold_timeout` is set, the kernel applies the card's `on_timeout` action — `scrap`, `proceed_with_findings`, or `escalate` (§4A). If `hold_timeout` is omitted, the card holds indefinitely. `on_timeout` is **required** when `hold_timeout` is set; the kernel rejects the config at load otherwise. |
| Andon trips mid-flight | new claims blocked; in-flight workers **drain-and-checkpoint** (§8). |
| Parent fanned-out, child scraps | fan-in policy decides (§6): `all` → parent holds/scraps; `quorum(k)`/`best_effort` → proceed. |

---

## 4. The Routing — `flow.yaml` (the engine/config seam)

A flow is **data**, not code. The same Kernel runs any flow by loading and validating
its routing.

### Station kinds (the two axes)

Not every station is the same animal. Each is classified on **two orthogonal axes**, and
the classification — not a guess — determines its runtime, cost, safety surface, and
checkpoint semantics.

**Axis 1 — execution model (`kind`):**

| `kind` | What it is | Runtime | Cost shape |
|---|---|---|---|
| `deterministic` | no LLM — a command or function | `Bun.spawn` a CLI / call code; capture stdout+exit | ~$0 |
| `transform` | typed data in → LLM reasoning → typed data out; **no tools, no loop** | one model call + coercive-parse (§7); a Unix filter | input-dominated, cheap |
| `agentic` | LLM with Read/Write/Bash in a **multi-turn loop** | the full Tool-Bridge + the Law (§7) | multi-turn, expensive |

**Axis 2 — effect (`effectful: true|false`, independent of `kind`):**
- **pure** — output is a function of input (a critic, a transcript parse). Replays
  cleanly from checkpoint (modulo the binding stamp, §5).
- **effectful** — billed external call or irreversible side effect (image-gen, publish,
  git commit, `ffmpeg` write). Needs the outbox + idempotency key (§5), *regardless of
  `kind`*.

The axes are independent. A critic is `transform`+pure. Image-gen is `transform`+effectful
(a single known external call, no tool loop). A Murdock-style coder is `agentic`+effectful.
The assembler is `deterministic`+effectful (it publishes). `grep`/`ffprobe` are
`deterministic`+pure.

**Why this is the load-bearing distinction:**
- **The *full* Law (§7) is scoped to `agentic` stations.** The multi-turn Tool-Bridge,
  arbitrary Bash, and the injection threat model exist *because there is a tool loop*. A
  `transform` station has no tools, no filesystem, no shell — its only safety surface is
  **output-schema validation**. A `deterministic` station still writes files and runs a
  command, so it gets a **Law-lite**: owned-path writes + an allowlisted command (no loop,
  no model, no injection vector). Every critic/briefer/director is a transform, so the
  scary surface is the agentic minority.
- **The outbox (§5) is scoped to `effectful` stations, any kind.** Pure stations replay
  for free; effectful ones need idempotency.
- **`transform` and `deterministic` stations are Unix filters** — `stdin → stdout`,
  composable, fixture-testable (§14), cacheable. `agentic` is the stateful exception (a
  worker at a bench, not a filter in a stream). The lean flow and the Unix pipe are the
  same shape; the back-edge (rework) is the one thing a pipe can't do.

> **MVP consequence (see §16):** a flow built only of `deterministic` + `transform`
> stations needs **none** of the agentic Tool-Bridge/Law. **Studio is exactly this** — so
> the first shippable Conduit can omit §7's agentic surface entirely and still run a
> customer's flow end-to-end. The agentic loop is added only when coding-style flows (A(i)-Team)
> require it.

Each station declares its `kind` in `flow.yaml` below (critics are implicitly
`transform`):

```yaml
flow: studio-ad-factory
project_root: ./campaign-2026
flow_version: 7                # bumped on any edit; pinned at run start (§5)

budgets:
  run:                         # consumption andon (§8)
    wall_clock_minutes: 90
    max_tokens: 2_000_000
  per_wave:                    # subtree andon keyed by parent_id — all children of one parent card (rev-1 H3)
    max_tokens: 400_000
    max_dispatches: 200
  per_card:
    max_execution_attempts: 5  # parse/integrity retries before scrap (rev-1 H5)
  liveness:
    no_progress_minutes: 5     # deadlock watchdog, distinct from consumption (rev-1 C5)

defaults:
  cap_policy: scrap            # scrap | proceed_with_findings
  on_dep_scrap: hold           # hold | scrap

stations:
  - id: brief
    worker: { kind: transform, role: briefer, model: gpt-4o-mini }
    wip: 25
    inputs:  [campaign_idea]
    outputs: [brief.json]      # owned paths — disjoint across concurrent cards (§9)

  - id: direct
    worker: { kind: transform, role: director, model: gpt-4o }
    wip: 10
    check:
      kind: gate               # gate | rank | market
      class: taste             # taste (market-replaceable) | risk (mandatory)
      critic: { role: prompt-critic, model: gpt-4o }
      on_reject: direct        # back-edge target (validated against lane graph)
      rework_cap: 3
      progress_signal: findings_hash   # hash findings, NOT artifact (rev-1 H2)

  - id: generate
    worker: { kind: transform, role: image-executor, model: gemini-2.5-flash-image }
    effectful: true            # billed external call → intent log (§5)
    materialize: on_approval   # JIT: only after the prompt proxy passes
    fan_out: 2                 # variants are the product, not candidates

  - id: review
    check:
      kind: rank
      class: risk              # brand-safety stays hard regardless of signal
      critic: { role: visual-reviewer, model: gemini-2.5-pro }
      on_reject: direct        # re-do the prompt, NOT re-materialize (rev-1 L3)
      rework_cap: 2

  - id: assemble
    worker: { kind: deterministic, role: assembler }
    fan_in: { policy: quorum, k: 1 }     # proceed if ≥1 of the 2 variants survives (rev-1 C4)
    effectful: true            # publishes the CSV → idempotency key (§5)
    adapter: meta-ads-csv

terminal_lanes: [done, scrap, hold]
output:
  tag_assets: true             # stamp every asset with a stable ID (§13)

security:                      # the Law (§7)
  bash: { allow: ["ffmpeg", "ffprobe"], deny_shell_metachars: true }
  network_egress: deny         # content-processing workers get no network by default
```

Every knob here came from the three flows diverging or from a rev-1 finding. The seam
is the whole product: *the flow is config; the kernel is the engine.*

---

## 4A. Channels — the flow's edges (triggers, HITL, delivery)

Stations are the *interior* of a flow; **channels are its edges** — how a run is triggered
and how the flow talks back to a human or system. A channel is a pluggable adapter
(`slack | cli | webhook | email | …`), **never a station**: stations transform work,
channels move it across the flow's boundary.

**Two directions:**

- **Ingress (trigger).** An external event becomes a parent card's substrate and starts a
  run — a Slack message in a watched channel, a webhook POST, a CLI invocation, a dropped
  file. Ingress maps the event onto the parent card's substrate and kicks Wave 1 (§9).
- **Egress** — four uses, each bindable per flow:
  1. **Status (genba)** — mirror the run's journal to a thread (a lightweight War Room for
     a non-technical operator).
  2. **HITL** — a `hold` lane or a `rank` selection solicits a human decision (§3, §4A).
  3. **Alerts** — andon / liveness-watchdog / scrap notifications (§8).
  4. **Delivery** — the final artifact (or a link) handed off.

**Adapter ≠ channel (keep two layers).** The **output adapter** *formats* the assembled
result (→ Meta CSV, FCPXML, a git commit); the **egress channel** *transports* it and runs
any human loop: `assemble → adapter (format) → egress channel (deliver)`. Format and
transport are separate concerns.

**Flow-computed rank candidates.** A rank check may present a
shortlist the flow already produced instead of running a critic:
`check: { kind: rank, candidates_from: work/board.json }` (a JSON array of
strings or `{id, label}` objects; mutually exclusive with `critic`, validated
at load). Optional surfaces: `ask_template:` (rendered via the SAME
renderPrompt pipeline as worker prompts, scope-checked against the station's
`inputs`), `ask_attach:` (files uploaded with the ask through the
outbox-guarded delivery path; containment-checked against the project root),
and `selection_out:` (the recorded human selection written as a station
artifact on resume, so downstream stations consume the pick as an input).
Replies arrive over the channel: Socket Mode `interactive` envelopes
(action_id = correlation id) or plain thread replies under the ask (a 1-based
shortlist index, an exact label, or `= Free Form Name` as an explicit human
override); a confirmed reply spawns `conduit resume` for the parked run.

**HITL over a channel = the `hold` lane wired to a channel.** The outbound message carries
a **correlation ID** (card/asset id). The human's reaction (emoji, button, reply) arrives
asynchronously, is mapped back to the card, and is injected as the selection / managerial
note (§3). It honors `hold_timeout`; when the timeout fires, the kernel applies the card's `on_timeout`
action (`scrap` | `proceed_with_findings` | `escalate`). `on_timeout` is **required** in
config whenever `hold_timeout` is set — the kernel rejects the config at load otherwise.
The safe default is `scrap` (fail closed); `proceed_with_findings` attaches "no selection
made" and continues; `escalate` re-posts the HITL prompt to an alert channel. For `rank`
selections, never silently auto-pick — use `proceed_with_findings` or `scrap`. Per §3,
`hold` consumes no budget and no worker slot.

**Channel egress is effectful (§5).** A send goes through the **outbox + idempotency key**,
so a resume never double-posts a delivery or double-asks an approval.

**File delivery (station-level `deliver:` block).** A station may declare
`deliver: { files: [...], thread_from?, caption? }` — the produced artifact(s) to hand off on
successful completion, addressed to whichever egress channel `resolveDeliveryChannel` selects
(the channel whose `uses` includes `delivery`; the first egress channel when none declare `uses`
at all). Validated at load: an empty `files` list, or a `deliver` block on a flow with no
delivery-capable egress channel (e.g. only a `hitl`-only channel declared), is rejected before
the flow ever runs. Declared files deliver **sequentially, in declared order**, each through the
same outbox-guarded send as any other effectful egress — Slack's three-step external upload
(`files.getUploadURLExternal` → raw byte POST → `files.completeUploadExternal`) — keyed by
`(flow_version, card, station, attempt, file)` **plus a content fingerprint**: a rework attempt
that produces a new artifact re-delivers, an unchanged same-attempt resume dedups. `thread_from`
names a field on the triggering ingress event's substrate (e.g. a Slack `thread_ts`); when the
substrate can't supply it (a CLI-triggered run, or a substrate missing that field) the file
delivers unthreaded and the skip is journaled — never a load-time or delivery error, since the
same flow may be Slack- or CLI-triggered. `caption`, when present, rides the upload's own
completion call as `initial_comment` — never a separate send. Any ambiguous or failed delivery (a
missing/empty file, an owned-paths breach, an unresolvable channel, or a pending intent a
`files.info`/thread-history reconciler cannot confirm) hard-pauses the card to `hold` rather than
re-running the station to force a retry; a reconciler that **can** prove the file already landed,
or definitively did not land, auto-resumes the safe crash window without a human.

**The ingress/always-on split (the one architectural fork).** Listening continuously on
Slack wants a long-lived process — but the kernel is a *per-run* daemon (§10A). Resolve it
by splitting: a **thin trigger-listener** (a webhook/events receiver whose only job is
"receive event → `conduit run`") stays up cheaply; the heavy per-run kernel is still
spawned per flow. Egress needs no listener — the running kernel posts directly. So
**egress-first is the MVP**: manual/CLI trigger, Slack for status/HITL/delivery; add the
ingress listener when you want hands-off triggering.

```yaml
channels:
  ingress: { type: cli }                      # cli | slack | webhook (manual for MVP)
  egress:
    - type: slack
      target: "#campaigns"
      uses: [status, hitl, alerts, delivery]  # any subset
      hold_timeout_minutes: 30
      on_timeout: scrap                        # scrap | proceed_with_findings | escalate; required when hold_timeout is set

stations:
  - id: deliver-photo
    worker: { kind: transform, ... }
    deliver:
      files: [work/edited.jpg]                 # non-empty; verbatim declared paths
      thread_from: thread_ts                   # optional — substrate field naming the reply thread
      caption: "here is your edit"             # optional — rides the upload as initial_comment
```

---

## 5. Cards, the Station Transaction & Checkpoint Soundness

### Card hierarchy
- **Parent (Epic):** the PRD / mission / campaign.
- **Child (Task):** dynamically spawned work items with `depends_on` edges and a
  declared owned-path set (`outputs`).

**Card payload (adopted from A(i)-Team's work-item shape):** `id, parent_id, type,
objective` (one behavioral sentence), `acceptance[]` (measurable criteria the critic
maps to), `context` (integration points), `outputs` (owned paths), `depends_on[]`,
`attempt` (rework generation), plus kernel fields `lane, status, owned_paths`. The
engine only reads the structural fields; the rest is opaque payload the worker/critic
interpret.

### The Start/Finish transaction (Bun IPC)
1. **START_WORK** — worker signals intent. Kernel performs the **atomic claim** (§7)
   and starts a **heartbeat lease** (§9).
2. **EXECUTION_LOOP** — worker reasons and invokes tools through the Tool-Bridge (§7).
3. **MARK_DONE** — worker emits a structured Work Summary + the typed station output.
4. **The Hook Gateway — deterministic only.** Runs the **Summary Hook**: a *pure code*
   integrity check (files touched ⊆ owned paths, output schema valid, artifacts exist).
   It does **not** run an LLM. *(Rev-1 H7 fix: the quality critic is a separate **QC
   station**, §6 — not inside this transaction.)*
5. **ACK_DONE** — Kernel commits the Work Summary, **checkpoints the station output**,
   and transitions the card.

### Checkpoint binding stamp (rev-1 C2 — replay soundness)
Each checkpoint is keyed by `(flow, card, station, attempt)` **and carries a binding
stamp**:

```
binding = hash(model_id, prompt_template_version, resolved_input_artifact_hashes, flow_version)
```

On resume, a completed station is skipped **only if its binding stamp matches the
current config.** On mismatch the checkpoint is **invalidated and the invalidation
cascades downstream** (any station whose `resolved_input_artifact_hashes` changed is
also invalid). `flow_version` is pinned at run start; resuming against a different
`flow.yaml` requires an explicit `--rebind` and re-validates every stamp.

### Effectful stations (rev-1 C3 — side effects aren't pure)
Effectfulness is orthogonal to `kind` (§4): a pure `transform` critic skips everything in
this subsection; an effectful `transform` (image-gen) and an `agentic` coder both need it.

A station marked `effectful: true` may have committed real-world side effects
(published ad, git commit, billed API call) that a return-value checkpoint cannot undo
or detect. For these:

- Before the side effect, write an **intent record** to the `outbox` table:
  `(card, station, attempt, idempotency_key, intent, status=pending)`.
- Perform the side effect using the `idempotency_key` (passed to the provider where
  supported).
- On success, mark the outbox row `committed` and checkpoint.
- On resume, **check the outbox before retrying**: a `pending` row means "the side
  effect may have happened" → reconcile against the provider (or escalate to `hold`),
  never blind-retry. This is what stops a crashed `assemble` from publishing duplicate
  live ads.

> Distinguish **expensive-but-discardable** (regenerate an image — safe to re-run, just
> costs money) from **irreversible** (publish, commit, send) — only the latter needs the
> outbox; the former just needs the checkpoint so resume doesn't re-bill (rev-1 cost
> recovery).

---

## 6. The Quality System

Stations are **work** stations (produce) or **QC** stations (judge). Keep two checks
strictly separate (rev-1 H7):

- **Integrity check** — the deterministic Summary Hook in the DONE transaction (§5).
- **Quality check** — a *separate QC station* (its own lane, its own claim, its own
  worker slot) running an LLM critic with a back-edge. Critics are almost always pure
  `transform` stations (data in, verdict out, no tools), so QC adds little to the
  agentic surface (§4) — it's cheap and safe by construction.

### Check archetypes (`check.kind`)
- **`gate`** — reject until *one* artifact passes (code; the edit plan). Drives
  convergence.
- **`rank`** — don't converge to one; *curate/score* a short-list. Terminal is a
  selection (human via `hold`, or downstream).
- **`market`** — the real grader is external (clicks, signups, watch-time). Internal
  critics become a cheap **pre-filter**; the fitness function lives downstream (§13).

### Check classes (`check.class`)
- **`taste`** — market-replaceable; thin as the external signal cheapens.
- **`risk`** — brand / safety / legal / compliance. **Mandatory regardless of signal
  economics** — the market won't catch an off-brand-but-converting ad in time. Risk
  gates stay hard even when taste gates go light.

### The rework loop and its FOUR bounding guards
The back-edge makes the flow cyclic, not a DAG — DAG engines structurally cannot
express it (this is why Conduit is a state machine, not Airflow). It is bounded by
**four** independent mechanisms (rev-1 added #2 and #4 over the original three):

1. **Per-card, per-gate rework cap → scrap** (or `proceed_with_findings`). Graceful.
   `rework_cap` is declared on each station's `check:` block, so the counter it bounds
   is scoped to the **(card, gate)** pair — a gate declaring `rework_cap: 3` grants that
   card three cycles *at that gate*, independent of what any other gate in the flow has
   already spent. (`cards.rework_count` remains the card's lifetime total across all
   gates; it is history and prompt-threading, not a budget.)
   *(A(i)-Team: `rejection_count ≥ cap → blocked`, enforced in the planner, not just
   the API.)*
2. **Per-execution-attempt cap.** Integrity-fail / parse-fail retries are bounded
   separately from quality rework (rev-1 H5) → on exhaustion, scrap with a
   `model-incompatible`/`integrity` reason.
3. **Progress monotonicity — on the *findings*, not the artifact** (rev-1 H2). A
   rejection whose **critic findings hash** matches the previous attempt's = "same
   defect, no progress" → exhausts the cap immediately. Hashing the artifact is wrong:
   LLM output churns cosmetically (false "progress") and can't be byte-compared.
4. **Budgets at three scopes** (rev-1 C5/H3): per-card, **per-wave/subtree** — a *wave*
   is operationally all cards sharing a `parent_id`, so token/dispatch consumption is
   tracked by `parent_id` partition and exceeding the cap scraps that subtree without
   halting the whole run — and the global **andon** (wall-clock + tokens) — *plus* the
   separate **liveness watchdog** (§8) that catches a *stalled* flow the consumption
   budgets miss.

### Fan-in failure policy (rev-1 C4)
`fan_in: { policy: all | quorum(k) | best_effort }`. Defines what a scrapped child does
to the assembler and parent. `all` → one scrap holds/scraps the parent (A(i)-Team:
don't ship a partial product). `quorum(k)`/`best_effort` → proceed and record the
dropped children. **`k` is a count (integer, `1 ≤ k ≤ fan_out`):** the minimum number of
children that must reach a non-scrap terminal lane for the fan-in to proceed. `k: 9` with
`fan_out: 10` = tolerate one scrapped child. A count, not a ratio — no rounding ambiguity
on small fan-outs, and the loader validates it against `fan_out` at load time. A child
that can never complete must not deadlock the parent — the liveness watchdog (§8) is the
backstop.

### Plan-first QC (Principle 4)
`materialize: on_approval` produces the expensive artifact only after its cheap proxy
passes. A post-materialize `rank` reject routes back to the *proxy* station by default
(re-do the prompt), not to re-materialization (rev-1 L3) — re-materializing the
expensive floor is capped separately if the flow allows it at all.

---

## 7. The Worker Runtime, Tool-Bridge & Atomic Claim (highest-risk surface)

Conduit drives arbitrary models with no host harness, so this layer carries the most
risk and gets the most poka-yoke.

> **Scope (the key simplification).** The atomic claim (below) applies to *every*
> station. The **tool-call loop and the Law apply to `agentic` stations only** (§4). A
> `transform` station makes a single model call with no tools — its only Law is
> output-schema validation; a `deterministic` station runs one allowlisted command. Since
> most stations (every critic, briefer, director) are transforms, the bulk of this
> section does not apply to them — and a flow with no agentic stations (Studio) needs
> none of it.

> **Model calls are kernel-mediated (every LLM station).** A worker never calls a model
> provider over the network itself — the **kernel's per-model adapter** makes the call on
> its behalf. This is what lets `network_egress: deny` hold for worker tools while
> `transform`/`agentic` stations still reach gpt-4o/gemini/local models, and it's where the
> coercive parser, retries, token accounting, and prompt-cache prefixing live — uniformly,
> across providers.

> **`kind: harness` is a separate, weaker-claim tier — not this section's Law.** A
> `kind: harness` station delegates its tool loop to an external headless agent harness
> (`claude -p`, `codex exec`, …) instead of the in-kernel Tool-Bridge described below. The
> harness owns its own tool loop, so the kernel cannot gate individual tool calls
> pre-execution the way it does here; containment is a documented **profile** at the
> process boundary (mandatory owned-paths integrity, secrets-by-allowlist, process-group
> termination, the ADR-0003 container wall, and the adversarial gate as the quality
> control) rather than the Law. `agentic` keeps its Law-grade meaning in this section
> unchanged. See [`docs/harness-containment.md`](docs/harness-containment.md) for the
> full profile.

### The atomic claim (rev-1 C6)
deps + WIP + worker-slot must become true at a **single linearization point**. They live
in different substrates (deps/WIP in SQLite; worker slot is physical), so the claim is a
single SQLite transaction that *also* reserves the pool slot:

```sql
-- one transaction; either all of it commits or none
INSERT INTO active_workers(card, station, slot, lease_until)
SELECT :card, :station, :slot, :lease
WHERE (SELECT count(*) FROM active_workers WHERE station=:station) < :wip
  AND NOT EXISTS (SELECT 1 FROM active_workers WHERE slot=:slot)
  AND deps_satisfied(:card);
-- then, in the same txn: UPDATE cards SET status='claimed' WHERE id=:card AND status='ready';
```

No claim is split across two substrates; a partial claim cannot happen. WIP occupancy is
measured from `active_workers WHERE station=:station` — the slot registry — not from
`cards.status`, which would miss `claimed` and `done_pending_ack` rows that also hold
slots. *(A(i)-Team used a single-substrate atomic hardlink for this; Conduit's DB+pool
split is harder and needs this in-transaction reservation.)*

### The tool-call loop
- **Sanitization sandwich (out).** Strict delimiters frame untrusted content — but this
  is **defense-in-depth, not the primary control** (rev-1 H6). Stable, cacheable context
  goes *first* (system + shared rubrics/brand), volatile per-card content *last*, so
  prefix caching actually fires (§12).
- **Coercive parsing (in) — poka-yoke.** Parse worker output *tolerantly*
  (schema-aligned recovery from fenced/garbled/prose-wrapped responses). A parse miss is
  a *paid re-bill* and happens on frontier models too. **Bounded:** parse retries count
  against `per_card.max_execution_attempts` (rev-1 H5); on exhaustion → scrap with
  `model-incompatible`. Parse-miss rate is a first-class journal metric (§11).
- **Per-model adapters.** Native function-calling vs prompted XML/JSON blocks, normalized
  to the kernel tool protocol. A model with no function-calling falls back to the prompted
  adapter or is rejected at the Bench (§14) before it's allowed on a flow.
- **The Law (permissioning).** Before any tool runs the Kernel checks Hooks:
  - **Path ownership** — writes ⊆ the card's `owned_paths`, with **symlink + relative-path
    resolution** (resolve to canonical absolute, reject escapes) (rev-1 H6).
  - **Bash positive allowlist** — only listed executables; **no shell metacharacters**
    (no pipes/redirects/`;`/backticks/`$()`) unless explicitly enabled. Denylists are
    insufficient against a cheap model + untrusted substrate.
  - **Network egress denied by default** for content-processing workers (injection
    exfiltration defense). For `agentic` workers this is enforced in-process: the kernel
    spawns the worker harness via `unshare --net` (a Linux network namespace with no
    external interfaces), so the subprocess has no network access regardless of what the
    substrate instructs it to attempt. Model calls are made by the kernel process through
    its per-model adapter — the worker never touches the network directly. (Requires Linux;
    the Docker base image from ADR-0003 provides a suitable environment.)
- **Injection threat model (explicit, rev-1 H6).** Ingested substrate (scraped ideas,
  video transcripts, PRDs) is **adversarial input**. Delimiter framing can be defeated;
  the real controls are the Law above. Assume the worker will attempt whatever the
  substrate tells it to.
- **Stream-piping (genba).** Worker output streams to the War Room + journal live.

---

## 8. Flow Control, Scale & the Two Andons

- **WIP is per station, never a global sum.** Dispatchable iff deps satisfied **AND**
  target station under WIP **AND** a worker slot free — ANDed at the atomic claim (§7).
  *(A(i)-Team: WIP is per-stage; never summed across stages.)*
- **Pull, not push.** Never dispatch into a full station; an idle worker pulls the next
  eligible card; the WIP cap is enforced atomically at claim (a soft counter races).
- **Single-piece vs multi-piece.** Autocut = one piece + intra-station concurrency;
  A(i)-Team / Studio-at-scale = many pieces + per-station WIP. Same kernel.
- **Fan-out / fan-in.** A parent explodes into N children (waves, §9); a deterministic
  assembler ($0) fans them back in per the fan-in policy (§6) and emits the flow's
  native artifact via an **output adapter**.
- **Heijunka (load leveling).** A global **token bucket** paces LLM requests across all
  lanes. *Watch (rev-1 F2):* under throttle, idle workers holding slots while waiting on
  the bucket can drop effective WIP below configured caps — dispatch should be
  bucket-aware.

### Two distinct andons
1. **Consumption andon** — wall-clock + tokens at run/wave/card scope. Trips on a
   *busy* runaway.
2. **Liveness watchdog** (rev-1 C5) — "no card has changed lane in `no_progress_minutes`
   AND no worker is active" → **immediate** deadlock alert *with the blocking reason*.
   Catches a *stalled* flow (dep scrapped, `hold` with no human, no idle worker) that
   consumes nothing and would otherwise hang until wall-clock.

**Andon drain semantics (rev-1 M4):** on trip, **new claims are blocked but in-flight
workers complete-and-checkpoint** (so the budget is a soft ceiling — the alert reports
the overshoot). Hard-kill is reserved for hung workers past their heartbeat lease.

---

## 9. Dynamic DAG & Waving

Conduit unfurls the plan at runtime rather than following a static script.

- **Wave 1 (Definition).** The parent card is processed by the **Architect** (planner
  LLM) and a **Reviewer** (critic). *The Architect is an LLM for the decomposition
  judgment only — never in the steady-state dispatch loop.* The Wave-1 Architect↔Reviewer
  loop is itself **rework-capped** (rev-1 L5) → escalates to `hold` on exhaustion.
- **Dynamic expansion + validation.** On approval, the Architect explodes the plan into
  child cards; the Kernel runs **dependency analysis adopted from A(i)-Team's
  `deps-check`**: a DFS **cycle detection** over `depends_on` (illegal data-dependency
  cycles are rejected) and categorization into `ready` (deps in a terminal lane) vs
  `waiting`. **Waves are emergent** — a card unblocks when its deps reach a terminal lane.
- **Disjoint ownership invariant (rev-1 H4).** Before committing children, the Kernel
  **validates that owned-path sets across concurrently-eligible cards are disjoint** —
  or forces a `depends_on` serialization edge for a shared path. The Architect (an LLM)
  proposes; the Kernel (deterministic) enforces. This closes the "two cards both own
  `config.ts`" race that path-ownership alone misses.
- **Two axes of motion, deliberately different:**
  - **Forward-only across waves.** An implementation failure → **hard pause for a human**
    (`hold`), never an automatic "nuclear reversal" to Wave 1 (state rot).
  - **Cyclic within a card's station progression.** The `work → check → rework` back-edge
    *is* a cycle, first-class and bounded by the four guards (§6).

---

## 10. The Controller — the deterministic tick (adopted from A(i)-Team)

The Kernel's scheduler is a **pure planner** that, each tick, reads board + deps + pool +
checkpoint + liveness state and emits a bounded **action plan** — then executes it. It is
the proven A(i)-Team controller, generalized.

- **Action kinds:** `dispatch`, `move`, `release`, `reclaim`, `setup-lane`/`spawn`,
  `fan-in`, `block`/`scrap`, `final-assemble`, plus a `needsJudgment` escalation channel.
- **Idempotent action IDs (generation-keyed).** Each action ID encodes the card's rework
  generation: `flow:card:dispatch:g<attempt>:worker:seq`. Dedup is keyed on the
  generation, so a *re-dispatch after a legitimate rework bounce* (new generation) is
  allowed, while a *duplicate of the same generation* is suppressed. *(A(i)-Team: this
  exact scheme — generation-keyed IDs — fixed a bug where prefix-dedup blocked legitimate
  rework re-dispatch.)*
- **Reclaim / compensation.** A dispatch whose effect never materialized (worker slot
  busy past the reclaim threshold with the card unstarted) is released and its action
  dropped so the next tick re-dispatches — a built-in saga/timeout compensation.
- **Fail-closed.** On any unreadable state the planner emits zero actions + a
  `needsJudgment` payload rather than guessing (Principle 9).
- **Adaptive cadence.** The tick returns its own next-wake interval — short when work is
  flowing, long when idle — so the loop is cheap at rest.

---

## 10A. Process Model — a resident per-run daemon

The kernel is a **long-lived process for the duration of a run** — not a tick re-entered
from outside. (A(i)-Team had to re-enter via `ScheduleWakeup` because it lived inside a
REPL it couldn't keep resident; Conduit owns its runtime.) Three reasons it can and should
be resident:

- **The deterministic loop is ~free to keep alive.** The runaway risk A(i)-Team feared was
  an *LLM* re-reading context every tick; Conduit's loop is Bun code — it can't run away in
  token cost and costs nothing to keep resident.
- **IPC requires a live parent.** Workers are `Bun.spawn` subprocesses that report back via
  `process.send`; if the kernel exited between ticks their `MARK_DONE` would be orphaned.
- **It enables event-driven scheduling.** The kernel reacts to worker IPC (a station
  finished → re-plan) and to lease/timer events, with a slow periodic safety tick as
  backstop — lower latency and less idle churn than polling.

**But the database is the authority, not process memory.** The §10 tick stays *stateless* —
each cycle recomputes the plan from SQLite (idempotent action IDs and all). The daemon is a
*convenience over the DB*, not the source of truth. If the host dies mid-run,
`conduit resume` reconciles from SQLite (heartbeat leases → `interrupted` → re-hydrate;
checkpoints → no re-billing; outbox → no double side-effects) and continues. Crash recovery
is identical to A(i)-Team's; only *where the loop runs* differs.

**Lifecycle.** `conduit run <flow.yaml>` spawns the kernel; it drives the flow to a terminal
state and exits. `conduit resume` re-attaches to an interrupted run. An **always-on
multi-flow service** (`conduitd` accepting a queue of runs across a fleet) is deferred — it
hits the multi-host boundary (§17 F1) and the SQLite single-writer ceiling (§11). The thin
**trigger-listener** (§4A) is the only always-on component the MVP might add, and it merely
shells out to `conduit run`.

---

## 11. Persistence — The Journal of Truth (`conduit.sqlite`)

Split into a **transactional state store** and a **hot append-only journal** to avoid
write-lock contention at batch scale (rev-1 M1).

**State DB (transactional, low-volume):**

| Table | Purpose |
|---|---|
| `cards` | `id, parent_id, lane, status, attempt, wave, owned_paths` |
| `station_outputs` | checkpointed typed output per `(card, station, attempt)` + **binding stamp** (§5) |
| `outbox` | effectful-side-effect intent log + idempotency keys (§5) |
| `active_workers` | claimed slots + **heartbeat lease** (`lease_until`) (§9, rev-1 H8) |
| `ingress_events` | ingress dedup log: `(event_id TEXT PRIMARY KEY, received_at INTEGER)` — prevents a listener restart from re-triggering billed runs (§4A) |

**Journal DB (append-only, high-volume, separate file/WAL):**

| Table | Purpose |
|---|---|
| `journal` | OTel spans, worker "thoughts," tool i/o, **per-station token/cost attribution** |
| `work_summaries` | one-to-many chain-of-custody summaries per card |
| `mutations` | proposed improvements — *human-gated* (§13) |
| `post_mortems` | run summaries + success metrics |

- **Liveness by lease, not PID (rev-1 H8).** A worker is alive iff its heartbeat lease is
  unexpired — PIDs are reused and meaningless across restarts/hosts. Reconcile uses the
  lease.
- **Cost attribution is first-class**, aligned to **OpenTelemetry GenAI** conventions so
  it ports to standard backends. The journal can emit the per-station cost table for any
  run on demand. **Never parse provider transcripts for telemetry** — the journal is the
  source of truth.
- **Retention (rev-1 M2):** journal rows compact to summaries after N runs; large tool
  i/o goes to content-addressed blobs with the journal holding refs. The journal has a
  size budget.
- **Secret handling (rev-1 L2):** prefer *not logging* secrets (allowlist what reaches the
  journal; never put raw env into worker context) over regex masking, which has guaranteed
  false negatives. Masking is defense-in-depth.

---

## 12. Cost Model & Economics (honest accounting)

The cost structure dictates the levers:

- **Text stations are input-dominated.** Critics emit tiny verdicts; they're expensive
  because they *read* rubrics/brand/prior-work. → Levers: context diet (the manifest),
  **prefix caching** (stable context first — this *constrains* the §7 sandwich layout),
  and per-station model choice (input-rate-driven).
- **Expensive-artifact stations are output-dominated** (image/video gen) — the
  incompressible floor. → Lever: produce *fewer* (plan-first QC), not cheaper input.
- **Checkpointing recovers cost** (§5): a failure at station N never re-pays 1…N-1.

**Honest figures (rev-1 M6).** The headline ~$0.21 is a **clean-path single run** (no
rework, no parse misses). The design's *normal* path includes rework, so:

```
E[cost] = clean_path + E[rework_cycles] × (maker + critic) + E[parse_misses] × maker_input
```

Compare like-for-like (frontier-clean vs conduit-clean, or both-with-rework) — the
order-of-magnitude claim must not pit a frontier *debugging session* against a conduit
*clean run*.

**Caching assumptions are explicit and testable (rev-1 M5).** Prefix caching only helps
if: the provider/model supports it, the shared context is a stable *prefix*, and the cache
**TTL outlives batch drain time** (provider caches are often ~5 min; a 200-child batch
under WIP + write contention can drain slower → late children miss the cache). Treat
batch-with-cache as a separate model from the single-run figure; pin the stable-prefix
ordering as mandatory.

---

## 13. Continuous Improvement (Kaizen)

**One pipe, an open signal bus:** `observe → propose mutation → Acceptance Bar → human-gate →
apply` (the `mutations` table + the Analyst). Kaizen is **not a fixed set of loops** — any signal
that joins to provenance and proposes a bounded change is a trigger. Signals move along **two
orthogonal axes**:

- **Taste calibration** — the **calibration cascade**: the internal critic, the human, and the
  market sit on a latency/fidelity gradient, and **each cheaper, faster signal is a learned proxy
  for the next, truer one** (tune the critic to predict the human's picks; calibrate the human's
  taste against the market). Adjusts `taste` gates; **bidirectional** (can loosen or retire a taste
  gate). Principle 3 read as a *learning* hierarchy.
- **Defect closure** — **escaped-defect** signals (code review, CI, incidents — human *or* agent)
  that reveal a defect class the in-flow check missed. Adjusts the **check** (rubric or a new Hook);
  **monotonic-tightening only**.

Every mutation, whatever its trigger or axis, clears **one Acceptance Bar** before it is applied:
**sufficient (and, for noisy signals, verified) signal → class-correct → backtested improvement over
the incumbent on held-out data → human gate → canary with a regression threshold.** Acceptance is a
declared threshold, not a judgement call. The full sketch — both axes, the bar, joins, ingest
shapes, open questions — is in [`docs/feedback-loops.md`](docs/feedback-loops.md) and
[`prd/drafts/kaizen-pipe.md`](prd/drafts/kaizen-pipe.md).

- **Now — skill crystallization (frequency-triggered).** The Analyst watches the journal
  for repeated work and proposes promoting it to **standard work** (a named skill,
  ideally deterministic/templated). The automated "$3 → $0.21" move. Needs only the
  journal. *Candidate filter = cost × repetition, gated by a **substitutability** signal
  (rev-1 M8): only propose where output **variance** across repetitions is low and the
  Skill-Lab check-pass-rate stays high — frequency ≠ value.*
- **Next — HITL preference (selection-triggered).** Accumulated human decisions — every
  `hold` selection, `rank` pick, and reject + managerial note — are *already journaled* and
  keyed to the card (§4A), so this loop needs **no new ingestion infra** and is available from
  day one of any HITL flow. It tunes the critic's rubric toward human selections (sharpening
  the cheap pre-filter so the human is bothered less) and can **retire its own gate**: when the
  human stops overriding the critic, drop the `hold` to auto-proceed. Cheaper than market —
  **build it before market.**
- **Next — defect closure (escaped-defect-triggered).** The correctness axis. A recurring defect
  class caught *after* the in-flow check — a branch-vs-main code review, a CI failure, an incident —
  proposes **tightening that check** (sharper rubric clause or a new deterministic Hook). Findings
  join to the card via the **integrity-gate write-set** (`file → card`, durable, git-independent),
  conveniently aided by a `Card:`-trailer micro-commit while a branch is unsquashed. Journal/VCS-only
  → no attribution infra → **build before market.** Gate on recurrence + verification (an LLM
  reviewer hallucinates; CI flakes); a single finding is ordinary `check → rework` (§6), not a
  mutation.
- **Later — market feedback (outcome-triggered).** Real signal (clicks, signups,
  watch-time) drives "more like the winners"; the internal critic degrades to a cheap
  pre-filter. Needs attribution infra → **defer it**; you can't tune a loop before the flow
  reliably produces cheap assets. Ingest the cheap, **deterministic** way first — a CSV/Excel
  export channel (schema-validated, no agentic surface) before any API poll or agentic
  discovery.

**Two joins are the spine.** Learning connects an outcome to the decisions that caused it:
**attribution** (`outcome ↔ asset` — the only non-backfillable piece, rev-1 F3) and
**provenance** (`asset ↔ prompt/rubric/model/skill` — already in the journal). They rendezvous
on a stable asset ID.

**Insurance to take now:** `output.tag_assets` stamps every asset with a stable ID
(into the Meta CSV / UTMs). Attribution is the *only* loop piece that can't be backfilled
(rev-1 F3) — verify the ID survives every downstream hop before relying on it.

**Tune taste; tighten checks; never loosen risk.** Taste calibration adjusts market-replaceable
`taste` gates; defect closure *tightens* a check of any class — tightening a risk check because a
defect escaped is the goal, not a violation. What is forbidden is the *loosening* direction on risk:
no mutation may **loosen or retire** a `risk` gate (brand/safety/legal), which stays at least as hard
as it is regardless of signal (§6). Precisely: **loosen/retire ⊆ taste; tighten ⊆ any class.** And
**mutations are never auto-applied (rev-1 M7)**: every mutation clears the Acceptance Bar above and a
human gate (don't let a narrow Hook-test alone promote — reconcile with Principle 9), and is graded
on **held-out** data, never the signal that produced it. Skills/tunes are **versioned**, rolled out
with a canary, and watched post-promotion for check-pass/rework-rate regression, with a rollback
path.

---

## 14. The Work Bench

- **Skill Lab.** Run a single station/skill against mock data in isolation — to observe
  output changes, A/B a station's model, and measure a candidate model's **parse-miss
  rate** before it's allowed on a flow (§7). `transform`/`deterministic` stations are
  Unix filters — fixture-test them directly (`echo input.json | station`); `agentic`
  stations need the full harness and are tested against recorded tool-loops.
- **Hook Tests (mandatory).** Every enforcement Hook ships with unit tests. The Law must
  be *deterministic and tested*. *(A(i)-Team: an untested/disabled Law is how a flow
  learns to `rm -rf` the wrong directory.)*
- **Config contract test (adopted from A(i)-Team).** A fixture test validates `flow.yaml`
  and the kernel's understanding of it agree — and (if any rule is mirrored across
  languages) fails on divergence, exactly as A(i)-Team's Go/TS stage-matrix fixture does.

---

## 15. Execution Lifecycle

1. **Boot.** Verify `project_root`, provision tools, run the Bench, pin `flow_version`,
   **state-reconcile** (lease-based, §11): cards `working` without a live lease →
   `interrupted` → re-hydrate (checking the outbox for effectful stations, §5).
2. **Wave 1 (Definition).** Parent → Architect + Reviewer (rework-capped).
3. **Expansion.** Architect explodes into child cards; Kernel validates **acyclic deps +
   disjoint owned paths** before committing (§9).
4. **Execution.** The deterministic tick (§10) dispatches via atomic claim; per-station
   WIP, budgets, the Law, and heartbeat leases are enforced.
5. **Quality.** QC stations gate/rank; rework loops run, bounded by the four guards.
6. **Assembly.** Deterministic fan-in (per policy) emits the native artifact via its
   adapter; assets are tagged; effectful publishes go through the outbox.
7. **Audit & Retro.** The Analyst distills the journal into a post-mortem and proposes
   (human-gated) mutations.

---

## 16. Implementation Priority

Ordered so the kernel that proves the flow-shop comes first and the highest-risk surfaces
are de-risked early.

The station taxonomy (§4) lets the **highest-risk surface (the agentic Tool-Bridge/Law)
be deferred** — a `transform`+`deterministic` flow (Studio) ships without it.

1. **State DB + the state machine (§3).** The `(lane, status)` model, transition matrix
   loader + validator, and the deterministic tick skeleton (§10). Nothing else is correct
   without this.
2. **The atomic claim + heartbeat lease (§7, §11).** The single-linearization-point claim
   and lease-based reconcile.
3. **IPC bridge + the `transform` worker runtime (§4, §7).** A single model call +
   coercive-parse + one model adapter + output-schema validation. No tools, no Bash, no
   path-ownership — this is the cheap, safe majority of stations.
4. **Checkpoint with binding stamp + the outbox (§5).** Cost recovery *and* effectful
   safety together — the difference between recovery and corruption.
5. **The rework engine + four guards + fan-in policy + liveness watchdog (§6, §8).**
6. **Output adapters + asset tagging + the egress channel (§4, §4A, §13).** Format the
   artifact (Meta CSV) *and* the Slack egress channel — status thread, HITL selection,
   alerts, delivery. A customer's flow needs the human loop, so egress is in the MVP.
7. **The Bench + Hook tests + config contract test (§14).** Make the Law testable before
   relying on it.

   > **◆ MVP flow.** Steps 1–7 run any `transform`+`deterministic` flow end-to-end —
   > **Studio ships here**: CLI/manual trigger, Slack for status/HITL/delivery, none of
   > the agentic surface built.

8. **The ingress trigger-listener (§4A).** A thin webhook/Slack-events receiver that
   shells out to `conduit run` — turns hands-off triggering on without making the kernel
   always-on. Small; deferred past the egress-first MVP.
9. **The agentic Tool-Bridge + the Law (§7).** The tool loop, the Bash positive
   allowlist, path-ownership enforcement, the injection threat model, bounded parse/exec
   retry. The highest-risk surface — built only when a coding-style flow (A(i)-Team)
   needs `agentic` stations. Deferred, not foundational.
10. **The Analyst (kaizen pipe), skill-detection trigger only (§13).** Market-feedback
    deferred.

---

## 17. Future-watch (tracked, not yet designed)

- **F1 — Multi-host scale-out.** SQLite, sockets, the token bucket, snapshots, and leases
  assume **one host**. Documented as the boundary; horizontal scale needs a different
  store (Postgres/Redis) and a distributed claim.
- **F2 — Bucket vs WIP interaction (§8).** Global throttle + local pull can collapse
  throughput; may need bucket-aware dispatch.
- **F3 — Market-loop attribution (§13).** Asset tagging is non-backfillable; verify the
  ID survives every hop before relying on it.
- **F4 — OTel GenAI conventions** are still evolving; pin the version, expect churn.

---

## Appendix A — The Three Flows, mapped

| Aspect | A(i)-Team | Studio (ad factory) | Autocut |
|---|---|---|---|
| Substrate → product | PRD → tested code | idea → assets + CSV | raw video → YouTube |
| Station kinds (§4) | agentic + transform + deterministic | **transform + deterministic** (no agentic) | transform + deterministic (no agentic) |
| Needs the §7 agentic surface? | yes | **no** | no |
| Flow | multi-piece WIP | batch fan-out/fan-in | single-piece + intra-station concurrency |
| Check depth (Principle 3) | heavy (4 gates) — expensive outer loop | light — cheap market signal | medium |
| `check.kind` | gate | rank → market | gate |
| `cap_policy` | scrap | proceed / market-graded | proceed_with_findings |
| Plan-first QC | code *is* its own proxy | critique prompt before image gen | critique edit plan before render |
| Effectful stations | git commit | publish CSV / ads | render + upload |
| Cost floor | input context | image generation | render |

The engine is identical across all three. The columns are entries in `flow.yaml`.

## Appendix B — Patterns modeled on the A(i)-Team reference implementation

| Conduit mechanism (§) | A(i)-Team pattern | Why it's proven |
|---|---|---|
| Lane graph + transition matrix (§3) | explicit stage enum + validated transition matrix, cross-language fixture-tested | runs every mission; fixture catches matrix drift between implementations |
| Earliest-flagged-station rework routing (§3) | handoff enforcement routing to the earliest implicated stage | routes test-coverage gaps to `testing` before `implementing`, not just "one step back" |
| Deterministic tick / action plan (§10) | pure planner that reads board state and emits a bounded action plan | replaced an LLM orchestrator that ran away 40h |
| Generation-keyed idempotent action IDs (§10) | action IDs encode the card's rework generation | fixed prefix-dedup that was blocking legitimate rework re-dispatch |
| Reclaim / timeout compensation (§10) | reclaim-stuck-slots pass on each tick | recovers dispatches whose effects never materialized |
| Rework cap → scrap, in the planner (§6) | planner-side cap enforcement, not just API-layer | bounds runaway in the durable layer where it can't be bypassed |
| Wall-clock backstop (§8) | controller-level wall-clock budget | the andon that would've stopped the 40h run |
| Per-station WIP, atomic claim (§7, §8) | atomic single-substrate claim for slot + WIP together | "WIP is per-stage, never global" |
| Dependency waves + DFS cycle detection (§9) | dep-check with DFS cycle detection and ready/waiting categorization | emergent waves; rejects illegal dep cycles before dispatch |
| `needsJudgment` fail-closed escalation (§10) | structured escalation payload instead of guessing on ambiguous state | "escalate ambiguity, never guess" |
| The Law = enforcement hooks + tests (§7, §14) | enforcement hooks with mandatory unit tests | a disabled hook once allowed an `rm -rf` — the Law is not optional |
| Work-item payload shape (§5) | `objective / acceptance / context / outputs / depends_on` item shape | the maker/critic contract that lets a critic map findings to criteria |
| OTel-aligned per-agent cost telemetry (§11) | per-agent token-usage aggregation in observer hooks | per-station cost attribution without parsing provider transcripts |

> The flow is config. The kernel is the product.
