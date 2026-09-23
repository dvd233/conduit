/**
 * Controller-driven production executor (WI-356).
 *
 * `runExecutor` drives one or more seeded cards from their initial lane to a
 * terminal lane (done / scrap / hold) by consuming the REAL planTick action
 * plan and routing via the REAL transition matrix. No LLM enters the control
 * loop — the adapter is invoked only inside station workers and the gate critic
 * (NFR-3).
 *
 * Wired primitives (DO NOT reinvent):
 *   - planTick (controller/tick.ts) — deterministic action plan
 *   - attemptClaim / beginWork (dispatch/claim.ts) — atomic slot reservation
 *   - checkCommandAllowed / runDeterministic (worker/deterministic.ts)
 *   - runTransformStation (worker/transform.ts)
 *   - renderPrompt (flow/render.ts)
 *   - buildOutputSchema (flow/schema.ts)
 *   - runGateRework (controller/gate-rework.ts)
 *   - computeBindingStamp / writeCheckpoint (checkpoint/checkpoint.ts)
 *   - checkConsumptionAndon / checkLiveness (control/watchdog.ts)
 *   - appendJournalSpan (persistence/db.ts)
 *
 * Observable contract (returns void; callers read side effects):
 *   - card lane/status in DB — getCard()
 *   - token/cost spans — getStationUsage() / getJournalSpans()
 *   - checkpoints.binding_stamp per completed station; matching stamp on resume
 *     skips the station and reuses stored output without re-billing (SPEC §5)
 *   - halt/escalation reasons — io.out / io.err
 */

import { readFileSync, writeFileSync, existsSync, realpathSync, readdirSync, statSync, readlinkSync, mkdirSync, unlinkSync } from 'node:fs';
import { join, resolve, sep, dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import type { Database } from 'bun:sqlite';

import type { RunEngineArgs, SpawnedWorker } from '../cli/main';
import { deriveSubflowRunId } from '../run/run-id';
import type { StartWorkMessage } from '../worker/ipc-protocol';
import { sanitizeStderrTail } from '../worker/ipc-protocol';
import type { ModelAdapter } from '../worker/adapter';
import { DEFAULT_RUN_ID, type ConduitDB, type JournalSpanInput } from '../persistence/db';
import type { FlowConfig, StationConfig, FanInPolicyConfig, StationOutput, Card } from '../types/kernel';
import { planTick } from './tick';
import { attemptClaim, beginWork, renewLease, reconcile } from '../dispatch/claim';
import { checkCommandAllowed, runDeterministic, deterministicCardEnv } from '../worker/deterministic';
import { runTransformStation, coerciveParse, computeFindingsHash } from '../worker/transform';
import type {
  HarnessRegistry, MountedInput, HarnessResult, KnownUsage, RateLimitSnapshot,
} from '../worker/harness-adapter';
import { usageFromThrow } from '../worker/harness-adapter';
import { harnessRetryDelayMs } from '../worker/harness-retry';
import { loadImageInput, hashImageInputs, assertImagePayloadWithinLimits } from '../worker/image-input';
import type { ImageInput } from '../worker/image-input';
import { renderPrompt } from '../flow/render';
import { buildOutputSchema } from '../flow/schema';
import { runGateRework } from './gate-rework';
import {
  computeBindingStamp,
  computeSkillAwarePromptTemplateVersion,
  writeCheckpoint,
  readCheckpoint,
  invalidateCheckpoint,
  cascadeInvalidation,
  ensureCheckpointSchema,
  writePendingIntent,
  commitIntent,
  discardIntent,
  reconcileOnResume,
} from '../checkpoint/checkpoint';
import type { FlowGraph } from '../checkpoint/checkpoint';
import { checkIntegrity } from '../worker/integrity';
import type { IntegrityResult } from '../worker/integrity';
import { checkConsumptionAndon, checkLiveness, planDrain, type ConsumptionAndon } from '../control/watchdog';
import { formatReleaseAt, soonestRateLimitGate } from '../run/run-state';
import type { WorkerSlot } from '../control/watchdog';
import { transition } from '../statemachine/transitions';
import type { FsmState, TransitionContext } from '../statemachine/transitions';
import { commitFanOut, evaluateFanIn } from '../dag/expand';
import type { ArchitectProposal, FanInPolicy, FanInState } from '../dag/expand';
import { runRankCheck, decideFromCandidates, parseCandidatesArtifact, type RankDecision } from '../quality/rank';
import { aggregateByWave, checkWaveBudget, countGateReworks, decideExecutionRetry } from '../quality/rework';
import type { CardUsage, BudgetCaps } from '../quality/rework';
import {
  egressSend,
  egressSendFile,
  postHitlHold,
  getRecordedHitlSelection,
  getRecordedHitlSelectionDetail,
  createSlackTransport,
  createFilesInfoReconciler,
  applyHoldTimeout,
  SLACK_FETCH_TIMEOUT_MS,
  SLACK_UPLOAD_TIMEOUT_MS,
} from '../channels/slack';
import type { OnTimeout } from '../channels/slack';
import { resolveDeliveryChannel } from '../flow/load';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Worker lease duration in seconds — long enough for any single station call. */
const LEASE_SECONDS = 600;

/**
 * Upper bound (ms) on how long the event-driven loop blocks awaiting the next
 * worker MARK_DONE before re-evaluating. A real worker that crashes never sends
 * MARK_DONE; this ceiling lets the loop wake to run reconcile + the wall-clock
 * andon rather than block forever. Synchronous test fakes complete inside
 * worker.send() and never reach this wait, so the timeout is production-only.
 */
const POOL_WAIT_TIMEOUT_MS = 250;

/** Worker ID prefix for this single-process executor. */
const WORKER_ID_PREFIX = 'executor';

/**
 * Default wall-clock bound (ms) for a harness invocation when the station
 * declares no `timeout_seconds` — harness attempts run 3-4 minutes by design
 * (SPEC §7 / the agentic-harness-worker PRD), far longer than a transform's
 * single model call, so this default is generous rather than reusing a
 * transform-scale timeout.
 */
const DEFAULT_HARNESS_TIMEOUT_MS = 5 * 60 * 1000;

/**
 * How long to park a rate-limited card when the provider reported no reset time.
 *
 * Deliberately short. A missing reset is a gap in the signal, not evidence of a
 * long cap, so the cheap mistake is waking early and re-parking on a fresh 429 —
 * the expensive one is idling a run for hours on a guess.
 */
const DEFAULT_RATE_LIMIT_PARK_SECONDS = 5 * 60;

/**
 * Longest a SINGLE park may be, regardless of how far out the provider's reset
 * is.
 *
 * A real session cap can reset hours away, and the run loop's release-gate wait
 * is a blocking sleep — so an uncapped park is a literal multi-hour `sleep()`
 * inside the executor. That should be a decision, not a side effect of trusting
 * whatever the provider reported.
 *
 * Capping it costs one extra CLI call per hour while a cap persists (the card
 * wakes, is capped again, re-parks) and buys three things: the consumption andon
 * is re-evaluated each time rather than once before a long sleep, the reset
 * estimate is refreshed instead of trusted for hours, and a run that is going
 * to halt on wall clock does so promptly rather than after sleeping through the
 * budget it had already exhausted.
 */
const MAX_RATE_LIMIT_PARK_SECONDS = 60 * 60;

/**
 * How many times in a row one card may park on a provider cap before the
 * kernel stops waiting and asks a human.
 *
 * A park deliberately spends NO execution attempt (issue #3), which means none
 * of the four rework guards (SPEC §6) bounds it: guard #1 skips a
 * 'rate_limited' card_log entry by design, guard #2's counter is untouched,
 * guard #3 has no findings hash to compare, and guard #4 halts the RUN — which
 * the ingress listener now resumes unattended, with a fresh per-process budget.
 * A station that fails in a way the classifier reads as a cap therefore parks,
 * wakes, fails, and parks again forever, spending nothing and alerting once.
 *
 * So the parks themselves are counted. The threshold is deliberately generous:
 * a real multi-hour cap is chunked into MAX_RATE_LIMIT_PARK_SECONDS pieces, so
 * this is roughly half a day of honest waiting before the card hard-pauses to
 * `hold` with a reason naming the repetition (SPEC: escalate ambiguity, never
 * guess). Consecutive means consecutive: any invocation at the station that
 * is not a cap — a success, a scrap, an integrity violation — resets the
 * count, so a cap that clears and returns later starts over. The count
 * includes the park being attempted, so the Nth consecutive cap holds the
 * card instead of parking it.
 */
const MAX_CONSECUTIVE_RATE_LIMIT_PARKS = 12;

/**
 * Convert a provider's absolute reset instant into a `cards.release_at` value.
 *
 * UNITS AND CLOCKS, both of which matter. `release_at` is compared against the
 * INJECTED now (tick.ts), in SECONDS — that is what keeps the release gate
 * deterministic and crash/resume-safe, exactly like the fan-out stagger's
 * `now + staggerSeconds`. The provider, meanwhile, reports a real wall-clock
 * epoch in milliseconds. Writing that epoch straight into the column would mix
 * both units AND both clocks, parking the card roughly fifty thousand years out.
 *
 * So we take the provider's reset as a DURATION against real time, and add that
 * duration to the kernel's own clock. A reset already in the past, or absent,
 * falls back to the default park rather than releasing instantly into the same
 * cap.
 */
export function releaseAtForRateLimit(
  injectedNowSeconds: number,
  resetAtMs: number | undefined,
  realNowMs: number,
): number {
  const remainingSeconds =
    resetAtMs !== undefined && resetAtMs > realNowMs
      ? Math.ceil((resetAtMs - realNowMs) / 1000)
      : DEFAULT_RATE_LIMIT_PARK_SECONDS;
  return injectedNowSeconds + Math.min(remainingSeconds, MAX_RATE_LIMIT_PARK_SECONDS);
}

/**
 * Map an adapter's structured usage onto the journal's columns (issue #5).
 *
 * WITHOUT a breakdown this reproduces the pre-#5 shape exactly — the total in
 * `inputTokens`, `outputTokens` 0, cache columns left NULL — so an adapter that
 * cannot split its usage is unchanged and still counted once by
 * getRunUsageTotals, which COALESCEs the absent columns.
 *
 * WITH one, `inputTokens` narrows to UNCACHED input and the other three classes
 * get their own columns. The four then sum to the same total the budget folds.
 */
function harnessJournalUsage(
  reported: KnownUsage,
  effectiveModel: string | undefined,
): JournalSpanInput['usage'] {
  const { breakdown } = reported;
  if (breakdown === undefined) {
    return {
      model: reported.model ?? effectiveModel ?? '',
      inputTokens: reported.tokens,
      outputTokens: 0,
      costUsd: reported.cost,
    };
  }
  return {
    // The provider's own label for what it billed beats the station's requested
    // model, which may be an alias.
    model: reported.model ?? effectiveModel ?? '',
    inputTokens: breakdown.inputTokens,
    outputTokens: breakdown.outputTokens,
    costUsd: reported.cost,
    cacheReadInputTokens: breakdown.cacheReadInputTokens,
    cacheCreationInputTokens: breakdown.cacheCreationInputTokens,
  };
}

/**
 * Flatten a capacity snapshot into journal attributes (issue #5).
 *
 * Recorded per call so "what did this run draw against the plan" is a query
 * rather than an inference from interactive usage bars.
 */
function rateLimitAttributes(snapshot: RateLimitSnapshot | undefined): Record<string, unknown> {
  if (snapshot === undefined) return {};
  const attrs: Record<string, unknown> = {};
  if (snapshot.status !== undefined) attrs['rate_limit_status'] = snapshot.status;
  if (snapshot.usingOverage !== undefined) attrs['rate_limit_using_overage'] = snapshot.usingOverage;
  for (const w of snapshot.windows) {
    attrs[`rate_limit_${w.name}_utilization`] = w.utilization;
    attrs[`rate_limit_${w.name}_resets_at`] = w.resetsAtMs;
  }
  return attrs;
}

/** True when some ready card is still gated behind a future release_at. */
function hasReleaseGatedCards(stateDb: Database, runId: string, nowSeconds: number): boolean {
  const row = stateDb
    .prepare(
      `SELECT 1 AS gated FROM cards
       WHERE run_id = $runId AND status = 'ready'
         -- >= not >: on the tick the gate OPENS, the card is dispatchable
         -- later in this same tick (planTick uses a strict release_at > now),
         -- but the liveness check runs first. With a strict >, a park longer than
         -- no_progress_minutes trips the watchdog on exactly the tick the card
         -- was about to run — killing the run one step before it recovered.
         AND release_at IS NOT NULL AND release_at >= $now
       LIMIT 1`,
    )
    // bun:sqlite's .get() yields null (not undefined) for no rows — a `!==
    // undefined` test here reports EVERY run as gated and silently disables the
    // liveness watchdog.
    .get({ $runId: runId, $now: nowSeconds }) as { gated: number } | null | undefined;
  return row != null;
}

/** Composite key for workerHandles entries — NUL separator prevents cardId injection. */
function slotKey(cardId: string, station: string): string {
  return `${cardId}\0${station}`;
}

// ---------------------------------------------------------------------------
// Public export
// ---------------------------------------------------------------------------

/**
 * Run the flow until all cards are in terminal lanes, the consumption andon
 * trips, or the liveness watchdog fires.
 *
 * `args.now` is called once per tick loop iteration and MUST NOT be called more
 * than necessary — injected clocks in tests may advance on every invocation.
 * The two documented exceptions both re-sample a FRESH instant because real
 * wall-clock time can elapse OUTSIDE that per-tick sample: the pool onMessage
 * handler (`handlerNow`, an async IPC callback) and the `trackingAdapter` call
 * wrapper (WI-33: stamps `lastAdapterActivityAt` when a synchronous model call
 * completes, since that call can itself run long past the tick's `currentNow`).
 */
export async function runExecutor(args: RunEngineArgs): Promise<void> {
  const {
    db,
    flow,
    now,
    adapter,
    io,
    concurrency = 1,
    spawn,
    onMessage,
    sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
  } = args;
  const runId = args.runId ?? DEFAULT_RUN_ID;
  const stateDb = db.getStateDb();

  // Ensure the checkpoint table exists (idempotent).
  ensureCheckpointSchema(stateDb);

  // ── Build context from flow config ────────────────────────────────────────
  const terminalLanes = new Set(flow.terminal_lanes ?? ['done', 'scrap', 'hold']);
  // The original input-validation work: lane names come from flow.yaml (`terminal_lanes`) with no charset
  // restriction — building a `NOT IN (...)` clause by string-interpolating them
  // is a SQL-injection / malformed-config crash sink. Bind them as named
  // parameters instead (see laneExclusionClause below).
  const { placeholders: terminalPlaceholders, params: terminalParams } = laneExclusionClause([
    ...terminalLanes,
  ]);

  const wipCaps: Record<string, number> = {};
  for (const [id, station] of Object.entries(flow.stations)) {
    wipCaps[id] = station.wip ?? 1;
  }

  // Derive the bash allowlist from all deterministic station commands. The
  // loader already validated each command against security.bash.allow, so
  // the union of all declared commands is the correct runtime allowlist.
  const commandAllowlist: string[] = [];
  for (const station of Object.values(flow.stations)) {
    if (station.kind === 'deterministic' && station.command) {
      commandAllowlist.push(station.command);
    }
  }

  // Build happyPathNext — use the loader-built surface (FR-2: never insertion order).
  const happyPathNext: Record<string, string | null> = {};
  if (flow.happyPathNext) {
    for (const [id, next] of Object.entries(flow.happyPathNext)) {
      happyPathNext[id] = next;
    }
  } else {
    // Pre-WI-351 fallback: insertion order (shouldn't happen with real flows).
    const ids = Object.keys(flow.stations);
    for (let i = 0; i < ids.length; i++) {
      happyPathNext[ids[i]!] = i + 1 < ids.length ? ids[i + 1]! : null;
    }
  }

  // ── Budget parameters ─────────────────────────────────────────────────────
  // Budget overrides (the original multi-flow engine work): a parent flow invoking this run as a subflow
  // passes its REMAINING budget; the tighter ceiling always wins (min), so a
  // child can never out-spend either its own declaration or its caller.
  const wallClockSeconds = Math.min(
    (flow.budgets?.run?.wall_clock_minutes ?? 10) * 60,
    args.budgetWallClockSeconds ?? Infinity,
  );
  const maxTokens = Math.min(
    flow.budgets?.run?.max_tokens ?? Infinity,
    args.budgetMaxTokens ?? Infinity,
  );
  const noProgressSeconds = (flow.budgets?.liveness?.no_progress_minutes ?? 3) * 60;
  const maxExecutionAttempts = flow.budgets?.per_card?.max_execution_attempts ?? 4;

  // Per-wave (parent_id subtree) budget caps — guard #4's wave scope (SPEC §6/§8).
  // A missing cap means "unbounded"; the gate is active only when at least one
  // cap is declared. Like the run andon, this tracks spend per run-invocation.
  const waveCaps: BudgetCaps = {
    maxTokens: flow.budgets?.per_wave?.max_tokens,
    maxDispatches: flow.budgets?.per_wave?.max_dispatches,
  };
  const waveBudgetActive = waveCaps.maxTokens !== undefined || waveCaps.maxDispatches !== undefined;

  // ── Mutable run state ─────────────────────────────────────────────────────
  let tokensSpent = 0;
  const runStartedAt = now();
  let lastLaneChangeAt = runStartedAt;
  // WI-33: last time a synchronous adapter call (transform/gate/rank) COMPLETED,
  // sampled fresh (not the stale per-tick `currentNow`) so a long in-process model
  // call registers as liveness progress even before it produces a lane change —
  // see the trackingAdapter wrapper below and the liveness-check call site.
  let lastAdapterActivityAt = runStartedAt;
  let halted = false;
  let andonTripped = false;

  // Pool mode: real out-of-process workers report MARK_DONE asynchronously over
  // IPC (vs. the synchronous in-process path). Both seams must be wired.
  const poolMode = spawn !== undefined && onMessage !== undefined;

  // Event-driven wake primitive. When the loop has dispatched all it can but
  // workers are still in flight, it awaits `waitForWorkerEvent`; the MARK_DONE
  // branch of the onMessage handler resolves it via `signalWorkerEvent`. Created
  // synchronously and awaited in the same run so a MARK_DONE queued on the IPC
  // channel cannot be lost (single-threaded JS: the handler only fires while we
  // await). A timeout bounds the wait so wall-clock/liveness still advance if a
  // worker dies without ever sending MARK_DONE.
  let wakeResolve: (() => void) | null = null;
  const signalWorkerEvent = (): void => {
    if (wakeResolve !== null) {
      const r = wakeResolve;
      wakeResolve = null;
      r();
    }
  };
  const waitForWorkerEvent = (): Promise<void> =>
    new Promise<void>((res) => {
      let timer: ReturnType<typeof setTimeout> | null = setTimeout(() => {
        timer = null;
        signalWorkerEvent();
      }, POOL_WAIT_TIMEOUT_MS);
      wakeResolve = () => {
        if (timer !== null) {
          clearTimeout(timer);
          timer = null;
        }
        res();
      };
    });

  // Worker handles keyed by `cardId\0station` — retained after spawn so the
  // drain path can call kill()/drain() on each in-flight worker at andon trip.
  const workerHandles = new Map<string, SpawnedWorker>();

  // Per-card token + dispatch attribution for the wave budget. `currentCardId`
  // is set around each station execution so the trackingAdapter can attribute a
  // model call (worker OR gate critic) to the card it ran for.
  const cardTokens = new Map<string, number>();
  const cardDispatches = new Map<string, number>();
  let currentCardId: string | null = null;

  // Per-async-context card attribution for the CONCURRENT transform path (v10):
  // when transform siblings run as overlapping in-process adapter calls, the
  // single `currentCardId` mutable can't tell them apart across interleaved
  // awaits. Each concurrent executeStation runs inside attributionStore.run({cardId}),
  // and trackingAdapter/foldHarnessUsage read the ALS store first, falling back to
  // `currentCardId` for the untouched serial path. No store bound → serial semantics.
  const attributionStore = new AsyncLocalStorage<{ cardId: string }>();
  const attributedCardId = (): string | null =>
    attributionStore.getStore()?.cardId ?? currentCardId;

  // Wrap the adapter to track cumulative token spend (NFR-3: adapter is the
  // ONLY model surface; the control loop itself never calls it).
  const trackingAdapter: ModelAdapter = {
    async call(req) {
      // The original adapter-liveness work: also stamp liveness progress the instant the call STARTS,
      // not only on completion. Without this, a legitimately long in-flight
      // call (a multi-hundred-thousand-token reasoning call, seen in
      // production on print-farm a pre-public review) leaves `lastAdapterActivityAt` at
      // whatever stale value the PRECEDING call left it at for the entire
      // duration of THIS call — so a liveness check evaluated while the call
      // is still in flight reads "no progress + no active worker" (in-process
      // calls never appear in `active_workers`) and kills a run that is, in
      // fact, actively making progress. Stamping fresh here means the instant
      // a call begins, the full no-progress window restarts from zero.
      lastAdapterActivityAt = now();
      const result = await adapter.call(req);
      // WI-33: stamp liveness progress the instant this call completes. Every
      // synchronous-path model call (transform worker, gate critic, rank check)
      // routes through this single wrapper, so this is the one place needed to
      // make adapter activity count as progress. Sampled with a FRESH now() call
      // (mirroring the pool onMessage handler's `handlerNow` precedent) rather
      // than reusing the tick's `currentNow`, because `currentNow` is sampled
      // ONCE at the top of the tick and can be stale by minutes once a
      // long-running synchronous call (this one) finishes.
      lastAdapterActivityAt = now();
      const spent = result.inputTokens + result.outputTokens;
      tokensSpent += spent;
      const attributed = attributedCardId();
      if (attributed !== null) {
        cardTokens.set(attributed, (cardTokens.get(attributed) ?? 0) + spent);
      }
      return result;
    },
  };

  // WI-567: harness usage never routes through trackingAdapter (a harness
  // maker never touches the ModelAdapter — WI-565), so it folds into the
  // SAME accumulators via this sibling closure instead, keyed off the same
  // `currentCardId` the synchronous dispatch path already sets. This is what
  // lets the existing consumptionAndon / wave-budget checks see harness spend.
  const foldHarnessUsage = (tokens: number): void => {
    tokensSpent += tokens;
    const attributed = attributedCardId();
    if (attributed !== null) {
      cardTokens.set(attributed, (cardTokens.get(attributed) ?? 0) + tokens);
    }
  };

  // WI-567 liveness fix (FR-8): stamp fresh liveness progress the instant a
  // harness invoke() resolves — mirroring trackingAdapter's lastAdapterActivityAt
  // stamp above. Without this, executeHarnessStation's lastLaneChangeAt reuses
  // the tick's stale `currentNow` (sampled once at the top of the tick, BEFORE
  // a long invoke() await resolves), and unlike transform/agentic there is no
  // compensating fresh stamp — a long-running harness attempt can trip a FALSE
  // liveness stall (checkLiveness takes max(lastLaneChangeAt,
  // lastAdapterActivityAt), watchdog.ts). Called at EVERY point a harness
  // invoke() settles — success, a paid failure, AND a throw (timeout/nonzero
  // exit) — since even a failed attempt consumed real wall-clock and must
  // count as progress; it cannot piggyback on foldHarnessUsage, which only
  // fires when usage is known.
  const stampHarnessActivity = (): void => {
    lastAdapterActivityAt = now();
  };

  const projectRoot = args.projectRoot ?? resolve(flow.project_root ?? '.');

  // ── Pool IPC handler ─────────────────────────────────────────────────────
  // Register exactly one inbound-IPC handler via the onMessage seam. The kernel
  // is the SOLE writer (NFR-3): all state-DB mutations for MARK_DONE/HEARTBEAT
  // happen here, never inside the worker. The handler is registered once before
  // the main loop; tests drive it synchronously from inside the spawn.send fake.
  // Active when onMessage is provided regardless of concurrency level — the pool
  // path fires whenever spawn+onMessage are both wired (see dispatch gate below).
  if (onMessage !== undefined) {
    onMessage((msg) => {
      // #7: this is an async IPC callback, NOT the per-tick loop body, so the
      // "now() once per tick" rule (see the runExecutor doc) does not apply here —
      // it samples the clock per inbound message. A HEARTBEAT must renew the lease
      // to the CURRENT instant (a stale tick timestamp would never push the lease
      // forward), and a MARK_DONE records real progress time. now() is monotonic
      // wall-clock in production; tests inject the clock they need.
      const handlerNow = now();
      if (msg.type === 'HEARTBEAT') {
        // Extend the worker's lease so it is not falsely reclaimed.
        renewLease(db, msg.cardId, msg.station, handlerNow, LEASE_SECONDS, runId);
        return;
      }

      if (msg.type === 'MARK_DONE') {
        const card = db.getCard(runId, msg.cardId);
        if (!card) return;
        if (terminalLanes.has(card.lane)) return; // idempotent: ignore duplicate for terminal cards

        // Idempotency guards: drop stale messages so a duplicate MARK_DONE
        // from a crashed/retried worker never double-advances the card.
        if (card.lane !== msg.station) return; // stale station
        if (card.attempt !== msg.attempt) return; // stale attempt
        const activeRow = stateDb
          .prepare('SELECT 1 AS ok FROM active_workers WHERE run_id = $runId AND card_id = $cardId AND station = $station')
          .get({ $runId: runId, $cardId: msg.cardId, $station: msg.station });
        if (!activeRow) return; // slot already released — completion already processed

        // #3: fold the worker's reported spend into the run + per-card budgets
        // BEFORE the outcome branches, so the consumption andon and the wave
        // budget see pooled-worker tokens (deterministic stations report 0).
        // Folded here (not via trackingAdapter, which only the in-process path
        // drives) because a pooled worker bills out-of-process.
        const reportedTokens = msg.usage?.tokens ?? 0;
        if (reportedTokens > 0) {
          tokensSpent += reportedTokens;
          cardTokens.set(msg.cardId, (cardTokens.get(msg.cardId) ?? 0) + reportedTokens);
        }

        if (msg.outcome === 'success') {
          // Post-work transition: INTEGRITY_PASS through the FSM (same as the
          // synchronous deterministic path). The card is in 'working' (beginWork
          // ran at dispatch); the synthetic done_pending_ack state collapses the
          // MARK_DONE→done_pending_ack→INTEGRITY_PASS pair into the single advance
          // the kernel applies here.
          const ctx = buildTransitionContext(
            msg.station, flow, happyPathNext, terminalLanes, maxExecutionAttempts,
          );
          const fsmState = syntheticDonePendingAck(card, REWORK_COUNT_UNREAD);
          const fsmResult = transition(fsmState, { type: 'INTEGRITY_PASS' }, ctx);

          if (!fsmResult.ok) {
            escalateToHold(stateDb, db, msg.cardId, msg.station, card,
              `FSM illegal_transition on INTEGRITY_PASS for pool station '${msg.station}'`, io.err, runId);
            return;
          }

          const nextState = fsmResult.next;
          const isTerminal = terminalLanes.has(nextState.lane);
          const resolvedStatus = isTerminal ? 'complete' : nextState.status;

          // Write checkpoint before advancing (mirrors sync path ordering).
          // The worker produced output on disk; the kernel records a sentinel
          // checkpoint to mark station completion for resume skip-on-stamp.
          const poolCheckpointOutput: StationOutput<unknown> = {
            payload: null,
            findings_hash: '',
            return_to: null,
            usage: { tokens: 0, cost: 0 },
          };
          writeCheckpoint(stateDb,
            { run: runId, flow: String(flow.version), card: msg.cardId, station: msg.station, attempt: card.attempt },
            { stamp: '', output: poolCheckpointOutput },
          );

          advanceCard(stateDb, db, msg.cardId, msg.station, card.lane, card.attempt,
            nextState.lane, resolvedStatus, 0, undefined, runId);

          workerHandles.delete(slotKey(msg.cardId, msg.station));
          lastLaneChangeAt = handlerNow;

          // Re-plan: evaluate fan-in and re-dispatch freed slot.
          pollAwaitingChildren(db, stateDb, runId, flow, terminalLanes);
        } else if (msg.outcome === 'failed') {
          // Pooled deterministic command failure (the original deterministic failure-reporting work / the pre-public deterministic failure-reporting review):
          // apply the SAME count-and-retry accounting as the synchronous path —
          // attempt bump + re-ready below per_card.max_execution_attempts,
          // named scrap at it — so one flaky command behaves identically at
          // concurrency=1 and K>1. The retry re-enters the planner as an
          // ordinary ready card and re-dispatches through the pool at the
          // bumped attempt. handlerNow is fresh (sampled per message), so the
          // handler's own lastLaneChangeAt stamp below covers liveness.
          countDeterministicFailure(
            stateDb, db, runId, msg.cardId, msg.station, card,
            {
              exitCode: msg.failure?.exitCode ?? 1,
              stderr: msg.failure?.stderrTail ?? '',
              ...(msg.failure?.timedOut !== undefined && { timedOut: msg.failure.timedOut }),
            },
            maxExecutionAttempts,
          );
          workerHandles.delete(slotKey(msg.cardId, msg.station));
          lastLaneChangeAt = handlerNow;
        } else {
          // failure outcome ('rework' / 'scrap'): route through the FSM with
          // cap_policy so rework back-edges and per-card caps are honoured,
          // mirroring the synchronous path.
          if (msg.outcome === 'rework') {
            const ctx = buildTransitionContext(
              msg.station, flow, happyPathNext, terminalLanes, maxExecutionAttempts,
            );
            // Per-gate rework budget (issue #1): count only the reworks this
            // station's own gate has already spent, so the pooled path caps
            // identically to the synchronous gate path below rather than
            // inheriting an upstream gate's exhausted lifetime counter.
            const fsmState = syntheticDonePendingAck(
              card, gateReworksSpent(db, runId, msg.cardId, msg.station),
            );
            const rejectResult = transition(fsmState, { type: 'QC_REJECT', returnTo: card.lane }, ctx);
            if (!rejectResult.ok) {
              escalateToHold(stateDb, db, msg.cardId, msg.station, card,
                `FSM illegal_transition on pool QC_REJECT for station '${msg.station}'`, io.err, runId);
            } else {
              const rejectNext = rejectResult.next;
              const reworkDelta = rejectNext.reworkCount - fsmState.reworkCount;
              const isTerminal = terminalLanes.has(rejectNext.lane);
              advanceCard(stateDb, db, msg.cardId, msg.station, card.lane, card.attempt,
                rejectNext.lane, isTerminal ? 'complete' : rejectNext.status, reworkDelta,
                `worker outcome: ${msg.outcome}`, runId);
            }
          } else {
            // Direct 'scrap' signal from the worker.
            scrapCardDirect(stateDb, db, msg.cardId, msg.station, card, `worker outcome: ${msg.outcome}`, runId);
          }
          workerHandles.delete(slotKey(msg.cardId, msg.station));
          lastLaneChangeAt = handlerNow;
        }

        // Wake the event-driven loop: a slot just freed and the card advanced,
        // so the planner should re-run rather than block on its worker-event wait.
        signalWorkerEvent();
      }
    });
  }

  // ── Main control loop ─────────────────────────────────────────────────────
  while (!halted) {
    // Call now() ONCE per iteration — injected clocks advance on each call.
    const currentNow = now();

    // ── Live pool reconcile: reclaim dead-lease workers ───────────────────
    // In pool mode a worker can crash without ever sending MARK_DONE. Its slot
    // would otherwise pin the WIP cap forever. reconcile() flips any working
    // card whose lease expired (heartbeats stopped) to 'interrupted'; the
    // promote step below then re-readies it. Heartbeats from live workers renew
    // the lease, so a genuinely-busy worker is never reclaimed.
    if (poolMode) {
      const { interrupted } = reconcile(db, currentNow, runId);
      // Reap the orphaned subprocesses whose slots we just reclaimed so a hung
      // worker does not linger after its card is freed for re-dispatch.
      for (const id of interrupted) {
        for (const [key, handle] of workerHandles) {
          if (key.startsWith(`${id}\0`)) {
            handle.kill?.();
            workerHandles.delete(key);
          }
        }
      }
    }

    // ── Promote dispatchable cards in non-terminal lanes to 'ready' ──────
    // No dependency system in this flow set → waiting cards are always ready.
    // 'interrupted' cards (re-hydrated by reclaimOrphanedWorkers / reconcile on
    // resume) are likewise promoted so a crashed-mid-work card actually re-runs
    // its station — checkpoint skip-on-resume then avoids re-billing pure work.
    stateDb
      .prepare(`UPDATE cards SET status = 'ready' WHERE run_id = $runId AND status IN ('waiting', 'interrupted') AND lane NOT IN (${terminalPlaceholders})`)
      .run({ $runId: runId, ...terminalParams });

    // ── Count active workers ──────────────────────────────────────────────
    const { n: activeCount } = stateDb
      .prepare('SELECT COUNT(*) AS n FROM active_workers WHERE run_id = $runId')
      .get({ $runId: runId }) as { n: number };

    // ── Liveness watchdog (check BEFORE planning so a stuck card is caught) ─
    // Compute blocking-reason flags from DB state (SPEC §8: watchdog must report WHY).

    // True iff any card is frozen in 'held' status (escalation path — awaits human).
    const { n: heldCount } = stateDb
      .prepare("SELECT COUNT(*) AS n FROM cards WHERE run_id = $runId AND status = 'held'")
      .get({ $runId: runId }) as { n: number };
    const hasHoldAwaitingHuman = heldCount > 0;

    // True iff at least one 'ready' card sits on a station that is already at its
    // WIP cap. Compute per-station active-worker counts and compare to wipCaps for
    // every station that has a ready card waiting.
    const stationsWithReadyCards = (
      stateDb
        .prepare("SELECT lane FROM cards WHERE run_id = $runId AND status = 'ready' GROUP BY lane")
        .all({ $runId: runId }) as Array<{ lane: string }>
    ).map((r) => r.lane);

    const perStationActive: Record<string, number> = {};
    if (stationsWithReadyCards.length > 0) {
      const stationActiveRows = stateDb
        .prepare('SELECT station, COUNT(*) AS n FROM active_workers WHERE run_id = $runId GROUP BY station')
        .all({ $runId: runId }) as Array<{ station: string; n: number }>;
      for (const row of stationActiveRows) {
        perStationActive[row.station] = row.n;
      }
    }

    const hasReadyButNoIdleWorker = stationsWithReadyCards.some((lane) => {
      const cap = wipCaps[lane] ?? 1; // match the same default the claim logic uses
      const active = perStationActive[lane] ?? 0;
      return active >= cap;
    });

    // hasScrappedDep: N/A — this executor does not model inter-card dependency edges.
    // Cards are always promoted from 'waiting' to 'ready' unconditionally (see the
    // UPDATE above). Leave false — no fabricated check.

    const livenessAlert = checkLiveness(
      {
        now: currentNow,
        lastLaneChangeAt,
        // WI-33: a completed synchronous adapter call also counts as progress
        // (see the trackingAdapter wrapper above) — otherwise a long draft↔gate
        // rework cycle whose model call(s) alone exceed noProgressSeconds gets
        // declared stalled the instant the NEXT tick's fresh `currentNow` is
        // sampled, even though the run was busy the whole time.
        lastAdapterActivityAt,
        activeWorkerCount: activeCount,
        // A card parked behind a future release_at (issue #3's rate-limit park,
        // or the v10 fan-out stagger) is scheduled, not stalled — the loop is
        // already sleeping toward that gate a few lines below.
        hasReleaseGatedCard: hasReleaseGatedCards(stateDb, runId, currentNow),
        hasScrappedDep: false,
        hasHoldAwaitingHuman,
        hasReadyButNoIdleWorker,
      },
      { noProgressSeconds },
    );
    if (livenessAlert.tripped) {
      io.err(
        `watchdog: liveness stall — ${livenessAlert.detail ?? 'no progress and no active workers'}`,
      );
      break;
    }

    // ── Per-wave (subtree) budget — guard #4's wave scope (SPEC §6/§8) ─────
    // Partition per-card token/dispatch spend by parent_id and scrap any subtree
    // that has blown its cap. Unlike the run andon, this does NOT halt the run —
    // sibling subtrees under other parents keep running (blast-radius isolation).
    if (waveBudgetActive) {
      const byWave = aggregateByWave(buildWaveUsages(stateDb, runId, cardTokens, cardDispatches));
      for (const [parentId, usage] of byWave) {
        const decision = checkWaveBudget(parentId, usage, waveCaps);
        if (decision.action === 'scrap_subtree') {
          const scrapped = scrapWaveSubtree(db, stateDb, runId, parentId, [...terminalLanes], io);
          if (scrapped > 0) lastLaneChangeAt = currentNow; // progress: cards moved to scrap
        }
      }
    }

    // ── Plan tick — deterministic action plan, no LLM ────────────────────
    const plan = planTick(db, {
      flow: String(flow.version),
      now: currentNow,
      issuedActionIds: new Set(), // Fresh per tick: claim is the real idempotency gate
      wipCaps,
      busyWakeSeconds: 0,
      idleWakeSeconds: 0,
      runId,
    });

    // ── Handle escalations (contradictory state → hold + surface via io) ──
    for (const esc of plan.escalations) {
      io.err(`escalation: card ${esc.cardId} — ${esc.detail}`);

      // FR-4 / WI-381: append hold card_log entries before freezing card state.
      // Lane is unchanged on hold (NEEDS_JUDGMENT), so sourceLane === destLane.
      // station = card.lane (the station the card was at when escalated).
      // Journal-first: same phantom-safe ordering as advanceCard.
      const heldCard = db.getCard(runId, esc.cardId);
      if (heldCard) {
        db.appendCardLog({
          runId,
          kind: 'entered_lane',
          cardId: esc.cardId,
          station: heldCard.lane,
          attempt: heldCard.attempt,
          sourceLane: heldCard.lane,
          destLane: heldCard.lane,
          reasonClass: 'hold',
        });
        db.appendCardLog({
          runId,
          kind: 'terminal',
          cardId: esc.cardId,
          station: heldCard.lane,
          attempt: heldCard.attempt,
          reason: esc.detail,
        });
      }

      // Freeze the card in 'held' status (NEEDS_JUDGMENT — lane unchanged).
      stateDb.prepare("UPDATE cards SET status = 'held' WHERE run_id = $runId AND id = $id").run({ $runId: runId, $id: esc.cardId });
    }

    // ── Terminal check (nothing left to dispatch or execute) ──────────────
    if (plan.actions.length === 0 && plan.escalations.length === 0) {
      // (b) Fan-in evaluation BEFORE the terminal break — critical placement.
      // When an awaiting_children parent's children are all terminal but planTick
      // returns no actions (the pre-seeded case), this fires the fan-in and
      // re-queues the parent. Without this early call the loop would break on
      // the stall check BEFORE the bottom pollAwaitingChildren can run.
      const earlyFanInAdvanced = pollAwaitingChildren(db, stateDb, runId, flow, terminalLanes);
      if (earlyFanInAdvanced) {
        lastLaneChangeAt = currentNow;
        continue; // re-enter: planTick can now dispatch the newly-ready parent
      }

      // (c) Held rank cards with a recorded human selection (WI-398).
      // When a rank station parked a card as held and `conduit reply` has since
      // recorded a selection, un-hold the card here so planTick can dispatch it.
      // Without this, the loop would break before the un-held card is dispatched.
      const heldRankAdvanced = pollHeldRankCards(db, stateDb, runId, flow);
      if (heldRankAdvanced) {
        lastLaneChangeAt = currentNow;
        continue; // re-enter: un-held rank card is now ready, planTick dispatches it
      }

      // (d) HITL hold-timeout (WI-398 AC5 / FR-14, SPEC §4A). A held rank card
      // whose human-decision window has elapsed with NO recorded selection is
      // resolved by the egress channel's on_timeout policy via the existing
      // slack.ts::applyHoldTimeout primitive (scrap / proceed_with_findings /
      // escalate). The synchronous executor exits while a card is held, so the
      // timeout is enforced OPPORTUNISTICALLY — on every run/resume past the
      // deadline. A recorded reply (handled above) always beats a timeout.
      const timeoutResolved = pollHeldTimeouts(db, stateDb, runId, flow, currentNow, happyPathNext, terminalLanes);
      if (timeoutResolved) {
        lastLaneChangeAt = currentNow;
        continue; // re-enter: timed-out card is now scrapped / advanced / escalated
      }

      const { n: nonTerminalCount } = stateDb
        .prepare(`SELECT COUNT(*) AS n FROM cards WHERE run_id = $runId AND lane NOT IN (${terminalPlaceholders})`)
        .get({ $runId: runId, ...terminalParams }) as { n: number };
      if (nonTerminalCount === 0) break; // All cards at terminal lanes.

      // Pool mode: the planner is dry, but real workers may still be in flight
      // (their cards sit claimed/working, which the stuck-diagnostic below would
      // misread as a deadlock). Block until the next MARK_DONE frees a slot and
      // advances a card, then re-plan. The wait is timeout-bounded so a worker
      // that died without reporting still lets the loop wake to reconcile + check
      // the wall-clock andon. Only the synchronous path falls through to the
      // stall diagnostic (its workers complete inside worker.send()).
      if (poolMode) {
        const { n: inFlight } = stateDb
          .prepare('SELECT COUNT(*) AS n FROM active_workers WHERE run_id = $runId')
          .get({ $runId: runId }) as { n: number };
        if (inFlight > 0) {
          await waitForWorkerEvent();
          continue;
        }
      }

      // ── Release-gate wait (v10 fan-out stagger) ─────────────────────────
      // A card can be READY yet held behind a future release_at (the cache-warming
      // stagger); planTick reports zero actions for it. That is NOT a stall — sleep
      // until the earliest gate elapses, then re-tick. The wait horizon is the time
      // remaining to the soonest gate (from the MIN(release_at) query below), floored
      // at 1s; the executor derives it here rather than from plan.nextWakeSeconds
      // (which the run loop drives with busy/idleWakeSeconds=0). The sleep is
      // injectable (default setTimeout): production advances the wall clock past the
      // gate; a test injects a fast sleep so its advancing clock re-ticks without
      // burning real time.
      const releaseGate = stateDb
        .prepare(
          `SELECT MIN(release_at) AS soonest FROM cards
           WHERE run_id = $runId AND status = 'ready' AND release_at IS NOT NULL AND release_at > $now`,
        )
        .get({ $runId: runId, $now: currentNow }) as { soonest: number | null };
      if (releaseGate.soonest !== null) {
        // Honor the consumption andon BEFORE sleeping: a run that blows its
        // wall-clock/token budget mid-stagger must halt, not sleep on until the
        // gate opens. (No reclaim pass is needed here — this branch is only reached
        // with zero in-flight workers: the poolMode block above returns while any
        // worker is live, and in-process transforms complete within their tick.)
        const gateAndon = checkConsumptionAndon(
          { runStartedAt, now: currentNow, tokensSpent },
          { wallClockSeconds, maxTokens },
        );
        if (gateAndon.tripped) {
          if (!andonTripped) {
            andonTripped = true;
            io.err(consumptionHaltMessage(gateAndon.reason, soonestRateLimitGate(db, runId, currentNow)));
          }
          halted = true;
          break;
        }
        const waitSeconds = Math.max(1, releaseGate.soonest - currentNow);
        await sleep(waitSeconds * 1000);
        continue;
      }

      // Some non-terminal cards remain but planTick has no actions. Distinguish:
      //   • status='held' — intentionally paused awaiting human intervention;
      //     already surfaced by the escalation loop above. Exit cleanly/quietly.
      //   • Any other non-terminal status — genuine stall/deadlock (e.g. a card
      //     stranded in done_pending_ack on a work lane, a status the promote step
      //     above does not advance). Surface a loud diagnostic so the operator
      //     knows the run did NOT complete silently. Never silently halt on a stall.
      const { n: stuckCount } = stateDb
        .prepare(
          `SELECT COUNT(*) AS n FROM cards WHERE run_id = $runId AND lane NOT IN (${terminalPlaceholders}) AND status <> 'held'`,
        )
        .get({ $runId: runId, ...terminalParams }) as { n: number };
      if (stuckCount > 0) {
        io.err(
          `stall: ${stuckCount} card(s) are stuck in non-terminal lanes with no available action — run is halting. ` +
            `Inspect each card with: conduit journal inspect <cardId>`,
        );
      }
      // Exit the loop regardless: either all remaining cards are intentionally held
      // (clean exit) or we just surfaced the stall diagnostic (loud halt). There is
      // no next iteration that could make progress — breaking here avoids a busy-spin
      // for the full noProgressSeconds window before the liveness watchdog would trip.
      break;
    }

    // ── Execute planned actions ───────────────────────────────────────────
    // Two-pass: dispatch first (so newly spawned workers are in active_workers
    // before the andon check), then reclaim (stale-slot cleanup). The andon
    // check fires between the two passes so planDrain sees in-flight workers
    // from BOTH previous ticks AND this tick's dispatches, without past-lease
    // reclaims having already cleared their slots.
    // Event-driven bookkeeping (pool mode): did we spawn anything this tick, and
    // did the concurrency cap force us to defer a ready card? If we deferred but
    // spawned nothing new, the loop must AWAIT a worker completion rather than
    // busy-spin re-planning the same blocked actions.
    let dispatchedThisTick = false;
    let capBlocked = false;
    // v10: transform siblings collected during this pass for concurrent execution
    // afterwards (the fan-out reviewer case). Empty at concurrency===1.
    const concurrentBatch: Array<{ cardId: string; station: string }> = [];

    for (const action of plan.actions) {
      if (action.kind === 'reclaim') continue; // handled in second pass below

      // kind === 'dispatch'
      const stationConfig = flow.stations[action.station];
      if (!stationConfig) continue; // Unknown station — skip (fail-closed)

      // ── Pool eligibility ──────────────────────────────────────────────────
      // Only PLAIN pure deterministic stations are spawned as out-of-process
      // workers. Excluded (these stay on the synchronous in-process path because
      // the pool's MARK_DONE handler does a single plain advance and cannot do
      // their extra work):
      //   • transform/agentic — need the adapter + stationOutput in-process;
      //   • fan-out — handleFanOutComplete must read the proposal + seed children;
      //   • effectful — need the outbox + idempotency key (writePendingIntent/commit);
      //   • enforce_owned_paths flows — the deterministic integrity gate runs
      //     in-process; a pooled worker would bypass it (fail-closed: stay in-process).
      //   • gate-checked (`check:`, the original deterministic-gate work) — the MARK_DONE handler's "plain
      //     advance" has no gate-critic call and no gate_verdict journaling; a
      //     pooled gated deterministic station would silently skip its check.
      //     The critic call needs the in-process trackingAdapter, so these stay
      //     synchronous just like transform/agentic stations.
      //   • deliver-block stations (WI-599) — the pool's MARK_DONE handler (below)
      //     does a plain advance with no call to performStationDelivery; a pooled
      //     deliver-block station would silently skip its file delivery entirely
      //     under concurrency>1 (Stockwell FINAL-REVIEW finding). Stay in-process
      //     until the pool path grows a delivery call of its own.
      const poolEligible =
        stationConfig.kind === 'deterministic' &&
        !isFanOutStation(stationConfig) &&
        !stationConfig.effectful &&
        stationConfig.gateCheck === undefined &&
        stationConfig.deliver === undefined &&
        flow.defaults?.enforceOwnedPaths !== true;

      if ((concurrency > 1 || poolMode) && spawn !== undefined && poolEligible) {
        // ── Pool path (plain pure deterministic stations, spawn+onMessage wired) ─
        // The concurrency cap bounds in-flight count (min(K, station.wip)); the
        // path activates when concurrency>1 OR when both spawn+onMessage are wired
        // (allowing concurrency=1 pool tests with an explicit onMessage seam).
        //
        // Enforce the run-level concurrency cap before attempting to claim.
        // attemptClaim enforces the station-level wip cap; this guard enforces
        // the global K ceiling so at most min(K, station.wip) workers are in flight.
        const { n: currentInFlight } = stateDb
          .prepare('SELECT COUNT(*) AS n FROM active_workers WHERE run_id = $runId')
          .get({ $runId: runId }) as { n: number };
        if (currentInFlight >= concurrency) {
          capBlocked = true; // a ready card was deferred by the K ceiling
          continue;
        }

        // Claim the slot (wip + readiness check inside one IMMEDIATE txn).
        // Spawn AFTER claim so we have a real pid to write. Then update the pid.
        const poolClaim = attemptClaim(db, {
          cardId: action.cardId,
          station: action.station,
          workerId: `${WORKER_ID_PREFIX}-${action.cardId}`,
          wipCap: stationConfig.wip,
          now: currentNow,
          leaseSeconds: LEASE_SECONDS,
          runId,
        });
        if (!poolClaim.ok) continue;

        // #4: claimed → working + (re)stamp the lease. Without this a pooled card
        // sits at 'claimed' for its whole in-flight life, invisible to the live
        // reconcile (which targets status='working'), so a hung worker could pin
        // its slot until a resume-only reclaim. beginWork makes in-flight pool
        // state consistent with the synchronous path.
        beginWork(db, action.cardId, action.station, currentNow, LEASE_SECONDS, runId);

        // Count this dispatch for per-wave budget (guard #4).
        cardDispatches.set(action.cardId, (cardDispatches.get(action.cardId) ?? 0) + 1);

        // The card's attempt is echoed in START_WORK → MARK_DONE so a stale
        // completion from a reclaimed-and-re-dispatched worker is dropped. Its
        // rework_count rides along too so the pooled deterministic worker can
        // inject CONDUIT_REWORK_COUNT/CONDUIT_ATTEMPT — the same env the
        // synchronous path builds (otherwise a pool-eligible station would see
        // these only at concurrency=1). PATCH 2.
        const claimedCard = db.getCard(runId, action.cardId);
        const claimedAttempt = claimedCard?.attempt ?? 0;
        const claimedReworkCount = claimedCard?.rework_count ?? 0;

        // Spawn + pid-write + START_WORK — all must succeed atomically from the
        // slot's perspective. If any step throws (OS limit, IPC failure), release
        // the claimed slot and reset the card to 'ready' so a later tick can
        // re-dispatch it, then re-throw so the caller sees the error.
        try {
          // Spawn the worker harness to get the real OS pid.
          const worker = spawn({ cardId: action.cardId, station: action.station });

          // Retain handle for drain/kill at andon-trip time.
          workerHandles.set(slotKey(action.cardId, action.station), worker);

          // Persist the real pid into the active_workers row so dead-PID reclaim
          // (WI-467) can fire against this slot.
          stateDb
            .prepare('UPDATE active_workers SET pid = $pid WHERE run_id = $runId AND card_id = $cardId AND station = $station')
            .run({ $pid: worker.pid, $runId: runId, $cardId: action.cardId, $station: action.station });

          // Send START_WORK with input references (paths/names), never artifact bytes.
          const inputRefs: string[] = stationConfig.inputs ?? [];
          const startWork: StartWorkMessage = {
            type: 'START_WORK',
            cardId: action.cardId,
            station: action.station,
            attempt: claimedAttempt,
            reworkCount: claimedReworkCount,
            inputRefs,
          };
          worker.send(startWork);
          dispatchedThisTick = true;
        } catch (spawnErr) {
          // #5: a spawn/IPC failure (EMFILE, fork failure, dead child) must NOT
          // abort the whole run — other lanes keep flowing. But it also must not
          // silently re-dispatch forever: a permanently-failing spawn would spin
          // the card ready→claim→throw→ready. So we fail CLOSED — release the
          // claimed slot, then ESCALATE the card to 'hold' for a human (the cause
          // is an environment/infra problem, not a card problem; "escalate
          // ambiguity, never guess"). The DELETE is guarded to the slot we just
          // claimed so it cannot disturb another worker. The run continues.
          const failedCard = db.getCard(runId, action.cardId);
          stateDb
            .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $cardId AND station = $station')
            .run({ $runId: runId, $cardId: action.cardId, $station: action.station });
          workerHandles.delete(slotKey(action.cardId, action.station));
          if (failedCard && failedCard.status !== 'held') {
            escalateToHold(
              stateDb, db, action.cardId, action.station, failedCard,
              `worker spawn/IPC failed at station '${action.station}': ` +
                `${spawnErr instanceof Error ? spawnErr.message : String(spawnErr)} — ` +
                `card held for human reconciliation (likely an environment limit, e.g. process/file-descriptor exhaustion)`,
              io.err,
              runId,
            );
            lastLaneChangeAt = currentNow; // status changed → progress; keeps liveness honest
          }
          continue;
        }

        lastLaneChangeAt = currentNow;

        // Per-wave budget check after each pool dispatch (guard #4 wave scope).
        // cardDispatches is incremented at dispatch (above), so the dispatch count
        // is current here regardless of when MARK_DONE lands (synchronously inside
        // worker.send() for test fakes, or asynchronously over IPC for real
        // workers — token spend for the latter is reconciled by the top-of-loop
        // wave check once MARK_DONE arrives). Checking here scraps an over-budget
        // subtree before the next dispatch.
        if (waveBudgetActive) {
          const byWave = aggregateByWave(buildWaveUsages(stateDb, runId, cardTokens, cardDispatches));
          for (const [parentId, usage] of byWave) {
            const decision = checkWaveBudget(parentId, usage, waveCaps);
            if (decision.action === 'scrap_subtree') {
              const scrapped = scrapWaveSubtree(db, stateDb, runId, parentId, [...terminalLanes], io);
              if (scrapped > 0) lastLaneChangeAt = currentNow;
            }
          }
        }
      } else if (
        concurrency > 1 &&
        stationConfig.kind === 'transform' &&
        !isFanOutStation(stationConfig) &&
        !stationConfig.effectful &&
        stationConfig.gateCheck === undefined &&
        stationConfig.deliver === undefined &&
        !isRankStation(stationConfig)
      ) {
        // ── Concurrent transform path (v10) ──────────────────────────────────
        // Under concurrency>1, plain transform stations (the fan-out reviewer
        // case) run as overlapping in-process adapter calls rather than one at a
        // time. Collected here and awaited together after this dispatch pass, so
        // the K ceiling and lane-change bookkeeping live in one place. Fan-out,
        // effectful, gated (check:), deliver-block, and rank transforms are
        // EXCLUDED — they keep their serial in-process semantics (proposal
        // seeding, outbox, critic, delivery, HITL) exactly as before.
        concurrentBatch.push({ cardId: action.cardId, station: action.station });
      } else {
        // ── Synchronous path (concurrency === 1 or non-concurrent station) ────
        // Attribute this dispatch (and any model spend during it) to the card for
        // the wave budget; cleared after the station returns.
        currentCardId = action.cardId;
        cardDispatches.set(action.cardId, (cardDispatches.get(action.cardId) ?? 0) + 1);

        const laneChanged = await executeStation({
          db,
          stateDb,
          runId,
          flow,
          stationConfig,
          stationId: action.station,
          cardId: action.cardId,
          trackingAdapter,
          commandAllowlist,
          happyPathNext,
          terminalLanes,
          maxExecutionAttempts,
          projectRoot,
          currentNow,
          runStartedAt,
          wallClockSeconds,
          maxTokens,
          getTokensSpent: () => tokensSpent,
          onAndonTrip: (reason: string) => {
            io.err(`andon: run halted — ${reason}`);
            halted = true;
          },
          err: io.err,
          harnessRegistry: args.harnessRegistry,
          foldHarnessUsage,
          stampHarnessActivity,
          sleep,
          runSubflow: args.runSubflow,
        });

        currentCardId = null;

        if (laneChanged) {
          lastLaneChangeAt = currentNow;
        }
        if (halted) break;
      }
    }

    // ── Concurrent transform batch (v10) ──────────────────────────────────
    // Run the transform siblings collected above as overlapping in-process
    // adapter calls, bounded by the run-level K ceiling. Each runs inside its own
    // AsyncLocalStorage attribution scope so trackingAdapter/foldHarnessUsage
    // credit spend to the right card across interleaved awaits. Cards beyond the
    // K budget stay 'ready' and dispatch on a later tick (capBlocked defers the
    // idle wake). When paired with the release_at stagger, the first sibling
    // dispatched a tick earlier has already warmed the shared prompt-prefix cache,
    // so this batch hits the warm cache instead of each re-paying the prefill.
    if (concurrentBatch.length > 0 && !halted) {
      currentCardId = null; // batch calls attribute via ALS, never the shared var
      const { n: inFlight } = stateDb
        .prepare('SELECT COUNT(*) AS n FROM active_workers WHERE run_id = $runId')
        .get({ $runId: runId }) as { n: number };
      const slots = Math.max(0, concurrency - inFlight);
      const toRun = concurrentBatch.slice(0, slots);
      if (concurrentBatch.length > toRun.length) capBlocked = true;
      if (toRun.length > 0) dispatchedThisTick = true;

      const laneChanges = await Promise.all(
        toRun.map((item) => {
          cardDispatches.set(item.cardId, (cardDispatches.get(item.cardId) ?? 0) + 1);
          return attributionStore.run({ cardId: item.cardId }, () =>
            executeStation({
              db,
              stateDb,
              runId,
              flow,
              stationConfig: flow.stations[item.station],
              stationId: item.station,
              cardId: item.cardId,
              trackingAdapter,
              commandAllowlist,
              happyPathNext,
              terminalLanes,
              maxExecutionAttempts,
              projectRoot,
              currentNow,
              runStartedAt,
              wallClockSeconds,
              maxTokens,
              getTokensSpent: () => tokensSpent,
              onAndonTrip: (reason: string) => {
                io.err(`andon: run halted — ${reason}`);
                halted = true;
              },
              err: io.err,
              harnessRegistry: args.harnessRegistry,
              foldHarnessUsage,
              stampHarnessActivity,
              sleep,
              runSubflow: args.runSubflow,
            }),
          );
        }),
      );
      if (laneChanges.some((changed) => changed)) lastLaneChangeAt = currentNow;
    }

    // ── Consumption andon (block new claims when budget is exhausted) ──────
    // Checked AFTER dispatch (first pass) but BEFORE reclaim (second pass).
    // This placement ensures:
    //   1. Workers spawned THIS tick are visible in active_workers for planDrain.
    //   2. Past-lease reclaims have NOT yet cleared their slots, so planDrain
    //      can hard_kill them (distinguishing planDrain from the reclaim path).
    const consumptionAndon = checkConsumptionAndon(
      { runStartedAt, now: currentNow, tokensSpent },
      { wallClockSeconds, maxTokens },
    );
    if (consumptionAndon.tripped && !andonTripped) {
      andonTripped = true;
      // This check follows the action batch, so it — not the release-gate
      // wait — is what usually fires when a card has JUST re-parked past the
      // budget. Both sites report through the same helper.
      io.err(consumptionHaltMessage(consumptionAndon.reason, soonestRateLimitGate(db, runId, currentNow)));

      const { n: activeCountNow } = stateDb
        .prepare('SELECT COUNT(*) AS n FROM active_workers WHERE run_id = $runId')
        .get({ $runId: runId }) as { n: number };

      if (activeCountNow > 0) {
        // Feed active slots to planDrain:
        // - hard_kill (leaseUntil <= now): call kill() + release the slot.
        // - drain (leaseUntil > now): leave the slot and onMessage handler alive
        //   so any MARK_DONE that arrives later is still checkpointed normally.
        //   Test fakes may expose a drain() callback to trigger MARK_DONE
        //   synchronously; production workers never have drain() and this
        //   optional call is a no-op (drain is not on the SpawnedWorker type).
        const slots = stateDb
          .prepare('SELECT card_id AS cardId, station, lease_until AS leaseUntil FROM active_workers WHERE run_id = $runId')
          .all({ $runId: runId }) as WorkerSlot[];
        const drainActions = planDrain(slots, currentNow);

        for (const da of drainActions) {
          const handle = workerHandles.get(slotKey(da.cardId, da.station));
          if (da.action === 'hard_kill') {
            handle?.kill?.();
            workerHandles.delete(slotKey(da.cardId, da.station));
            stateDb
              .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station')
              .run({ $runId: runId, $id: da.cardId, $station: da.station });
          } else {
            // Passive drain: call optional drain() shim on test fakes (production
            // SpawnedWorker never has drain()). Then yield so that any
            // queueMicrotask(MARK_DONE) callbacks in fake workers fire BEFORE we
            // break — letting the onMessage handler checkpoint them while still live.
            (handle as { drain?: () => void } | undefined)?.drain?.();
          }
        }

        // Yield once so queueMicrotask(MARK_DONE) callbacks from passive-drain
        // workers fire here (within the same async tick), letting the onMessage
        // handler write their checkpoints before the loop exits.
        await new Promise<void>((resolve) => queueMicrotask(resolve));
      }

      halted = true;
      break;
    }

    // ── Second pass: reclaim stale slots (after andon check) ─────────────
    // Reclaims run after the andon so that past-lease workers present at
    // andon-trip time are handled by planDrain (which calls kill()) rather
    // than being silently cleared here.
    for (const action of plan.actions) {
      if (action.kind !== 'reclaim') continue;
      stateDb
        .transaction(() => {
          stateDb
            .prepare("UPDATE cards SET status = 'ready' WHERE run_id = $runId AND id = $id")
            .run({ $runId: runId, $id: action.cardId });
          stateDb
            .prepare(
              'DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station',
            )
            .run({ $runId: runId, $id: action.cardId, $station: action.station });
        })
        .immediate();
    }

    // (b) Awaiting-children polling seam (WI-397 fills this in).
    // After each action batch, check whether any awaiting_children parents can
    // advance to their resume_at lane (all children terminal). The stub returns
    // false unconditionally; WI-397 wires fan-in evaluation and parent re-queuing.
    const fanInAdvanced = pollAwaitingChildren(db, stateDb, runId, flow, terminalLanes);
    if (fanInAdvanced) {
      lastLaneChangeAt = currentNow;
    }

    // Pool mode: if the concurrency cap deferred ready cards this tick but we
    // spawned nothing new, re-planning immediately would busy-spin the same
    // blocked actions. Block until an in-flight worker reports so a slot frees,
    // then re-plan. Guarded on in-flight > 0 so synchronous fakes (which complete
    // inside worker.send(), leaving no in-flight workers) never wait.
    if (poolMode && capBlocked && !dispatchedThisTick && !halted) {
      const { n: inFlight } = stateDb
        .prepare('SELECT COUNT(*) AS n FROM active_workers WHERE run_id = $runId')
        .get({ $runId: runId }) as { n: number };
      if (inFlight > 0) {
        await waitForWorkerEvent();
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Per-wave (subtree) budget helpers — guard #4's wave scope (SPEC §6/§8)
// ---------------------------------------------------------------------------

/** Build per-card usage records (parent_id, tokens, dispatches) for the wave budget. */
function buildWaveUsages(
  stateDb: Database,
  runId: string,
  cardTokens: Map<string, number>,
  cardDispatches: Map<string, number>,
): CardUsage[] {
  const rows = stateDb
    .prepare('SELECT id, parent_id FROM cards WHERE run_id = $runId AND parent_id IS NOT NULL')
    .all({ $runId: runId }) as Array<{ id: string; parent_id: string }>;
  return rows.map((r) => ({
    cardId: r.id,
    parentId: r.parent_id,
    tokens: cardTokens.get(r.id) ?? 0,
    dispatches: cardDispatches.get(r.id) ?? 0,
  }));
}

/**
 * The original input-validation work: lane names come from `flow.yaml` (`terminal_lanes`) with no charset
 * restriction, so building a `NOT IN (...)` SQL clause by string-interpolating
 * them is a SQL-injection / malformed-config crash sink. Bind them as NAMED
 * parameters instead — bun:sqlite does not reliably bind a mix of a named-param
 * object and trailing positional args in one call, so every lane gets its own
 * `$lN` placeholder that merges into the same named-params object as the
 * statement's other bound values. Returns the `NOT IN (...)`-ready placeholder
 * list and the params object to spread into the statement's bound params.
 */
function laneExclusionClause(lanes: readonly string[]): {
  placeholders: string;
  params: Record<string, string>;
} {
  const params: Record<string, string> = {};
  lanes.forEach((lane, i) => {
    params[`$l${i}`] = lane;
  });
  return { placeholders: lanes.map((_, i) => `$l${i}`).join(', '), params };
}

/**
 * Scrap every non-terminal card in a subtree (all cards sharing parent_id) whose
 * wave budget was exceeded. Journal-first card_log (entered_lane + terminal),
 * then the state update + worker-slot release. Returns the count scrapped. The
 * run is NOT halted — sibling subtrees under other parents keep running, which
 * is the whole point of the wave scope vs. the run-scope andon (SPEC §6).
 */
function scrapWaveSubtree(
  db: ConduitDB,
  stateDb: Database,
  runId: string,
  parentId: string,
  terminalLanes: string[],
  io: { err(l: string): void },
): number {
  const { placeholders, params } = laneExclusionClause(terminalLanes);
  const children = stateDb
    .prepare(`SELECT id, lane, attempt FROM cards WHERE run_id = $runId AND parent_id = $p AND lane NOT IN (${placeholders})`)
    .all({ $runId: runId, $p: parentId, ...params }) as Array<{ id: string; lane: string; attempt: number }>;
  for (const c of children) {
    // Journal-first: append the transition + terminal entries before the state
    // commit, same phantom-safe ordering as advanceCard.
    db.appendCardLog({
      runId,
      kind: 'entered_lane',
      cardId: c.id,
      station: c.lane,
      attempt: c.attempt,
      sourceLane: c.lane,
      destLane: 'scrap',
      reasonClass: 'scrap',
    });
    db.appendCardLog({
      runId, kind: 'terminal', cardId: c.id, station: c.lane, attempt: c.attempt, reason: 'wave_budget' });
    stateDb
      .transaction(() => {
        stateDb.prepare("UPDATE cards SET lane = 'scrap', status = 'scrapped' WHERE run_id = $runId AND id = $id").run({ $runId: runId, $id: c.id });
        stateDb.prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id').run({ $runId: runId, $id: c.id });
      })
      .immediate();
  }
  if (children.length > 0) {
    io.err(`andon: wave budget exceeded — scrapped subtree '${parentId}' (${children.length} card(s))`);
  }
  return children.length;
}

// ---------------------------------------------------------------------------
// Station execution
// ---------------------------------------------------------------------------

interface ExecuteStationArgs {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  flow: FlowConfig;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  trackingAdapter: ModelAdapter;
  commandAllowlist: string[];
  happyPathNext: Record<string, string | null>;
  terminalLanes: Set<string>;
  maxExecutionAttempts: number;
  projectRoot: string;
  currentNow: number;
  runStartedAt: number;
  wallClockSeconds: number;
  maxTokens: number;
  getTokensSpent: () => number;
  onAndonTrip: (reason: string) => void;
  /** Error-surfacing channel — forwarded to escalateToHold so in-station escalations are visible on stderr (Change 2). */
  err: (msg: string) => void;
  /**
   * Registry of engine-config-defined harness adapters (WI-560), threaded from
   * RunEngineArgs so a `kind: harness` station can resolve its named adapter.
   * Optional/undefined for flows with no harness stations.
   */
  harnessRegistry?: HarnessRegistry;
  /**
   * Folds harness-reported usage into the SAME run/wave budget accumulators
   * trackingAdapter feeds (WI-567) — a harness maker never calls
   * trackingAdapter directly, so this is the sibling path that makes the
   * consumption andon and wave budget see harness spend.
   */
  foldHarnessUsage: (tokens: number) => void;
  /**
   * Stamps fresh liveness progress the instant a harness invoke() resolves
   * (WI-567 FR-8 fix) — the sibling of trackingAdapter's lastAdapterActivityAt
   * stamp for the harness path, which has no ModelAdapter call to hook.
   */
  stampHarnessActivity: () => void;
  /** Injectable delay (issue #3 retry backoff); shared with the run loop's wait. */
  sleep: (ms: number) => Promise<void>;
  /**
   * Seam that runs a `kind: subflow` station's child flow to terminal state
   * (the original multi-flow engine work) — threaded from RunEngineArgs. Absent → subflow stations
   * escalate to hold (configuration failure, never a silent skip).
   */
  runSubflow?: SubflowSeam;
}

/**
 * Execute one dispatch action against a station.
 *
 * Returns true when the card's lane changed (used to update lastLaneChangeAt).
 * Returns false when the claim failed or the andon halted execution
 * before the card could advance.
 */
async function executeStation(args: ExecuteStationArgs): Promise<boolean> {
  const {
    db,
    stateDb,
    runId,
    stationConfig,
    stationId,
    cardId,
    trackingAdapter,
    commandAllowlist,
    happyPathNext,
    terminalLanes,
    maxExecutionAttempts,
    projectRoot,
    currentNow,
    runStartedAt,
    wallClockSeconds,
    maxTokens,
    getTokensSpent,
    onAndonTrip,
    flow,
    err,
    harnessRegistry,
    foldHarnessUsage,
    stampHarnessActivity,
    sleep,
    runSubflow,
  } = args;

  // ── Atomic claim ──────────────────────────────────────────────────────────
  const claimResult = attemptClaim(db, {
    cardId,
    station: stationId,
    workerId: `${WORKER_ID_PREFIX}-${cardId}`,
    wipCap: stationConfig.wip,
    now: currentNow,
    leaseSeconds: LEASE_SECONDS,
    runId,
  });
  if (!claimResult.ok) return false;

  // ── Begin work (claimed → working) ────────────────────────────────────────
  beginWork(db, cardId, stationId, currentNow, LEASE_SECONDS, runId);

  // ── Dispatch by station kind ──────────────────────────────────────────────

  // ── Rank station (WI-398): must be checked BEFORE kind-based dispatch ────
  // A rank station can be declared with `kind: 'deterministic'` or
  // `kind: 'transform'` as a placeholder command/worker; the real routing is
  // determined by `rankCheck !== undefined`. Checking here ensures we branch
  // on the semantics (rank behavior) rather than the placeholder kind.
  if (isRankStation(stationConfig)) {
    return await executeRankStation({ db, stateDb, runId, stationConfig, stationId, cardId, trackingAdapter, happyPathNext, terminalLanes, maxExecutionAttempts, flow, projectRoot, err });
  }

  if (stationConfig.kind === 'deterministic') {
    return await executeDeterministicStation({
      db,
      stateDb,
      runId,
      stationConfig,
      stationId,
      cardId,
      commandAllowlist,
      happyPathNext,
      terminalLanes,
      flow,
      projectRoot,
      maxExecutionAttempts,
      // The original deterministic-gate work: threaded through so a `check:` gate on this deterministic
      // station can invoke the critic model and account for it on the andon,
      // exactly like a gated transform station.
      trackingAdapter,
      currentNow,
      runStartedAt,
      wallClockSeconds,
      maxTokens,
      getTokensSpent,
      onAndonTrip,
      // Review #1: a deterministic maker may declare an AGENTIC critic
      // (check.critic.harness) — the registry must reach runGateCheckOrAdvance.
      harnessRegistry,
      // the pre-public deterministic failure-reporting review: fresh liveness stamp for slow-failing commands mid-retry.
      stampActivity: stampHarnessActivity,
      // issue #26 AC3: a gate critic's billed spend must reach the SAME
      // run/wave accumulators the harness/subflow maker paths already fold
      // into — a deterministic maker itself never calls foldHarnessUsage, but
      // its `check.critic.harness` critic can.
      foldHarnessUsage,
      err,
    });
  }

  if (stationConfig.kind === 'transform') {
    return await executeTransformStation({
      db,
      stateDb,
      runId,
      stationConfig,
      stationId,
      cardId,
      trackingAdapter,
      happyPathNext,
      terminalLanes,
      maxExecutionAttempts,
      projectRoot,
      flow,
      currentNow,
      runStartedAt,
      wallClockSeconds,
      maxTokens,
      getTokensSpent,
      onAndonTrip,
      // Review #1: a transform maker may declare an AGENTIC critic
      // (check.critic.harness) — the registry must reach runGateCheckOrAdvance.
      harnessRegistry,
      // issue #26 AC3: same reasoning as the deterministic branch above.
      foldHarnessUsage,
      err,
    });
  }

  if (stationConfig.kind === 'harness') {
    return await executeHarnessStation({
      db,
      stateDb,
      runId,
      stationConfig,
      stationId,
      cardId,
      // The gate-critic surface ONLY — a harness maker never touches this
      // directly (WI-565 AC: "a harness MAKER under no gate never touches the
      // ModelAdapter"); it is threaded through solely for runGateCheckOrAdvance.
      trackingAdapter,
      happyPathNext,
      terminalLanes,
      maxExecutionAttempts,
      projectRoot,
      flow,
      currentNow,
      runStartedAt,
      wallClockSeconds,
      maxTokens,
      getTokensSpent,
      onAndonTrip,
      harnessRegistry,
      foldHarnessUsage,
      stampHarnessActivity,
      sleep,
      err,
    });
  }

  if (stationConfig.kind === 'subflow') {
    return await executeSubflowStation({
      db,
      stateDb,
      runId,
      stationConfig,
      stationId,
      cardId,
      trackingAdapter,
      happyPathNext,
      terminalLanes,
      maxExecutionAttempts,
      projectRoot,
      flow,
      currentNow,
      runStartedAt,
      wallClockSeconds,
      maxTokens,
      getTokensSpent,
      onAndonTrip,
      harnessRegistry,
      // Child spend folds into the SAME run/wave accumulators as harness
      // usage (the original multi-flow engine work: one budget ceiling per user intent).
      foldHarnessUsage,
      stampHarnessActivity,
      runSubflow,
      err,
    });
  }

  // Unsupported kind (agentic requires the in-kernel Tool-Bridge, SPEC §7) —
  // release slot without advancing.
  releaseSlot(stateDb, cardId, stationId, runId);
  return false;
}

// ---------------------------------------------------------------------------
// Deterministic station
// ---------------------------------------------------------------------------

interface DeterministicArgs {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  commandAllowlist: string[];
  happyPathNext: Record<string, string | null>;
  terminalLanes: Set<string>;
  flow: FlowConfig;
  projectRoot: string;
  maxExecutionAttempts: number;
  /**
   * The original deterministic-gate work: a deterministic station may declare a `check:` gate. The critic
   * is a transform call over the station's declared inputs+outputs (the
   * artifacts exist on disk regardless of whether the maker was a command or
   * an LLM transform), so the deterministic completion path needs the SAME
   * model surface + andon accounting the transform path already threads
   * through — trackingAdapter is the only surface that may call a model
   * (NFR-3); currentNow/runStartedAt/wallClockSeconds/maxTokens/getTokensSpent
   * feed the post-gate-call consumption andon check.
   */
  trackingAdapter: ModelAdapter;
  currentNow: number;
  runStartedAt: number;
  wallClockSeconds: number;
  maxTokens: number;
  getTokensSpent: () => number;
  onAndonTrip: (reason: string) => void;
  /**
   * WI-570 follow-up (review #1): threaded through so this maker's
   * `check.critic.harness` (agentic critic) can be resolved by
   * runGateCheckOrAdvance — the gate machinery is maker-kind-agnostic, so
   * every maker kind must supply the registry, not just a harness maker.
   */
  harnessRegistry?: HarnessRegistry;
  /**
   * Fresh liveness stamp (the stampHarnessActivity seam) — called when a
   * deterministic command settles in failure so a slow-failing command's real
   * wall-clock counts as progress during count-and-retry (the pre-public deterministic failure-reporting review).
   */
  stampActivity?: () => void;
  /**
   * issue #26 AC3: threaded through so a `check.critic.harness` (AGENTIC)
   * critic gating this deterministic maker can fold its OWN billed spend into
   * the same run/wave accumulators the maker path feeds — a deterministic
   * command never calls a harness/model itself, but its gate critic can.
   */
  foldHarnessUsage: (tokens: number) => void;
  /** Error-surfacing channel threaded through from runExecutor's io.err. */
  err: (msg: string) => void;
}

// ---------------------------------------------------------------------------
// Integrity + checkpoint-cascade helpers (SPEC §5, §6)
// ---------------------------------------------------------------------------

/** Build the artifact-dependency graph cascadeInvalidation walks. */
function flowGraphFromFlow(flow: FlowConfig): FlowGraph {
  return {
    stations: Object.entries(flow.stations).map(([id, s]) => ({
      id,
      inputs: s.inputs ?? [],
      outputs: s.outputs ?? [],
    })),
  };
}

/**
 * SPEC §5: a binding-stamp mismatch cascades downstream — every station that
 * transitively consumes this station's outputs must also re-run, so a config
 * change never lets a downstream station skip-replay a checkpoint built on
 * stale inputs. Invalidates the downstream checkpoints for this card/attempt.
 *
 * The per-station hash chain usually catches this implicitly (a downstream
 * station folds upstream output hashes into its own stamp), but the explicit
 * cascade closes the gap when a downstream input is not a hashed on-disk
 * artifact. Returns the downstream station ids that were invalidated.
 */
function cascadeInvalidateDownstream(
  stateDb: Database,
  flow: FlowConfig,
  cardId: string,
  attempt: number,
  stationId: string,
): string[] {
  const downstream = cascadeInvalidation(flowGraphFromFlow(flow), [stationId]).filter(
    (s) => s !== stationId,
  );
  for (const ds of downstream) {
    invalidateCheckpoint(stateDb, { flow: String(flow.version), card: cardId, station: ds, attempt });
  }
  return downstream;
}

/**
 * A point-in-time signature (content hash) of every file under `root`.
 *
 * the integrity-hash work: mtime+size (the prior signature) is attacker-forgeable — `touch -d`
 * / `utimensat` restores a file's original timestamp after an out-of-bounds
 * write, and an escape that happens to land at the same byte count then never
 * enters `diffTouched`'s comparison at all. This is a security control (it is
 * how an owned_paths escape via a container/LLM tool-loop gets caught), so the
 * signature must be sound against a hostile write, not just an accidental one.
 * Hash the actual bytes instead — walking the tree already dominates the cost,
 * and hashing adds roughly 10ms on a 33k-file tree, which is acceptable for a
 * once-per-station integrity gate.
 */
function snapshotTree(root: string): Map<string, string> {
  const snap = new Map<string, string>();
  const walk = (dir: string): void => {
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return; // dir vanished or unreadable — nothing to record
    }
    for (const e of entries) {
      const full = join(dir, e.name);
      if (e.isDirectory()) {
        walk(full);
      } else if (e.isFile()) {
        try {
          snap.set(full, createHash('sha256').update(readFileSync(full)).digest('hex'));
        } catch {
          /* race: file removed between readdir and read — skip */
        }
      } else if (e.isSymbolicLink()) {
        // WI-568 rework: a symlink dirent is NEITHER isDirectory() NOR isFile()
        // (readdir's withFileTypes reports the link itself, not its target), so
        // it was previously invisible to this snapshot entirely — a harness
        // laundering an owned_paths escape through a freshly-created symlink
        // would never appear in diffTouched's touched-set. Record a hash of the
        // symlink's OWN target string (readlinkSync — do NOT follow the link,
        // both to detect the symlink's creation/retargeting and to avoid a
        // cycle) so its creation or retargeting is itself a touched-path event;
        // checkIntegrity resolves the link's real target via resolveOwnedPath
        // when the touched path is later validated. the integrity-hash work: hash the target
        // string rather than record mtime, for the same forgeability reason as
        // the file case above.
        try {
          snap.set(full, `symlink:${createHash('sha256').update(readlinkSync(full)).digest('hex')}`);
        } catch {
          /* race: link removed between readdir and readlink — skip */
        }
      }
    }
  };
  walk(root);
  return snap;
}

/** Absolute paths of files created or modified between two tree snapshots. */
function diffTouched(before: Map<string, string>, after: Map<string, string>): string[] {
  const touched: string[] = [];
  for (const [path, sig] of after) {
    if (before.get(path) !== sig) touched.push(path);
  }
  return touched;
}

/**
 * MARK_DONE integrity gate (SPEC §5 step 4 / §6): verify the files a station
 * wrote stay within the card's owned_paths.
 *
 * Enforced ONLY when the card declares ownership (owned_paths non-empty). An
 * empty set preserves the legacy project-root-only confinement the executor
 * already applies inline — full owned_paths enforcement across every flow is a
 * later build-order step (it depends on every flow populating owned_paths).
 * Artifact-existence is intentionally NOT enforced here (declaredArtifacts: [])
 * so side-effect-only stations are not falsely failed; this gate owns path
 * containment. Returns the IntegrityResult, or null when the gate is skipped.
 */
function runOwnedPathsIntegrity(
  projectRoot: string,
  ownedPaths: readonly string[],
  touchedPaths: readonly string[],
): IntegrityResult | null {
  if (ownedPaths.length === 0) return null;
  return checkIntegrity({
    projectRoot,
    ownedPaths,
    touchedPaths,
    declaredArtifacts: [],
    output: null,
    validateOutput: () => true,
  });
}

/** One-line description of an integrity failure set for hold/log messages. */
function describeIntegrity(result: IntegrityResult): string {
  if (result.ok) return 'ok';
  return result.failures
    .map((f) => (f.code === 'schema_invalid' ? f.code : `${f.code}:${f.path}`))
    .join(', ');
}

// ---------------------------------------------------------------------------
// Deterministic-output capture + safety net (the deterministic-output work)
// ---------------------------------------------------------------------------

/**
 * the deterministic-output work fix 1 — a deterministic station that declares EXACTLY ONE `outputs:`
 * entry, exits 0, and writes non-empty stdout is common for commands that emit
 * their result to stdout instead of writing a file (e.g. `bun gen.mjs` doing
 * `process.stdout.write(JSON.stringify(...))`). Persist that stdout to the
 * declared output artifact so the flow doesn't silently drop it.
 *
 * Deliberately scoped to the single-output case only — a multi-output station
 * gets no stdout capture (there is no way to know which declared output the
 * stdout belongs to).
 *
 * Never clobbers a file the command already wrote itself (checked via
 * existsSync BEFORE writing) — stations that legitimately write their own
 * declared output (e.g. a DuckDB `COPY ... TO`) keep their exact behaviour.
 */
function persistStdoutToSingleDeclaredOutput(
  projectRoot: string,
  stationConfig: StationConfig,
  stdout: string,
): void {
  const outputs = stationConfig.outputs ?? [];
  if (outputs.length !== 1) return; // scope: single declared output only
  if (stdout.length === 0) return; // nothing to persist
  const outputPath = join(projectRoot, outputs[0]);
  if (existsSync(outputPath)) return; // station already wrote its own file — never clobber
  writeFileSync(outputPath, stdout);
}

/**
 * the deterministic-output work fix 2 — the post-run safety net. After the command runs (and after
 * fix 1's stdout capture has had its chance to fill the single-output case),
 * any declared `outputs:` artifact still missing on disk means the station
 * silently produced nothing. Returns the missing output paths (empty = all
 * declared outputs are present).
 */
function findMissingDeclaredOutputs(projectRoot: string, stationConfig: StationConfig): string[] {
  const outputs = stationConfig.outputs ?? [];
  return outputs.filter((output) => !existsSync(join(projectRoot, output)));
}

/**
 * One operator-diagnosable line for a failed deterministic command — shared by
 * the count-and-retry path (journal + scrap reason) and the effectful hold
 * escalation, so both name the station, exit code, timeout flag, and stderr.
 */
function describeDeterministicFailure(
  stationId: string,
  result: { exitCode: number; stderr: string; timedOut?: boolean },
): string {
  // sanitizeStderrTail strips control characters (log-injection vector — a
  // hostile command could forge journal/card_log "lines" or corrupt terminal
  // rendering) before slicing. Applied HERE, not only at the worker, so the
  // synchronous path's raw Bun stderr gets the same treatment as a pooled
  // worker's IPC-delivered tail.
  const stderrTail = sanitizeStderrTail(result.stderr, 200);
  return (
    `deterministic station '${stationId}' failed (exit ${result.exitCode}` +
    `${result.timedOut ? ', timed out' : ''})` +
    (stderrTail.length > 0 ? `: ${stderrTail}` : '')
  );
}

/**
 * Count a deterministic command failure toward the attempt cap (found live:
 * a QC gate that flagged an edit retried at attempt 0 FOREVER — the failure
 * branches released the slot without incrementing attempt or consulting
 * per_card.max_execution_attempts, so "fail closed" degraded to "retry
 * eternally, silently").
 *
 * Semantics now mirror the transform path's parse-retry accounting:
 *   - the durable attempt counter increments on every nonzero exit;
 *   - reaching the cap routes the card to scrap with a reason naming the
 *     station, exit code, timeout flag, and a stderr tail;
 *   - below the cap the card re-readies for a genuine retry (flaky-command
 *     tolerance), with the failure journaled so retries are visible.
 * Returns true (the card changed: scrapped or re-readied for retry).
 *
 * PURE stations only. The effectful branch must NOT come through here: bumping
 * the attempt recomputes the outbox idempotency key, so a "retry" would re-FIRE
 * a side effect whose outcome is unknown — that branch escalates to hold
 * instead (SPEC §5, never blind-retry a publish/commit).
 *
 * Serves both dispatch paths (the pre-public deterministic failure-reporting review): the synchronous loop calls it
 * in-line; the pool's MARK_DONE('failed') handler calls it with the worker's
 * reported failure detail, so one flaky command behaves identically at
 * concurrency=1 and K>1.
 */
function countDeterministicFailure(
  stateDb: Database,
  db: ConduitDB,
  runId: string,
  cardId: string,
  stationId: string,
  card: { lane: string; attempt: number },
  result: { exitCode: number; stderr: string; timedOut?: boolean },
  maxExecutionAttempts: number,
  /**
   * Fresh liveness stamp (the stampHarnessActivity seam). The command that just
   * settled may have consumed minutes of real wall-clock; without a fresh stamp
   * the loop's stale tick-start `currentNow` is all the watchdog sees, and a
   * slow-failing command could read as a stall mid-retry.
   */
  stampActivity?: () => void,
): boolean {
  stampActivity?.();
  const reason = describeDeterministicFailure(stationId, result);

  // Guard #2's boundary is owned by the FSM and mirrored by decideExecutionRetry
  // (quality/rework.ts) — consult the shared predicate rather than re-deriving
  // `attempt + 1 >= cap` a third time, so the boundary can never drift here.
  const retryDecision = decideExecutionRetry({
    executionAttempt: card.attempt,
    maxExecutionAttempts,
  });
  if (retryDecision.action === 'scrap') {
    // Cap reached — scrap NAMED (advanceCard writes the card_log trail).
    advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, 'scrap', 'scrapped', 0, reason, runId);
    return true;
  }

  // Below the cap: durable attempt bump + re-ready + slot release in ONE
  // transaction (same direct-SQL convention as the rank hold park), so a
  // crash between them cannot leave a counted-but-unreleased slot.
  const failedAttempt = card.attempt + 1;
  db.appendJournalSpan({
    runId,
    cardId,
    station: stationId,
    attempt: card.attempt,
    name: 'deterministic.retry',
    attributes: { reason, next_attempt: failedAttempt, cap: maxExecutionAttempts },
  });
  stateDb
    .transaction(() => {
      stateDb
        .prepare(
          // status='working' guard mirrors releaseSlot: never clobber a card
          // some other path has already parked (held/scrapped) since dispatch.
          "UPDATE cards SET attempt = $attempt, status = 'ready' WHERE run_id = $runId AND id = $id AND status = 'working'",
        )
        .run({ $attempt: failedAttempt, $runId: runId, $id: cardId });
      stateDb
        .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station')
        .run({ $runId: runId, $id: cardId, $station: stationId });
    })
    .immediate();
  return true;
}

async function executeDeterministicStation(args: DeterministicArgs): Promise<boolean> {
  const {
    db, stateDb, runId, stationConfig, stationId, cardId, commandAllowlist, happyPathNext, terminalLanes,
    projectRoot, flow, maxExecutionAttempts, trackingAdapter, currentNow, runStartedAt, wallClockSeconds,
    maxTokens, getTokensSpent, onAndonTrip, harnessRegistry, stampActivity, foldHarnessUsage, err,
  } = args;

  // Punch-list #8 — per-station wall-clock timeout for the spawned command.
  // Convert configured seconds → ms; undefined (absent) preserves the unbounded
  // behaviour exactly. A hung command would otherwise stay an *active* worker
  // forever, so the liveness watchdog never trips — a real deadlock.
  const timeoutMs =
    stationConfig.timeout_seconds !== undefined ? stationConfig.timeout_seconds * 1000 : undefined;

  // Read card state for sourceLane / attempt (needed for card_log entries).
  const card = db.getCard(runId, cardId);
  if (!card) {
    releaseSlot(stateDb, cardId, stationId, runId);
    return false;
  }

  // MARK_DONE integrity baseline (SPEC §5/§6): snapshot the project tree BEFORE
  // the command runs so we can later verify its writes stayed within the card's
  // owned_paths. Only taken when the flow opts in AND the card declares
  // ownership — otherwise the gate is a no-op and we skip the scan entirely.
  const integrityBaseline =
    flow.defaults?.enforceOwnedPaths === true && (card.owned_paths ?? []).length > 0
      ? snapshotTree(projectRoot)
      : null;
  const checkDeterministicIntegrity = (): IntegrityResult | null => {
    if (!integrityBaseline) return null;
    const touched = diffTouched(integrityBaseline, snapshotTree(projectRoot));
    const result = runOwnedPathsIntegrity(projectRoot, card.owned_paths, touched);
    return result && !result.ok ? result : null;
  };

  // ── Effectful-station outbox discipline (Change 1 / SPEC §5 exactly-once) ──
  // For effectful=true stations the command spawn is the irreversible side effect.
  // Before firing, reconcile against any prior outbox record for this execution
  // to prevent double-execution on resume.  Pure (effectful=false) stations keep
  // their current behaviour exactly — no outbox calls at all.
  if (stationConfig.effectful) {
    const idempotencyKey = `${flow.version}:${cardId}:${stationId}:${card.attempt}`;
    const reconcile = reconcileOnResume(stateDb, idempotencyKey);

    if (reconcile.action === 'skip') {
      // Effect already committed on a prior run — do not re-fire the command.
      // Proceed to post-work routing (gate check if configured, else FSM
      // INTEGRITY_PASS) as if the command succeeded.
      //
      // Intentionally asymmetric with the fire path below: findMissingDeclaredOutputs
      // and persistStdoutToSingleDeclaredOutput are NOT run here — the command
      // already produced (and, on that prior run, validated) its declared outputs,
      // and this run never captured its own stdout to persist.
      //
      // Known cost tradeoff: runGateCheckOrAdvance still re-invokes the gate
      // critic below on every resume that lands here, even though the effect
      // itself is skipped — the critic is a pure, comparatively cheap model call
      // next to re-firing the (already-committed) effect, so this is left as-is
      // rather than caching the prior verdict. A cached-verdict optimization is
      // possible later if repeated resumes make this cost material.
      return runGateCheckOrAdvance({
        db, stateDb, runId, stationConfig, stationId, cardId, card,
        stationOutput: readDeterministicStationOutput(stationConfig, projectRoot),
        trackingAdapter, projectRoot, flow, happyPathNext, terminalLanes, maxExecutionAttempts,
        currentNow, runStartedAt, wallClockSeconds, maxTokens, getTokensSpent, onAndonTrip, harnessRegistry, foldHarnessUsage, err,
      });
    }

    if (reconcile.action === 'escalate_hold') {
      // Pending intent of unknown outcome — never blind-retry.
      escalateToHold(stateDb, db, cardId, stationId, card, `effectful deterministic station '${stationId}' has a pending outbox intent with unknown outcome (key: ${idempotencyKey}); manual reconciliation required`, err, runId);
      return false;
    }

    // action === 'fire' — first execution; write PENDING before the side effect.
    writePendingIntent(stateDb, {
      flow: String(flow.version),
      card: cardId,
      station: stationId,
      attempt: card.attempt,
      idempotencyKey,
      intent: { kind: 'deterministic', station: stationId, command: stationConfig.command, args: stationConfig.args ?? [] },
    });

    const result = await runDeterministic(
      { command: stationConfig.command!, args: stationConfig.args ?? [] },
      { allowlist: commandAllowlist, cwd: projectRoot, timeoutMs, env: deterministicCardEnv(card.rework_count ?? 0, card.attempt ?? 0) },
    );

    if (result.ok) {
      // Side effect landed — commit the intent, then advance via the FSM.
      commitIntent(stateDb, idempotencyKey);

      // the deterministic-output work fix 1 — persist stdout to the single declared output when the
      // command emitted its result to stdout instead of writing the file.
      persistStdoutToSingleDeclaredOutput(projectRoot, stationConfig, result.stdout);

      // the deterministic-output work fix 2 — safety net: a declared output still missing after the
      // run (and after the stdout capture above) means the station silently
      // produced nothing. The effect already landed (irreversible), so — same
      // as an owned_paths breach below — this hard-pauses to hold rather than
      // auto-retrying.
      const missingOutputs = findMissingDeclaredOutputs(projectRoot, stationConfig);
      if (missingOutputs.length > 0) {
        escalateToHold(stateDb, db, cardId, stationId, card, `deterministic-output-missing: ${missingOutputs.join(', ')}`, err, runId);
        return false;
      }

      // MARK_DONE integrity gate: a write outside owned_paths is a containment
      // breach → hard-pause to hold for a human (never auto-retry a Law-class
      // violation). The effect already landed, so a human must reconcile.
      const violation = checkDeterministicIntegrity();
      if (violation) {
        escalateToHold(stateDb, db, cardId, stationId, card, `integrity violation (owned_paths): project root modified during effectful deterministic station '${stationId}' by an undeclared write — either the station escaped its owned_paths, or another process wrote to the project root while it ran: ${describeIntegrity(violation)}`, err, runId);
        return false;
      }

      return runGateCheckOrAdvance({
        db, stateDb, runId, stationConfig, stationId, cardId, card,
        stationOutput: readDeterministicStationOutput(stationConfig, projectRoot),
        trackingAdapter, projectRoot, flow, happyPathNext, terminalLanes, maxExecutionAttempts,
        currentNow, runStartedAt, wallClockSeconds, maxTokens, getTokensSpent, onAndonTrip, harnessRegistry, foldHarnessUsage, err,
      });
    } else {
      // Command failed with the intent still PENDING. A nonzero exit does NOT
      // prove the side effect never escaped (a publish can land and the
      // process still die on the way out) — this is exactly the
      // unknown-outcome case the outbox exists for. It must NOT go through
      // countDeterministicFailure: bumping the attempt would recompute the
      // idempotency key, so the "retry" would blind-re-FIRE the effect
      // (SPEC §5: never blind-retry a publish/commit). Escalate to hold
      // immediately instead — the same fail-closed terminus the pending-intent
      // reconcile would reach on the next dispatch, one cycle earlier and with
      // the failure named for the operator.
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `effectful ${describeDeterministicFailure(stationId, result)} — outbox intent '${idempotencyKey}' left pending (outcome unknown); manual reconciliation required`,
        err, runId,
      );
      return false;
    }
  }

  // ── Pure (effectful=false) deterministic station — original behaviour ──────
  const result = await runDeterministic(
    { command: stationConfig.command!, args: stationConfig.args ?? [] },
    { allowlist: commandAllowlist, cwd: projectRoot, timeoutMs, env: deterministicCardEnv(card.rework_count ?? 0, card.attempt ?? 0) },
  );

  if (result.ok) {
    // the deterministic-output work fix 1 — persist stdout to the single declared output when the
    // command emitted its result to stdout instead of writing the file.
    persistStdoutToSingleDeclaredOutput(projectRoot, stationConfig, result.stdout);

    // the deterministic-output work fix 2 — safety net: a declared output still missing after the run
    // (and after the stdout capture above) means the station silently produced
    // nothing. Fail loudly rather than advancing the card as if it succeeded —
    // same hard-pause-to-hold convention as the owned_paths integrity gate below.
    const missingOutputs = findMissingDeclaredOutputs(projectRoot, stationConfig);
    if (missingOutputs.length > 0) {
      escalateToHold(stateDb, db, cardId, stationId, card, `deterministic-output-missing: ${missingOutputs.join(', ')}`, err, runId);
      return false;
    }

    // MARK_DONE integrity gate (SPEC §5/§6): a write outside owned_paths is a
    // containment breach → hard-pause to hold rather than advancing.
    const violation = checkDeterministicIntegrity();
    if (violation) {
      escalateToHold(stateDb, db, cardId, stationId, card, `integrity violation (owned_paths): project root modified during deterministic station '${stationId}' by an undeclared write — either the station escaped its owned_paths, or another process wrote to the project root while it ran: ${describeIntegrity(violation)}`, err, runId);
      return false;
    }

    // Post-work routing (Step 4 / SPEC §3 / SPEC §7): run the `check:` gate
    // when configured (the original deterministic-gate work — a deterministic station's critic reads the
    // station's declared inputs+outputs off disk exactly like a transform
    // station's critic does); a fan-out proposal is read and routed via
    // handleFanOutComplete; otherwise INTEGRITY_PASS through the FSM so
    // routing stays the single source of truth.
    return runGateCheckOrAdvance({
      db, stateDb, runId, stationConfig, stationId, cardId, card,
      stationOutput: readDeterministicStationOutput(stationConfig, projectRoot),
      trackingAdapter, projectRoot, flow, happyPathNext, terminalLanes, maxExecutionAttempts,
      currentNow, runStartedAt, wallClockSeconds, maxTokens, getTokensSpent, onAndonTrip, harnessRegistry, foldHarnessUsage, err,
    });
  } else {
    // Command failed → count it toward the attempt cap: retry below the cap,
    // scrap NAMED at it — never a silent infinite re-dispatch. (the pre-public deterministic failure-reporting review's
    // count-and-retry unifies sync and pooled semantics; it supersedes the
    // WI-686 direct-scrap that fixed the same silent busy-retry.)
    return countDeterministicFailure(
      stateDb, db, runId, cardId, stationId, card, result, maxExecutionAttempts, stampActivity,
    );
  }
}

/**
 * A deterministic station has no typed `stationOutput` the way a transform
 * station does — its command writes files directly. `stationOutput.payload`
 * is only consulted downstream when the station is ALSO a fan-out station
 * (handleFanOutComplete reads the parsed ArchitectProposal off it), so this
 * parses the declared output file in that case only and returns a harmless
 * `{ payload: null }` otherwise — the gate-check block below never reads
 * `.payload` on its own account (the critic re-reads inputs/outputs off disk).
 */
function readDeterministicStationOutput(stationConfig: StationConfig, projectRoot: string): { payload: unknown } {
  if (!isFanOutStation(stationConfig)) return { payload: null };
  const outputFile = stationConfig.outputs?.[0];
  let payload: unknown = null;
  if (outputFile) {
    try { payload = JSON.parse(readFileSync(join(projectRoot, outputFile), 'utf-8')); } catch { /* pass null */ }
  }
  return { payload };
}

// ---------------------------------------------------------------------------
// Gate check + post-work routing (the original deterministic-gate work) ────────────────────────────────
// ---------------------------------------------------------------------------

interface GateCheckOrAdvanceArgs {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  card: { lane: string; attempt: number; rework_count?: number };
  /** Only consulted by a 'pass' verdict (or the no-gate path) when this station is a fan-out station. */
  stationOutput: { payload: unknown };
  trackingAdapter: ModelAdapter;
  projectRoot: string;
  flow: FlowConfig;
  happyPathNext: Record<string, string | null>;
  terminalLanes: Set<string>;
  maxExecutionAttempts: number;
  currentNow: number;
  runStartedAt: number;
  wallClockSeconds: number;
  maxTokens: number;
  getTokensSpent: () => number;
  onAndonTrip: (reason: string) => void;
  /**
   * Engine-config harness adapter registry (WI-560), threaded through so an
   * AGENTIC critic (gateConfig.criticHarness, WI-570) can be resolved by name.
   * Optional — undefined for flows with no harness makers or critics.
   */
  harnessRegistry?: HarnessRegistry;
  /**
   * issue #26 AC3: folds a harness CRITIC's own billed spend into the SAME
   * run/wave budget accumulators trackingAdapter/foldHarnessUsage already feed
   * for the maker path — a gate critic never calls trackingAdapter (the maker
   * kinds that host `check:` may not even touch a ModelAdapter at all, e.g. a
   * deterministic maker), so without this the critic's own spend is invisible
   * to the consumption andon and the wave budget.
   */
  foldHarnessUsage: (tokens: number) => void;
  err: (msg: string) => void;
}

// ---------------------------------------------------------------------------
// File delivery at station completion (WI-599)
// ---------------------------------------------------------------------------

/**
 * Slack's per-file upload cap in bytes, used by the production transport's
 * pre-upload size gate (WI-597 `maxUploadBytes`, PRD edge case "File exceeds
 * Slack's size limit — fail diagnosably at delivery, not with a raw API
 * error"). Overridable via `SLACK_MAX_UPLOAD_BYTES` for workspaces with a
 * different plan-level limit; falls back to this default when the env var is
 * absent, non-numeric, or non-positive.
 */
const DEFAULT_SLACK_MAX_UPLOAD_BYTES = 1024 * 1024 * 1024; // 1 GiB

function resolveSlackMaxUploadBytes(): number {
  const raw = process.env.SLACK_MAX_UPLOAD_BYTES;
  if (raw === undefined) return DEFAULT_SLACK_MAX_UPLOAD_BYTES;
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : DEFAULT_SLACK_MAX_UPLOAD_BYTES;
}

/**
 * Wall-clock bound (ms) on the Slack JSON Web API calls and the reconciler
 * probe. Overridable via `SLACK_FETCH_TIMEOUT_MS`; falls back to the transport
 * default when the env var is absent, non-numeric, zero, or negative (same
 * validation idiom as resolveSlackMaxUploadBytes). Exported so the Socket Mode
 * and doctor probes in cli/main.ts reuse the same operator-tunable bound.
 */
export function resolveSlackFetchTimeoutMs(): number {
  const raw = process.env.SLACK_FETCH_TIMEOUT_MS;
  if (raw === undefined) return SLACK_FETCH_TIMEOUT_MS;
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : SLACK_FETCH_TIMEOUT_MS;
}

/**
 * Base host (NO /api suffix) for every production Slack call — Web API and
 * Socket Mode's apps.connections.open. Overridable via `SLACK_API_BASE_URL`;
 * falls back to DEFAULT_SLACK_HOST when unset, empty, or not a valid absolute
 * http(s) URL. Callers append exactly one `/api` segment — this must never
 * carry an `/api` suffix itself (slack.ts's DEFAULT_SLACK_API_BASE already
 * has one; the env var here is the only `SLACK_API_BASE_URL` in the codebase).
 */
const DEFAULT_SLACK_HOST = 'https://slack.com';

export function resolveSlackApiBaseUrl(): string {
  const raw = process.env.SLACK_API_BASE_URL;
  if (raw === undefined || raw === '') return DEFAULT_SLACK_HOST;
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return DEFAULT_SLACK_HOST;
    // Return the normalized origin, not `raw` verbatim: WHATWG URL parsing
    // trims surrounding whitespace and tolerates a trailing slash, both of
    // which would otherwise survive into `${base}/api` and produce a
    // double-slash path or an unparseable concatenated URL at call sites.
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return DEFAULT_SLACK_HOST;
  }
}

/**
 * Single source of truth for the Socket Mode apps.connections.open endpoint URL.
 * Composes exactly one `/api` segment onto resolveSlackApiBaseUrl() so the base
 * host + Web API root normalization is shared with every other Slack call site.
 * The production Socket Mode seam (buildProductionSocketSeam in cli/main.ts)
 * must call this rather than hand-writing the composition, so the unit test can
 * assert on the real seam expression instead of a re-implementation.
 */
export function resolveSocketConnectionsOpenUrl(): string {
  return `${resolveSlackApiBaseUrl()}/api/apps.connections.open`;
}

/**
 * Wall-clock bound (ms) on the raw byte-upload POST only. Overridable via
 * `SLACK_UPLOAD_TIMEOUT_MS`; falls back to the transport default on an
 * absent/non-numeric/zero/negative value.
 */
export function resolveSlackUploadTimeoutMs(): number {
  const raw = process.env.SLACK_UPLOAD_TIMEOUT_MS;
  if (raw === undefined) return SLACK_UPLOAD_TIMEOUT_MS;
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : SLACK_UPLOAD_TIMEOUT_MS;
}

/**
 * Confine a `deliver.files` entry to the project root before it is read and
 * shipped to an external service. Delivery is an egress surface, so this runs
 * UNCONDITIONALLY — independent of the enforce_owned_paths write-gate opt-in.
 *
 * Two layers, mirroring SPEC §7:
 *   1. Lexical — the entry resolved against projectRoot must stay under it.
 *      Catches an absolute path or a `..`-escape even when the target does not
 *      exist on disk (so it composes with load-time literal rejection).
 *   2. Symlink — when the target exists, its realpath (and the root's) must
 *      still be contained. Catches a symlink UNDER the root that points OUT.
 *
 * A not-yet-existing target passes containment on the lexical check alone; the
 * downstream read (egressSendFile) raises its own typed missing-file error, so
 * we do not conflate "missing" with "escaping" here.
 */
function deliverPathWithinRoot(
  projectRoot: string,
  file: string,
): { ok: true; absPath: string } | { ok: false; reason: string } {
  const under = (child: string, root: string): boolean =>
    child === root || child.startsWith(root + sep);

  const lexicalRoot = resolve(projectRoot);
  const lexicalAbs = resolve(projectRoot, file);
  if (!under(lexicalAbs, lexicalRoot)) {
    return { ok: false, reason: 'absolute or ..-escaping path' };
  }

  // Symlink layer — only meaningful when the target actually exists.
  let canonicalFile: string;
  try {
    canonicalFile = realpathSync(lexicalAbs);
  } catch {
    // Missing/unreachable target: lexically contained is sufficient here; the
    // read downstream reports the missing file with a clean typed error.
    return { ok: true, absPath: lexicalAbs };
  }
  const canonicalRoot = realpathSync(lexicalRoot);
  if (!under(canonicalFile, canonicalRoot)) {
    return { ok: false, reason: 'symlink resolves outside the project root' };
  }
  return { ok: true, absPath: lexicalAbs };
}

/**
 * Resolve a card's thread address from the triggering ingress event's
 * substrate (WI-599, FR-6/FR-8, decision 6 — team-lead Option A).
 *
 * The `deliver.thread_from` field names a key in the substrate JSON the
 * ingress listener projected when the run was spawned (e.g. a Slack event's
 * `thread_ts`). Substrate lives on `ingress_events`, keyed by run_id — NOT
 * per-card as the WI-599 item Context originally (incorrectly) assumed. A run
 * with no triggering ingress event (a CLI-driven run) or whose substrate lacks
 * the named field returns `undefined` — this is a normal, expected degrade
 * (the same flow may be Slack- or CLI-triggered), never an error: the caller
 * delivers unthreaded and journals the skip.
 *
 * Exported so WI-600 can reuse it with the same (db, card, thread_from) shape.
 */
export function resolveThreadAddress(
  db: ConduitDB,
  card: { run_id: string },
  threadFrom: string,
): string | undefined {
  const substrateJson = db.getIngressSubstrateForRun(card.run_id);
  if (substrateJson === null) return undefined;

  let substrate: Record<string, unknown>;
  try {
    substrate = JSON.parse(substrateJson) as Record<string, unknown>;
  } catch {
    return undefined;
  }

  const value = substrate[threadFrom];
  return typeof value === 'string' ? value : undefined;
}

/**
 * Deliver a completed station's declared `deliver.files` (WI-596) through the
 * flow's resolved delivery-capable egress channel, sequentially and in
 * declared order (FR-10, decision 8).
 *
 * Runs AFTER the MARK_DONE owned-paths integrity gate has already passed for
 * the station's own writes — this is a SEPARATE, second containment check
 * scoped to the specific files the deliver block names (a station could touch
 * many files within owned_paths but only declare some of them for delivery).
 *
 * Each file's delivery is itself outbox-guarded by egressSendFile (WI-598), so
 * this function is safe to re-run on resume: a committed delivery for an
 * unchanged file is a no-op (dedup), and only the outstanding files re-fire —
 * the loop below does not need its own resume bookkeeping.
 *
 * On ANY failure (no resolvable channel, an owned_paths breach, a missing/
 * empty file, or an ambiguous pending intent with no reconciler) the card is
 * hard-paused to hold and station work is NOT re-run to force re-delivery
 * (FR-11/FR-14, NFR-3) — delivery failures are never silently swallowed nor
 * auto-retried by re-executing the maker.
 *
 * Returns `{ ok: false }` when the card was escalated to hold (caller must
 * stop and return without advancing); `{ ok: true }` when every declared file
 * either delivered or was already committed.
 */
async function performStationDelivery(args: {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  flow: FlowConfig;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  card: { lane: string; attempt: number; owned_paths?: string[] };
  projectRoot: string;
  err: (msg: string) => void;
}): Promise<{ ok: boolean }> {
  const { db, stateDb, runId, flow, stationConfig, stationId, cardId, card, projectRoot, err } = args;

  const deliver = stationConfig.deliver;
  if (!deliver) return { ok: true };

  // WI-596 load-time validation already guarantees a resolvable channel exists
  // for any flow that declares a deliver block, but resolve defensively here
  // too rather than trusting that invariant blindly at runtime.
  const channel = resolveDeliveryChannel(flow.channels?.egress);
  if (!channel) {
    escalateToHold(
      stateDb, db, cardId, stationId, card,
      `station '${stationId}' declares a deliver block but no delivery-capable egress channel could be resolved`,
      err, runId,
    );
    return { ok: false };
  }

  const botToken = process.env.SLACK_BOT_TOKEN ?? '';
  const transport = createSlackTransport({
    botToken,
    apiBaseUrl: `${resolveSlackApiBaseUrl()}/api`,
    maxUploadBytes: resolveSlackMaxUploadBytes(),
    fetchTimeoutMs: resolveSlackFetchTimeoutMs(),
    uploadTimeoutMs: resolveSlackUploadTimeoutMs(),
  });
  const enforceOwnedPaths = flow.defaults?.enforceOwnedPaths === true;
  const ownedPaths = card.owned_paths ?? [];

  // WI-599 AC2/FR-6/FR-8: resolve the thread address ONCE from the triggering
  // ingress event's substrate (same run, same triggering event for every file
  // this station delivers). Absent — no ingress event (CLI run) or the
  // substrate lacks the named field — is a normal degrade, never an error: the
  // file(s) below deliver unthreaded and the skip is journaled per-file.
  const threadTs =
    deliver.thread_from !== undefined
      ? resolveThreadAddress(db, { run_id: runId }, deliver.thread_from)
      : undefined;

  for (const file of deliver.files) {
    // UNCONDITIONAL project-root containment (symlink-resolved) — delivery is
    // an EGRESS surface that reads and ships bytes to an external service, so
    // it must be contained regardless of the enforce_owned_paths write-gate
    // opt-in below. Load validation rejects absolute / `..`-escaping literals
    // (fail fast), but a symlink under projectRoot pointing outside can only be
    // caught here at read time (SPEC §7 resolves symlinks for exactly this).
    const contained = deliverPathWithinRoot(projectRoot, file);
    if (!contained.ok) {
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `station '${stationId}' deliver.files declares '${file}' which resolves outside the project root (${contained.reason})`,
        err, runId,
      );
      return { ok: false };
    }
    const absPath = contained.absPath;

    // Owned-paths gate on the DECLARED file — a second, narrower containment
    // check than the station's own MARK_DONE gate (that gate covers everything
    // TOUCHED; this covers only what is being delivered). Skipped when the
    // flow does not opt in or the card declares no ownership, mirroring the
    // MARK_DONE gate's own opt-in convention.
    if (enforceOwnedPaths && ownedPaths.length > 0) {
      const containment = checkIntegrity({
        projectRoot,
        ownedPaths,
        touchedPaths: [file],
        declaredArtifacts: [],
        output: null,
        validateOutput: () => true,
      });
      if (!containment.ok) {
        escalateToHold(
          stateDb, db, cardId, stationId, card,
          `station '${stationId}' deliver.files declares '${file}' outside owned_paths: ${describeIntegrity(containment)}`,
          err, runId,
        );
        return { ok: false };
      }
    }
    // Run-scoped, attempt-keyed prefix (FR-5, decision 5): the run id leads the
    // prefix AND egressSendFile scopes its outbox rows by the same run, so two
    // runs with the same card/station/attempt/bytes (card ids repeat across
    // runs) never collide into a silent skip. A rework attempt producing a new
    // artifact re-delivers; a same-attempt resume of unchanged bytes dedups via
    // egressSendFile's own content-fingerprint outbox key.
    const keyPrefix = `${runId}/${flow.version}/${cardId}/${stationId}/${card.attempt}/${file}`;

    // WI-602: a real files.info/thread-history probe upgrades the hold-only
    // base (no reconciler) to a safe auto-resume — a pending intent whose
    // outcome the probe can determine (landed/not_landed) no longer needs a
    // human to unblock it; only a genuinely ambiguous outcome still escalates.
    const reconciler = createFilesInfoReconciler({
      botToken,
      apiBaseUrl: `${resolveSlackApiBaseUrl()}/api`,
      channel: channel.target ?? '',
      threadTs,
      fetchTimeoutMs: resolveSlackFetchTimeoutMs(),
    });

    let result: Awaited<ReturnType<typeof egressSendFile>>;
    try {
      result = await egressSendFile(
        db,
        transport,
        {
          runId,
          channel: channel.target ?? '',
          filePath: absPath,
          keyPrefix,
          threadTs,
          caption: deliver.caption,
        },
        reconciler,
      );
    } catch (deliverErr) {
      // Missing/empty file, or a transport lacking uploadFile — WI-598 raises a
      // typed error naming the path BEFORE any outbox write. Never re-run the
      // station to force a fix; hold for a human (NFR-3).
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `delivery failed for station '${stationId}' file '${file}': ${(deliverErr as Error).message}`,
        err, runId,
      );
      return { ok: false };
    }

    if (result.escalatedToHold) {
      // Either no reconciler was available, or the WI-602 probe itself came
      // back 'unknown' (fail-closed — never guess landed) — either way the
      // outcome is ambiguous and never blind-retried (WI-598 mirrors
      // egressSend's outbox FSM). Journal the reconcile decision so the
      // ambiguity is diagnosable in explain before escalating.
      db.appendJournalSpan({
        runId,
        cardId,
        station: stationId,
        attempt: card.attempt,
        name: 'delivery.reconcile',
        attributes: { file, reconcileDecision: result.reconcileDecision ?? 'escalate' },
      });
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `delivery ambiguous (pending, reconcile decision: ${result.reconcileDecision ?? 'no reconciler'}) for station '${stationId}' file '${file}'`,
        err, runId,
      );
      return { ok: false };
    }

    // Journal every delivery outcome per attempt (FR-12) — visible in `conduit
    // explain` like other effectful sends, whether this call actually posted or
    // was a dedup skip of an already-committed delivery. Also journals the
    // thread-resolution outcome (FR-8) — a declared thread_from that the
    // substrate could not supply is a degrade, not a silent no-op — and the
    // WI-602 reconcile decision, when a pending intent was reconciled on entry.
    db.appendJournalSpan({
      runId,
      cardId,
      station: stationId,
      attempt: card.attempt,
      name: 'delivery.sent',
      attributes: {
        file,
        channel: channel.target ?? '',
        posted: result.posted,
        ...(result.reconcileDecision !== undefined ? { reconcileDecision: result.reconcileDecision } : {}),
        ...(deliver.thread_from !== undefined
          ? {
              threadFrom: deliver.thread_from,
              threadResolution:
                threadTs !== undefined
                  ? 'threaded'
                  : 'thread_from absent from substrate — delivered unthreaded (degrade)',
            }
          : {}),
        ...(threadTs !== undefined ? { threadTs } : {}),
      },
    });
  }

  return { ok: true };
}

/**
 * Run a station's `check:` gate (if configured) and route the card, or —
 * absent a gate — fall through to fan-out detection / plain INTEGRITY_PASS.
 *
 * Extracted (the original deterministic-gate work) so a `kind: deterministic` station behind a `check:`
 * gate gets EXACTLY the same critic invocation + rework routing a gated
 * `kind: transform` station gets: the gate machinery (`runGateRework`) never
 * cared whether the maker was an LLM transform or a deterministic command —
 * it reads the station's declared inputs+outputs off disk either way. Before
 * this fix, `executeDeterministicStation` routed straight to INTEGRITY_PASS
 * and NEVER consulted `stationConfig.gateCheck`, so a deterministic station's
 * `check:` validated at load time and rendered in `conduit explain`, but the
 * critic was never invoked and no `gate_verdict` card_log row was ever
 * written — the card silently advanced past a "checked" station that was
 * never actually checked.
 *
 * Used by both `executeDeterministicStation` (all three completion paths:
 * effectful skip-on-resume, effectful fire-success, and the plain pure path)
 * and `executeTransformStation` (its single completion path, after the
 * checkpoint write / output-artifact write / owned_paths integrity gate).
 * The two call sites' gate blocks used to be a verbatim ~90-line duplication
 * (code-review finding); this is now the one implementation both maker kinds
 * route through, with the transform path passing its typed `stationOutput`
 * (the parsed payload) rather than `readDeterministicStationOutput`'s
 * best-effort disk re-read.
 */
async function runGateCheckOrAdvance(args: GateCheckOrAdvanceArgs): Promise<boolean> {
  const {
    db, stateDb, runId, stationConfig, stationId, cardId, card, stationOutput, trackingAdapter, projectRoot,
    flow, happyPathNext, terminalLanes, maxExecutionAttempts, currentNow, runStartedAt, wallClockSeconds,
    maxTokens, getTokensSpent, onAndonTrip, harnessRegistry, foldHarnessUsage, err,
  } = args;

  // ── Run gate check (if configured) ───────────────────────────────────────
  if (stationConfig.gateCheck) {
    // Guard #1's counter, scoped to THIS station's gate (issue #1). Derived
    // ONCE and used for BOTH the gate's cap check and the FSM's own cap check
    // below, so the two can never disagree about how much budget is left.
    const gateReworkCount = gateReworksSpent(db, runId, cardId, stationId);

    // WI-570 rework: an unresolvable check.critic.harness name (config error,
    // not caught at load time — WI-563's UNKNOWN_HARNESS_ADAPTER only covers a
    // kind:harness MAKER's worker.harness, not a gate's critic.harness) throws
    // inside runGateRework rather than returning a GateDecision. Uncaught, that
    // throw propagated all the way out of the tick loop and crashed the whole
    // run, leaving the card stranded at status='working' (worse than a plain
    // crash — a resume would see a stale claim). Escalate to hold instead,
    // mirroring the maker-side adapter-unresolved pattern above (~2383):
    // escalate ambiguity, never guess, never crash the run over one card's
    // config error.
    let gateDecision: Awaited<ReturnType<typeof runGateRework>>;
    try {
      gateDecision = await runGateRework({
        db,
        runId,
        cardId,
        workerStationId: stationId,
        attempt: card.attempt,
        maxExecutionAttempts,
        gateReworkCount,
        gateConfig: stationConfig.gateCheck,
        adapter: trackingAdapter,
        harnessRegistry,
        projectRoot,
        validBackEdges: flow.back_edges ?? [],
        // Pass the flow-level cap policy so gate-rework can signal 'rework' for
        // proceed_with_findings (rather than 'scrap') when the rework cap trips.
        capPolicy: flow.defaults?.capPolicy ?? 'scrap',
      });
    } catch (gateErr) {
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `gate check failed for station '${stationId}': ${(gateErr as Error).message}`,
        err, runId, true,
      );
      return false;
    }

    // ── issue #26 AC3/AC5: fold + journal the critic's OWN billed spend ──────
    // Only an AGENTIC (harness) critic populates `criticUsage` — a transform
    // critic's spend is already folded via trackingAdapter inside
    // runTransformStation, and double-folding it here would double-count
    // (see CriticUsage's doc comment in quality/gate.ts).
    //
    // PLACEMENT: this runs BEFORE the andon check below on purpose. A critic
    // that itself blows the run budget must trip the andon on its own spend,
    // not a tick later — folding first is what makes that true; checking the
    // andon first would let one over-budget critic call slip through free.
    if (gateDecision.criticUsage !== undefined) {
      const { adapterName, model, durationMs, usage } = gateDecision.criticUsage;
      const usageKnown = !('unknown' in usage);
      if (usageKnown) {
        // Budgets fold the TOTAL, mirroring the maker path's foldHarnessUsage
        // call — never the pieces of a breakdown.
        foldHarnessUsage(usage.tokens);
      }
      // SPAN NAME: '<station>.harness-critic', deliberately NOT '<station>.harness'.
      // countConsecutiveRateLimitParks (~line 4469) walks journal spans
      // backwards by the EXACT name '<station>.harness' to bound
      // MAX_CONSECUTIVE_RATE_LIMIT_PARKS; a critic span sharing that name
      // would silently corrupt that streak (either hard-pausing a maker that
      // never actually parked, or masking one that did).
      db.appendJournalSpan({
        runId,
        cardId,
        station: stationId,
        attempt: card.attempt,
        name: `${stationId}.harness-critic`,
        adapter: adapterName,
        durationMs,
        usageUnknown: !usageKnown,
        usage: usageKnown ? harnessJournalUsage(usage, model) : undefined,
        attributes: usageKnown ? rateLimitAttributes(usage.rateLimit) : {},
      });
    }

    // Andon check after gate model call.
    const andon2 = checkConsumptionAndon(
      { runStartedAt, now: currentNow, tokensSpent: getTokensSpent() },
      { wallClockSeconds, maxTokens },
    );
    if (andon2.tripped) {
      onAndonTrip(`${andon2.reason} budget exceeded`);
      // Card is in 'working', not 'done' — release slot.
      releaseSlot(stateDb, cardId, stationId, runId);
      return false;
    }

    // ── Append gate_verdict BEFORE applying the decision ─────────────────────
    // Append-before-commit: same journal-first strategy as advanceCard (WI-381).
    // A phantom pre-commit entry from a crash between this append and the
    // state-db commit below is deduped on replay by the UNIQUE(card_id, station,
    // attempt, 'gate_verdict') constraint (FR-7). All GateReworkDecision branches
    // now carry verdict/findings/returnTo/attempt (WI-382 widening).
    db.appendCardLog({
      runId,
      kind: 'gate_verdict',
      cardId,
      station: stationId,
      attempt: card.attempt,
      verdict: gateDecision.verdict,
      findings: gateDecision.findings,
      returnTo: gateDecision.returnTo,
    });

    // Build the FSM context and a transient done_pending_ack state for this card.
    // The FsmState is synthesised only to QUERY the FSM for the legal next state;
    // done_pending_ack is never persisted (the executor transitions atomically
    // from 'working' to whatever the FSM resolves).
    const ctx = buildTransitionContext(stationId, flow, happyPathNext, terminalLanes, maxExecutionAttempts);
    const fsmState = syntheticDonePendingAck(card, gateReworkCount);

    switch (gateDecision.action) {
      case 'pass': {
        // Fan-out stations route via FAN_OUT even when the gate passes.
        // The gate validates quality but the routing decision is still FAN_OUT
        // (parent → awaiting_children, children seeded at child_entry) rather
        // than INTEGRITY_PASS (parent → station-order successor). Without this
        // check, a gated fan-out station's 'pass' branch would fire INTEGRITY_PASS
        // and advance the parent to merge (AC6 violation — wrong routing).
        if (isFanOutStation(stationConfig)) {
          return handleFanOutComplete({
            db,
            stateDb,
            runId,
            stationConfig,
            stationId,
            cardId,
            stationOutput,
            card,
            happyPathNext,
            terminalLanes,
            maxExecutionAttempts,
            flow,
            err,
            now: currentNow,
          });
        }

        // WI-599: deliver any declared files now that the station has genuinely
        // passed (integrity gate + quality gate) — never on a rework/scrap verdict.
        const passDelivery = await performStationDelivery({
          db, stateDb, runId, flow, stationConfig, stationId, cardId, card, projectRoot, err,
        });
        if (!passDelivery.ok) return false;

        // Gate approved — route through INTEGRITY_PASS so the FSM is the single
        // source of truth for the next (lane, status).
        const passResult = transition(fsmState, { type: 'INTEGRITY_PASS' }, ctx);
        if (!passResult.ok) {
          // Contradictory state — escalate rather than silently advancing.
          escalateToHold(stateDb, db, cardId, stationId, card, `FSM illegal_transition on gate INTEGRITY_PASS for station '${stationId}'`, err, runId);
          return false;
        }
        const passNext = passResult.next;
        const isTerminal = terminalLanes.has(passNext.lane);
        const resolvedPassStatus = isTerminal ? 'complete' : passNext.status;
        advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, passNext.lane, resolvedPassStatus, 0, undefined, runId);
        return true;
      }
      case 'rework': {
        // Gate rejected (genuine progress or cap+proceed_with_findings) — route
        // through QC_REJECT so the FSM decides: under-cap → rework to returnTo;
        // at-cap+proceed_with_findings → advance forward.
        //
        // no_progress is already handled as 'scrap' by gate-rework (it does NOT
        // return 'rework' for no_progress); this branch only receives genuine
        // under-cap rejects and at-cap+proceed_with_findings rejects.
        const rejectResult = transition(fsmState, { type: 'QC_REJECT', returnTo: gateDecision.returnTo }, ctx);
        if (!rejectResult.ok) {
          // Contradictory state (e.g. returnTo is not a valid back-edge) — escalate.
          escalateToHold(stateDb, db, cardId, stationId, card, `FSM illegal_transition on QC_REJECT(returnTo=${gateDecision.returnTo}) for station '${stationId}'`, err, runId);
          return false;
        }
        const rejectNext = rejectResult.next;
        if (rejectNext.lane === 'scrap') {
          // FSM routed to scrap (at-cap with capPolicy=scrap — though gate-rework
          // normally returns 'scrap' directly in that case, this is a safety net).
          advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, 'scrap', 'scrapped', 0, 'rework_cap', runId);
          return true;
        }
        // reworkDelta = FSM's new reworkCount minus what the card currently has.
        const reworkDelta = rejectNext.reworkCount - fsmState.reworkCount;
        const isRejectTerminal = terminalLanes.has(rejectNext.lane);
        const resolvedRejectStatus = isRejectTerminal ? 'complete' : rejectNext.status;
        advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, rejectNext.lane, resolvedRejectStatus, reworkDelta, undefined, runId);
        return true;
      }
      case 'scrap': {
        // Direct scrap decisions from gate-rework (rework_cap with capPolicy=scrap,
        // no_progress, invalid_verdict, or a propagated gate-check reason —
        // 'model-incompatible' for a transform critic, a distinct
        // 'harness-critic-*' reason for an agentic one, a pre-public engine review) bypass
        // the FSM.
        //
        // no_progress note: no_progress does NOT honour proceed_with_findings —
        // unchanged findings means "stop", unconditionally. This is intentional.
        //
        // A no_progress scrap represents a completed (but unproductive) rework
        // attempt — increment rework_count alongside attempt so the card's history
        // reflects every dispatch that consumed the cap budget (guards #1 and #3).
        const scrapReworkDelta = gateDecision.reason === 'no_progress' ? 1 : 0;
        advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, 'scrap', 'scrapped', scrapReworkDelta, gateDecision.reason, runId);
        return true;
      }
      default: {
        // Defensive: GateReworkDecision is a closed union ('pass' | 'rework' |
        // 'scrap') and runGateRework never returns anything else today, so this
        // branch is unreachable through any legitimate code path. It exists so
        // that an unexpected action value — from a future gate-rework.ts change
        // or an unsafe cast upstream — hard-escalates instead of falling through
        // to the "no gate" block below and silently bypassing the gate entirely.
        escalateToHold(stateDb, db, cardId, stationId, card, `unexpected gate decision action '${(gateDecision as { action: string }).action}' for station '${stationId}'`, err, runId);
        return false;
      }
    }
  }

  // ── No gate: fan-out detection, then INTEGRITY_PASS through the FSM ───────
  if (isFanOutStation(stationConfig)) {
    return handleFanOutComplete({
      db,
      stateDb,
      runId,
      stationConfig,
      stationId,
      cardId,
      stationOutput,
      card,
      happyPathNext,
      terminalLanes,
      maxExecutionAttempts,
      flow,
      err,
      now: currentNow,
    });
  }

  // WI-599: deliver any declared files for a no-gate station that has passed
  // the MARK_DONE owned-paths integrity gate (checked by the caller before
  // routing here).
  const noGateDelivery = await performStationDelivery({
    db, stateDb, runId, flow, stationConfig, stationId, cardId, card, projectRoot, err,
  });
  if (!noGateDelivery.ok) return false;

  const noGateCtx = buildTransitionContext(stationId, flow, happyPathNext, terminalLanes, maxExecutionAttempts);
  const noGateFsmState = syntheticDonePendingAck(card, REWORK_COUNT_UNREAD);
  const noGateResult = transition(noGateFsmState, { type: 'INTEGRITY_PASS' }, noGateCtx);

  if (!noGateResult.ok) {
    // Contradictory state — escalate rather than silently advancing.
    escalateToHold(stateDb, db, cardId, stationId, card, `FSM illegal_transition on INTEGRITY_PASS for no-gate station '${stationId}'`, err, runId);
    return false;
  }

  const noGateNext = noGateResult.next;
  const isNoGateTerminal = terminalLanes.has(noGateNext.lane);
  const resolvedNoGateStatus = isNoGateTerminal ? 'complete' : noGateNext.status;
  advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, noGateNext.lane, resolvedNoGateStatus, 0, undefined, runId);
  return true;
}

// ---------------------------------------------------------------------------
// Transform station (with optional gate check)
// ---------------------------------------------------------------------------

interface TransformArgs {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  trackingAdapter: ModelAdapter;
  happyPathNext: Record<string, string | null>;
  terminalLanes: Set<string>;
  maxExecutionAttempts: number;
  projectRoot: string;
  flow: FlowConfig;
  currentNow: number;
  runStartedAt: number;
  wallClockSeconds: number;
  maxTokens: number;
  getTokensSpent: () => number;
  onAndonTrip: (reason: string) => void;
  /**
   * WI-570 follow-up (review #1): threaded through so this maker's
   * `check.critic.harness` (agentic critic) can be resolved by
   * runGateCheckOrAdvance — the gate machinery is maker-kind-agnostic, so
   * every maker kind must supply the registry, not just a harness maker.
   */
  harnessRegistry?: HarnessRegistry;
  /**
   * issue #26 AC3: threaded through so a `check.critic.harness` (AGENTIC)
   * critic gating this transform maker can fold its OWN billed spend into the
   * same run/wave accumulators the maker path feeds — a transform maker's own
   * model call already folds via trackingAdapter, but a harness critic does not.
   */
  foldHarnessUsage: (tokens: number) => void;
  /** Error-surfacing channel threaded through from runExecutor's io.err. */
  err: (msg: string) => void;
}

async function executeTransformStation(args: TransformArgs): Promise<boolean> {
  const {
    db,
    stateDb,
    runId,
    stationConfig,
    stationId,
    cardId,
    trackingAdapter,
    happyPathNext,
    terminalLanes,
    maxExecutionAttempts,
    projectRoot,
    flow,
    currentNow,
    runStartedAt,
    wallClockSeconds,
    maxTokens,
    getTokensSpent,
    onAndonTrip,
    harnessRegistry,
    foldHarnessUsage,
    err,
  } = args;

  // ── Read card state (attempt counter for journaling) ──────────────────────
  const card = db.getCard(runId, cardId);
  if (!card) {
    releaseSlot(stateDb, cardId, stationId, runId);
    return false;
  }

  // ── Accumulate prior gate rejection findings for feedback (FR-5) ──────────
  // When the card has been reworked at least once, read ALL prior gate_verdict
  // reject entries (chronological, id ASC) and concatenate their findings into
  // a single feedback string rendered into the maker prompt via {{feedback}}.
  // Truncated to FEEDBACK_MAX_CHARS (NFR-4) so accumulated history stays bounded.
  const FEEDBACK_MAX_CHARS = 8000;
  const feedbackString: string | undefined =
    (card.rework_count ?? 0) > 0
      ? db
          .getCardLogForRun(runId, cardId)
          .filter((e) => e.kind === 'gate_verdict' && e.verdict === 'reject')
          .flatMap((e) => (e.kind === 'gate_verdict' ? e.findings : []))
          .join('\n')
          .slice(0, FEEDBACK_MAX_CHARS)
      : undefined;

  // ── Compute binding stamp BEFORE the model call (Issue A: skip-on-resume) ──
  // The stamp folds in every input that could change the result: disk inputs,
  // feedback (rework history), model id, prompt version, and flow version.
  // Computing it here — after feedbackString is known — ensures a rework
  // attempt with identical disk inputs gets a DIFFERENT stamp than a pre-rework
  // execution, so a resume cannot skip-replay a stale pre-feedback checkpoint.
  // (FR-6: hash order is irrelevant; computeBindingStamp sorts inputArtifactHashes.)
  const inputHashes = stationConfig.inputs.map((inputName) => {
    try {
      // seed.json is card-scoped (child's owned dir), not at projectRoot.
      // Hash from the same location as renderPrompt reads — otherwise sibling
      // children with different seeds would get identical stamps.
      const inputPath = inputName === 'seed.json' && card.owned_paths[0]
        ? join(card.owned_paths[0], 'seed.json')
        : join(projectRoot, inputName);
      const content = readFileSync(inputPath);
      return createHash('sha256').update(content).digest('hex');
    } catch {
      return '';
    }
  });

  if (feedbackString !== undefined) {
    inputHashes.push(createHash('sha256').update(feedbackString).digest('hex'));
  }

  // ── Load declared image inputs (WI-419, FR-2) ────────────────────────────
  // Images are loaded ONCE here and reused for both the binding stamp and the
  // model call — avoiding a double read. The loader error (WI-413) names the
  // offending path and is intentionally NOT swallowed (unlike the text-hash
  // try/catch above), so a missing declared image surfaces immediately (AC6 /
  // FR-7). Only declared entries are opened — never a directory scan (AC4 /
  // NFR-2).
  const images: ImageInput[] =
    (stationConfig.image_inputs ?? []).map((decl) => loadImageInput(projectRoot, decl.path));

  // Fold image-byte hashes into the stamp alongside text-input hashes so a
  // changed image invalidates the checkpoint (AC3).
  inputHashes.push(...hashImageInputs(images));

  // ── Pre-dispatch payload guard (WI-420, FR-9) ─────────────────────────────
  // Reject oversized or too-many images with a clear kernel error BEFORE the
  // binding stamp is computed or the model call is issued — so a pathological
  // image surfaces immediately, not as an opaque gateway 400.
  assertImagePayloadWithinLimits(images);

  // WI-557 (FR-4): a worker.uses station's promptTemplateVersion combines the
  // declared prompt_version with each injected skill's content hash, in
  // worker.uses order — so editing a skill body/references file invalidates
  // this station's checkpoint (and cascades) on resume. A non-uses station is
  // unaffected: skill_content_hashes is absent and the stamp input is the bare
  // prompt_version, exactly as before this item.
  const promptTemplateVersion =
    stationConfig.skill_content_hashes !== undefined && stationConfig.skill_content_hashes.length > 0
      ? computeSkillAwarePromptTemplateVersion(stationConfig.prompt_version ?? '', stationConfig.skill_content_hashes)
      : stationConfig.prompt_version ?? '';

  const bindingStamp = computeBindingStamp({
    modelId: stationConfig.model!,
    promptTemplateVersion,
    inputArtifactHashes: inputHashes,
    flowVersion: flow.version,
  });

  // ── Effectful-station outbox discipline (Change 1 / SPEC §5 exactly-once) ──
  // For effectful=true stations the model call is the billed side effect.
  // Reconcile against any prior outbox record BEFORE deciding to run the model.
  // stationOutput is set here on the 'skip' path so the existing checkpoint read
  // below is bypassed for effectful stations that already committed.
  // For pure (effectful=false) stations this block is never entered.
  let effectfulSkipOutput: Awaited<ReturnType<typeof readCheckpoint>> = null;
  // Idempotency key of a PENDING outbox intent written THIS dispatch (fire path
  // only). Held so a non-retryable scrap below can abandon the intent rather
  // than leave it dangling for a future replay to escalate_hold on.
  let pendingIntentKey: string | null = null;

  if (stationConfig.effectful) {
    const idempotencyKey = `${flow.version}:${cardId}:${stationId}:${card.attempt}`;
    const reconcile = reconcileOnResume(stateDb, idempotencyKey);

    if (reconcile.action === 'escalate_hold') {
      // Pending intent of unknown outcome — never blind-retry the model call.
      escalateToHold(stateDb, db, cardId, stationId, card, `effectful transform station '${stationId}' has a pending outbox intent with unknown outcome (key: ${idempotencyKey}); manual reconciliation required`, err, runId);
      return false;
    }

    if (reconcile.action === 'skip') {
      // Effect already committed on a prior run — the model call must not be
      // re-issued. Reuse the stored checkpoint (committed = model call succeeded
      // = checkpoint was written before commitIntent).
      const committedCheckpoint = readCheckpoint(stateDb, {
        run: runId,
        flow: String(flow.version),
        card: cardId,
        station: stationId,
        attempt: card.attempt,
      });
      if (!committedCheckpoint) {
        // Invariant violation: committed intent but no checkpoint. Fail-closed.
        escalateToHold(stateDb, db, cardId, stationId, card, `effectful transform station '${stationId}' has a committed outbox intent but no checkpoint (key: ${idempotencyKey}); data inconsistency`, err, runId);
        return false;
      }
      // Signal to the stationOutput resolution below to use this checkpoint.
      effectfulSkipOutput = committedCheckpoint;
    } else {
      // action === 'fire' — first execution; write PENDING before the model call.
      writePendingIntent(stateDb, {
        flow: String(flow.version),
        card: cardId,
        station: stationId,
        attempt: card.attempt,
        idempotencyKey,
        intent: { kind: 'transform', station: stationId, outputs: stationConfig.outputs },
      });
      pendingIntentKey = idempotencyKey;
    }
  }

  // ── Checkpoint skip-on-resume (SPEC §5) ──────────────────────────────────
  // For pure (effectful=false) stations: reuse stored output on matching stamp.
  // For effectful 'skip' resume: use the committed checkpoint loaded above.
  // For effectful 'fire': no checkpoint exists yet — will run the model call.
  const existingCheckpoint = effectfulSkipOutput ?? readCheckpoint(stateDb, {
    run: runId,
    flow: String(flow.version),
    card: cardId,
    station: stationId,
    attempt: card.attempt,
  });

  // SPEC §5 cascade: if a checkpoint exists but its binding stamp no longer
  // matches (model/prompt/inputs/flow changed), this station re-executes — and
  // every downstream station that consumes its outputs must be invalidated too,
  // so none skip-replays a checkpoint built on now-stale inputs.
  if (
    effectfulSkipOutput === null &&
    existingCheckpoint !== null &&
    existingCheckpoint.stamp !== bindingStamp
  ) {
    cascadeInvalidateDownstream(stateDb, flow, cardId, card.attempt, stationId);
  }

  let stationOutput = (effectfulSkipOutput !== null || existingCheckpoint?.stamp === bindingStamp)
    ? existingCheckpoint?.output ?? null
    : null;

  if (stationOutput === null) {
    // ── Render prompt from template ─────────────────────────────────────────
    // A worker.uses station's composed prompt_content (skills-in-order, then
    // the local prompt) is preferred over the raw prompt_file (WI-555, D1
    // carrier option A) — the one allowed touch to this render path.
    const promptTemplate = stationConfig.prompt_content ?? readFileSync(stationConfig.prompt_file!, 'utf-8');
    const prompt = renderPrompt(promptTemplate, stationConfig.inputs, projectRoot, feedbackString, (stationConfig.image_inputs ?? []).map((d) => d.path), card.owned_paths);

    // ── Build output schema ─────────────────────────────────────────────────
    const schema = buildOutputSchema(stationConfig.output_schema?.fields ?? []);

    // ── Run transform (model call + journal + schema validate) ───────────────
    // Guard #2 (SPEC §6): `maxExecutionAttempts` bounds transport-level
    // parse/validate retries within a SINGLE dispatch (not a per-card cumulative
    // cap). Each retry here is a paid re-bill — guard #1 (per-card rework cap)
    // and guard #4 (the andon) bound cumulative per-card spend independently.
    const transformResult = await runTransformStation({
      cardId,
      station: stationId,
      // Thread the run id so the transform's usage span is attributed to this
      // run rather than the DEFAULT_RUN_ID sweep (the original per-run usage-attribution work).
      runId,
      attempt: card.attempt,
      maxExecutionAttempts,
      model: stationConfig.model!,
      prompt,
      params: stationConfig.params ?? {},
      schema,
      adapter: trackingAdapter,
      db,
      // Pass images only when present so text-only calls carry no images field
      // (NFR-1: ModelCall.images stays undefined for stations without image_inputs).
      images: images.length > 0 ? images : undefined,
      // Punch-list #8 — bound each gateway call when the station declares a
      // timeout. seconds → ms; undefined (absent) → the adapter's explicit
      // engine-default timeout (not truly unbounded — the original transform-timeout work).
      timeoutMs:
        stationConfig.timeout_seconds !== undefined ? stationConfig.timeout_seconds * 1000 : undefined,
    });

    // ── Andon check after model call ──────────────────────────────────────────
    // Check BEFORE advancing the card so that a tripped andon leaves the card
    // in its current lane (working), not at 'done'.
    const andon = checkConsumptionAndon(
      { runStartedAt, now: currentNow, tokensSpent: getTokensSpent() },
      { wallClockSeconds, maxTokens },
    );
    if (andon.tripped) {
      onAndonTrip(`${andon.reason} budget exceeded`);
      // Release the slot so the card doesn't remain stuck in 'working'.
      releaseSlot(stateDb, cardId, stationId, runId);
      return false;
    }

    // ── Handle model-incompatible scrap ───────────────────────────────────────
    if (transformResult.status === 'scrapped') {
      // The effect did not land (non-retryable classification). Abandon the
      // PENDING outbox intent written above so a future replay of this scrapped
      // attempt does not escalate_hold on an effect that never occurred. No-op
      // for pure stations (pendingIntentKey stays null).
      if (pendingIntentKey !== null) {
        discardIntent(stateDb, pendingIntentKey);
      }
      advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, 'scrap', 'scrapped', 0, transformResult.reason, runId);
      return true;
    }

    stationOutput = transformResult.output;

    // ── Write checkpoint (binding stamp) ───────────────────────────────────
    writeCheckpoint(stateDb, { run: runId, flow: String(flow.version), card: cardId, station: stationId, attempt: card.attempt }, {
      stamp: bindingStamp,
      output: stationOutput,
    });

    // ── Commit outbox intent for effectful stations (effect succeeded) ──────
    // commitIntent after writeCheckpoint so the intent is only committed when
    // the output is safely persisted — if we crash between writeCheckpoint and
    // commitIntent the next resume sees pending+checkpoint and escalates_hold.
    // (Acceptable: human reconciliation is safer than a blind re-bill.)
    if (stationConfig.effectful) {
      const idempotencyKey = `${flow.version}:${cardId}:${stationId}:${card.attempt}`;
      commitIntent(stateDb, idempotencyKey);
    }
  }

  // ── Write output artifacts to disk ────────────────────────────────────────
  // Gate critics read these artifacts — they must exist before renderPrompt runs.
  // Issue C: guard each output path against traversal outside the project root.
  //
  // Two roots are needed for the two branches of the guard:
  //   resolvedProjectRoot — realpath'd (symlink-resolved). Used on the normal
  //     branch where the parent dir exists and realpathSync succeeds. This is
  //     the symlink-escape defense — a malicious ancestor symlink cannot launder
  //     an escape.
  //   lexicalProjectRoot — lexically normalised (resolve(), no realpathSync). Used
  //     on the fallback branch when the parent dir does not exist yet (ENOENT).
  //     A not-yet-created subdir cannot be symlink-resolved; comparing the lexical
  //     target against the lexical root is the correct and sufficient check here.
  // v10 output_scope: 'owned_dir' writes declared outputs into the card's
  // owned_paths[0] directory (the same card-scoped location seed.json lives in)
  // instead of project root, so N homogeneous fan-out children each produce
  // their OWN artifacts rather than clobbering one shared name (SPEC §9:
  // outputs are disjoint across concurrent cards). Fail-closed: a scoped card
  // whose owned_paths[0] is not an existing directory is a config violation —
  // thrown, matching the escape-guard convention below.
  const outputBase = (() => {
    if (stationConfig.output_scope !== 'owned_dir') return projectRoot;
    const owned = card.owned_paths?.[0];
    if (owned === undefined) {
      throw new Error(
        `Station '${stationId}' declares output_scope: owned_dir but card '${cardId}' has no ` +
          `owned_paths — cannot resolve an owned output directory`,
      );
    }
    const ownedDir = resolve(projectRoot, owned);
    let isDir = false;
    try {
      isDir = statSync(ownedDir).isDirectory();
    } catch {
      isDir = false;
    }
    if (!isDir) {
      throw new Error(
        `Station '${stationId}' declares output_scope: owned_dir but card '${cardId}' ` +
          `owned_paths[0] '${owned}' is not an existing directory — create it before fan-out ` +
          `(same rule as the per-child seed)`,
      );
    }
    return ownedDir;
  })();

  const resolvedProjectRoot = (() => {
    try {
      return realpathSync(outputBase);
    } catch {
      return resolve(outputBase);
    }
  })();
  const lexicalProjectRoot = resolve(outputBase);

  const payload = stationOutput.payload;
  for (const outputName of stationConfig.outputs) {
    // Guard: lexically normalize (catches `../` traversal).
    const lexicalTarget = resolve(join(outputBase, outputName));

    // Guard: symlink-aware check on the parent directory so a malicious
    // ancestor symlink cannot launder an escape into a false allow.
    // If the parent doesn't exist yet (first write into a new subdir), fall back
    // to the LEXICAL root check — comparing the lexical target against the lexical
    // project root avoids the false-reject that occurs when the project root is
    // reached through a symlink and the output dir has not been created yet.
    const parentDir = join(lexicalTarget, '..');
    let resolvedTarget: string;
    let parentExists = false;
    try {
      const resolvedParent = realpathSync(parentDir);
      // join(resolvedParent, basename(lexicalTarget)) reassembles the canonical path.
      const fileName = lexicalTarget.slice(parentDir.length).replace(/^[\\/]+/, '');
      resolvedTarget = join(resolvedParent, fileName);
      parentExists = true;
    } catch {
      resolvedTarget = lexicalTarget;
    }

    // Choose the appropriate root for the confinement check:
    //   - Parent exists → use realpath-based root (full symlink-escape defense).
    //   - Parent ENOENT  → use lexical root (safe for new subdirs under a symlinked root).
    const rootForCheck = parentExists ? resolvedProjectRoot : lexicalProjectRoot;

    if (!resolvedTarget.startsWith(rootForCheck + sep) && resolvedTarget !== rootForCheck) {
      // Output would escape the project root — throw to surface as a fatal config violation.
      throw new Error(
        `Output path '${outputName}' resolves outside the project root '${rootForCheck}'. ` +
          `Station '${stationId}' output paths must not traverse above the project root.`,
      );
    }

    writeFileSync(join(outputBase, outputName), JSON.stringify(payload, null, 2), 'utf-8');
  }

  // ── MARK_DONE integrity gate (SPEC §5/§6) ─────────────────────────────────
  // A transform station writes exactly its declared outputs, so those ARE its
  // touched paths. When the flow opts in, verify they stay within the card's
  // owned_paths. A violation is a containment breach → invalidate the checkpoint
  // (so a resume never skip-replays the out-of-bounds output) and hard-pause to
  // hold for a human.
  if (flow.defaults?.enforceOwnedPaths === true) {
    // Under output_scope: owned_dir the outputs were written into owned_paths[0],
    // so the touched set must be the owned-dir-joined paths — checking the bare
    // names would test project-root locations nothing wrote to (a false violation
    // for scoped stations, a false pass for the actual writes).
    const touchedOutputs =
      stationConfig.output_scope === 'owned_dir' && card.owned_paths?.[0] !== undefined
        ? stationConfig.outputs.map((name) => join(card.owned_paths![0]!, name))
        : stationConfig.outputs;
    const violation = runOwnedPathsIntegrity(projectRoot, card.owned_paths ?? [], touchedOutputs);
    if (violation && !violation.ok) {
      invalidateCheckpoint(stateDb, { run: runId, flow: String(flow.version), card: cardId, station: stationId, attempt: card.attempt });
      escalateToHold(stateDb, db, cardId, stationId, card, `integrity violation (owned_paths): project root modified during transform station '${stationId}' by an undeclared write — either the station escaped its owned_paths, or another process wrote to the project root while it ran: ${describeIntegrity(violation)}`, err, runId);
      return false;
    }
  }

  // ── Gate check (if configured) + post-work routing (the original deterministic-gate work) ───────────
  // Delegates to the shared helper also used by executeDeterministicStation —
  // see runGateCheckOrAdvance's doc comment. stationOutput here is the typed
  // parsed payload from this station's own model call / checkpoint (not a
  // disk re-read), unlike the deterministic path's readDeterministicStationOutput.
  return runGateCheckOrAdvance({
    db, stateDb, runId, stationConfig, stationId, cardId, card,
    stationOutput,
    trackingAdapter, projectRoot, flow, happyPathNext, terminalLanes, maxExecutionAttempts,
    currentNow, runStartedAt, wallClockSeconds, maxTokens, getTokensSpent, onAndonTrip, harnessRegistry, foldHarnessUsage, err,
  });
}

// ---------------------------------------------------------------------------
// Harness station (WI-565 — the agentic precursor tier, docs/harness-containment.md)
// ---------------------------------------------------------------------------

interface HarnessArgs {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  /** Gate-critic surface only — a harness MAKER never touches this directly. */
  trackingAdapter: ModelAdapter;
  happyPathNext: Record<string, string | null>;
  terminalLanes: Set<string>;
  maxExecutionAttempts: number;
  projectRoot: string;
  flow: FlowConfig;
  currentNow: number;
  runStartedAt: number;
  wallClockSeconds: number;
  maxTokens: number;
  getTokensSpent: () => number;
  onAndonTrip: (reason: string) => void;
  harnessRegistry?: HarnessRegistry;
  /** Folds harness-reported usage into the run/wave budget accumulators (WI-567). */
  foldHarnessUsage: (tokens: number) => void;
  /** Stamps fresh liveness progress the instant a harness invoke() resolves (WI-567 FR-8 fix). */
  stampHarnessActivity: () => void;
  /**
   * Injectable delay, shared with the run loop's release-gate wait. Used for
   * retry backoff (issue #3); tests inject a no-op so the suite does not pay
   * real wall-clock seconds for a deliberate failure path.
   */
  sleep: (ms: number) => Promise<void>;
  err: (msg: string) => void;
}

/**
 * Execute a `kind: harness` station: a transform whose single model call is
 * replaced by one bounded invocation of a named external harness adapter
 * (docs/harness-containment.md — the agentic precursor tier).
 *
 * HYBRID contract (WI-565):
 *   - INVOKES like a transform — resolve the adapter by name from the
 *     run-context registry (never a raw command from flow.yaml), render the
 *     prompt + {{feedback}} exactly as renderPrompt does for a transform maker,
 *     one bounded `adapter.invoke` call.
 *   - COLLECTS OUTPUTS FROM DISK like a deterministic station — the harness
 *     writes its declared output files itself during invoke; this reads them
 *     back (findMissingDeclaredOutputs, then coerciveParse + buildOutputSchema,
 *     mirroring the transform coercive-parse contract) rather than serializing
 *     a returned payload as the transform path does.
 *
 * A parse miss or missing declared output is a HARD non-advance to `hold`
 * (escalateToHold) — mirroring the deterministic path's
 * `deterministic-output-missing → escalateToHold` and the project's
 * fail-closed principle. Never a silent advance, and never a scrap here (the
 * bounded retry-then-scrap semantics are a separate, later item).
 *
 * The checkpoint binding-stamp inputs fold in the resolved adapter's identity
 * (alongside the station's model id and prompt-template version) so a change
 * of harness adapter invalidates the checkpoint like any other stamp input —
 * this call site is WI-572's real caller; that item owns the eventual
 * computeBindingStamp hashing change.
 *
 * Out of scope here (separate dependent items, per the WI-565 brief): the
 * mandatory owned-paths integrity gate, attempt-cap/scrap semantics beyond
 * this single hard-non-advance, per-attempt usage journaling, and effectful
 * outbox discipline.
 */
async function executeHarnessStation(args: HarnessArgs): Promise<boolean> {
  const {
    db, stateDb, runId, stationConfig, stationId, cardId, trackingAdapter, happyPathNext, terminalLanes,
    maxExecutionAttempts, projectRoot, flow, currentNow, runStartedAt, wallClockSeconds, maxTokens,
    getTokensSpent, onAndonTrip, harnessRegistry, foldHarnessUsage, stampHarnessActivity, sleep, err,
  } = args;

  const card = db.getCard(runId, cardId);
  if (!card) {
    releaseSlot(stateDb, cardId, stationId, runId);
    return false;
  }

  // ── Resolve the adapter BY NAME from the run-context registry ─────────────
  // A flow can only ever supply a name string (loader-validated); the registry
  // is engine config, never flow.yaml. A missing registry or unknown name is a
  // configuration failure — escalate to hold rather than silently skipping.
  const adapterName = stationConfig.harness;
  const resolved =
    adapterName !== undefined && harnessRegistry !== undefined
      ? harnessRegistry.resolve(adapterName)
      : { ok: false as const, error: `no harness adapter registry configured for station '${stationId}'` };

  if (!resolved.ok) {
    escalateToHold(
      stateDb, db, cardId, stationId, card,
      `harness adapter unresolved for station '${stationId}': ${resolved.error}`,
      err, runId, true,
    );
    return false;
  }
  const harnessAdapter = resolved.adapter;

  // ── Effective model (WI-589, FR-10): station declares intent, adapter
  // config is the deployment default — station wins. Computed ONCE so the
  // binding stamp and the actual CLI invocation always agree (previously the
  // stamp folded only the station's (possibly absent) model while the CLI
  // only ever saw the adapter's config default — a silent divergence that
  // let a changed adapter default wrongly skip on resume).
  const effectiveModel = stationConfig.model ?? harnessAdapter.model;

  // ── Accumulate prior gate rejection findings for feedback (FR-5) ──────────
  // Identical to the transform path: {{feedback}} is prompt-threaded, zero new
  // machinery (WI-565 AC2).
  const FEEDBACK_MAX_CHARS = 8000;
  const feedbackString: string | undefined =
    (card.rework_count ?? 0) > 0
      ? db
          .getCardLogForRun(runId, cardId)
          .filter((e) => e.kind === 'gate_verdict' && e.verdict === 'reject')
          .flatMap((e) => (e.kind === 'gate_verdict' ? e.findings : []))
          .join('\n')
          .slice(0, FEEDBACK_MAX_CHARS)
      : undefined;

  // ── Compute binding stamp BEFORE the invocation (skip-on-resume, WI-565 AC5) ──
  // Mirrors the transform path's stamp computation exactly, with one addition:
  // the resolved adapter's identity rides the stamp's dedicated adapterName
  // input (WI-572), so a harness swap changes the stamp like any other
  // identity-relevant input.
  const inputHashes = stationConfig.inputs.map((inputName) => {
    try {
      const inputPath = inputName === 'seed.json' && card.owned_paths[0]
        ? join(card.owned_paths[0], 'seed.json')
        : join(projectRoot, inputName);
      const content = readFileSync(inputPath);
      return createHash('sha256').update(content).digest('hex');
    } catch {
      return '';
    }
  });

  if (feedbackString !== undefined) {
    inputHashes.push(createHash('sha256').update(feedbackString).digest('hex'));
  }

  const bindingStamp = computeBindingStamp({
    modelId: effectiveModel ?? '',
    promptTemplateVersion: stationConfig.prompt_version ?? '',
    inputArtifactHashes: inputHashes,
    flowVersion: flow.version,
    // WI-572 / review #7: the adapter's identity rides the dedicated
    // adapterName stamp input (appended only when present, so non-harness
    // stamps are untouched) rather than being folded into modelId — a harness
    // swap changes the stamp like any other identity-relevant input, and
    // modelId stays semantically the model id.
    adapterName: harnessAdapter.name,
  });

  // ── WI-571: effectful-station outbox discipline (Phase 3, FR-11) ────────
  // Mirrors the transform effectful path exactly (SPEC §5 exactly-once): for
  // stationConfig.effectful, reconcile against any prior outbox record BEFORE
  // deciding to invoke the harness — a harness invocation IS the billed/
  // irreversible side effect here, same as a transform's model call. Never
  // entered for a pure (effectful=false) station.
  let effectfulSkipOutput: Awaited<ReturnType<typeof readCheckpoint>> = null;
  // Idempotency key of a PENDING outbox intent written THIS dispatch (fire
  // path only) — held so a WI-566 cap-exhaustion scrap below can abandon the
  // intent rather than leave it dangling for a future replay to escalate_hold on.
  let pendingIntentKey: string | null = null;

  if (stationConfig.effectful) {
    const idempotencyKey = `${flow.version}:${cardId}:${stationId}:${card.attempt}`;
    const reconcile = reconcileOnResume(stateDb, idempotencyKey);

    if (reconcile.action === 'escalate_hold') {
      // Pending intent of unknown outcome — never blind-retry the harness invoke.
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `effectful harness station '${stationId}' has a pending outbox intent with unknown outcome (key: ${idempotencyKey}); manual reconciliation required`,
        err, runId, true,
      );
      return false;
    }

    if (reconcile.action === 'skip') {
      // Effect already committed on a prior run — the harness must not be
      // re-invoked. Reuse the stored checkpoint (committed = invoke succeeded
      // = checkpoint was written before commitIntent).
      const committedCheckpoint = readCheckpoint(stateDb, {
        run: runId, flow: String(flow.version), card: cardId, station: stationId, attempt: card.attempt,
      });
      if (!committedCheckpoint) {
        // Invariant violation: committed intent but no checkpoint. Fail-closed.
        escalateToHold(
          stateDb, db, cardId, stationId, card,
          `effectful harness station '${stationId}' has a committed outbox intent but no checkpoint (key: ${idempotencyKey}); data inconsistency`,
          err, runId, true,
        );
        return false;
      }
      effectfulSkipOutput = committedCheckpoint;
    } else {
      // action === 'fire' — first execution; write PENDING before the invoke.
      writePendingIntent(stateDb, {
        flow: String(flow.version), card: cardId, station: stationId, attempt: card.attempt,
        idempotencyKey,
        intent: { kind: 'harness', station: stationId, outputs: stationConfig.outputs },
      });
      pendingIntentKey = idempotencyKey;
    }
  }

  const existingCheckpoint = effectfulSkipOutput ?? readCheckpoint(stateDb, {
    run: runId, flow: String(flow.version), card: cardId, station: stationId, attempt: card.attempt,
  });

  if (
    effectfulSkipOutput === null &&
    existingCheckpoint !== null &&
    existingCheckpoint.stamp !== bindingStamp
  ) {
    cascadeInvalidateDownstream(stateDb, flow, cardId, card.attempt, stationId);
  }

  let stationOutput: { payload: unknown } | null =
    (effectfulSkipOutput !== null || existingCheckpoint?.stamp === bindingStamp)
      ? existingCheckpoint?.output ?? null
      : null;

  if (stationOutput === null) {
    // ── Render prompt + mount declared inputs (AC1) ──────────────────────────
    const promptTemplate = stationConfig.prompt_content ?? readFileSync(stationConfig.prompt_file!, 'utf-8');
    const prompt = renderPrompt(
      promptTemplate, stationConfig.inputs, projectRoot, feedbackString, [], card.owned_paths,
    );

    // Declared inputs are MOUNTED (name + path), not inlined bytes — the
    // reserved synthetic inputs ('feedback', 'seed.json') have no on-disk
    // artifact of their own and are threaded via the prompt only.
    const mountedInputs: MountedInput[] = stationConfig.inputs
      .filter((name) => name !== 'feedback' && name !== 'seed.json')
      .map((name) => ({ name, path: join(projectRoot, name) }));

    const timeoutMs =
      stationConfig.timeout_seconds !== undefined
        ? stationConfig.timeout_seconds * 1000
        : DEFAULT_HARNESS_TIMEOUT_MS;

    // ── WI-566: bounded retry loop, mirroring transform.ts runTransformStation
    // (`while callsMade < maxExecutionAttempts`). Every distinct failure class
    // is counted against the SAME durable cap and retried; on exhaustion the
    // card SCRAPS (never holds) with a reason that NAMES which class exhausted
    // it — distinct from a transform's 'model-incompatible' scrap.
    const schema = buildOutputSchema(stationConfig.output_schema?.fields ?? []);
    const outputFile = stationConfig.outputs[0];
    let callsMade = 0;
    let scrapReason = 'harness-invocation-failed';

    while (callsMade < maxExecutionAttempts) {
      // WI-567: attemptIndex is the PRE-increment call count (mirrors
      // transform.ts's `attemptIndex = ctx.attempt + callsMade`), so per-attempt
      // journal rows land at card.attempt, card.attempt+1, ... in dispatch order.
      const attemptIndex = card.attempt + callsMade;
      const invokeStartedAt = Date.now();

      // WI-568 rework: snapshot the project tree BEFORE this attempt's invoke()
      // (fresh per attempt — a prior failed attempt's stale files must never be
      // misattributed to a later one) so the integrity check below can catch ANY
      // file the harness touched, not just its declared output. Mirrors the
      // effectful deterministic path's snapshotTree/diffTouched pattern
      // (~1455-1468): an external agent CLI has raw Read/Write/Bash tool access
      // bounded only by the WI-561 runner's cwd-confinement to projectRoot, not
      // by owned_paths, so declared-outputs alone is not a complete touched-set
      // for this station kind (unlike transform, where the executor itself
      // performs the only write, from the model's structured response).
      const integrityBaseline = snapshotTree(projectRoot);

      let invokeResult: HarnessResult;
      try {
        // The original adapter-liveness work: stamp fresh liveness progress BEFORE awaiting invoke() too
        // (mirrors trackingAdapter.call's start-stamp above) — a long in-flight
        // harness attempt must keep the liveness clock fresh for its WHOLE
        // duration, not just at the resolution/throw stamps below.
        stampHarnessActivity();
        invokeResult = await harnessAdapter.invoke({
          prompt,
          inputs: mountedInputs,
          tools: stationConfig.tools ?? [],
          timeoutMs,
          model: effectiveModel,
        });
      } catch (invokeErr) {
        // WI-567 FR-8 fix: stamp fresh liveness progress even on a thrown
        // attempt (timeout/nonzero-exit/untagged) — the invoke() call still
        // consumed real wall-clock time and must count as progress.
        stampHarnessActivity();

        // ── Provider rate limit: PARK, do not spend an attempt (issue #3) ────
        // A cap is not a failed attempt. Retrying it cannot work until the
        // window resets, and because the retry loop had no delay, three
        // attempts were consumed in ~3 seconds — scrapping the card and
        // discarding paid work from earlier attempts along with it.
        //
        // So: leave `callsMade` alone, stamp cards.release_at with the reset the
        // provider itself reported, and hand the card back to 'ready' at the
        // same lane. The existing release gate (planTick) keeps it
        // undispatchable until then, and the run loop already sleeps to the
        // soonest gate with the consumption andon checked FIRST — so a cap
        // longer than the run's wall-clock budget halts rather than idling.
        if ((invokeErr as { code?: string }).code === 'harness-rate-limited') {
          const resetAtMs = (invokeErr as { resetAtMs?: number }).resetAtMs;
          const releaseAt = releaseAtForRateLimit(currentNow, resetAtMs, Date.now());
          // issue #26 AC5: a rate limit does not refund tokens already spent —
          // a call that ran for minutes before hitting the cap was still
          // billed. Recovered the SAME way as the general-throw catch below
          // (usageFromThrow), and folded/journaled BEFORE the park so the
          // run/wave budget sees it even though this attempt spends no
          // execution attempt. Safe to fold here: `attributes.outcome` stays
          // 'harness-rate-limited' regardless, so countConsecutiveRateLimitParks
          // (which keys on that attribute, not usageUnknown) is unaffected.
          const parkedUsage = usageFromThrow(invokeErr);
          let parkedUsageKnown = false;
          let parkedJournalUsage: JournalSpanInput['usage'];
          if (parkedUsage !== undefined && !('unknown' in parkedUsage)) {
            parkedUsageKnown = true;
            foldHarnessUsage(parkedUsage.tokens);
            parkedJournalUsage = harnessJournalUsage(parkedUsage, effectiveModel);
          }
          db.appendJournalSpan({
            runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.harness`,
            adapter: harnessAdapter.name, durationMs: Date.now() - invokeStartedAt,
            usageUnknown: !parkedUsageKnown, usage: parkedJournalUsage,
            attributes: {
              // Issue #5 ask (4): a usage_unknown row that names WHY. This is
              // what separates "capped, cost nothing" from "ran for minutes and
              // its spend is unrecorded" without inferring it from duration.
              outcome: 'harness-rate-limited',
              rate_limit_release_at: releaseAt,
              rate_limit_reset_reported: resetAtMs ?? null,
              ...rateLimitAttributes((invokeErr as { rateLimit?: RateLimitSnapshot }).rateLimit),
            },
          });
          // WI-571 follow-up: an EFFECTFUL station cannot park through a cap.
          // `pendingIntentKey !== null` means this dispatch already wrote a
          // PENDING outbox intent before the invoke (the 'fire' path above) —
          // the harness invocation itself is the billed/irreversible side
          // effect (SPEC §5), same as a transform's model call or a
          // deterministic station's command. A harness maker holds Read/
          // Write/Bash for up to `timeoutMs` and can run for minutes before
          // hitting a session cap, so 'harness-rate-limited' says only how
          // the invocation ENDED, not whether its tool calls landed first —
          // unlike the pure-station park below, where nothing was written to
          // the outbox and no side effect could have occurred.
          //
          // Parking here would be a blind retry in disguise: a park spends no
          // execution attempt, so the NEXT dispatch recomputes the identical
          // idempotency key (`${flow.version}:${cardId}:${stationId}:${card.attempt}`)
          // and reconcileOnResume can only ever answer 'escalate_hold' for a
          // pending intent of unknown outcome — it never resolves it. So a
          // parked effectful card would sit advertised as resumable (the
          // whole point of a park, and what the ingress listener's unattended
          // resume trusts) right up until the moment it is actually resumed,
          // when it discovers the same unresolved intent and holds anyway —
          // just later, after silently promising a working retry that could
          // never succeed. Escalate directly instead, onto the terminal
          // 'hold' lane, and leave the pending intent exactly as it is: we do
          // not know whether the invocation's side effects landed, so
          // discarding it would be the same guess in the other direction. A
          // human resolves it.
          if (pendingIntentKey !== null) {
            escalateToHold(
              stateDb, db, cardId, stationId, card,
              `effectful harness station '${stationId}' hit a provider rate limit with a pending outbox ` +
                `intent of unknown outcome (key: ${pendingIntentKey}); the invocation may have produced ` +
                `irreversible side effects before the cap, so it cannot be safely retried or parked — ` +
                `manual reconciliation required`,
              err, runId, true,
            );
            return false;
          }
          parkCardUntil(
            stateDb, db, cardId, stationId, card, releaseAt, runId,
            buildTransitionContext(stationId, flow, happyPathNext, terminalLanes, maxExecutionAttempts),
            (invokeErr as Error).message, err,
          );
          return false;
        }

        callsMade++;
        // Adapter-throw classification follows the transform/openai-adapter
        // precedent (transform.ts branches on err.code === 'vision-unsupported').
        // An UNTAGGED throw still scraps under a generic named reason — loud,
        // never a silent failure to classify.
        const code = (invokeErr as { code?: string }).code;
        scrapReason =
          code === 'harness-timeout'
            ? 'harness-timeout'
            : code === 'harness-nonzero-exit'
              ? 'harness-nonzero-exit'
              : `harness-invocation-failed: ${(invokeErr as Error).message}`;
        // issue #26 AC5: a thrown invocation was still BILLED for whatever it
        // did before it died (a timeout or nonzero exit does not refund
        // tokens already consumed). usageFromThrow is the ONE reader for
        // usage attached to a harness throw — never cast and reach for
        // `.usage` directly. When the adapter recovered a real figure, fold
        // it into the run/wave budget and journal it instead of a fabricated
        // usageUnknown row; when it returns undefined (the adapter genuinely
        // could not recover one — e.g. a claude wall-clock timeout, which
        // never emits a terminal result event to read usage from), this is
        // WI-567 AC2's original behaviour, unchanged: explicitly unknown,
        // never a fabricated zero, and nothing folds into the budget.
        const thrownUsage = usageFromThrow(invokeErr);
        let thrownUsageKnown = false;
        let thrownJournalUsage: JournalSpanInput['usage'];
        if (thrownUsage !== undefined && !('unknown' in thrownUsage)) {
          thrownUsageKnown = true;
          foldHarnessUsage(thrownUsage.tokens);
          thrownJournalUsage = harnessJournalUsage(thrownUsage, effectiveModel);
        }
        db.appendJournalSpan({
          runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.harness`,
          adapter: harnessAdapter.name, durationMs: Date.now() - invokeStartedAt,
          usageUnknown: !thrownUsageKnown, usage: thrownJournalUsage,
          attributes: { outcome: scrapReason },
        });
        // Issue #3: back off before the next attempt. Without this,
        // max_execution_attempts doubled as the wall-clock retry policy and any
        // persistent-but-temporary fault burned the whole budget in seconds.
        // Skipped once the cap is spent — nothing follows, so waiting only
        // delays the scrap.
        if (callsMade < maxExecutionAttempts) {
          await sleep(harnessRetryDelayMs(callsMade));
        }
        continue;
      }

      const durationMs = Date.now() - invokeStartedAt;

      // WI-567 FR-8 fix: stamp fresh liveness progress the instant invoke()
      // resolves successfully — covers the success path AND every paid-failure
      // branch below (missing/unparseable/invalid output), since they all fall
      // through from this single point.
      stampHarnessActivity();

      // WI-567 AC3/AC4: resolve this attempt's usage ONCE — a PAID attempt (the
      // adapter returned structured usage) folds into the SAME run/wave budget
      // accumulators trackingAdapter feeds, regardless of whether the OUTPUT
      // later turns out invalid (the call still happened and was billed).
      // Usage MUST come from the adapter's structured result, never scraped text.
      let usageKnown = false;
      let reportedTokens = 0;
      let reportedCost = 0;
      let journalUsage: JournalSpanInput['usage'];
      let rateLimitAttrs: Record<string, unknown> = {};
      if (!('unknown' in invokeResult.usage)) {
        const reported = invokeResult.usage;
        usageKnown = true;
        reportedTokens = reported.tokens;
        reportedCost = reported.cost;
        // Budgets fold the TOTAL, unchanged by issue #5's column split — the
        // breakdown below is a reporting concern, not an accounting one.
        foldHarnessUsage(reportedTokens);
        journalUsage = harnessJournalUsage(reported, effectiveModel);
        rateLimitAttrs = rateLimitAttributes(reported.rateLimit);
      }

      // ── WI-568 / Findings 6+7: MANDATORY owned-paths integrity gate ──────────
      // Runs on EVERY resolved attempt, the instant invoke() settles and BEFORE
      // the output-validity branches (missing / unparseable / schema-invalid) can
      // `continue`. Deferring it to the success branch (the prior bug) let a rogue
      // write outside owned_paths slip through whenever the SAME attempt also
      // returned malformed or missing output: every invalid branch continued past
      // the gate, the next attempt's fresh baseline folded the rogue file in, and
      // the card scrapped on cap exhaustion with the containment breach never
      // detected, never held, never journaled (Finding 6). A breach must escalate
      // to hold regardless of whether the output later validates.
      //
      // UNCONDITIONAL for kind:harness (unlike the transform path's
      // enforce_owned_paths opt-in) and checked against the FULL touched-set
      // (diffTouched vs the fresh per-attempt baseline), NOT just declared
      // outputs — an agent CLI has raw Read/Write/Bash bounded only by the
      // runner's cwd-confinement to projectRoot, not by owned_paths, so a
      // declared-outputs-only check would miss an undeclared rogue write.
      // checkIntegrity already symlink-canonicalizes and fails closed on an
      // unresolvable path, so plain-escape, symlink-escape, and ambiguous cases
      // are all covered by the reused helper.
      //
      // Finding 7: an EMPTY owned_paths set is fail-closed HERE — never a silent
      // pass. The shared runOwnedPathsIntegrity treats empty as opt-out (correct
      // for the deterministic path, which this must not change), but for a harness
      // nothing is a legal write target, so ANY touched file is a breach; only a
      // harness that touched nothing at all proceeds.
      const ownedPaths = card.owned_paths ?? [];
      const touchedPaths = diffTouched(integrityBaseline, snapshotTree(projectRoot));
      const integrityViolation: IntegrityResult | null =
        ownedPaths.length === 0
          ? touchedPaths.length > 0
            ? { ok: false, failures: touchedPaths.map((p) => ({ code: 'path_escape' as const, path: p })) }
            : null
          : runOwnedPathsIntegrity(projectRoot, ownedPaths, touchedPaths);
      if (integrityViolation && !integrityViolation.ok) {
        db.appendJournalSpan({
          runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.harness`,
          adapter: harnessAdapter.name, durationMs, usageUnknown: !usageKnown, usage: journalUsage,
          attributes: { outcome: `integrity_violation: ${describeIntegrity(integrityViolation)}` },
        });
        escalateToHold(
          stateDb, db, cardId, stationId, card,
          `integrity violation (owned_paths): project root modified during harness station '${stationId}' by an undeclared write — either the station escaped its owned_paths, or another process wrote to the project root while it ran: ${describeIntegrity(integrityViolation)}`,
          err, runId, true,
        );
        return false;
      }

      // ── Collect declared outputs FROM DISK (hybrid: like deterministic) ────
      // "validated present" == the declared output file exists on disk — the
      // harness wrote it itself during invoke.
      const missingOutputs = findMissingDeclaredOutputs(projectRoot, stationConfig);
      if (missingOutputs.length > 0) {
        callsMade++;
        scrapReason = `harness-output-missing: ${missingOutputs.join(', ')}`;
        db.appendJournalSpan({
          runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.harness`,
          adapter: harnessAdapter.name, durationMs, usageUnknown: !usageKnown, usage: journalUsage,
          attributes: { outcome: scrapReason },
        });
        continue;
      }

      // ── Coercive-parse + schema-validate ────────────────────────────────────
      // Reuses the exact transform recovery logic (coerciveParse) and the same
      // buildOutputSchema validator a transform station validates against.
      let payload: unknown = null;
      if (outputFile !== undefined) {
        try {
          payload = coerciveParse(readFileSync(join(projectRoot, outputFile), 'utf-8'));
        } catch {
          payload = null;
        }
      }
      if (payload === null) {
        callsMade++;
        scrapReason = `harness-output-unparseable: station '${stationId}' output '${outputFile ?? '(none declared)'}' is not valid JSON`;
        db.appendJournalSpan({
          runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.harness`,
          adapter: harnessAdapter.name, durationMs, usageUnknown: !usageKnown, usage: journalUsage,
          attributes: { outcome: scrapReason },
        });
        continue;
      }
      const validated = schema.validate(payload);
      if (!validated.ok) {
        callsMade++;
        scrapReason = `harness-output-invalid: station '${stationId}': ${validated.error}`;
        db.appendJournalSpan({
          runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.harness`,
          adapter: harnessAdapter.name, durationMs, usageUnknown: !usageKnown, usage: journalUsage,
          attributes: { outcome: scrapReason },
        });
        continue;
      }

      // ── Success ──────────────────────────────────────────────────────────
      // The mandatory owned-paths integrity gate already ran on this resolved
      // attempt (above, before the output-validity branches), so a clean arrival
      // here means the touched-set was fully contained within owned_paths.
      callsMade++;

      // Same StationOutput envelope shape a transform station builds (WI-289).
      // (This is the kernel-level card usage field, independent of the journal
      // row below — an unknown adapter usage fills it with a harmless zero
      // rather than leaving the required field unset.)
      const output: StationOutput<unknown> = {
        payload: validated.value,
        findings_hash: computeFindingsHash(validated.value),
        return_to: null, // harness maker always proceeds — no back-edge, mirrors transform
        usage: usageKnown ? { tokens: reportedTokens, cost: reportedCost } : { tokens: 0, cost: 0 },
      };
      stationOutput = output;

      // Hash every declared output artifact for the journal row (WI-567 AC1) —
      // name + sha256, never raw bytes, mirroring the containment profile's
      // "hashes, never transcripts" posture.
      const artifactHashes: Record<string, string> = {};
      for (const name of stationConfig.outputs) {
        try {
          artifactHashes[name] = createHash('sha256').update(readFileSync(join(projectRoot, name))).digest('hex');
        } catch {
          /* best-effort — presence of this exact file was already confirmed above */
        }
      }
      db.appendJournalSpan({
        runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.harness`,
        adapter: harnessAdapter.name, durationMs, usageUnknown: !usageKnown, usage: journalUsage,
        // Issue #5: the capacity snapshot rides along on the priced row, so
        // "what did this run draw against the plan" is a query rather than an
        // inference from interactive usage bars.
        attributes: { artifact_hashes: artifactHashes, outcome: 'success', ...rateLimitAttrs },
      });

      // ── Write checkpoint (binding stamp) ───────────────────────────────────
      writeCheckpoint(stateDb, { run: runId, flow: String(flow.version), card: cardId, station: stationId, attempt: card.attempt }, {
        stamp: bindingStamp,
        output,
      });

      // ── Commit outbox intent for effectful stations (effect succeeded) ────
      // commitIntent AFTER writeCheckpoint so the intent is only committed
      // once the output is safely persisted — if we crash between the two,
      // the next resume sees pending+checkpoint and escalates_hold (WI-571
      // AC3), never blind-re-fires.
      if (stationConfig.effectful) {
        const idempotencyKey = `${flow.version}:${cardId}:${stationId}:${card.attempt}`;
        commitIntent(stateDb, idempotencyKey);
      }
      break;
    }

    if (stationOutput === null) {
      // Cap exhausted without a successful attempt — scrap (never a silent
      // advance, never a hold), naming the failure class that exhausted it.
      // No checkpoint on a scrapped attempt. Abandon a dangling PENDING outbox
      // intent (WI-571) so a future replay of this scrapped attempt never
      // escalate_holds on an effect that never landed.
      if (pendingIntentKey !== null) {
        discardIntent(stateDb, pendingIntentKey);
      }
      advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, 'scrap', 'scrapped', 0, scrapReason, runId);
      return true;
    }
  }

  // ── Finding 9: consumption andon for the GATELESS maker path ─────────────
  // A gated harness gets its andon check inside runGateCheckOrAdvance (after the
  // gate model call). A gateless maker has no such call site, so without this a
  // budget-busting harness invocation whose usage was just folded advances
  // straight to done and the andon only trips one tick late. Mirror the transform
  // maker's post-call check (~line 2181): check BEFORE advancing so a tripped
  // andon leaves the card in its current (working) lane, not at done. Scoped to
  // the gateless path so the gated path's own check (which already runs) is left
  // exactly as-is.
  if (!stationConfig.gateCheck) {
    const andon = checkConsumptionAndon(
      { runStartedAt, now: currentNow, tokensSpent: getTokensSpent() },
      { wallClockSeconds, maxTokens },
    );
    if (andon.tripped) {
      onAndonTrip(`${andon.reason} budget exceeded`);
      releaseSlot(stateDb, cardId, stationId, runId);
      return false;
    }
  }

  // ── Gate check (if configured) + post-work routing (the original deterministic-gate work) ───────────
  // Routes through the SAME shared helper a transform/deterministic maker
  // uses (WI-565 AC4) — the harness output is indistinguishable from a
  // transform's typed stationOutput at this point. harnessRegistry threaded
  // through so an AGENTIC critic (WI-570) can be resolved by name.
  return runGateCheckOrAdvance({
    db, stateDb, runId, stationConfig, stationId, cardId, card,
    stationOutput,
    trackingAdapter, projectRoot, flow, happyPathNext, terminalLanes, maxExecutionAttempts,
    currentNow, runStartedAt, wallClockSeconds, maxTokens, getTokensSpent, onAndonTrip,
    harnessRegistry, foldHarnessUsage, err,
  });
}


// ---------------------------------------------------------------------------
// Subflow station (the original multi-flow engine work — flow-as-station composition)
// ---------------------------------------------------------------------------

/** Everything the seam needs to run one child flow to terminal state. */
export interface SubflowInvocation {
  /** Absolute child flow.yaml path (loader-resolved, cycle/depth-validated). */
  flowPath: string;
  /** Derived child run id (deriveSubflowRunId) — deterministic per attempt. */
  runId: string;
  /** Absolute path of the seed artifact for the child's entry station, if the
   *  calling station declares an input. */
  seedPath?: string;
  /** The parent run's project root — the child runs in the SAME root so the
   *  parent reads the child's declared outputs directly (v1 contract). */
  projectRoot: string;
  /** Parent's REMAINING run budget — the child's ceiling (min with its own). */
  budgetMaxTokens?: number;
  budgetWallClockSeconds?: number;
  /** Lineage for the journal (parent run id + calling station). */
  parentRunId: string;
  parentStation: string;
}

/** Terminal report of one child-flow invocation. */
export type SubflowOutcome =
  | { outcome: 'done'; tokens?: number; costUsd?: number }
  | { outcome: 'scrap' | 'halted' | 'error'; reason: string; tokens?: number; costUsd?: number };

export type SubflowSeam = (invocation: SubflowInvocation) => Promise<SubflowOutcome>;

interface SubflowArgs {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  trackingAdapter: ModelAdapter;
  happyPathNext: Record<string, string | null>;
  terminalLanes: Set<string>;
  maxExecutionAttempts: number;
  projectRoot: string;
  flow: FlowConfig;
  currentNow: number;
  runStartedAt: number;
  wallClockSeconds: number;
  maxTokens: number;
  getTokensSpent: () => number;
  onAndonTrip: (reason: string) => void;
  harnessRegistry?: HarnessRegistry;
  foldHarnessUsage: (tokens: number) => void;
  stampHarnessActivity: () => void;
  runSubflow?: SubflowSeam;
  err: (msg: string) => void;
}

/**
 * Execute a `kind: subflow` station: run the referenced child flow to terminal
 * state through the injected seam, with the four composition guarantees the
 * PRD pins (prd/drafts/multi-flow-engine.md, slice D):
 *
 *   - BUDGET: the child is launched with the parent's REMAINING run budget as
 *     its ceiling, and its journaled spend folds back into the parent's
 *     run/wave accumulators — one ceiling per user intent, counted once.
 *   - FAILURE: a child terminating in scrap/halt/error fails the attempt
 *     NAMED; the bounded retry loop mirrors the harness station (WI-566), and
 *     cap exhaustion scraps with the child's reason — never a silent advance.
 *   - IDEMPOTENT RESUME: the checkpoint binding stamp folds the child flow
 *     path + input hashes; a resumed parent skips a completed child, and a
 *     re-dispatched attempt derives the SAME child run id so the child's own
 *     lease/fingerprint machinery absorbs the duplicate invocation.
 *   - LINEAGE: every attempt journals a `<station>.subflow` span carrying the
 *     child run id, so parent-run → child-run lineage is queryable.
 */
async function executeSubflowStation(args: SubflowArgs): Promise<boolean> {
  const {
    db, stateDb, runId, stationConfig, stationId, cardId, trackingAdapter, happyPathNext,
    terminalLanes, maxExecutionAttempts, projectRoot, flow, currentNow, runStartedAt,
    wallClockSeconds, maxTokens, getTokensSpent, onAndonTrip, harnessRegistry,
    foldHarnessUsage, stampHarnessActivity, runSubflow, err,
  } = args;

  const card = db.getCard(runId, cardId);
  if (!card) {
    releaseSlot(stateDb, cardId, stationId, runId);
    return false;
  }

  const childFlowPath = stationConfig.flow;
  if (childFlowPath === undefined || runSubflow === undefined) {
    // Configuration failure — loader guarantees `flow` for kind: subflow, and
    // production always wires the seam; a gap in either is operator-visible.
    escalateToHold(
      stateDb, db, cardId, stationId, card,
      childFlowPath === undefined
        ? `subflow station '${stationId}' has no child flow path (loader invariant violated)`
        : `subflow station '${stationId}' cannot run: no subflow runner is configured in this environment`,
      err, runId, true,
    );
    return false;
  }

  // ── Binding stamp (skip-on-resume): child flow path + input hashes ───────
  const inputHashes = stationConfig.inputs.map((inputName) => {
    try {
      return createHash('sha256').update(readFileSync(join(projectRoot, inputName))).digest('hex');
    } catch {
      return '';
    }
  });
  const bindingStamp = computeBindingStamp({
    modelId: '',
    promptTemplateVersion: '',
    inputArtifactHashes: inputHashes,
    flowVersion: flow.version,
    adapterName: `subflow:${childFlowPath}`,
  });

  const existingCheckpoint = readCheckpoint(stateDb, {
    run: runId, flow: String(flow.version), card: cardId, station: stationId, attempt: card.attempt,
  });
  if (existingCheckpoint !== null && existingCheckpoint.stamp !== bindingStamp) {
    cascadeInvalidateDownstream(stateDb, flow, cardId, card.attempt, stationId);
  }

  let stationOutput: { payload: unknown } | null =
    existingCheckpoint?.stamp === bindingStamp ? existingCheckpoint.output : null;

  if (stationOutput === null) {
    const seedInput = stationConfig.inputs[0];
    let callsMade = 0;
    let scrapReason = 'subflow-failed';

    while (callsMade < maxExecutionAttempts) {
      const attemptIndex = card.attempt + callsMade;
      const childRunId = deriveSubflowRunId(runId, stationId, attemptIndex);
      const invokeStartedAt = Date.now();

      // Parent's remaining budget = the child's ceiling. Infinity → undefined
      // (no caller-imposed ceiling; the child's own declaration governs).
      const remainingTokens = maxTokens - getTokensSpent();
      const remainingWallClock = wallClockSeconds - (currentNow - runStartedAt);

      let result: SubflowOutcome;
      try {
        result = await runSubflow({
          flowPath: childFlowPath,
          runId: childRunId,
          ...(seedInput !== undefined && { seedPath: join(projectRoot, seedInput) }),
          projectRoot,
          ...(Number.isFinite(remainingTokens) && { budgetMaxTokens: Math.max(0, Math.floor(remainingTokens)) }),
          ...(Number.isFinite(remainingWallClock) && { budgetWallClockSeconds: Math.max(0, Math.floor(remainingWallClock)) }),
          parentRunId: runId,
          parentStation: stationId,
        });
      } catch (invokeErr) {
        result = {
          outcome: 'error',
          reason: `subflow runner threw: ${invokeErr instanceof Error ? invokeErr.message : String(invokeErr)}`,
        };
      }

      // A child run consuming wall-clock is liveness progress regardless of outcome.
      stampHarnessActivity();
      const durationMs = Date.now() - invokeStartedAt;

      // FR-18: fold the child's journaled spend into the parent's run/wave
      // accumulators exactly once, on every outcome that reports it — a failed
      // child still spent real tokens.
      const usageKnown = result.tokens !== undefined;
      if (result.tokens !== undefined) foldHarnessUsage(result.tokens);
      // A subflow reports one rolled-up scalar, with no per-class split to
      // pass on: keep the pre-#5 shape (total in inputTokens, cache columns
      // left NULL) rather than fabricating a breakdown. getRunUsageTotals
      // COALESCEs the missing columns, so this still counts exactly once.
      const journalUsage = usageKnown
        ? { model: '', inputTokens: result.tokens ?? 0, outputTokens: 0, costUsd: result.costUsd ?? 0 }
        : undefined;

      if (result.outcome !== 'done') {
        callsMade++;
        scrapReason = `subflow-${result.outcome}: ${result.reason}`;
        db.appendJournalSpan({
          runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.subflow`,
          adapter: `subflow:${childFlowPath}`, durationMs, usageUnknown: !usageKnown, usage: journalUsage,
          attributes: { child_run_id: childRunId, outcome: scrapReason },
        });
        continue;
      }

      // Child done — its declared outputs are the station's contract. A child
      // that completed without producing them is a child-flow bug: named
      // attempt failure, same retry/cap discipline as every other class.
      const missingOutputs = findMissingDeclaredOutputs(projectRoot, stationConfig);
      if (missingOutputs.length > 0) {
        callsMade++;
        scrapReason = `subflow-output-missing: ${missingOutputs.join(', ')}`;
        db.appendJournalSpan({
          runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.subflow`,
          adapter: `subflow:${childFlowPath}`, durationMs, usageUnknown: !usageKnown, usage: journalUsage,
          attributes: { child_run_id: childRunId, outcome: scrapReason },
        });
        continue;
      }

      // ── Success ────────────────────────────────────────────────────────────
      callsMade++;
      const output: StationOutput<unknown> = {
        payload: null, // like a deterministic station, outputs live on disk
        findings_hash: computeFindingsHash(inputHashes.join(':')),
        return_to: null,
        usage: { tokens: result.tokens ?? 0, cost: result.costUsd ?? 0 },
      };
      stationOutput = output;

      db.appendJournalSpan({
        runId, cardId, station: stationId, attempt: attemptIndex, name: `${stationId}.subflow`,
        adapter: `subflow:${childFlowPath}`, durationMs, usageUnknown: !usageKnown, usage: journalUsage,
        attributes: { child_run_id: childRunId, outcome: 'success' },
      });

      writeCheckpoint(stateDb, { run: runId, flow: String(flow.version), card: cardId, station: stationId, attempt: card.attempt }, {
        stamp: bindingStamp,
        output,
      });
      break;
    }

    if (stationOutput === null) {
      // Cap exhausted — scrap with the child's named failure (never silent).
      advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, 'scrap', 'scrapped', 0, scrapReason, runId);
      return true;
    }
  }

  // Gateless consumption-andon check (mirrors the harness maker, Finding 9):
  // the child's folded spend must be able to trip the andon BEFORE advancing.
  if (!stationConfig.gateCheck) {
    const andon = checkConsumptionAndon(
      { runStartedAt, now: currentNow, tokensSpent: getTokensSpent() },
      { wallClockSeconds, maxTokens },
    );
    if (andon.tripped) {
      onAndonTrip(`${andon.reason} budget exceeded`);
      releaseSlot(stateDb, cardId, stationId, runId);
      return false;
    }
  }

  return runGateCheckOrAdvance({
    db, stateDb, runId, stationConfig, stationId, cardId, card,
    stationOutput,
    trackingAdapter, projectRoot, flow, happyPathNext, terminalLanes, maxExecutionAttempts,
    currentNow, runStartedAt, wallClockSeconds, maxTokens, getTokensSpent, onAndonTrip,
    harnessRegistry, foldHarnessUsage, err,
  });
}

// ---------------------------------------------------------------------------
// FSM routing helpers
// ---------------------------------------------------------------------------

/**
 * Build a `TransitionContext` for a single station execution.
 *
 * The context encodes the per-station routing parameters the FSM needs to
 * decide the legal next state.  Called immediately before firing `transition()`
 * so that every routing decision goes through the kernel FSM rather than being
 * computed inline in the executor (single-source-of-truth guarantee).
 *
 * `reworkCap` comes from the station's gate config (or 0 for ungated stations,
 * which means QC_REJECT is never a valid event for them — the FSM enforces this
 * via the `validBackEdges` check).
 */
function buildTransitionContext(
  stationId: string,
  flow: FlowConfig,
  happyPathNext: Record<string, string | null>,
  terminalLanes: Set<string>,
  maxExecutionAttempts: number,
): TransitionContext {
  const stationConfig = flow.stations[stationId];
  const reworkCap = stationConfig?.gateCheck?.reworkCap ?? 0;
  const capPolicy = flow.defaults?.capPolicy ?? 'scrap';
  const onDepScrap = flow.defaults?.onDepScrap ?? 'scrap';

  return {
    happyPathNext,
    terminalLanes: [...terminalLanes],
    reworkCap,
    maxExecutionAttempts,
    capPolicy,
    onDepScrap,
    validBackEdges: flow.back_edges ?? [],
  };
}

/**
 * Synthesise an `FsmState` representing a card at completion: lane unchanged,
 * status='done_pending_ack'.
 *
 * IMPORTANT: this FsmState is a TRANSIENT QUERY object — it is synthesised
 * solely to ask the FSM "given this card has finished work, what is the legal
 * next state?".  The executor does NOT persist `done_pending_ack`; the
 * MARK_DONE → done_pending_ack → INTEGRITY_PASS/QC_REJECT sequence is
 * logically atomic from the state-DB's perspective (we atomically jump from
 * 'working' to whatever the FSM says comes after done_pending_ack).
 */
function syntheticDonePendingAck(
  card: { lane: string; attempt: number },
  /**
   * Reworks already spent AT THE GATE this transition is being evaluated
   * against — `gateReworksSpent(...)`, never `card.rework_count` (issue #1).
   * The FSM compares this against `ctx.reworkCap`, which is that same gate's
   * declared `check.rework_cap`, so a lifetime counter here let an upstream
   * gate's reworks exhaust this gate's budget.
   *
   * REQUIRED even at the INTEGRITY_PASS / FAN_OUT call sites, where the FSM
   * ignores it: an optional parameter defaulting to the lifetime scalar is
   * exactly the footgun this rename exists to remove.
   */
  gateReworkCount: number,
): FsmState {
  return {
    lane: card.lane,
    status: 'done_pending_ack',
    executionAttempt: card.attempt,
    reworkCount: gateReworkCount,
  };
}

/**
 * `gateReworkCount` for `syntheticDonePendingAck` call sites whose event is NOT
 * QC_REJECT. Only QC_REJECT reads `state.reworkCount` (transitions.ts ~186);
 * INTEGRITY_PASS and FAN_OUT ignore it entirely, so those sites would otherwise
 * pay a card_log read on a hot path purely to fill a parameter.
 *
 * A named constant rather than a default value: the parameter stays REQUIRED,
 * so a QC_REJECT site still cannot silently inherit the lifetime scalar
 * (issue #1), while the sites that provably never read it don't query for it.
 */
const REWORK_COUNT_UNREAD = 0;

/**
 * Reworks this card has already spent at `stationId`'s gate, read from the
 * card_log (see `countGateReworks`). Guard #1's counter — deliberately NOT
 * `cards.rework_count`, which is the card's lifetime total across every gate.
 */
function gateReworksSpent(
  db: ConduitDB,
  runId: string,
  cardId: string,
  stationId: string,
): number {
  return countGateReworks(db.getCardLogForRun(runId, cardId), stationId);
}

/**
 * Escalate a card to 'held' status because the FSM returned an illegal
 * transition (contradictory state).  This is the same path used by the
 * executor's escalation loop for plan.escalations — fail-closed rather than
 * silently advancing.
 *
 * Writes the hold card_log entries and freezes the card in 'held' before
 * returning so callers can surface an error message and stop advancing.
 *
 * `err` is the io.err channel threaded from runExecutor — emits to stderr for
 * parity with the plan.escalations path (Change 2: surface in-station escalations).
 */
function escalateToHold(
  stateDb: Database,
  db: ConduitDB,
  cardId: string,
  stationId: string,
  card: { lane: string; attempt: number },
  detail: string,
  err: (msg: string) => void,
  runId: string = DEFAULT_RUN_ID,
  /**
   * When true, also moves the card's LANE to the terminal 'hold' lane (the
   * applyHoldTimeout 'escalate' / postHitlHold convention — slack.ts ~337),
   * instead of leaving lane unchanged (the default, status-only convention
   * every other caller relies on — see executor-cardlog.test.ts's "lane
   * unchanged, status held" sanity check). Used by the harness path (WI-565
   * W4 seam): a harness output-validation failure routes the card fully off
   * the station into the flow's 'hold' terminal lane, not just a held status
   * parked at the station.
   */
  moveToHoldLane: boolean = false,
): void {
  const destLane = moveToHoldLane ? 'hold' : card.lane;
  db.appendCardLog({
    runId,
    kind: 'entered_lane',
    cardId,
    station: stationId,
    attempt: card.attempt,
    sourceLane: card.lane,
    destLane,
    reasonClass: 'hold',
  });
  db.appendCardLog({
    runId,
    kind: 'terminal',
    cardId,
    station: stationId,
    attempt: card.attempt,
    reason: detail,
  });
  // Review #2: release the active_workers slot in the SAME transaction that
  // freezes the card — a held card is no longer working, so it must not keep
  // consuming a WIP slot. Leaving the row behind permanently blocked sibling
  // cards at a wip:1 station (reconcile only reclaims status='working' cards)
  // and kept the liveness watchdog from tripping (activeWorkerCount > 0), so
  // the run hung until the wall-clock andon instead of surfacing the hold.
  stateDb
    .transaction(() => {
      if (moveToHoldLane) {
        stateDb.prepare("UPDATE cards SET lane = 'hold', status = 'held' WHERE run_id = $runId AND id = $id").run({ $runId: runId, $id: cardId });
      } else {
        stateDb.prepare("UPDATE cards SET status = 'held' WHERE run_id = $runId AND id = $id").run({ $runId: runId, $id: cardId });
      }
      stateDb
        .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station')
        .run({ $runId: runId, $id: cardId, $station: stationId });
    })
    .immediate();
  // Surface the escalation on stderr — parity with the plan.escalations path
  // which calls io.err directly in the main loop (SPEC: escalate AND surface to a human).
  err(`escalation: card ${cardId} held — ${detail}`);
}

/**
 * How many provider-cap parks this card has taken in a row at `stationId`,
 * INCLUDING the one being taken now.
 *
 * Counted from the JOURNAL, not the card_log, and that choice is load-bearing:
 * card_log carries `UNIQUE(run_id, card_id, station, attempt, kind)` and is
 * written with INSERT OR IGNORE, while a park deliberately leaves `attempt`
 * alone — so every park after the first at a given station collapses into the
 * first one's row and a card_log-based counter would read 1 forever. The
 * journal is append-only and every harness outcome writes a
 * `<station>.harness` span carrying an `outcome` attribute, so trailing
 * 'harness-rate-limited' spans are exactly the consecutive parks: a success, a
 * scrap reason, or an integrity violation all write their own span and break
 * the streak.
 */
function countConsecutiveRateLimitParks(
  db: ConduitDB,
  runId: string,
  cardId: string,
  stationId: string,
): number {
  const spans = db.getJournalSpansForRun(runId, cardId);
  const spanName = `${stationId}.harness`;
  let streak = 0;
  for (let i = spans.length - 1; i >= 0; i--) {
    const span = spans[i]!;
    if (span.name !== spanName) continue;
    if (span.attributes.outcome !== 'harness-rate-limited') break;
    streak++;
  }
  return streak;
}

/**
 * Park a rate-limited card until `releaseAtSeconds`, spending no attempt (issue #3).
 *
 * `releaseAtSeconds` is in the INJECTED clock's frame — see releaseAtForRateLimit.
 *
 * Routed through `transition()` like every other post-work move — the FSM stays
 * the sole authority for what (lane, status) is legal, and an illegal move
 * escalates rather than being forced through with a raw UPDATE.
 *
 * Releases the active_workers slot in the SAME transaction that re-readies the
 * card, for the reason escalateToHold documents: a parked card is not working,
 * and leaving the row behind would block siblings at a wip:1 station forever
 * (reconcile only reclaims status='working' cards) while suppressing the
 * liveness watchdog.
 */
function parkCardUntil(
  stateDb: Database,
  db: ConduitDB,
  cardId: string,
  stationId: string,
  card: { lane: string; attempt: number; rework_count?: number },
  releaseAtSeconds: number,
  runId: string,
  ctx: TransitionContext,
  detail: string,
  err: (msg: string) => void,
): void {
  // Nothing else bounds a park (see MAX_CONSECUTIVE_RATE_LIMIT_PARKS), so the
  // repetition is what escalates. Checked BEFORE the move, so the cap counts
  // the parks already survived rather than the one being made.
  const parks = countConsecutiveRateLimitParks(db, runId, cardId, stationId);
  if (parks >= MAX_CONSECUTIVE_RATE_LIMIT_PARKS) {
    escalateToHold(
      stateDb, db, cardId, stationId, card,
      `card '${cardId}' has hit a provider rate limit ${parks} times in a row at '${stationId}' ` +
        `without making progress — the cap is not clearing, or the failure is being misreported ` +
        `as one. Held rather than parked again. Last detail: ${detail}`,
      err, runId,
      // Onto the 'hold' TERMINAL lane, not merely a held status at the station
      // (the harness path's convention). A card left held at its own lane is
      // still `lane != 'done'` with a live gate, which is exactly the shape
      // getRunParkedRelease reads as parked — the run would be recorded
      // 'parked' again and the listener would resume it into the same wall.
      true,
    );
    return;
  }

  const fsmResult = transition(
    {
      lane: card.lane,
      status: 'working',
      executionAttempt: card.attempt,
      reworkCount: card.rework_count ?? 0,
    },
    { type: 'RATE_LIMITED' },
    ctx,
  );
  if (!fsmResult.ok) {
    escalateToHold(stateDb, db, cardId, stationId, card,
      `FSM illegal_transition on RATE_LIMITED for harness station '${stationId}'`, err, runId);
    return;
  }

  db.appendCardLog({
    runId,
    kind: 'entered_lane',
    cardId,
    station: stationId,
    attempt: card.attempt,
    sourceLane: card.lane,
    destLane: fsmResult.next.lane,
    // NOT 'rework' — countGateReworks counts rework-classed entries against a
    // gate's cap, and a provider cap must never consume one.
    reasonClass: 'rate_limited',
  });

  stateDb
    .transaction(() => {
      stateDb
        .prepare(
          `UPDATE cards SET status = $status, release_at = $releaseAt
           WHERE run_id = $runId AND id = $id`,
        )
        .run({ $status: fsmResult.next.status, $releaseAt: releaseAtSeconds, $runId: runId, $id: cardId });
      stateDb
        .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station')
        .run({ $runId: runId, $id: cardId, $station: stationId });
    })
    .immediate();

  err(
    `rate limited: card ${cardId} parked at '${stationId}' until release_at=` +
      `${releaseAtSeconds} (no attempt consumed) — ${detail}`,
  );
}

/**
 * The consumption andon's halt line. A run halted while parked behind a
 * provider cap did not run away — it was told to wait and could not afford to
 * (issue #7). The generic line read as a failure, and nothing named the time
 * after which `conduit resume` would succeed.
 */
function consumptionHaltMessage(reason: ConsumptionAndon['reason'], rateLimitReleaseAt: number | null): string {
  if (rateLimitReleaseAt === null) return `andon: run halted — ${reason} budget exceeded`;
  return (
    `andon: run halted — ${reason} budget exceeded while parked behind a provider rate limit; ` +
    `soonest release_at=${rateLimitReleaseAt} (${formatReleaseAt(rateLimitReleaseAt)}) — ` +
    `cards are untouched and the run is resumable after that time`
  );
}

// ---------------------------------------------------------------------------
// DB mutation helpers
// ---------------------------------------------------------------------------

/**
 * Advance a card to a new (lane, status), release its active_workers slot, and
 * optionally increment rework_count — all in a single atomic transaction.
 *
 * Card-log ordering (WI-381 / FR-7 / FR-8):
 *   The journal DB is a separate SQLite file from the state DB; true cross-file
 *   atomicity is impossible. We write the card_log entry BEFORE the state-db
 *   commit so that a crash between the two writes leaves a phantom pre-commit
 *   entry in the journal. That phantom is harmless: the UNIQUE(card_id, station,
 *   attempt, kind) constraint in appendCardLog deduplicates the entry when the
 *   executor replays the same advance on resume, so no duplicate can persist and
 *   no committed card-state is ever orphaned.
 */
function advanceCard(
  stateDb: Database,
  db: ConduitDB,
  cardId: string,
  stationId: string,
  sourceLane: string,
  attempt: number,
  nextLane: string,
  nextStatus: string,
  reworkDelta: number,
  terminalReason?: string,
  runId: string = DEFAULT_RUN_ID,
): void {
  // ── Journal-first: append BEFORE the state-db commit ─────────────────────
  // reasonClass derivation: rework when reworkDelta > 0, scrap when routing to
  // the scrap terminal lane, forward for every other advance (including 'done').
  db.appendCardLog({
    runId,
    kind: 'entered_lane',
    cardId,
    station: stationId,
    attempt,
    sourceLane,
    destLane: nextLane,
    reasonClass: reworkDelta > 0 ? 'rework' : nextLane === 'scrap' ? 'scrap' : 'forward',
  });

  // FR-4: terminal entries for scrap only (hold terminal is written in the
  // escalation loop; reaching 'done' MUST NOT produce a terminal entry).
  if (nextLane === 'scrap' && terminalReason !== undefined) {
    db.appendCardLog({
      runId,
      kind: 'terminal',
      cardId,
      station: stationId,
      attempt,
      reason: terminalReason,
    });
  }

  // ── State-db commit ───────────────────────────────────────────────────────
  stateDb
    .transaction(() => {
      if (reworkDelta > 0) {
        // Bump attempt alongside rework_count so the next execution at this
        // station writes card_log entries and a checkpoint at a distinct
        // (station, attempt) key — preventing UNIQUE-key collisions with the
        // reject/rework entries written at the current attempt (FR-7 / WI-386).
        stateDb
          .prepare(
            `UPDATE cards SET lane = $lane, status = $status, rework_count = rework_count + $delta, attempt = attempt + 1 WHERE run_id = $runId AND id = $id`,
          )
          .run({ $lane: nextLane, $status: nextStatus, $delta: reworkDelta, $runId: runId, $id: cardId });
      } else {
        stateDb
          .prepare('UPDATE cards SET lane = $lane, status = $status WHERE run_id = $runId AND id = $id')
          .run({ $lane: nextLane, $status: nextStatus, $runId: runId, $id: cardId });
      }
      stateDb
        .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station')
        .run({ $runId: runId, $id: cardId, $station: stationId });
    })
    .immediate();
}

/**
 * Direct-scrap FSM routing shared by the pooled MARK_DONE 'scrap' worker
 * outcome and the synchronous deterministic-failure path (WI-686). A
 * deterministic command is a pure function of its inputs
 * (worker/worker-entry.ts:92-98) — re-running it after a failure reproduces
 * the identical result, so a rework/retry back-edge is futile. The
 * fail-closed terminal outcome is therefore an immediate scrap (a single
 * execution), never a retry-to-cap or — the WI-686 bug — a silent,
 * non-terminating re-dispatch loop.
 */
function scrapCardDirect(
  stateDb: Database,
  db: ConduitDB,
  cardId: string,
  stationId: string,
  card: Pick<Card, 'lane' | 'attempt'>,
  reason: string,
  runId: string = DEFAULT_RUN_ID,
): void {
  advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, 'scrap', 'scrapped', 0, reason, runId);
}

/** Release the active_workers slot without changing the card's state. */
function releaseSlot(stateDb: Database, cardId: string, stationId: string, runId: string = DEFAULT_RUN_ID): void {
  stateDb
    .transaction(() => {
      stateDb
        .prepare("UPDATE cards SET status = 'ready' WHERE run_id = $runId AND id = $id AND status = 'working'")
        .run({ $runId: runId, $id: cardId });
      stateDb
        .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station')
        .run({ $runId: runId, $id: cardId, $station: stationId });
    })
    .immediate();
}

// ---------------------------------------------------------------------------
// Rank station (WI-398)
// ---------------------------------------------------------------------------

interface RankStationArgs {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  trackingAdapter: ModelAdapter;
  happyPathNext: Record<string, string | null>;
  terminalLanes: Set<string>;
  maxExecutionAttempts: number;
  flow: FlowConfig;
  /** Effective project root — resolves candidates_from / ask_attach / selection_out (the original HITL reply-and-resume work). */
  projectRoot: string;
  err: (msg: string) => void;
}

/**
 * Execute a rank station (WI-398).
 *
 * A rank station runs the rank critic via quality/rank.ts::runRankCheck and
 * routes based on the resulting RankDecision — deliberately has NO auto-pick
 * path (FR-14 / NFR-3).
 *
 * Resume path (recorded selection): if `getRecordedHitlSelection` returns a
 * non-null value (human replied via `conduit reply`), advance the card directly
 * to `happyPathNext` — no re-run of the critic, no re-post to Slack.
 *
 * First-time path:
 *   await_selection  — outbox-guarded `postHitlHold`, park card as held.
 *   proceed_with_findings — advance to happyPathNext (no HITL needed).
 *   scrap / scrapped  — route to scrap terminal.
 */
async function executeRankStation(args: RankStationArgs): Promise<boolean> {
  const {
    db,
    stateDb,
    runId,
    stationConfig,
    stationId,
    cardId,
    trackingAdapter,
    happyPathNext,
    terminalLanes,
    maxExecutionAttempts,
    flow,
    projectRoot,
    err,
  } = args;

  const rankCheck = stationConfig.rankCheck!;

  const card = db.getCard(runId, cardId);
  if (!card) {
    releaseSlot(stateDb, cardId, stationId, runId);
    return false;
  }

  // ── Resume path: recorded selection un-parks the card ────────────────────
  // A human has replied (via `conduit reply` / WI-394). Advance directly to
  // happyPathNext without re-running the critic or re-posting the HITL prompt.
  const recordedSelection = getRecordedHitlSelection(db, cardId, runId);
  if (recordedSelection !== null) {
    // The original HITL reply-and-resume work FR-4: surface the selection as a flow-facing artifact so
    // downstream stations consume the pick like any other input. Written
    // BEFORE the card advances — the next station's dispatch must find the
    // file on disk. The write is idempotent (same selection → same bytes) and
    // path-guarded like every other kernel-written output.
    if (rankCheck.selectionOut !== undefined) {
      const detail = getRecordedHitlSelectionDetail(db, cardId, runId);

      // Containment (SPEC §7): confine the write target to the project root,
      // symlink-resolved. deliverPathWithinRoot catches a lexical `..`-escape and
      // an EXISTING symlinked leaf; a bare startsWith (the previous check) missed
      // a symlink INSIDE the root pointing out. The target may not exist yet, so
      // the parent's realpath is re-checked below — a symlinked parent dir under
      // the root must not launder the write outside it.
      // These escalations run on the RESUME path with a human selection already
      // recorded, so they route the card to the terminal 'hold' lane
      // (moveToHoldLane) rather than holding in-place: a status-only hold at the
      // rank lane would be immediately re-armed by pollHeldRankCards (a recorded
      // selection un-holds it), livelocking against the containment failure. A
      // contradictory selection_out is a genuine dead-end for a human, not a
      // retryable park (SPEC: escalate ambiguity, never auto-reverse).
      const contained = deliverPathWithinRoot(projectRoot, rankCheck.selectionOut);
      if (!contained.ok) {
        escalateToHold(
          stateDb, db, cardId, stationId, card,
          `rank station '${stationId}' selection_out '${rankCheck.selectionOut}' resolves outside the project root (${contained.reason})`,
          err, runId, true,
        );
        return false;
      }
      const lexicalTarget = contained.absPath;
      // Deterministic bytes — the checkpoint binding stamp hashes this artifact as
      // a downstream input, so the SAME selection must produce byte-identical
      // content. No Date.now(): the correlation id already uniquely names the
      // human's answer; a wall-clock stamp would break resume determinism.
      const selectionBytes = JSON.stringify(
        { selection: recordedSelection, correlation_id: detail?.correlationId ?? null },
        null,
        2,
      );
      try {
        const parentDir = dirname(lexicalTarget);
        mkdirSync(parentDir, { recursive: true });
        // Re-confine the (now-created) parent by its realpath — the target itself
        // could only be checked lexically above since it does not exist yet.
        const canonicalRoot = realpathSync(resolve(projectRoot));
        const canonicalParent = realpathSync(parentDir);
        if (canonicalParent !== canonicalRoot && !canonicalParent.startsWith(canonicalRoot + sep)) {
          escalateToHold(
            stateDb, db, cardId, stationId, card,
            `rank station '${stationId}' selection_out '${rankCheck.selectionOut}' parent resolves outside the project root`,
            err, runId, true,
          );
          return false;
        }
        writeFileSync(lexicalTarget, selectionBytes, 'utf-8');
      } catch (writeError) {
        const detailMsg = writeError instanceof Error ? writeError.message : 'unknown error';
        escalateToHold(
          stateDb, db, cardId, stationId, card,
          `rank station '${stationId}' could not write selection_out '${rankCheck.selectionOut}': ${detailMsg}`,
          err, runId, true,
        );
        return false;
      }

      // Owned-paths integrity gate — like deliver.files, when the flow opts into
      // enforce_owned_paths a kernel-written artifact must land inside the card's
      // owned_paths, not merely inside the project root. checkIntegrity requires
      // the touched path to exist, so this gates AFTER the (root-confined) write
      // and unlinks on a breach so nothing outside owned_paths persists.
      const enforceOwnedPaths = flow.defaults?.enforceOwnedPaths === true;
      const ownedPaths = card.owned_paths ?? [];
      if (enforceOwnedPaths && ownedPaths.length > 0) {
        const containment = checkIntegrity({
          projectRoot,
          ownedPaths,
          touchedPaths: [rankCheck.selectionOut],
          declaredArtifacts: [],
          output: null,
          validateOutput: () => true,
        });
        if (!containment.ok) {
          try {
            unlinkSync(lexicalTarget);
          } catch {
            /* best-effort cleanup — the escalation below is the real signal */
          }
          escalateToHold(
            stateDb, db, cardId, stationId, card,
            `rank station '${stationId}' selection_out '${rankCheck.selectionOut}' resolves outside owned_paths: ${describeIntegrity(containment)}`,
            err, runId, true,
          );
          return false;
        }
      }
    }

    const nextLane = happyPathNext[stationId] ?? null;
    if (!nextLane) {
      releaseSlot(stateDb, cardId, stationId, runId);
      return false;
    }
    const isTerminal = terminalLanes.has(nextLane);
    advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, nextLane, isTerminal ? 'complete' : 'ready', 0, undefined, runId);
    return true;
  }

  // ── First-time path ──────────────────────────────────────────────────────
  // Two candidate sources, mutually exclusive at load (the original HITL reply-and-resume work):
  //   candidates_from — the FLOW already ranked; read its artifact, no model.
  //   critic          — the original WI-398 path, unchanged.
  let decision: RankDecision;
  if (rankCheck.candidatesFrom !== undefined) {
    // Containment (SPEC §7): symlink-resolved, matching deliver.files — a bare
    // startsWith would let a symlink under the root read a file outside it.
    const contained = deliverPathWithinRoot(projectRoot, rankCheck.candidatesFrom);
    if (!contained.ok) {
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `rank station '${stationId}' candidates_from '${rankCheck.candidatesFrom}' resolves outside the project root (${contained.reason})`,
        err, runId,
      );
      return false;
    }
    let rawCandidates: string;
    try {
      rawCandidates = readFileSync(contained.absPath, 'utf-8');
    } catch {
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `rank station '${stationId}' could not read candidates_from '${rankCheck.candidatesFrom}'`,
        err, runId,
      );
      return false;
    }
    const candidates = parseCandidatesArtifact(rawCandidates);
    if (candidates === null) {
      // Fail-closed: never present a garbled board to a human (the original HITL reply-and-resume work).
      escalateToHold(
        stateDb, db, cardId, stationId, card,
        `rank station '${stationId}' candidates_from '${rankCheck.candidatesFrom}' is not ` +
          `a JSON array of strings or {id, label} objects`,
        err, runId,
      );
      return false;
    }
    decision = decideFromCandidates({
      candidates,
      hitlEnabled: rankCheck.hitlEnabled,
      noSelectionPolicy: rankCheck.noSelectionPolicy,
    });
  } else {
    let prompt: string;
    try {
      prompt = readFileSync(rankCheck.criticPromptFile, 'utf-8');
    } catch {
      escalateToHold(
        stateDb,
        db,
        cardId,
        stationId,
        card,
        `rank station '${stationId}' could not read criticPromptFile '${rankCheck.criticPromptFile}'`,
        err,
        runId,
      );
      return false;
    }

    decision = await runRankCheck({
      cardId,
      station: stationId,
      // Attribute the rank critic's usage span to this run (the original per-run usage-attribution work).
      runId,
      attempt: card.attempt,
      maxExecutionAttempts,
      model: rankCheck.criticModel,
      prompt,
      params: {},
      adapter: trackingAdapter,
      db,
      candidateIds: [],
      hitlEnabled: rankCheck.hitlEnabled,
      noSelectionPolicy: rankCheck.noSelectionPolicy,
    });
  }

  switch (decision.action) {
    case 'await_selection': {
      // Guard: hitlEnabled requires a flow-level egress channel — and the HITL
      // prompt must go to the channel that declares `uses: [hitl]`, NOT merely
      // the first egress channel (a delivery-only channel could be listed first).
      // This mirrors how hitlEnabled is derived at load. Fall back to the first
      // channel only for hand-built configs that declare no `uses` at all.
      const egress = flow.channels?.egress;
      const egressChannel =
        egress?.find((ch) => Array.isArray(ch.uses) && ch.uses.includes('hitl')) ?? egress?.[0];
      if (!egressChannel) {
        escalateToHold(
          stateDb,
          db,
          cardId,
          stationId,
          card,
          `rank station '${stationId}' has hitlEnabled but flow declares no egress channel`,
          err,
          runId,
        );
        return false;
      }

      // Stable correlationId — deterministic from (runId, cardId, stationId,
      // attempt), no randomness. runId LEADS so the id is run-unique: card ids
      // repeat across runs, and this string IS the outbox idempotency key AND the
      // card_log reason findRunForHitlCorrelation keys on — without runId, run B
      // would collide with run A's committed outbox row (never posting its ask)
      // and a button tap would resolve to the newest run, flipping the wrong
      // card. Crash-recovery still locates the pending row (same inputs → same
      // key) and skips the re-post via reconcileOnResume.
      const correlationId = `hitl::${runId}::${cardId}::${stationId}::${card.attempt}`;
      const botToken = process.env.SLACK_BOT_TOKEN ?? '';
      // This transport posts the ask text AND uploads ask_attach files
      // (egressSendFile below), so it needs both budgets.
      const transport = createSlackTransport({
        botToken,
        apiBaseUrl: `${resolveSlackApiBaseUrl()}/api`,
        fetchTimeoutMs: resolveSlackFetchTimeoutMs(),
        uploadTimeoutMs: resolveSlackUploadTimeoutMs(),
      });

      // WI-600, FR-7/FR-8: thread the HITL prompt on the run's triggering
      // message when its ingress substrate supplies the CONVENTIONAL 'thread_ts'
      // field (text/HITL sends have no deliver block to name a thread_from, so
      // unlike WI-599 this reads a fixed, literal substrate key). Reuses the
      // WI-599 helper — substrate lives on ingress_events keyed by run_id, never
      // re-derived here. Absent substrate/field is a normal degrade (CLI-
      // triggered runs have no ingress event): send unthreaded, journal the skip.
      const threadTs = resolveThreadAddress(db, { run_id: runId }, 'thread_ts');
      db.appendJournalSpan({
        runId,
        cardId,
        station: stationId,
        attempt: card.attempt,
        name: 'hitl.thread_resolution',
        attributes: {
          threadFrom: 'thread_ts',
          threadResolution:
            threadTs !== undefined
              ? 'threaded'
              : 'thread_ts absent from substrate — sent unthreaded (degrade)',
        },
      });

      // The original HITL reply-and-resume work FR-2: the flow shapes its own ask. ask_template renders
      // through the SAME renderPrompt pipeline as a worker prompt ({{artifact}}
      // placeholders resolve against the station's declared inputs — scope
      // validated at load). Absent → the kernel's default text, unchanged.
      let askText = `Rank results: ${decision.shortList.join(', ')}. Please select a candidate.`;
      if (rankCheck.askTemplateFile !== undefined) {
        try {
          const askTemplate = readFileSync(rankCheck.askTemplateFile, 'utf-8');
          askText = renderPrompt(
            askTemplate,
            stationConfig.inputs,
            projectRoot,
            undefined,
            (stationConfig.image_inputs ?? []).map((d) => d.path),
            card.owned_paths,
          );
        } catch (renderError) {
          const detail = renderError instanceof Error ? renderError.message : 'unknown error';
          escalateToHold(
            stateDb, db, cardId, stationId, card,
            `rank station '${stationId}' could not render ask_template: ${detail}`,
            err, runId,
          );
          return false;
        }
      }

      // The original HITL reply-and-resume work FR-2: files that ride WITH the ask (the artifact the human
      // is approving), through the same outbox-guarded upload as a deliver:
      // block — keyed per (correlationId, file) so a crash never re-uploads,
      // sent BEFORE the ask text so the question is the last thing in-thread.
      // An escalated (ambiguous pending) upload hard-pauses like a delivery.
      for (const attachFile of rankCheck.askAttach ?? []) {
        // Containment guard (review: critical) — an attach path must resolve
        // INSIDE the project root, or a hostile flow could exfiltrate host files
        // to the channel. Symlink-resolved (SPEC §7), matching deliver.files: a
        // bare startsWith would miss a symlink under the root pointing outside.
        const contained = deliverPathWithinRoot(projectRoot, attachFile);
        if (!contained.ok) {
          escalateToHold(
            stateDb, db, cardId, stationId, card,
            `rank station '${stationId}' ask_attach '${attachFile}' resolves outside the project root (${contained.reason})`,
            err, runId,
          );
          return false;
        }
        let attachResult;
        try {
          attachResult = await egressSendFile(db, transport, {
            runId,
            channel: egressChannel.target ?? '',
            filePath: contained.absPath,
            // keyPrefix embeds correlationId, which now leads with runId (Fix 1),
            // so the outbox key is already run-scoped — no separate runId needed.
            keyPrefix: `${correlationId}::attach::${attachFile}`,
            threadTs,
          }, createFilesInfoReconciler({
            botToken,
            apiBaseUrl: `${resolveSlackApiBaseUrl()}/api`,
            channel: egressChannel.target ?? '',
            threadTs,
            fetchTimeoutMs: resolveSlackFetchTimeoutMs(),
          }));
        } catch (attachError) {
          const detail = attachError instanceof Error ? attachError.message : 'unknown error';
          escalateToHold(
            stateDb, db, cardId, stationId, card,
            `rank station '${stationId}' ask_attach '${attachFile}' failed: ${detail}`,
            err, runId,
          );
          return false;
        }
        if (attachResult.escalatedToHold) {
          escalateToHold(
            stateDb, db, cardId, stationId, card,
            `rank station '${stationId}' ask_attach '${attachFile}' has an ambiguous pending upload — reconcile before re-running`,
            err, runId,
          );
          return false;
        }
      }

      // egressSend IS the outbox guard (writePendingIntent → post → commitIntent
      // internally). On pending → reconcileOnResume → escalate_hold (no double-post).
      // On committed → skip. On none → write-pending → post → commit.
      const askResult = await egressSend(db, transport, {
        channel: egressChannel.target ?? '',
        text: askText,
        idempotencyKey: correlationId,
        correlationId,
        threadTs,
      });

      // The original HITL reply-and-resume work FR-3b: journal the ask's thread address + shortlist so an
      // inbound channel message replying IN THIS THREAD can be routed back as
      // the selection (listener-side findHitlAskByThreadTs). Only when this
      // call actually posted — a resume that skipped (already committed) has
      // already journaled its ask on the original post.
      if (askResult.posted && askResult.ts !== undefined) {
        db.appendJournalSpan({
          runId,
          cardId,
          station: stationId,
          attempt: card.attempt,
          name: 'hitl.ask',
          attributes: {
            correlation_id: correlationId,
            channel: egressChannel.target ?? '',
            // The thread REPLIES arrive in — found live (arcane-flows studio,
            // first phone pick): Slack replies carry the thread ROOT's ts.
            // When the ask was threaded on the triggering message, that root
            // is the trigger (threadTs), NOT the ask message itself; only an
            // unthreaded ask becomes its own thread root.
            ts: threadTs ?? askResult.ts,
            ask_ts: askResult.ts,
            short_list: decision.shortList,
          },
        });
      }

      // Surface the correlation id on the card_log so `conduit reply` (WI-394)
      // can key on it. Written as a terminal entry — INSERT OR IGNORE semantics
      // via appendCardLog (idempotent on replay).
      db.appendCardLog({
        runId,
        kind: 'terminal',
        cardId,
        station: stationId,
        attempt: card.attempt,
        reason: correlationId,
      });

      // Park the card as held — lane unchanged (rank station holds in-place,
      // not at a terminal lane, so the liveness check sees it as intentional).
      stateDb
        .transaction(() => {
          stateDb
            .prepare("UPDATE cards SET status = 'held' WHERE run_id = $runId AND id = $id")
            .run({ $runId: runId, $id: cardId });
          stateDb
            .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station')
            .run({ $runId: runId, $id: cardId, $station: stationId });
        })
        .immediate();

      return true; // card status changed (held)
    }

    case 'proceed_with_findings': {
      const nextLane = happyPathNext[stationId] ?? null;
      if (!nextLane) {
        releaseSlot(stateDb, cardId, stationId, runId);
        return false;
      }
      const isTerminal = terminalLanes.has(nextLane);
      advanceCard(
        stateDb,
        db,
        cardId,
        stationId,
        card.lane,
        card.attempt,
        nextLane,
        isTerminal ? 'complete' : 'ready',
        0,
        undefined,
        runId,
      );
      return true;
    }

    case 'scrap':
    case 'scrapped': {
      const reason =
        decision.action === 'scrap' ? 'no_selection' : 'model-incompatible';
      advanceCard(stateDb, db, cardId, stationId, card.lane, card.attempt, 'scrap', 'scrapped', 0, reason, runId);
      return true;
    }
  }
}

// ---------------------------------------------------------------------------
// Fan-out seams (WI-396) — named extension points for WI-397 and WI-398
// ---------------------------------------------------------------------------

/**
 * True when a station carries the complete WI-393 fan-out topology declaration:
 * fan_out count + child_entry, child_terminal, and resume_at.
 * A station with only some fields is NOT treated as a fan-out station — all
 * four fields are required so partially-configured stations fail-closed.
 */
function isFanOutStation(stationConfig: StationConfig): boolean {
  return (
    stationConfig.fan_out !== undefined &&
    stationConfig.child_entry !== undefined &&
    stationConfig.child_terminal !== undefined &&
    stationConfig.resume_at !== undefined
  );
}

/**
 * True when a station is a RANK station (WI-398): `rankCheck !== undefined`
 * is the single discriminator. The rank station runs a critic, optionally posts
 * the short-list to a HITL egress channel, and parks the card as held awaiting
 * a human selection — the kernel NEVER auto-picks (FR-14 / NFR-3).
 */
function isRankStation(stationConfig: StationConfig): boolean {
  return stationConfig.rankCheck !== undefined;
}

/**
 * Structural guard for an ArchitectProposal: `children` must be an array of
 * ProposedChild-shaped objects (id: string, depends_on: string[], owned_paths: string[]).
 * Returns false for malformed payloads — the caller escalates the parent to
 * held and seeds no children (fail-closed: never seed on ambiguous input).
 */
function isArchitectProposal(payload: unknown): payload is ArchitectProposal {
  if (typeof payload !== 'object' || payload === null) return false;
  const p = payload as Record<string, unknown>;
  if (!Array.isArray(p.children)) return false;
  for (const child of p.children) {
    if (typeof child !== 'object' || child === null || Array.isArray(child)) return false;
    const c = child as Record<string, unknown>;
    if (typeof c.id !== 'string') return false;
    if (!Array.isArray(c.depends_on)) return false;
    if (!Array.isArray(c.owned_paths)) return false;
  }
  return true;
}

interface FanOutCompleteArgs {
  db: ConduitDB;
  stateDb: Database;
  runId: string;
  stationConfig: StationConfig;
  stationId: string;
  cardId: string;
  stationOutput: { payload: unknown };
  card: { lane: string; attempt: number; rework_count?: number };
  happyPathNext: Record<string, string | null>;
  terminalLanes: Set<string>;
  maxExecutionAttempts: number;
  flow: FlowConfig;
  err: (msg: string) => void;
  /** Injected epoch seconds — used to stamp the fan-out cache-warming stagger. */
  now: number;
}

/**
 * (a) Fan-out-complete handler — named extension seam for WI-396.
 *
 * Called from executeTransformStation when the station finishes its transform
 * work and is declared as a fan-out station (isFanOutStation returns true).
 * Parses the ArchitectProposal from the station output, validates it fail-closed
 * via the EXISTING dag/expand.ts commitFanOut primitive (acyclic deps + disjoint
 * ownership), seeds valid children at the declared child_entry station, and
 * transitions the parent to awaiting_children via the FSM's FAN_OUT event (lane
 * unchanged — INTEGRITY_PASS would advance it to the station-order successor).
 *
 * Failure modes (parent escalated to held, no children seeded):
 *   - Structurally non-conforming payload (children not array / child not object)
 *   - commitFanOut ExpansionError (dependency_cycle / overlapping_owned_paths /
 *     unknown_dependency) — the whole proposal is rejected, never partially applied
 *   - FSM illegal_transition on FAN_OUT (contradictory card state)
 *
 * Extension seams:
 *   (b) pollAwaitingChildren — main-loop seam that fires after every action batch;
 *       WI-397 fills in fan-in evaluation and parent re-queuing at resume_at.
 *   (c) handleHeldExitFanOut — stub that WI-398 fills in for HITL held-exit at
 *       fan-out stations (human reply un-holds the parent and resumes fan-out).
 */
function handleFanOutComplete(args: FanOutCompleteArgs): boolean {
  const {
    db,
    stateDb,
    runId,
    stationConfig,
    stationId,
    cardId,
    stationOutput,
    card,
    happyPathNext,
    terminalLanes,
    maxExecutionAttempts,
    flow,
    err,
    now,
  } = args;

  // ── Structural validation ────────────────────────────────────────────────
  // Guard before operate: validate the payload shape before touching the DB.
  if (!isArchitectProposal(stationOutput.payload)) {
    escalateToHold(
      stateDb,
      db,
      cardId,
      stationId,
      card,
      `fan-out station '${stationId}' produced a non-conforming ArchitectProposal — ` +
        `expected { children: ProposedChild[] }`,
      err,
    );
    return false;
  }

  const proposal: ArchitectProposal = stationOutput.payload;

  // ── Defense-in-depth: skip re-seeding if parent is already awaiting_children ─
  // On reclaim → re-dispatch a card whose fan-out committed before the crash,
  // the parent may already be awaiting_children. commitFanOut's idempotency
  // guard also handles this, but reading the live status here avoids the FSM
  // transition attempting a FAN_OUT event on an already-transitioned card.
  const liveCard = db.getCard(runId, cardId);
  if (liveCard?.status === 'awaiting_children') {
    return true;
  }

  // ── Validate + seed children (WIRE, don't reimplement — dag/expand.ts) ───
  // commitFanOut runs validateExpansion (cycle detection + disjoint ownership)
  // atomically with insertCard — either all children are inserted or none are.
  const expansion = commitFanOut(db, runId, cardId, proposal, { onPathConflict: 'reject' });

  if (!expansion.ok) {
    // Dependency cycle, overlapping paths, or unknown dependency — reject whole
    // proposal. Never seed partial expansions; they leave orphaned cards.
    escalateToHold(
      stateDb,
      db,
      cardId,
      stationId,
      card,
      `fan-out station '${stationId}' proposal rejected: ${expansion.error.code}`,
      err,
      runId,
    );
    return false;
  }

  // ── Route seeded children to child_entry ───────────────────────────────────
  // commitFanOut seeds children at lane='intake', status='waiting'.
  // Move each to the topology-declared child_entry, status='ready', so the
  // promote step and planTick pick them up in the next main-loop iteration.
  const childEntry = stationConfig.child_entry!;
  stateDb
    .prepare(
      "UPDATE cards SET lane = $lane, status = 'ready' WHERE run_id = $runId AND parent_id = $parentId AND status = 'waiting'",
    )
    .run({ $lane: childEntry, $runId: runId, $parentId: cardId });

  // ── Cache-warming stagger (v10) ───────────────────────────────────────────
  // When the fan-out station declares child_stagger_seconds, hold every child
  // EXCEPT the first (lowest id — the same one planTick dispatches first, since
  // it orders by id ASC) behind release_at = now + stagger. The first child
  // dispatches immediately and warms the shared prompt-prefix cache; the rest
  // fire once the stagger elapses and hit the warm cache. release_at is an
  // absolute epoch-second gate compared against the injected now() in planTick,
  // so it survives crash/resume (a resume re-derives the remaining wait from the
  // persisted release_at). MIN(id) uses the same lexicographic order as planTick's
  // ORDER BY id ASC, so the un-gated child and the first-dispatched child match.
  const staggerSeconds = stationConfig.child_stagger_seconds ?? 0;
  if (staggerSeconds > 0) {
    stateDb
      .prepare(
        `UPDATE cards SET release_at = $releaseAt
         WHERE run_id = $runId AND parent_id = $parentId
           AND id <> (SELECT MIN(id) FROM cards WHERE run_id = $runId AND parent_id = $parentId)`,
      )
      .run({ $releaseAt: now + staggerSeconds, $runId: runId, $parentId: cardId });
  }

  // ── Transition parent via FAN_OUT (FSM is the single source of truth) ─────
  // FAN_OUT: done_pending_ack → awaiting_children, lane UNCHANGED.
  // INTEGRITY_PASS would advance the parent to happyPathNext['plan'] = 'merge',
  // which is incorrect — the parent must wait for its children (AC6).
  const ctx = buildTransitionContext(stationId, flow, happyPathNext, terminalLanes, maxExecutionAttempts);
  const fsmState = syntheticDonePendingAck(card, REWORK_COUNT_UNREAD);
  const fanOutResult = transition(fsmState, { type: 'FAN_OUT' }, ctx);

  if (!fanOutResult.ok) {
    // Contradictory state — escalate rather than silently advancing.
    escalateToHold(
      stateDb,
      db,
      cardId,
      stationId,
      card,
      `FSM illegal_transition on FAN_OUT for fan-out station '${stationId}'`,
      err,
      runId,
    );
    return false;
  }

  // ── Persist parent: status → awaiting_children, lane unchanged; release slot ─
  stateDb
    .transaction(() => {
      stateDb
        .prepare("UPDATE cards SET status = 'awaiting_children' WHERE run_id = $runId AND id = $id")
        .run({ $runId: runId, $id: cardId });
      stateDb
        .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $id AND station = $station')
        .run({ $runId: runId, $id: cardId, $station: stationId });
    })
    .immediate();

  return true;
}

/**
 * (b) Awaiting-children polling seam — stub for WI-397.
 *
 * Called from the main runExecutor loop after every action batch. When WI-397
 * lands, this stub is replaced with real fan-in evaluation (evaluateFanIn from
 * dag/expand.ts) and parent re-queuing: when all children of an awaiting_children
 * parent are terminal, the parent is advanced to its declared resume_at lane with
 * status='ready' so the next planTick dispatches it.
 *
 * Returns true iff at least one parent was advanced (main loop updates
 * lastLaneChangeAt so the liveness watchdog sees the progress).
 */
function pollAwaitingChildren(
  db: ConduitDB,
  stateDb: Database,
  runId: string,
  flow: FlowConfig,
  terminalLanes: Set<string>,
): boolean {
  // Find all parents currently waiting for their children to complete.
  const awaitingRows = stateDb
    .prepare("SELECT id, lane FROM cards WHERE run_id = $runId AND status = 'awaiting_children'")
    .all({ $runId: runId }) as Array<{ id: string; lane: string }>;

  if (awaitingRows.length === 0) return false;

  let anyAdvanced = false;

  for (const row of awaitingRows) {
    const parentId = row.id;
    const fanOutLane = row.lane; // the fan-out station id (parent.lane is unchanged from FAN_OUT)

    // Read the topology from the fan-out station config.
    const fanOutStation = flow.stations[fanOutLane];
    if (!fanOutStation?.resume_at) continue; // no resume_at → not a fan-out parent

    const resumeAt = fanOutStation.resume_at;

    // Fan-in policy is declared on the RESUME_AT station (the merge station),
    // NOT on the fan-out station. This mirrors how the reference flow declares
    // fan_in on its `assemble`/`merge` station, not on `plan`.
    const resumeStation = flow.stations[resumeAt];

    // Fan-in evaluation is OPT-IN: only fire when the resume_at station explicitly
    // declares a fan_in policy. A resume_at station without fan_in is not yet a
    // fan-in merge station — leave the parent in awaiting_children.
    if (resumeStation?.fan_in === undefined) continue;

    const policy = toFanInPolicy(resumeStation.fan_in);

    // Gather all children of this parent.
    const childRows = stateDb
      .prepare('SELECT id, lane FROM cards WHERE run_id = $runId AND parent_id = $p ORDER BY id')
      .all({ $runId: runId, $p: parentId }) as Array<{ id: string; lane: string }>;

    const childIds = childRows.map((c) => c.id);
    const terminalOutcomes = childRows
      .filter((c) => terminalLanes.has(c.lane))
      .map((c) => ({ id: c.id, lane: c.lane }));

    // ── GATE: all children must be terminal before evaluating (AC4) ───────────
    // While ANY child is non-terminal (in flight), leave the parent in
    // awaiting_children even if the survivor count already meets quorum k.
    // evaluateFanIn's quorum branch returns 'proceed' early on survivorCount >= k,
    // so we must guard on (terminalCount === total) BEFORE calling it.
    if (terminalOutcomes.length < childIds.length) continue;

    const fanInState: FanInState = { childIds, terminalOutcomes };
    const decision = evaluateFanIn(policy, fanInState);

    // 'wait' is unreachable here (all children terminal), but guard anyway.
    if (decision.action === 'wait') continue;

    // Read the full card for attempt/lane (needed for card_log — journal-first).
    const card = db.getCard(runId, parentId);
    if (!card) continue;

    if (decision.action === 'proceed') {
      // Journal-first: append before state-db commit (WI-381 / FR-7).
      db.appendCardLog({
        runId,
        kind: 'entered_lane',
        cardId: parentId,
        station: fanOutLane,
        attempt: card.attempt,
        sourceLane: fanOutLane,
        destLane: resumeAt,
        reasonClass: 'forward',
      });
      // Move parent to resume_at, status='ready' — planTick dispatches it next.
      // The resume lane is ALWAYS resume_at, never the fan-out station's `next`
      // (which is the station-order successor, not the post-fan-in target).
      stateDb
        .transaction(() => {
          stateDb
            .prepare("UPDATE cards SET lane = $lane, status = 'ready' WHERE run_id = $runId AND id = $id")
            .run({ $lane: resumeAt, $runId: runId, $id: parentId });
        })
        .immediate();
      anyAdvanced = true;
    } else {
      // decision.action === 'hold_parent': quorum unmet or a child scrapped.
      // Set status='held' — NOT left in awaiting_children which would deadlock.
      db.appendCardLog({
        runId,
        kind: 'entered_lane',
        cardId: parentId,
        station: fanOutLane,
        attempt: card.attempt,
        sourceLane: fanOutLane,
        destLane: fanOutLane,
        reasonClass: 'hold',
      });
      db.appendCardLog({
        runId,
        kind: 'terminal',
        cardId: parentId,
        station: fanOutLane,
        attempt: card.attempt,
        reason: `fan-in ${decision.reason}`,
      });
      stateDb
        .prepare("UPDATE cards SET status = 'held' WHERE run_id = $runId AND id = $id")
        .run({ $runId: runId, $id: parentId });
      anyAdvanced = true; // update lastLaneChangeAt — progress was made
    }
  }

  return anyAdvanced;
}

/**
 * Convert the flow-config fan_in declaration (number | FanInPolicyConfig | undefined)
 * to the dag/expand.ts FanInPolicy shape that evaluateFanIn expects.
 *
 * - undefined  → 'all' (every child must reach a non-scrap terminal lane)
 * - number k   → quorum(k) shorthand
 * - structured → map policy field to kind field
 */
function toFanInPolicy(config: number | FanInPolicyConfig | undefined): FanInPolicy {
  if (config === undefined) return { kind: 'all' };
  if (typeof config === 'number') return { kind: 'quorum', k: config };
  switch (config.policy) {
    case 'quorum': return { kind: 'quorum', k: config.k };
    case 'all': return { kind: 'all' };
    case 'best_effort': return { kind: 'best_effort' };
  }
}

/**
 * (c) Held-exit for rank HITL stations (WI-398).
 *
 * When a held card at a rank station has a human selection recorded (via
 * `conduit reply` / WI-394), un-hold it (status → ready, lane unchanged) so
 * the next planTick dispatches it. The rank station handler then reads the
 * selection and advances to happyPathNext rather than re-running the critic.
 *
 * If no selection is recorded, the card stays held — the kernel NEVER
 * auto-picks a candidate (AC3 / FR-14 / NFR-3).
 *
 * @returns true iff the card was successfully un-held.
 */
function handleHeldExitFanOut(db: ConduitDB, cardId: string, _stationId: string, runId: string = DEFAULT_RUN_ID): boolean {
  const selection = getRecordedHitlSelection(db, cardId, runId);
  if (selection === null) {
    // No human selection recorded yet — stay held.
    return false;
  }

  // Un-hold: status → ready, lane unchanged. The next planTick dispatches
  // the card at the rank station where it reads the selection and advances.
  const changes = db
    .getStateDb()
    .prepare("UPDATE cards SET status = 'ready' WHERE run_id = $runId AND id = $id AND status = 'held'")
    .run({ $runId: runId, $id: cardId }).changes;

  return changes > 0;
}

/**
 * Poll all held cards at rank stations and un-hold any that have a recorded
 * human selection. Called in the main loop's terminal check (WI-398) so that
 * a resume run with a recorded selection can advance past the rank station.
 *
 * @returns true iff at least one held rank card was un-held.
 */
function pollHeldRankCards(db: ConduitDB, stateDb: Database, runId: string, flow: FlowConfig): boolean {
  const heldRows = stateDb
    .prepare("SELECT id, lane FROM cards WHERE run_id = $runId AND status = 'held'")
    .all({ $runId: runId }) as Array<{ id: string; lane: string }>;

  if (heldRows.length === 0) return false;

  let anyAdvanced = false;
  for (const row of heldRows) {
    const stationConfig = flow.stations[row.lane];
    if (!stationConfig || !isRankStation(stationConfig)) continue;
    if (handleHeldExitFanOut(db, row.id, row.lane)) {
      anyAdvanced = true;
    }
  }
  return anyAdvanced;
}

/**
 * Most recent HITL correlation id surfaced on a card's log, or null.
 *
 * The await_selection path (executeRankStation) records the stable correlation
 * id as a `terminal` card_log entry. Its presence is what distinguishes a
 * genuine HITL selection-wait from an escalation hold (contradictory state),
 * which carries no correlation id and must NEVER be auto-resolved by a timeout.
 */
function hitlCorrelationId(db: ConduitDB, runId: string, cardId: string): string | null {
  const log = db.getCardLogForRun(runId, cardId);
  for (let i = log.length - 1; i >= 0; i--) {
    const entry = log[i]!;
    if (entry.kind === 'terminal' && entry.reason.startsWith('hitl::')) return entry.reason;
  }
  return null;
}

/** Read the durable `hitl.held_at` journal span for a card, or null if unset. */
function readHeldAt(db: ConduitDB, runId: string, cardId: string): number | null {
  const spans = db.getJournalSpansForRun(runId, cardId);
  for (let i = spans.length - 1; i >= 0; i--) {
    const span = spans[i]!;
    if (span.name === 'hitl.held_at' && typeof span.attributes.held_at === 'number') {
      return span.attributes.held_at as number;
    }
  }
  return null;
}

/**
 * Poll held HITL cards and apply the egress channel's hold-timeout policy to any
 * whose human-decision window has elapsed (WI-398 AC5 / FR-14, SPEC §4A).
 *
 * The production executor is single-process and synchronous: when the only
 * remaining work is a held card it exits cleanly, and a human later runs
 * `conduit reply` + `conduit resume`. The timeout is therefore enforced
 * OPPORTUNISTICALLY — every time the executor runs past the deadline it
 * re-evaluates held cards and, if no selection arrived in time, applies the
 * configured on_timeout via the EXISTING slack.ts::applyHoldTimeout primitive.
 * It NEVER auto-picks a rank winner (applyHoldTimeout.autoSelected is
 * structurally false).
 *
 * "Held since" is read from a durable `hitl.held_at` journal span measured on
 * the SAME injected clock as `currentNow`, so the deadline is deterministic and
 * survives crash/resume. It is stamped lazily on first observation — which is
 * the holding run's own terminal-check poll (≈ park time) — and if the holding
 * run never reached this poll, the window simply starts later (fail-open: a HITL
 * card is never scrapped EARLY).
 *
 * Routing per policy:
 *   scrap    — applyHoldTimeout routes the card to the scrap terminal.
 *   escalate — applyHoldTimeout parks it on the 'hold' terminal lane for a human.
 *   proceed_with_findings — at a rank station this means advance PAST the station
 *     (no winner picked, FR-14); applyHoldTimeout's generic ready-in-place would
 *     re-dispatch the rank critic and re-hold, so route to happyPathNext instead.
 *
 * @returns true iff at least one held card was resolved by a timeout policy.
 */
function pollHeldTimeouts(
  db: ConduitDB,
  stateDb: Database,
  runId: string,
  flow: FlowConfig,
  currentNow: number,
  happyPathNext: Record<string, string | null>,
  terminalLanes: Set<string>,
): boolean {
  // The HITL hold was posted to egress[0] (see executeRankStation); the timeout
  // policy is read from that same channel so post and timeout always agree.
  const channel = flow.channels?.egress?.[0];
  const timeoutSeconds = channel?.hold_timeout_seconds;
  const onTimeout = channel?.on_timeout;
  if (timeoutSeconds === undefined || onTimeout === undefined) return false;

  // Fail-closed: only the three SPEC §4A policies are honoured. An unrecognised
  // on_timeout is never coerced into a card mutation.
  if (onTimeout !== 'scrap' && onTimeout !== 'proceed_with_findings' && onTimeout !== 'escalate') {
    return false;
  }
  const policy: OnTimeout = onTimeout;

  const heldRows = stateDb
    .prepare("SELECT id, lane, attempt FROM cards WHERE run_id = $runId AND status = 'held'")
    .all({ $runId: runId }) as Array<{ id: string; lane: string; attempt: number }>;
  if (heldRows.length === 0) return false;

  let anyResolved = false;
  for (const row of heldRows) {
    const stationConfig = flow.stations[row.lane];
    if (!stationConfig || !isRankStation(stationConfig)) continue;

    // Only genuine HITL selection-waits time out — an escalation hold (no
    // correlation id) awaits human judgment and must never be auto-resolved.
    const correlationId = hitlCorrelationId(db, runId, row.id);
    if (correlationId === null) continue;

    // A recorded reply beats a timeout — leave it for the reply-driven held-exit.
    if (getRecordedHitlSelection(db, row.id, runId) !== null) continue;

    // Lazy-stamp "held since" on the injected clock; the window is measured from
    // when the timeout poller first observed the hold (≈ park time).
    let heldAt = readHeldAt(db, runId, row.id);
    if (heldAt === null) {
      db.appendJournalSpan({
        runId,
        cardId: row.id,
        station: row.lane,
        attempt: row.attempt,
        name: 'hitl.held_at',
        attributes: { held_at: currentNow, correlation_id: correlationId },
      });
      heldAt = currentNow;
    }
    if (currentNow - heldAt < timeoutSeconds) continue; // still within the window

    if (policy === 'proceed_with_findings') {
      // Advance PAST the rank station (no winner picked) rather than letting
      // applyHoldTimeout re-ready the card in place, which would re-hold it.
      const nextLane = happyPathNext[row.lane] ?? null;
      if (nextLane === null) continue;
      const isTerminal = terminalLanes.has(nextLane);
      db.appendJournalSpan({
        runId,
        cardId: row.id,
        station: row.lane,
        attempt: row.attempt,
        name: 'hitl.timeout',
        attributes: { applied: policy, correlation_id: correlationId },
      });
      advanceCard(stateDb, db, row.id, row.lane, row.lane, row.attempt, nextLane, isTerminal ? 'complete' : 'ready', 0, undefined, runId);
      anyResolved = true;
      continue;
    }

    // scrap | escalate — applyHoldTimeout's direct routing is terminal-correct.
    const result = applyHoldTimeout(db, correlationId, policy, runId);
    if (result.matched) {
      db.appendJournalSpan({
        runId,
        cardId: row.id,
        station: row.lane,
        attempt: row.attempt,
        name: 'hitl.timeout',
        attributes: { applied: policy, correlation_id: correlationId },
      });
      anyResolved = true;
    }
  }
  return anyResolved;
}
