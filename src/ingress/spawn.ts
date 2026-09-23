/**
 * WI-406 — Atomic accept-dedup-spawn orchestration (SPEC §9 / NFR-2, NFR-3, FR-1, FR-7).
 *
 * runSpawnPath is the listener's core path for a valid event. It:
 *   1. Builds and projects the substrate envelope (WI-403 composition) and
 *      derives the event's run id (the original ingress-attribution work) — pure, so it happens first.
 *   2. Atomically records the event_id as 'accepted' before spawning (NFR-2),
 *      with attribution (flow, payload, run id) in the same statement so any
 *      later re-drive can relaunch the event faithfully — then claims a run
 *      slot (the original listener-backpressure work): if all slots are busy the row STAYS 'accepted' and the
 *      re-drive sweep launches it when a slot frees ('queued' outcome).
 *   3. Counts the spawn attempt before calling the spawn seam — crash recovery
 *      leaves a countable 'accepted' row for boot re-drive (FR-1, AC8).
 *   4. Calls the spawn seam (Bun.spawn abstracted for testability) with the
 *      flow path, inline substrate, and derived run id.
 *   5. On a successful LAUNCH: marks 'spawned' + appends an 'accepted' log entry
 *      and returns — the caller acks here (the original acknowledgement-on-accept work).
 *   6. On a failed launch: marks 'failed' (no double-count), resolves the alert
 *      channel, fires the alert seam, and appends a 'spawn_failed' log entry.
 *   7. After a successful launch the child's EXIT is handled off the request
 *      path (watchChildExit): a non-zero exit gets the same FR-7 treatment as a
 *      failed launch — unless the run PARKED behind a provider rate limit
 *      (issue #7, ./parked.ts) — and the run slot is released there.
 *
 * Launch vs exit (the original acknowledgement-on-accept work). The seam resolves when the child is LAUNCHED, not
 * when it finishes; it reports the child's terminal state through the optional
 * `exited` promise. Before this split the production seam awaited `proc.exited`,
 * so runSpawnPath — and with it the webhook's HTTP response — stayed pending for
 * the entire run (hours, for a render). Consequently:
 *
 *   - spawn_state 'spawned' now means LAUNCHED, not "ran to a clean exit". A
 *     listener crash mid-run leaves the row 'spawned', so ingress re-drive no
 *     longer covers it; the run's own resume/journal machinery owns recovery
 *     from that point (and a re-driven completed run would dedupe at the run
 *     layer anyway, so the coverage was nominal).
 *   - the run slot is held from launch to child exit, so max_concurrent_runs
 *     keeps meaning concurrent RUNS rather than concurrent webhook responses.
 *
 * Only the two outermost I/O effects — spawn and alert — are seamed. All
 * persistence and envelope logic uses the real modules (WI-401, WI-403, WI-404).
 */
import { buildEnvelope, projectSubstrate } from './envelope';
import { inspectParkedRun, recordPark } from './parked';
import { deriveIngressRunId } from './run-id';
import type { RunSlots } from './run-slots';
import type { ConduitDB } from '../persistence/db';
import type { FlowConfig } from '../types/kernel';

// ---------------------------------------------------------------------------
// Public types (pinned by spawn.test.ts)
// ---------------------------------------------------------------------------

export interface SpawnInvocation {
  flowPath: string;
  inputInline: string;
  /**
   * Run id derived from the event id (the original ingress-attribution work) — passed as `--run-id` so
   * concurrent events never collide on the default run, and a re-drive of the
   * same event resumes its own run instead of forking a duplicate.
   */
  runId: string;
}

/** Terminal state of a launched `conduit run` child (the original acknowledgement-on-accept work). */
export interface SpawnExit {
  /** Child process exit code — non-zero is a run failure. */
  code: number;
}

export interface SpawnSeamResult {
  /** True once the child has been LAUNCHED — not run to completion (the original acknowledgement-on-accept work). */
  ok: boolean;
  /** Why the launch itself failed (only meaningful when ok is false). */
  error?: string;
  /**
   * Resolves when the launched child reaches its terminal state (the original acknowledgement-on-accept work).
   * The production seam supplies it; a seam with no observable child (test
   * doubles, callers that do not track their children) omits it, and the launch
   * is then treated as complete when the seam resolves — the before acknowledgement-on-accept shape,
   * which keeps single-seam callers and their tests behaving exactly as before.
   */
  exited?: Promise<SpawnExit>;
}

export type SpawnSeam = (invocation: SpawnInvocation) => Promise<SpawnSeamResult>;

export interface SpawnFailedAlert {
  flowId: string;
  channel: string;
  eventId: string;
  reason: string;
}

export type AlertSeam = (alert: SpawnFailedAlert) => Promise<void>;

export interface SpawnPathDeps {
  db: ConduitDB;
  spawn: SpawnSeam;
  alert: AlertSeam;
  /** Listener-global fallback alert target when the failing flow declares no egress. */
  globalAlertChannel: string;
  /**
   * Total-attempt cap (first spawn + all re-drives), shared with redriveOnBoot.
   * A live redelivery of a 'failed' event whose spawn_attempts has reached this
   * cap is suppressed instead of re-spawned — mirrors listRedrivable(cap) so the
   * hot path cannot re-launch a permanently-failed event on every provider retry.
   */
  redriveCap: number;
  /**
   * Run-slot gate (the original listener-backpressure work): bounds concurrent spawned runs listener-wide.
   * When absent, spawning is ungated (single-seam tests, legacy callers).
   * An accepted event that finds no free slot is left 'accepted' in
   * ingress_events and logged 'queued' — the re-drive sweep launches it when
   * a slot frees.
   */
  slots?: RunSlots;
  /**
   * Listener clock, unix MILLISECONDS (issue #7): the exit watcher reads it to
   * confirm a parked run against its cards. Defaults to Date.now.
   */
  now?: () => number;
}

export interface SpawnPathInput {
  source: string;
  eventId: string;
  receivedAt: number;
  authVerified: boolean;
  headers: Record<string, unknown>;
  body: unknown;
  attachments?: unknown[];
  flowId: string;
  flowPath: string;
  flow: FlowConfig;
  /** Binding's JSON-path projection (WI-402). When absent, the full envelope is passed. */
  substrateMapping?: Record<string, string>;
}

export type SpawnPathResult =
  /** Accepted and LAUNCHED — the run is executing (the original acknowledgement-on-accept work), not finished. */
  | { outcome: 'accepted'; runId: string }
  | { outcome: 'duplicate' }
  | { outcome: 'spawn_failed' }
  /** Accepted and recorded, but all run slots are busy — spawns via the sweep. */
  | { outcome: 'queued'; runId: string };

// ---------------------------------------------------------------------------
// Core path
// ---------------------------------------------------------------------------

/**
 * Runs the accept → count → build → spawn → mark → log sequence for one event.
 *
 * Exactly-once guarantee: `acceptIngressEvent` is the atomic gate. If the row
 * is already 'accepted' or 'spawned', we log 'duplicate' and return without
 * spawning. If the row is 'failed' (re-drive), `acceptIngressEvent` transitions
 * it back to 'accepted' and preserves the accumulated spawn_attempts count.
 */
export async function runSpawnPath(
  deps: SpawnPathDeps,
  input: SpawnPathInput,
): Promise<SpawnPathResult> {
  const { db, spawn, alert, globalAlertChannel, redriveCap, slots, now = Date.now } = deps;
  const {
    source,
    eventId,
    receivedAt,
    authVerified,
    headers,
    body,
    attachments,
    flowId,
    flowPath,
    flow,
    substrateMapping,
  } = input;

  // ── Step 0: Cap the live re-drive path (The original input-validation work) ───────────────────────
  // A provider that retries on failure (GitHub, Stripe, …) re-delivers the same
  // event_id. acceptIngressEvent would transition a 'failed' row back to
  // 'accepted' and we would re-spawn on EVERY redelivery, forever. Mirror the
  // redriveOnBoot cap: if a previously-failed event has already reached
  // redriveCap attempts, suppress it here without accepting or spawning.
  const existing = db.getIngressEvent(eventId);
  if (
    existing !== null &&
    existing.spawn_state === 'failed' &&
    existing.spawn_attempts >= redriveCap
  ) {
    db.appendIngressLog({
      source,
      eventId,
      outcome: 'spawn_failed',
      reason: 'redrive cap exhausted',
    });
    return { outcome: 'spawn_failed' };
  }

  // ── Step 1: Build and project the substrate envelope (WI-403) ───────────
  // Pure computation, hoisted ABOVE the accept (the original ingress-attribution work): the projected
  // substrate is part of the attribution written atomically with the accept,
  // so a crash after a winning accept still leaves a faithfully re-drivable row.
  const envelope = buildEnvelope({ source, eventId, receivedAt, authVerified, headers, body, attachments });
  const substrate = projectSubstrate(envelope, substrateMapping);
  const inputInline = JSON.stringify(substrate);
  const runId = deriveIngressRunId(eventId);

  // ── Step 2: Atomic accept-before-spawn (NFR-2) ──────────────────────────
  // Attribution (owning flow, payload, derived run id) rides the same atomic
  // statement — everything a later re-drive needs to relaunch this event.
  const acceptResult = db.acceptIngressEvent(eventId, receivedAt, {
    flowId,
    flowPath,
    runId,
    substrateJson: inputInline,
  });
  if (!acceptResult.accepted) {
    db.appendIngressLog({ source, eventId, outcome: 'duplicate' });
    return { outcome: 'duplicate' };
  }

  // ── Step 2b: Claim a run slot (the original listener-backpressure work) ────────────────────────────────
  // Synchronous with the accept above (no await between) so the sweep can
  // never observe a won-accept row without its in-flight registration.
  //   'full'      → all slots busy: leave the row 'accepted' (attempts NOT
  //                 counted — queueing is not a spawn attempt) and let the
  //                 re-drive sweep launch it when a slot frees.
  //   'duplicate' → this event is ALREADY being launched by the sweep (a
  //                 provider redelivery re-accepted a 'failed' row mid-redrive):
  //                 suppress the second spawn; the in-flight launch will mark
  //                 the row's final state.
  const acquisition = slots?.tryAcquire(eventId) ?? 'acquired';
  if (acquisition === 'full') {
    db.appendIngressLog({
      source,
      eventId,
      outcome: 'queued',
      reason: 'all run slots busy — will spawn when a slot frees',
    });
    return { outcome: 'queued', runId };
  }
  if (acquisition === 'duplicate') {
    db.appendIngressLog({
      source,
      eventId,
      outcome: 'duplicate',
      reason: 'a launch for this event is already in flight',
    });
    return { outcome: 'duplicate' };
  }

  // Slot ownership (the original acknowledgement-on-accept work): the gate bounds concurrent RUNS, so once a
  // child is live the release belongs to its exit watcher, not to this call.
  let slotHandedOff = false;
  try {
    // ── Step 3: Count attempt before spawning (FR-1 / AC8) ──────────────────
    // Incrementing here means a crash mid-spawn leaves a countable 'accepted'
    // row — boot recovery via listRedrivable can re-drive it safely.
    db.incrementSpawnAttempts(eventId);

    // ── Step 4: Launch conduit run via the injected seam ────────────────────
    // Guard: the seam may throw (e.g. ENOENT, network error). Treat a throw as
    // { ok: false } and fall through to the existing FR-7 failure path below so
    // the row is never left silently in 'accepted' state with no alert.
    let spawnResult: SpawnSeamResult;
    try {
      spawnResult = await spawn({ flowPath, inputInline, runId });
    } catch (err) {
      spawnResult = { ok: false, error: err instanceof Error ? err.message : String(err) };
    }

    if (spawnResult.ok) {
      // ── Step 5a: Launched ─────────────────────────────────────────────────
      // 'spawned' means LAUNCHED from here on (the original acknowledgement-on-accept work) — see the header.
      db.markIngressSpawned(eventId);
      db.appendIngressLog({ source, eventId, outcome: 'accepted' });

      // The marks above are done, so the watcher can never observe a row this
      // call has not finished writing.
      if (spawnResult.exited !== undefined) {
        slotHandedOff = true;
        void watchChildExit(
          { db, alert, globalAlertChannel, now, ...(slots !== undefined && { slots }) },
          { source, eventId, runId, flowId, flow },
          spawnResult.exited,
        );
      }

      return { outcome: 'accepted', runId };
    }

    // ── Step 5b: Launch failure — mark, alert, log (FR-7) ──────────────────
    // markIngressFailed only transitions spawn_state; attempts are already counted
    // by incrementSpawnAttempts above — no double-count.
    db.markIngressFailed(eventId);

    const alertChannel = flow.channels?.egress?.[0]?.target ?? globalAlertChannel;
    await alert({
      flowId,
      channel: alertChannel,
      eventId,
      reason: spawnResult.error ?? 'spawn failed',
    });

    db.appendIngressLog({
      source,
      eventId,
      outcome: 'spawn_failed',
      reason: spawnResult.error,
    });

    return { outcome: 'spawn_failed' };
  } finally {
    // Release AFTER the row's final state is marked, so the kicked sweep never
    // sees a freed slot alongside a still-'accepted' row for a finished launch.
    // A handed-off slot stays held: its child is still running.
    if (!slotHandedOff) slots?.release(eventId);
  }
}

// ---------------------------------------------------------------------------
// Post-launch child supervision (the original acknowledgement-on-accept work)
// ---------------------------------------------------------------------------

interface ChildExitDeps {
  db: ConduitDB;
  alert: AlertSeam;
  globalAlertChannel: string;
  now: () => number;
  slots?: RunSlots;
}

interface ChildExitContext {
  source: string;
  eventId: string;
  runId: string;
  flowId: string;
  flow: FlowConfig;
}

/** Tell the channel a launch died — best effort, and never rejects on its caller. */
async function fireSpawnFailedAlert(alert: AlertSeam, notice: SpawnFailedAlert): Promise<void> {
  try {
    await alert(notice);
  } catch {
    // Alerting is best effort once the ack is gone; the 'spawn_failed' log
    // entry is the durable record.
  }
}

/**
 * Supervise a launched child off the request path.
 *
 * A non-zero exit gets exactly the FR-7 treatment the before acknowledgement-on-accept synchronous path
 * gave it — markIngressFailed (attempts already counted at launch), alert, and a
 * 'spawn_failed' ingress_log entry — only now the webhook has already acked, so
 * the failure surfaces through the alert and the log rather than the response.
 * A clean exit leaves the row 'spawned' and writes nothing further.
 *
 * A PARKED run also exits non-zero (issue #7): it did not complete, but
 * nothing failed — it is waiting on a provider reset and the listener resumes
 * it (ingress/parked.ts). The row stays 'spawned' (its launch succeeded, and
 * a 'failed' row would be re-driven with a `conduit run` that is a no-op for
 * an existing run), the log records 'parked', and the channel is told once.
 *
 * The run slot is released here: that is what keeps max_concurrent_runs meaning
 * concurrent RUNS. Release happens after the row's final state is marked, so a
 * sweep kicked by the release never sees a freed slot next to a stale row.
 *
 * Never rejects — it runs detached, so an escaping throw would be an unhandled
 * rejection with no caller to report to (same discipline as the post-ack Slack
 * pipeline).
 */
async function watchChildExit(
  deps: ChildExitDeps,
  ctx: ChildExitContext,
  exited: Promise<SpawnExit>,
): Promise<void> {
  const { db, alert, globalAlertChannel, now, slots } = deps;
  const { source, eventId, runId, flowId, flow } = ctx;

  try {
    let reason: string | null = null;
    try {
      const exit = await exited;
      if (exit.code !== 0) reason = `conduit run exited with code ${exit.code}`;
    } catch (err) {
      // The seam could not observe the child's exit at all — treat the run as
      // failed rather than leaving the row permanently 'spawned'.
      reason = err instanceof Error ? err.message : String(err);
    }

    if (reason === null) return; // clean exit — the row stays 'spawned'

    const alertChannel = flow.channels?.egress?.[0]?.target ?? globalAlertChannel;

    const parked = inspectParkedRun(db, runId, Math.floor(now() / 1000));
    if (parked !== null) {
      await recordPark(db, alert, { source, eventId, flowId, channel: alertChannel, runId, releaseAt: parked.releaseAt });
      return;
    }

    // Mark -> start alert -> log, and the alert is NOT awaited. A transport
    // that never SETTLES (not merely one that throws) would otherwise block the
    // durable ingress_log entry below AND the slot release in the finally,
    // pinning a run slot for the life of the listener — at max_concurrent_runs
    // 1, one hung post is a listener that never launches again, with nothing in
    // the log to say why. Same rule as watchRedrivenChildExit and recordPark.
    db.markIngressFailed(eventId);
    void fireSpawnFailedAlert(alert, { flowId, channel: alertChannel, eventId, reason });
    db.appendIngressLog({ source, eventId, outcome: 'spawn_failed', reason });
  } catch {
    // Persistence itself failed (disk error on the state/journal db). Nothing
    // left to report to — swallow so the detached watcher cannot crash the
    // listener, and still free the slot below.
  } finally {
    slots?.release(eventId);
  }
}
