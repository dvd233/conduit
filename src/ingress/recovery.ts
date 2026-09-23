/**
 * Boot-time bounded re-drive and escalation for recoverable ingress events (WI-407).
 *
 * On listener startup, redriveOnBoot deterministically recovers:
 *   - 'accepted' rows — a crash between record and spawn (event recorded, launch never ran)
 *   - 'failed' rows still under the attempt cap (transient failure, re-drive eligible)
 *
 * Design invariants (enforced by the test suite):
 *   1. The RespawnSeam is LAUNCH-ONLY — it must not touch ingress_events or
 *      ingress_log. redriveOnBoot owns all bookkeeping: increment → launch → mark → log.
 *   2. Re-drive does NOT re-enter WI-406's accept-gate (which would dedup 'accepted'
 *      rows). This function snapshots listRedrivable(cap) once, then for each row:
 *      incrementSpawnAttempts BEFORE calling the seam, marks spawn_state from the
 *      result, and appends an 'redriven' ingress_log entry.
 *   3. Attempt counting is CONTINUOUS with WI-406: the first spawn already counted
 *      as attempt 1. With cap=N, a row at N is at the boundary (excluded);
 *      a row at N-1 is re-driven exactly once, incrementing it to N.
 *   4. A 'permanent_failure' result exhausts the row's spawn_attempts to >= cap
 *      so that listRedrivable skips it on the next boot (no unbounded auto-retry).
 *   5. Every FAILURE outcome fires the alert seam, exactly as the hot spawn path
 *      does (#8) — a launch that fails permanently or transiently, and a launched
 *      child that dies. Alerting is bookkeeping, so it lives here rather than in
 *      the launch-only seam (invariant 1), and it is BEST EFFORT: fireRedriveAlert
 *      is started but never awaited, so neither a throwing alert nor one that
 *      never settles can block markIngressFailed, the ingress_log entry, or the
 *      slot release. Before this, a re-driven failure only reached ingress_log,
 *      so a halted run stayed silent on every operator channel.
 *   6. The seam resolves on LAUNCH, not on exit (the original acknowledgement-on-accept work). A sweep therefore
 *      finishes in milliseconds instead of running as long as the runs it
 *      recovered, and boot no longer waits out a recovered render before the
 *      listener serves. The launched child keeps its run slot until it exits and
 *      is marked failed there if it dies (watchRedrivenChildExit).
 *
 * Mirrors the increment-before-effect discipline from the kernel outbox
 * (src/checkpoint/checkpoint.ts): the increment is persisted before the launch,
 * so a crash mid-re-drive still stays bounded on the next boot.
 */

import type { ConduitDB, IngressEventRecord } from '../persistence/db';
import {
  resolveAlertChannel,
  UNATTRIBUTED_FLOW_ID,
  type RedriveAlerting,
} from './alert-channel';
import { inspectParkedRun, recordPark } from './parked';
import type { RunSlots } from './run-slots';
import type { SpawnExit } from './spawn';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type RedriveResult = 'spawned' | 'transient_failure' | 'permanent_failure';

/**
 * A re-drive outcome that also reports the launched child's exit (the original acknowledgement-on-accept work).
 *
 * The seam resolves as soon as the child is LAUNCHED, so a sweep no longer
 * blocks for the whole duration of the run it recovered. `exited` (present only
 * with 'spawned') lets redriveOnBoot hold the run slot until the child really
 * finishes and mark the row failed if it dies — the same launch/exit split the
 * hot spawn path uses (see spawn.ts).
 */
export interface RedriveLaunch {
  result: RedriveResult;
  exited?: Promise<SpawnExit>;
}

/**
 * Launch-only respawn seam injected by the listener bootstrap.
 * Must NOT touch ingress_events or ingress_log — redriveOnBoot owns that bookkeeping.
 *
 * Returning a bare RedriveResult means "the launch is over when this resolves"
 * (test doubles and callers that do not track their children); returning a
 * RedriveLaunch with `exited` reports the child's terminal state separately.
 */
export type RespawnSeam = (event: IngressEventRecord) => Promise<RedriveResult | RedriveLaunch>;

/**
 * Failure-alerting deps for the re-drive paths (#8), mirroring the hot spawn
 * path's `alert` + `globalAlertChannel` pair in SpawnPathDeps.
 *
 * Channel resolution matches the hot path: `channels` is the listener's
 * already-resolved per-flow map (each flow's first egress target, falling back
 * to the listener-global channel), and `globalAlertChannel` catches a row whose
 * owning flow is unknown here — a pre-v9 row with a null flow_id, or a flow
 * that was quarantined after the row was accepted.
 */
export type { RedriveAlerting } from './alert-channel';

export interface RedriveDeps {
  db: ConduitDB;
  respawn: RespawnSeam;
  /**
   * Failure-alert seam + channel resolution (#8). Optional: a caller that does
   * not alert (unit-level drivers) simply leaves re-drive failures to
   * ingress_log. The listener ALWAYS wires it — see listener.test.ts.
   */
  alerts?: RedriveAlerting;
  /** Total-attempt cap (first spawn + all re-drives). Rows at spawn_attempts === cap are excluded. */
  cap: number;
  /** Label written to the 'redriven' ingress_log entries. Defaults to 'boot-recovery'. */
  source?: string;
  /**
   * Run-slot gate (the original listener-backpressure work), shared with the hot spawn path. When present:
   *   - a row whose event id is already IN FLIGHT is skipped, not re-driven —
   *     its child is live (the slot is held from launch to exit, the original acknowledgement-on-accept work)
   *     and a naive sweep would burn its spawn_attempts on run-lease conflicts;
   *   - a row with no free slot ends the sweep — the remaining (received_at-
   *     ordered) rows stay queued and launch on the next kick or tick.
   * When absent, sweeping is ungated (before listener backpressure behavior).
   */
  slots?: RunSlots;
  /**
   * Listener clock, unix MILLISECONDS (issue #7): the exit watcher reads it to
   * confirm a parked run against its cards. Defaults to Date.now.
   */
  now?: () => number;
}

export interface RedriveReport {
  /** Event ids successfully re-launched (spawn_state transitioned to 'spawned'). */
  spawned: string[];
  /** Event ids that hit a transient failure (spawn_state remains 'failed', still under cap). */
  failed: string[];
  /** Event ids that hit a permanent failure (spawn_state remains 'failed', cap exhausted). */
  permanentlyFailed: string[];
  /**
   * Event ids left for a later sweep (the original listener-backpressure work): launch already in flight, or
   * no free run slot. NOT failures — attempts are untouched and no log entry
   * is written for them.
   */
  deferred: string[];
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/**
 * Re-drive all recoverable ingress events on boot.
 *
 * Snapshot is taken ONCE at the start. Each eligible row is processed as:
 *   incrementSpawnAttempts → respawn → mark spawn_state → appendIngressLog('redriven')
 *
 * Returns a summary report of all outcomes.
 */
export async function redriveOnBoot(deps: RedriveDeps): Promise<RedriveReport> {
  const { db, respawn, cap, source = 'boot-recovery', slots, alerts, now = Date.now } = deps;

  const report: RedriveReport = { spawned: [], failed: [], permanentlyFailed: [], deferred: [] };

  // Snapshot eligible rows once — do not re-query mid-loop so concurrent writes
  // or state changes during re-drive do not alter the set being processed.
  // listRedrivable orders by received_at ASC, so a queued burst launches in
  // arrival order (the original listener-backpressure work).
  const redrivable = db.listRedrivable(cap);

  for (const [index, event] of redrivable.entries()) {
    // ── Run-slot gate (the original listener-backpressure work) ───────────────────────────────────────────
    const acquisition = slots?.tryAcquire(event.event_id) ?? 'acquired';
    if (acquisition === 'duplicate') {
      // A launch for this event is in flight RIGHT NOW (hot path or an earlier
      // sweep) — its row just hasn't reached a terminal state yet. Re-driving
      // it would only burn an attempt on a run-lease conflict. Skip; attempts
      // untouched.
      report.deferred.push(event.event_id);
      continue;
    }
    if (acquisition === 'full') {
      // Capacity exhausted — everything else in this (ordered) snapshot waits
      // for a freed slot. The release kick or the next tick resumes the queue.
      report.deferred.push(...redrivable.slice(index).map((e) => e.event_id));
      break;
    }

    // The original acknowledgement-on-accept work: a launched child owns its slot until it exits; the release
    // then belongs to the exit watcher rather than to this iteration.
    let slotHandedOff = false;
    try {
      // Persist the attempt increment BEFORE launching — mirrors outbox discipline.
      // A crash after this point but before completion leaves the row counted, so
      // the next boot stays within the cap.
      db.incrementSpawnAttempts(event.event_id);

      // Delegate the actual launch to the seam (does not touch ingress_events/log).
      const launch = normalizeRedriveLaunch(await respawn(event));

      if (launch.result === 'spawned') {
        // 'spawned' means LAUNCHED (the original acknowledgement-on-accept work) — a child that later dies is
        // marked failed by the exit watcher below.
        db.markIngressSpawned(event.event_id);
        report.spawned.push(event.event_id);
      } else if (launch.result === 'transient_failure') {
        db.markIngressFailed(event.event_id);
        // Alert on a transient launch failure too (#8). The hot path alerts on
        // EVERY launch failure without grading it, and an operator cannot tell
        // a "will retry" from a "gave up" by the silence — the run is stalled
        // either way until a later sweep succeeds. Mirroring it keeps one
        // failure story across both paths; the reason text carries the nuance.
        void fireRedriveAlert(alerts, event, 're-drive launch failed — will retry while under the attempt cap');
        report.failed.push(event.event_id);
      } else {
        // permanent_failure: mark failed and exhaust the remaining cap headroom so
        // listRedrivable skips this row on future boots (spawn_attempts reaches >= cap).
        db.markIngressFailed(event.event_id);
        exhaustCapForPermanentFailure(db, event.event_id, event.spawn_attempts, cap);
        // The loudest case (#8): the row is now excluded from listRedrivable, so
        // this alert is the ONLY thing that will ever surface the event again.
        void fireRedriveAlert(alerts, event, 're-drive launch failed permanently — the event will not be retried');
        report.permanentlyFailed.push(event.event_id);
      }

      // Record every re-drive attempt in the ingress log, regardless of outcome.
      db.appendIngressLog({ source, eventId: event.event_id, outcome: 'redriven' });

      if (launch.result === 'spawned' && launch.exited !== undefined) {
        slotHandedOff = true;
        void watchRedrivenChildExit(
          { db, source, now, ...(alerts !== undefined && { alerts }), ...(slots !== undefined && { slots }) },
          event,
          launch.exited,
        );
      }
    } finally {
      // Release AFTER the row's terminal state is marked (never re-throws on a
      // seam throw path without freeing the slot). A handed-off slot stays held:
      // its child is still running.
      if (!slotHandedOff) slots?.release(event.event_id);
    }
  }

  return report;
}

/** Accept both seam shapes: a bare result, or a launch that reports its exit. */
function normalizeRedriveLaunch(value: RedriveResult | RedriveLaunch): RedriveLaunch {
  return typeof value === 'string' ? { result: value } : value;
}

/**
 * Supervise a re-driven child after the sweep has moved on (the original acknowledgement-on-accept work).
 *
 * Mirrors the hot path's watchChildExit: a non-zero exit marks the row failed —
 * the same end state the before acknowledgement-on-accept sweep reached when the seam reported a
 * non-zero exit as 'transient_failure' — and logs it, so a re-driven run that
 * dies is visible rather than silently stuck at 'spawned'. Attempts are NOT
 * touched: the attempt was counted before the launch. Never rejects (it runs
 * detached); always frees the slot.
 *
 * A PARKED run exits non-zero too (issue #7) and is not a failure: the row
 * stays 'spawned', the log records 'parked', and the sweep resumes it once
 * its gate passes (ingress/parked.ts). A pre-v9 row carries no run_id, so it
 * cannot be recognised as parked and keeps the failure path.
 */
async function watchRedrivenChildExit(
  deps: { db: ConduitDB; source: string; now: () => number; alerts?: RedriveAlerting; slots?: RunSlots },
  event: IngressEventRecord,
  exited: Promise<SpawnExit>,
): Promise<void> {
  const { db, source, now, alerts, slots } = deps;
  const eventId = event.event_id;
  try {
    let reason: string | null = null;
    try {
      const exit = await exited;
      if (exit.code !== 0) reason = `re-driven run exited with code ${exit.code}`;
    } catch (err) {
      // The seam could not observe the child's exit at all — treat the run as
      // failed rather than leaving the row permanently 'spawned'.
      reason = err instanceof Error ? err.message : String(err);
    }
    if (reason === null) return; // clean exit — the row stays 'spawned'

    if (event.run_id !== null) {
      const parked = inspectParkedRun(db, event.run_id, Math.floor(now() / 1000));
      if (parked !== null) {
        await recordPark(db, alerts?.alert, {
          source,
          eventId,
          flowId: event.flow_id ?? UNATTRIBUTED_FLOW_ID,
          channel: resolveAlertChannel(alerts, event.flow_id),
          runId: event.run_id,
          releaseAt: parked.releaseAt,
        });
        return;
      }
    }

    // Mark → start alert → log. The alert is NOT awaited: a stalled transport
    // (a promise that never settles, not just one that throws) must never block
    // the durable ingress_log entry or the slot release in the finally below —
    // fireRedriveAlert already swallows rejection internally, so firing it
    // without awaiting loses nothing but the send-order guarantee, which this
    // path never needed.
    db.markIngressFailed(eventId);
    void fireRedriveAlert(alerts, event, reason);
    db.appendIngressLog({ source, eventId, outcome: 'spawn_failed', reason });
  } catch {
    // Persistence failure in a detached watcher — nothing left to report to.
  } finally {
    slots?.release(eventId);
  }
}

/**
 * Fire the failure alert for one re-driven event (#8) — best effort.
 *
 * Never throws: a dead alert transport must not abort a sweep, skip the
 * ingress_log entry, or strand a run slot, exactly as in watchChildExit. The
 * channel resolves the way the hot path resolves it (the flow's first egress
 * target, else the listener-global channel); a row with no flow attribution
 * (pre-v9) alerts on the global channel under an explicit placeholder id.
 */
async function fireRedriveAlert(
  alerts: RedriveAlerting | undefined,
  event: IngressEventRecord,
  reason: string,
): Promise<void> {
  if (alerts === undefined) return;
  const flowId = event.flow_id;
  try {
    await alerts.alert({
      flowId: flowId ?? UNATTRIBUTED_FLOW_ID,
      channel: resolveAlertChannel(alerts, flowId),
      eventId: event.event_id,
      reason,
    });
  } catch {
    // Best effort — the ingress_log entry written by the caller is the durable
    // record of this failure.
  }
}

// ---------------------------------------------------------------------------
// Periodic re-drive (the original ingress-attribution work, FR-3)
// ---------------------------------------------------------------------------

/** Handle for a running periodic re-drive; stop() halts future sweeps. */
export interface PeriodicRedrive {
  stop(): void;
  /**
   * Run a sweep NOW (the original listener-backpressure work): fired when a run slot frees so queued events
   * launch immediately instead of waiting out the interval. If a sweep is
   * already in flight the kick is coalesced into ONE follow-up sweep that runs
   * when the current one finishes (the in-flight sweep snapshotted its rows
   * before the slot freed, so it may not see the newly-launchable work).
   */
  kick(): void;
}

export interface PeriodicRedriveDeps extends Omit<RedriveDeps, 'source'> {
  /** Sweep interval in milliseconds. */
  intervalMs: number;
  /**
   * Scheduling seam (default: real setInterval/clearInterval) — injected so
   * tests drive ticks deterministically.
   */
  schedule?: (tick: () => void, ms: number) => { cancel(): void };
  /** Per-sweep observability sink (default: silent). */
  onSweep?: (report: RedriveReport) => void;
  /**
   * Runs after each sweep, inside the same serialisation (issue #7): the
   * listener resumes due parked runs here, so the one schedule and its
   * slot-release kick drive both re-drives and resumes — no second timer.
   */
  afterSweep?: () => Promise<unknown>;
}

/**
 * Run the bounded re-drive sweep on an interval, so a transiently-failed event
 * recovers while the listener is UP — not only at the next boot (the original ingress-attribution work:
 * "re-drive only happens at listener boot; under a steady listener the event
 * is effectively dropped until someone restarts the process").
 *
 * Shares redriveOnBoot's implementation and therefore all four of its design
 * invariants (launch-only seam, no accept-gate re-entry, continuous attempt
 * counting, permanent-failure cap exhaustion). Sweeps are serialized: a tick
 * that fires while the previous sweep is still awaiting its respawns is
 * skipped — attempts stay bounded even when a sweep outlives the interval.
 */
export function startPeriodicRedrive(deps: PeriodicRedriveDeps): PeriodicRedrive {
  const { db, respawn, cap, intervalMs, onSweep, afterSweep, slots, alerts, now } = deps;
  const schedule =
    deps.schedule ??
    ((tick: () => void, ms: number) => {
      const id = setInterval(tick, ms);
      return { cancel: () => clearInterval(id) };
    });

  let sweepInFlight = false;
  let kickPending = false;
  let stopped = false;

  const runSweep = () => {
    if (sweepInFlight || stopped) return;
    sweepInFlight = true;
    void redriveOnBoot({
      db,
      respawn,
      cap,
      source: 'periodic-recovery',
      ...(slots !== undefined && { slots }),
      ...(alerts !== undefined && { alerts }),
      ...(now !== undefined && { now }),
    })
      .then(async (report) => {
        if (stopped) return;
        onSweep?.(report);
        try {
          await afterSweep?.();
        } catch {
          // A persistence error in the parked-run resume must not become an
          // unhandled rejection that wedges the loop — the next tick sweeps
          // again, the same discipline as the detached exit watchers.
        }
      })
      .finally(() => {
        sweepInFlight = false;
        // A slot freed while this sweep was running (its snapshot predates the
        // release) — run the coalesced follow-up so queued work isn't stranded
        // until the next interval tick.
        if (kickPending && !stopped) {
          kickPending = false;
          runSweep();
        }
      });
  };

  const handle = schedule(runSweep, intervalMs);

  return {
    stop() {
      stopped = true;
      handle.cancel();
    },
    kick() {
      if (stopped) return;
      if (sweepInFlight) {
        kickPending = true;
        return;
      }
      runSweep();
    },
  };
}

/**
 * Exhaust remaining spawn_attempts headroom so a permanently-failed event is
 * excluded from future listRedrivable(cap) calls.
 *
 * The initial increment (done before respawn) already moved attempts to
 * snapshotAttempts + 1. This function adds the remaining increments to reach cap.
 */
function exhaustCapForPermanentFailure(
  db: ConduitDB,
  eventId: string,
  snapshotAttempts: number,
  cap: number,
): void {
  // After the pre-respawn increment, attempts = snapshotAttempts + 1.
  // We need (cap - (snapshotAttempts + 1)) more increments to reach the boundary.
  let attemptsAfterPreIncrement = snapshotAttempts + 1;
  while (attemptsAfterPreIncrement < cap) {
    db.incrementSpawnAttempts(eventId);
    attemptsAfterPreIncrement++;
  }
}
