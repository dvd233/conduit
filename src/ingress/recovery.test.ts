/**
 * Tests for boot-time bounded re-drive and escalation (WI-407, D1/FR-7/NFR-3).
 *
 * On listener startup, redriveOnBoot deterministically recovers
 * recorded-but-never-spawned events (spawn_state 'accepted' — a crash between
 * record and spawn) and still-recoverable 'failed' events, WITHOUT ever
 * exceeding the attempt cap or re-billing an already-spawned run.
 *
 * COMPOSITION (real WI-401 + WI-406): it uses the real ConduitDB ingress
 * methods (listRedrivable / incrementSpawnAttempts / markIngressSpawned /
 * markIngressFailed / appendIngressLog) and delegates the actual re-launch to an
 * injected respawn seam (which the listener wires to the WI-406 spawn path with
 * payload reconstruction). Only the respawn seam is stubbed.
 *
 * KEY DESIGN POINTS pinned by these tests:
 *  - The re-drive does NOT re-enter WI-406's accept-gate (that would dedup an
 *    'accepted' row). redriveOnBoot owns the bookkeeping: it increments the
 *    attempt, calls the launch-only respawn seam, then marks spawn_state from the
 *    result and logs 'redriven'. The respawn seam itself does NOT touch
 *    ingress_events / ingress_log.
 *  - Attempt counting is CONTINUOUS with WI-406: the first spawn already counted
 *    as attempt 1, so listRedrivable(cap) (spawn_attempts < cap) makes the cap
 *    bound TOTAL attempts. A 'failed' row at cap is the boundary and is excluded.
 *  - The increment is persisted BEFORE the respawn (mirrors the outbox
 *    increment-before-effect discipline) so a crash mid-re-drive stays bounded.
 *  - A 'permanent_failure' result excludes the row from FUTURE re-drive (it is
 *    surfaced for explicit human action, never auto-retried), distinct from a
 *    'transient_failure' which remains eligible while under the cap.
 *
 * Contract this file pins for src/ingress/recovery.ts:
 *
 *   export type RedriveResult = 'spawned' | 'transient_failure' | 'permanent_failure';
 *   export type RespawnSeam = (event: IngressEventRecord) => Promise<RedriveResult>;
 *
 *   export interface RedriveDeps {
 *     db: ConduitDB;
 *     respawn: RespawnSeam;
 *     cap: number;            // total-attempt cap (first spawn + all re-drives)
 *     source?: string;        // label for the 'redriven' ingress_log rows
 *   }
 *
 *   export interface RedriveReport {
 *     spawned: string[];            // re-spawned successfully (marked 'spawned')
 *     failed: string[];             // transient failure (marked 'failed', still eligible if under cap)
 *     permanentlyFailed: string[];  // permanent (marked 'failed', excluded from future re-drive)
 *   }
 *
 *   export function redriveOnBoot(deps: RedriveDeps): Promise<RedriveReport>;
 *
 * NOTE: depends on WI-406 landing incrementSpawnAttempts + the de-incremented
 * markIngressFailed in db.ts; the seed helpers below use that reconciled contract.
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openConduitDB, type ConduitDB, type IngressEventRecord } from '../persistence/db';
import {
  redriveOnBoot,
  startPeriodicRedrive,
  type RedriveAlerting,
  type RedriveResult,
  type RespawnSeam,
  type RedriveReport,
} from './recovery';
import type { AlertSeam, SpawnFailedAlert } from './spawn';
import { createRunSlots } from './run-slots';

const CAP = 3;

let dir: string;
let db: ConduitDB;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'conduit-recovery-'));
  db = openConduitDB({
    stateDbPath: join(dir, 'state.sqlite'),
    journalDbPath: join(dir, 'journal.sqlite'),
  });
});

afterEach(() => {
  try {
    db.close();
  } catch {
    /* already closed */
  }
  rmSync(dir, { recursive: true, force: true });
});

// ── Seed helpers — compose the REAL reconciled WI-401/WI-406 ingress methods ──
// markIngressFailed no longer increments (WI-406 reconciliation); the attempt
// count comes solely from incrementSpawnAttempts.

function seedAccepted(eventId: string, attempts: number): void {
  db.acceptIngressEvent(eventId, 1000);
  for (let i = 0; i < attempts; i++) db.incrementSpawnAttempts(eventId);
}

function seedFailed(eventId: string, attempts: number): void {
  seedAccepted(eventId, attempts);
  db.markIngressFailed(eventId);
}

function seedSpawned(eventId: string): void {
  seedAccepted(eventId, 1);
  db.markIngressSpawned(eventId);
}

interface RespawnRecorder {
  seam: RespawnSeam;
  calls: string[];
  attemptsAtCall: Record<string, number>;
}
/** Records each respawn call + the spawn_attempts value visible AT respawn time. */
function recordingRespawn(resultFor: (eventId: string) => RedriveResult): RespawnRecorder {
  const calls: string[] = [];
  const attemptsAtCall: Record<string, number> = {};
  const seam: RespawnSeam = async (event) => {
    calls.push(event.event_id);
    attemptsAtCall[event.event_id] = db.getIngressEvent(event.event_id)!.spawn_attempts;
    return resultFor(event.event_id);
  };
  return { seam, calls, attemptsAtCall };
}

const allSpawn = (): RedriveResult => 'spawned';
/** Every re-drive fails transiently — the attempt-capped path. */
const allTransient = (): RedriveResult => 'transient_failure';

// ===========================================================================
// AC1 — redrive accepted + failed under cap: increment-before-respawn, log redriven
// ===========================================================================

describe('redriveOnBoot core re-drive (AC1)', () => {
  it('re-drives accepted and under-cap failed rows, incrementing before respawn and logging redriven', async () => {
    seedAccepted('e-accepted', 0); // crash between record and spawn → attempts 0
    seedFailed('e-failed', 1); // failed once → attempts 1 (under cap)

    const respawn = recordingRespawn(allSpawn);
    const report = await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, source: 'boot' });

    // Both eligible rows were re-driven.
    expect(respawn.calls.sort()).toEqual(['e-accepted', 'e-failed']);
    // The attempt was incremented and PERSISTED before the respawn ran.
    expect(respawn.attemptsAtCall['e-accepted']).toBe(1); // 0 → 1
    expect(respawn.attemptsAtCall['e-failed']).toBe(2); // 1 → 2

    // Success marks them spawned at the incremented attempt count.
    expect(db.getIngressEvent('e-accepted')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    expect(db.getIngressEvent('e-failed')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 2 });

    // Every re-drive is recorded as outcome 'redriven'.
    const redriven = db.getIngressLog({ outcome: 'redriven' }).map((e) => e.eventId).sort();
    expect(redriven).toEqual(['e-accepted', 'e-failed']);
    expect(report.spawned.sort()).toEqual(['e-accepted', 'e-failed']);
  });

  it('does nothing and reports no re-drives when there is nothing recoverable', async () => {
    const respawn = recordingRespawn(allSpawn);
    const report = await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });
    expect(respawn.calls).toHaveLength(0);
    expect(report).toEqual({ spawned: [], failed: [], permanentlyFailed: [], deferred: [] });
  });
});

// ===========================================================================
// AC2 — cap boundary: N-1 re-driven exactly once → N; N is at the boundary, excluded
// ===========================================================================

describe('cap boundary is deterministic (AC2, Q2)', () => {
  it('re-drives a row at cap-1 exactly once (incrementing it to the cap) and never again', async () => {
    seedFailed('e-edge', CAP - 1); // attempts 2, under cap 3
    const respawn = recordingRespawn(allTransient);

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });
    expect(respawn.calls).toEqual(['e-edge']); // re-driven once
    expect(db.getIngressEvent('e-edge')).toMatchObject({ spawn_state: 'failed', spawn_attempts: CAP });

    // A second boot must NOT re-drive it — it is now at the cap boundary.
    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });
    expect(respawn.calls).toEqual(['e-edge']); // still exactly one call total
  });

  it('never re-drives a failed row already at the cap', async () => {
    seedFailed('e-atcap', CAP); // attempts 3 === cap → boundary, excluded
    const respawn = recordingRespawn(allTransient);

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });
    expect(respawn.calls).toHaveLength(0);
  });
});

// ===========================================================================
// AC3 — a 'spawned' row is never re-driven (exactly-once across restart)
// ===========================================================================

describe('spawned rows are never re-driven (AC3, NFR-3)', () => {
  it('does not re-drive an already-spawned event and leaves it untouched', async () => {
    seedSpawned('e-done');
    seedFailed('e-retry', 1); // a genuinely recoverable sibling

    const respawn = recordingRespawn(allSpawn);
    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });

    expect(respawn.calls).toEqual(['e-retry']); // only the failed row
    expect(db.getIngressEvent('e-done')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
  });
});

// ===========================================================================
// AC4 — a failed row at the cap stays failed and queryable (surfaced, not retried)
// ===========================================================================

describe('at-cap failures are surfaced, not retried (AC4, FR-7)', () => {
  it('leaves an at-cap failed row in spawn_state failed and excluded from re-drive', async () => {
    seedFailed('e-exhausted', CAP);
    const respawn = recordingRespawn(allTransient);

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });

    expect(respawn.calls).toHaveLength(0); // never auto-retried
    // Remains durably 'failed' and queryable for explicit human re-drive.
    expect(db.getIngressEvent('e-exhausted')).toMatchObject({ spawn_state: 'failed', spawn_attempts: CAP });
    expect(db.listRedrivable(CAP).map((r) => r.event_id)).not.toContain('e-exhausted');
  });
});

// ===========================================================================
// AC5 — a classified-permanent failure is excluded from future re-drive
// ===========================================================================

describe('permanent failures are not retried even under the cap (AC5)', () => {
  it('re-drives a permanent-failing row once, marks it failed, and excludes it from future re-drive', async () => {
    seedFailed('e-perm', 1); // under cap
    seedFailed('e-transient', 1); // a transient sibling at the same attempt count

    const respawn = recordingRespawn((id) =>
      id === 'e-perm' ? 'permanent_failure' : 'transient_failure',
    );

    const report = await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });

    expect(respawn.calls.sort()).toEqual(['e-perm', 'e-transient']); // both attempted once this boot
    expect(report.permanentlyFailed).toEqual(['e-perm']);
    expect(db.getIngressEvent('e-perm')).toMatchObject({ spawn_state: 'failed' });

    // The permanent row is no longer eligible; the transient one still is (under cap).
    const redrivable = db.listRedrivable(CAP).map((r) => r.event_id);
    expect(redrivable).not.toContain('e-perm');
    expect(redrivable).toContain('e-transient');

    // A second boot re-drives only the transient row — the permanent one is never retried.
    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });
    expect(respawn.calls.filter((id) => id === 'e-perm')).toHaveLength(1);
    expect(respawn.calls.filter((id) => id === 'e-transient')).toHaveLength(2);
  });
});

// ===========================================================================
// AC6 — increment is persisted BEFORE the respawn (crash mid-re-drive stays bounded)
// ===========================================================================

describe('increment-before-respawn keeps a crash bounded (AC6)', () => {
  it('persists the incremented spawn_attempts before invoking the respawn seam', async () => {
    seedFailed('e-crash', 1); // attempts 1

    // Capture the durable attempt count visible at the instant of respawn — this
    // is what a crash "right here" (after increment, before completion) leaves
    // behind, so the next boot is bounded by the cap.
    const respawn = recordingRespawn(allSpawn);
    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });

    expect(respawn.attemptsAtCall['e-crash']).toBe(2); // already incremented before the seam ran
  });

  it('counts only one attempt per boot, so a row at cap-1 reaches exactly the cap', async () => {
    // Establishes the bound: a single boot increments a given row at most once.
    seedFailed('e-bounded', CAP - 1);
    const respawn = recordingRespawn(allTransient);

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });

    expect(db.getIngressEvent('e-bounded')!.spawn_attempts).toBe(CAP); // not CAP+1, not beyond
  });
});

// ===========================================================================
// The original ingress-attribution work FR-3 — periodic re-drive: the sweep runs while the listener is UP
// ===========================================================================

describe('startPeriodicRedrive (the original ingress-attribution work, FR-3)', () => {
  /** Manual scheduling seam: capture the tick and fire it deterministically. */
  function manualSchedule() {
    let tick: (() => void) | null = null;
    let cancelled = false;
    const schedule = (fn: () => void, _ms: number) => {
      tick = fn;
      return { cancel: () => { cancelled = true; } };
    };
    return {
      schedule,
      fire: () => tick?.(),
      get cancelled() { return cancelled; },
    };
  }

  /** Await until the in-flight sweep settles (one macrotask turn is enough). */
  const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

  it('re-drives a failed event on a tick, without a listener restart', async () => {
    seedFailed('e-periodic', 1);
    const respawn = recordingRespawn(allSpawn);
    const clock = manualSchedule();

    const handle = startPeriodicRedrive({
      db, respawn: respawn.seam, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
    });

    clock.fire();
    await settle();

    expect(respawn.calls).toEqual(['e-periodic']);
    expect(db.getIngressEvent('e-periodic')!.spawn_state).toBe('spawned');
    handle.stop();
  });

  it('a throwing afterSweep does not stop the next tick from sweeping (issue #7)', async () => {
    // afterSweep is the parked-run resume; a persistence error there must not
    // become an unhandled rejection that leaves the sweep loop wedged.
    seedFailed('e-after', 1);
    const respawn = recordingRespawn(allSpawn);
    const clock = manualSchedule();
    let afterSweeps = 0;

    const handle = startPeriodicRedrive({
      db, respawn: respawn.seam, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
      afterSweep: async () => { afterSweeps++; throw new Error('state db unavailable'); },
    });

    clock.fire();
    await settle();
    expect(afterSweeps).toBe(1);

    seedFailed('e-next', 1);
    clock.fire();
    await settle();

    expect(afterSweeps).toBe(2);
    expect(respawn.calls).toEqual(['e-after', 'e-next']);
    handle.stop();
  });

  it("labels its ingress_log rows 'periodic-recovery' (distinct from boot-recovery)", async () => {
    seedFailed('e-label', 1);
    const respawn = recordingRespawn(allSpawn);
    const clock = manualSchedule();

    const handle = startPeriodicRedrive({
      db, respawn: respawn.seam, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
    });
    clock.fire();
    await settle();
    handle.stop();

    const redriven = db.getIngressLog().filter((e) => e.outcome === 'redriven');
    expect(redriven).toHaveLength(1);
    expect(redriven[0]!.source).toBe('periodic-recovery');
  });

  it('skips a tick that fires while the previous sweep is still awaiting respawns', async () => {
    seedFailed('e-overlap', 1);
    let resolveRespawn: ((r: RedriveResult) => void) | null = null;
    let calls = 0;
    const slowRespawn: RespawnSeam = () => {
      calls++;
      return new Promise<RedriveResult>((resolve) => { resolveRespawn = resolve; });
    };
    const clock = manualSchedule();

    const handle = startPeriodicRedrive({
      db, respawn: slowRespawn, cap: CAP, intervalMs: 1, schedule: clock.schedule,
    });

    clock.fire();          // sweep 1 starts, blocks in the respawn seam
    clock.fire();          // fires mid-sweep — must be skipped, not queued
    clock.fire();
    expect(calls).toBe(1); // exactly one respawn in flight; overlapping ticks skipped

    resolveRespawn!('spawned');
    await settle();
    expect(db.getIngressEvent('e-overlap')!.spawn_state).toBe('spawned');
    // The attempt count proves the skipped ticks never double-drove the row.
    expect(db.getIngressEvent('e-overlap')!.spawn_attempts).toBe(2);
    handle.stop();
  });

  it('a tick with nothing redrivable is a no-op (no respawns, no log rows)', async () => {
    const respawn = recordingRespawn(allSpawn);
    const clock = manualSchedule();

    const handle = startPeriodicRedrive({
      db, respawn: respawn.seam, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
    });
    clock.fire();
    await settle();
    handle.stop();

    expect(respawn.calls).toHaveLength(0);
    expect(db.getIngressLog()).toHaveLength(0);
  });

  it('stop() cancels the schedule and suppresses further sweeps', async () => {
    seedFailed('e-stop', 1);
    const respawn = recordingRespawn(allSpawn);
    const clock = manualSchedule();

    const handle = startPeriodicRedrive({
      db, respawn: respawn.seam, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
    });
    handle.stop();
    expect(clock.cancelled).toBe(true);

    clock.fire(); // a straggler tick after stop must not sweep
    await settle();
    expect(respawn.calls).toHaveLength(0);
  });

  it('reports each sweep to onSweep', async () => {
    seedFailed('e-report', 1);
    const respawn = recordingRespawn(allSpawn);
    const clock = manualSchedule();
    const reports: RedriveReport[] = [];

    const handle = startPeriodicRedrive({
      db, respawn: respawn.seam, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
      onSweep: (r) => reports.push(r),
    });
    clock.fire();
    await settle();
    handle.stop();

    expect(reports).toHaveLength(1);
    expect(reports[0]!.spawned).toEqual(['e-report']);
  });
});

// ===========================================================================
// The original listener-backpressure work — slot-aware sweeping and the release kick
// ===========================================================================

describe('run-slot-aware re-drive (the original listener-backpressure work)', () => {
  it('skips an in-flight event without burning an attempt (long-running spawn vs. sweep race)', async () => {
    // A launch for e-live is in flight (a queued row the sweep is starting), so
    // the row still reads 'accepted'. Before listener backpressure the sweep would "re-drive" it into
    // a run-lease conflict, burning spawn_attempts toward the cap.
    seedAccepted('e-live', 1);
    seedFailed('e-retry', 1);
    const slots = createRunSlots({ capacity: 10 });
    slots.tryAcquire('e-live'); // the hot path's live claim
    const respawn = recordingRespawn(allSpawn);

    const report = await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, slots });

    expect(respawn.calls).toEqual(['e-retry']);
    expect(report.deferred).toEqual(['e-live']);
    // Attempts untouched, no 'redriven' log row for the skipped event.
    expect(db.getIngressEvent('e-live')).toMatchObject({ spawn_state: 'accepted', spawn_attempts: 1 });
    expect(db.getIngressLog({ outcome: 'redriven' }).map((e) => e.eventId)).toEqual(['e-retry']);
    // The skip did NOT release the hot path's slot.
    expect(slots.inFlight('e-live')).toBe(true);
  });

  it('a busy hot-path slot does not starve the sweep — it re-drives serially through the free capacity', async () => {
    db.acceptIngressEvent('e-1', 1000);
    db.acceptIngressEvent('e-2', 2000);
    db.acceptIngressEvent('e-3', 3000);
    const slots = createRunSlots({ capacity: 2 });
    slots.tryAcquire('other-hot-path-run'); // 1 of 2 slots pinned by a live run
    const respawn = recordingRespawn(allSpawn);

    const report = await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, slots });

    // One slot stays free throughout: the sweep launches each row serially
    // (acquire → respawn-to-completion → release), in received_at order.
    expect(respawn.calls).toEqual(['e-1', 'e-2', 'e-3']);
    expect(report.deferred).toEqual([]);
  });

  it('defers every remaining row the instant capacity is exhausted mid-sweep', async () => {
    db.acceptIngressEvent('e-1', 1000);
    db.acceptIngressEvent('e-2', 2000);
    db.acceptIngressEvent('e-3', 3000);
    // Simulate a hot-path event snatching the only slot the moment e-1 frees it.
    let stolen = false;
    const slots = createRunSlots({
      capacity: 1,
      onRelease: () => {
        if (!stolen) {
          stolen = true;
          slots.tryAcquire('hot-path-steal');
        }
      },
    });
    const respawn = recordingRespawn(allSpawn);

    const report = await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, slots });

    // e-1 re-drove; its freed slot was immediately stolen, so e-2/e-3 defer.
    expect(respawn.calls).toEqual(['e-1']);
    expect(report.deferred).toEqual(['e-2', 'e-3']);
    expect(db.getIngressEvent('e-2')).toMatchObject({ spawn_state: 'accepted', spawn_attempts: 0 });
    expect(db.getIngressEvent('e-3')).toMatchObject({ spawn_state: 'accepted', spawn_attempts: 0 });
  });

  it('with max_concurrent_runs=1 a queued burst re-drives serially, in arrival order', async () => {
    // The listener-backpressure work incident shape: N photos accepted at once, one serial model box.
    db.acceptIngressEvent('photo-3', 3000);
    db.acceptIngressEvent('photo-1', 1000);
    db.acceptIngressEvent('photo-2', 2000);
    const slots = createRunSlots({ capacity: 1 });
    let concurrent = 0;
    let peak = 0;
    const respawn: RespawnSeam = async () => {
      concurrent++;
      peak = Math.max(peak, concurrent);
      await new Promise((resolve) => setTimeout(resolve, 0)); // a "run" spanning a turn
      concurrent--;
      return 'spawned';
    };
    const recorder = recordingRespawn(allSpawn);
    const wrapped: RespawnSeam = async (event) => {
      await recorder.seam(event);
      return respawn(event);
    };

    const report = await redriveOnBoot({ db, respawn: wrapped, cap: CAP, slots });

    expect(peak).toBe(1); // never two runs at once
    expect(recorder.calls).toEqual(['photo-1', 'photo-2', 'photo-3']);
    expect(report.spawned).toEqual(['photo-1', 'photo-2', 'photo-3']);
    expect(slots.inFlightCount()).toBe(0);
  });

  it('releases the slot even when the respawn seam throws', async () => {
    seedFailed('e-throw', 1);
    const slots = createRunSlots({ capacity: 1 });
    const throwing: RespawnSeam = async () => {
      throw new Error('seam exploded');
    };

    await expect(redriveOnBoot({ db, respawn: throwing, cap: CAP, slots })).rejects.toThrow('seam exploded');
    expect(slots.inFlightCount()).toBe(0);
  });
});

describe('periodic re-drive kick (the original listener-backpressure work)', () => {
  /** Await until the in-flight sweep settles (one macrotask turn is enough). */
  const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

  function manualSchedule() {
    let tick: (() => void) | null = null;
    const schedule = (fn: () => void, _ms: number) => {
      tick = fn;
      return { cancel: () => {} };
    };
    return { schedule, fire: () => tick?.() };
  }

  it('kick() runs a sweep immediately without waiting for the interval', async () => {
    seedFailed('e-kick', 1);
    const respawn = recordingRespawn(allSpawn);
    const clock = manualSchedule();

    const handle = startPeriodicRedrive({
      db, respawn: respawn.seam, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
    });

    handle.kick(); // no clock.fire() — the kick alone must sweep
    await settle();

    expect(respawn.calls).toEqual(['e-kick']);
    handle.stop();
  });

  it('a kick during an in-flight sweep coalesces into exactly one follow-up sweep', async () => {
    seedFailed('e-first', 1);
    const sweeps: string[][] = [];
    let resolveRespawn: ((r: RedriveResult) => void) | null = null;
    const gated: RespawnSeam = (event) => {
      if (sweeps.length === 0) sweeps.push([]);
      sweeps[sweeps.length - 1]!.push(event.event_id);
      return new Promise<RedriveResult>((resolve) => { resolveRespawn = resolve; });
    };
    const clock = manualSchedule();
    const reports: RedriveReport[] = [];

    const handle = startPeriodicRedrive({
      db, respawn: gated, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
      onSweep: (r) => { reports.push(r); sweeps.push([]); },
    });

    clock.fire(); // sweep 1 starts, blocks in the respawn seam
    // A slot frees mid-sweep: three kicks arrive — they must coalesce to ONE follow-up.
    handle.kick();
    handle.kick();
    handle.kick();

    // Sweep 1 finishes; e-first spawned. Seed new work the follow-up should find.
    seedFailed('e-second', 1);
    resolveRespawn!('spawned');
    await settle(); // sweep 1 settles → coalesced follow-up starts and blocks
    resolveRespawn!('spawned');
    await settle();
    handle.stop();

    expect(reports).toHaveLength(2); // one original + ONE coalesced follow-up
    expect(db.getIngressEvent('e-second')!.spawn_state).toBe('spawned');
  });

  it('kick() after stop() is a no-op', async () => {
    seedFailed('e-late', 1);
    const respawn = recordingRespawn(allSpawn);
    const clock = manualSchedule();

    const handle = startPeriodicRedrive({
      db, respawn: respawn.seam, cap: CAP, intervalMs: 60_000, schedule: clock.schedule,
    });
    handle.stop();
    handle.kick();
    await settle();

    expect(respawn.calls).toHaveLength(0);
  });
});

// ===========================================================================
// The original acknowledgement-on-accept work — the respawn seam resolves on LAUNCH; the child's exit is
// supervised afterwards
// ===========================================================================

describe('launch-vs-exit re-drive (the original acknowledgement-on-accept work)', () => {
  const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

  /** A respawn seam that launches immediately and hands the exit back later. */
  function launchingRespawn(): {
    seam: RespawnSeam;
    calls: string[];
    exit(eventId: string, code: number): void;
  } {
    const calls: string[] = [];
    const ends = new Map<string, (exit: { code: number }) => void>();
    const seam: RespawnSeam = async (event) => {
      calls.push(event.event_id);
      return {
        result: 'spawned',
        exited: new Promise<{ code: number }>((resolve) => ends.set(event.event_id, resolve)),
      };
    };
    return { seam, calls, exit: (eventId, code) => ends.get(eventId)!({ code }) };
  }

  it('finishes the sweep while the re-driven run is still executing, holding its slot', async () => {
    // Distinct received_at values so the sweep's arrival order is unambiguous.
    db.acceptIngressEvent('e-slow', 1000);
    db.incrementSpawnAttempts('e-slow');
    db.markIngressFailed('e-slow');
    db.acceptIngressEvent('e-queued', 2000);
    const slots = createRunSlots({ capacity: 1 });
    const respawn = launchingRespawn();

    // Before acknowledgement-on-accept this could not resolve until the recovered run finished.
    const report = await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, slots });

    expect(respawn.calls).toEqual(['e-slow']);
    expect(report.spawned).toEqual(['e-slow']);
    expect(db.getIngressEvent('e-slow')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 2 });
    // The live child still owns the only slot, so the next row defers rather
    // than starting a second concurrent run.
    expect(slots.inFlight('e-slow')).toBe(true);
    expect(report.deferred).toEqual(['e-queued']);

    respawn.exit('e-slow', 0);
    await settle();
    expect(slots.inFlightCount()).toBe(0);
    // A clean exit leaves the recovered row alone.
    expect(db.getIngressEvent('e-slow')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 2 });
  });

  it('marks a re-driven run failed when its child exits non-zero, without burning another attempt', async () => {
    seedFailed('e-dies', 1);
    const slots = createRunSlots({ capacity: 1 });
    const respawn = launchingRespawn();

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, slots, source: 'periodic-recovery' });
    respawn.exit('e-dies', 9);
    await settle();

    // Same end state the before acknowledgement-on-accept sweep reached on a non-zero exit ('transient
    // failure'), reported asynchronously — attempts still counted exactly once.
    expect(db.getIngressEvent('e-dies')).toMatchObject({ spawn_state: 'failed', spawn_attempts: 2 });
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['redriven', 'spawn_failed']);
    // Still under the cap, so a later sweep can try once more.
    expect(db.listRedrivable(CAP).map((r) => r.event_id)).toEqual(['e-dies']);
    expect(slots.inFlightCount()).toBe(0);
  });

  it('still accepts a bare result from a seam that does not report exits', async () => {
    seedFailed('e-legacy', 1);
    const slots = createRunSlots({ capacity: 1 });

    const report = await redriveOnBoot({
      db, respawn: recordingRespawn(allSpawn).seam, cap: CAP, slots,
    });

    expect(report.spawned).toEqual(['e-legacy']);
    // No exit to wait for — the slot frees when the seam resolves, as before.
    expect(slots.inFlightCount()).toBe(0);
  });
});

// ===========================================================================
// Issue #8 — every re-drive failure fires the alert seam, exactly like the hot
// spawn path. Before this, a re-driven run that died only wrote a 'spawn_failed'
// row to ingress_log and NEVER alerted, so a halted run stayed silent.
// ===========================================================================

describe('re-drive failure alerting (#8)', () => {
  const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

  /** Captures every alert the re-drive fires. */
  function recordingAlert(behaviour: 'ok' | 'throw' = 'ok') {
    const calls: SpawnFailedAlert[] = [];
    const alert: AlertSeam = async (a) => {
      calls.push(a);
      if (behaviour === 'throw') throw new Error('alert transport down');
    };
    return { alert, calls };
  }

  /** The listener-shaped alerting deps: per-flow channels + the global fallback. */
  function alerting(alert: AlertSeam): RedriveAlerting {
    return {
      alert,
      channels: { flowA: '#flow-a-alerts' },
      globalAlertChannel: '#listener-global',
    };
  }

  /** Seed a 'failed' row that carries v9 flow attribution. */
  function seedAttributedFailure(eventId: string, flowId: string, receivedAt = 1000): void {
    db.acceptIngressEvent(eventId, receivedAt, {
      flowId,
      flowPath: `/flows/${flowId}.yaml`,
      runId: `run-${eventId}`,
      substrateJson: '{}',
    });
    db.incrementSpawnAttempts(eventId);
    db.markIngressFailed(eventId);
  }

  /** A respawn seam that launches immediately and hands the exit back later. */
  function launchingRespawn(): {
    seam: RespawnSeam;
    exit(eventId: string, code: number): void;
    fail(eventId: string, err: Error): void;
  } {
    const resolvers = new Map<string, (exit: { code: number }) => void>();
    const rejecters = new Map<string, (err: Error) => void>();
    const seam: RespawnSeam = async (event) => ({
      result: 'spawned',
      exited: new Promise<{ code: number }>((resolve, reject) => {
        resolvers.set(event.event_id, resolve);
        rejecters.set(event.event_id, reject);
      }),
    });
    return {
      seam,
      exit: (eventId, code) => resolvers.get(eventId)!({ code }),
      fail: (eventId, err) => rejecters.get(eventId)!(err),
    };
  }

  it('alerts when a re-driven child exits non-zero, on the flow\'s own channel', async () => {
    seedAttributedFailure('e-dies', 'flowA');
    const slots = createRunSlots({ capacity: 1 });
    const respawn = launchingRespawn();
    const alerts = recordingAlert();

    await redriveOnBoot({
      db, respawn: respawn.seam, cap: CAP, slots, alerts: alerting(alerts.alert),
    });
    expect(alerts.calls).toHaveLength(0); // launch succeeded — nothing to alert yet

    respawn.exit('e-dies', 9);
    await settle();

    expect(alerts.calls).toEqual([
      {
        flowId: 'flowA',
        channel: '#flow-a-alerts',
        eventId: 'e-dies',
        reason: 're-driven run exited with code 9',
      },
    ]);
    expect(db.getIngressEvent('e-dies')!.spawn_state).toBe('failed');
  });

  it('alerts when the exit promise rejects, using the rejection message', async () => {
    seedAttributedFailure('e-lost', 'flowA');
    const respawn = launchingRespawn();
    const alerts = recordingAlert();

    await redriveOnBoot({
      db, respawn: respawn.seam, cap: CAP, alerts: alerting(alerts.alert),
    });
    respawn.fail('e-lost', new Error('child handle vanished'));
    await settle();

    expect(alerts.calls).toHaveLength(1);
    expect(alerts.calls[0]).toMatchObject({
      eventId: 'e-lost',
      flowId: 'flowA',
      reason: 'child handle vanished',
    });
  });

  it('fires no alert when the re-driven child exits cleanly', async () => {
    seedAttributedFailure('e-ok', 'flowA');
    const respawn = launchingRespawn();
    const alerts = recordingAlert();

    await redriveOnBoot({
      db, respawn: respawn.seam, cap: CAP, alerts: alerting(alerts.alert),
    });
    respawn.exit('e-ok', 0);
    await settle();

    expect(alerts.calls).toEqual([]);
    expect(db.getIngressEvent('e-ok')!.spawn_state).toBe('spawned');
  });

  it('alerts on a permanent respawn failure — the row will never be retried', async () => {
    seedAttributedFailure('e-perm', 'flowA');
    const alerts = recordingAlert();

    await redriveOnBoot({
      db,
      respawn: async () => 'permanent_failure',
      cap: CAP,
      alerts: alerting(alerts.alert),
    });

    expect(alerts.calls).toHaveLength(1);
    expect(alerts.calls[0]).toMatchObject({
      flowId: 'flowA',
      channel: '#flow-a-alerts',
      eventId: 'e-perm',
    });
    expect(alerts.calls[0]!.reason).toContain('permanent');
    // And the row really is out of the re-drive set — nothing else will surface it.
    expect(db.listRedrivable(CAP).map((r) => r.event_id)).toEqual([]);
  });

  it('alerts on a transient respawn failure, mirroring the hot path', async () => {
    seedAttributedFailure('e-transient', 'flowA');
    const alerts = recordingAlert();

    await redriveOnBoot({
      db,
      respawn: async () => 'transient_failure',
      cap: CAP,
      alerts: alerting(alerts.alert),
    });

    expect(alerts.calls).toHaveLength(1);
    expect(alerts.calls[0]).toMatchObject({ flowId: 'flowA', eventId: 'e-transient' });
  });

  it('falls back to the global channel for a pre-v9 row with no flow attribution', async () => {
    seedFailed('e-legacy-row', 1); // no attribution — flow_id is null
    const alerts = recordingAlert();

    await redriveOnBoot({
      db,
      respawn: async () => 'permanent_failure',
      cap: CAP,
      alerts: alerting(alerts.alert),
    });

    expect(alerts.calls).toHaveLength(1);
    expect(alerts.calls[0]).toMatchObject({
      channel: '#listener-global',
      eventId: 'e-legacy-row',
      flowId: 'unknown',
    });
  });

  it('falls back to the global channel when the flow declares no egress target', async () => {
    seedAttributedFailure('e-no-egress', 'flowNoEgress');
    const alerts = recordingAlert();

    await redriveOnBoot({
      db,
      respawn: async () => 'permanent_failure',
      cap: CAP,
      alerts: alerting(alerts.alert),
    });

    expect(alerts.calls[0]).toMatchObject({
      flowId: 'flowNoEgress',
      channel: '#listener-global',
    });
  });

  it('alerts from a periodic sweep, not just from boot recovery', async () => {
    const alerts = recordingAlert();
    let tick: (() => void) | null = null;
    const handle = startPeriodicRedrive({
      db,
      respawn: async () => 'permanent_failure',
      cap: CAP,
      intervalMs: 60_000,
      schedule: (fn) => {
        tick = fn;
        return { cancel: () => {} };
      },
      alerts: alerting(alerts.alert),
    });

    seedAttributedFailure('e-swept', 'flowA', 900);
    tick!();
    await settle();
    handle.stop();

    expect(alerts.calls).toHaveLength(1);
    expect(alerts.calls[0]).toMatchObject({ eventId: 'e-swept', channel: '#flow-a-alerts' });
  });

  it('a throwing alert still marks the row failed, logs spawn_failed, and frees the slot', async () => {
    seedAttributedFailure('e-alert-throws', 'flowA');
    const slots = createRunSlots({ capacity: 1 });
    const respawn = launchingRespawn();
    const alerts = recordingAlert('throw');

    await redriveOnBoot({
      db, respawn: respawn.seam, cap: CAP, slots, alerts: alerting(alerts.alert),
    });
    respawn.exit('e-alert-throws', 3);
    await settle();

    expect(alerts.calls).toHaveLength(1); // it was attempted…
    expect(db.getIngressEvent('e-alert-throws')!.spawn_state).toBe('failed');
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['redriven', 'spawn_failed']);
    expect(slots.inFlightCount()).toBe(0);
  });

  it('a throwing alert does not abort the sweep or lose the redriven log entry', async () => {
    seedAttributedFailure('e-one', 'flowA', 1000);
    seedAttributedFailure('e-two', 'flowA', 2000);
    const alerts = recordingAlert('throw');

    const report = await redriveOnBoot({
      db,
      respawn: async () => 'permanent_failure',
      cap: CAP,
      alerts: alerting(alerts.alert),
    });

    expect(report.permanentlyFailed).toEqual(['e-one', 'e-two']);
    expect(alerts.calls.map((a) => a.eventId)).toEqual(['e-one', 'e-two']);
    expect(db.getIngressLog({ outcome: 'redriven' })).toHaveLength(2);
  });

  it('stays silent when no alerting deps are wired (ungated callers)', async () => {
    seedAttributedFailure('e-unwired', 'flowA');
    const respawn = launchingRespawn();

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP });
    respawn.exit('e-unwired', 4);
    await settle();

    // No alert seam to call — the durable log is still the record of the failure.
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['redriven', 'spawn_failed']);
  });

  it('does not block the boot sweep on an alert that never settles', async () => {
    seedAttributedFailure('e-hang-1', 'flowA', 1000);
    seedAttributedFailure('e-hang-2', 'flowA', 2000);
    let alertCalls = 0;
    const neverSettlingAlert: AlertSeam = async () => {
      alertCalls++;
      return new Promise<void>(() => {
        /* never settles — simulates a stalled alert transport */
      });
    };

    const report = await Promise.race([
      redriveOnBoot({
        db,
        respawn: async () => 'permanent_failure',
        cap: CAP,
        alerts: alerting(neverSettlingAlert),
      }),
      new Promise<RedriveReport>((_, reject) =>
        setTimeout(() => reject(new Error('redriveOnBoot hung on a pending alert')), 250),
      ),
    ]);

    expect(report.permanentlyFailed).toEqual(['e-hang-1', 'e-hang-2']);
    expect(db.getIngressLog({ outcome: 'redriven' }).map((e) => e.eventId).sort()).toEqual([
      'e-hang-1',
      'e-hang-2',
    ]);
    expect(alertCalls).toBe(2); // the alert was started for both rows, just never settled
  });

  it('does not block the exit watcher on an alert that never settles', async () => {
    seedAttributedFailure('e-exit-hang', 'flowA');
    const slots = createRunSlots({ capacity: 1 });
    const respawn = launchingRespawn();
    const neverSettlingAlert: AlertSeam = async () =>
      new Promise<void>(() => {
        /* never settles — simulates a stalled alert transport */
      });

    await redriveOnBoot({
      db, respawn: respawn.seam, cap: CAP, slots, alerts: alerting(neverSettlingAlert),
    });
    respawn.exit('e-exit-hang', 7);

    await Promise.race([
      settle(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('exit watcher hung on a pending alert')), 250)),
    ]);

    expect(db.getIngressEvent('e-exit-hang')!.spawn_state).toBe('failed');
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['redriven', 'spawn_failed']);
    expect(slots.inFlightCount()).toBe(0);
  });
});

// ===========================================================================
// Issue #7 — a re-driven run that PARKS is not a failed re-drive
// ===========================================================================

describe('re-driven exit watcher × parked run (issue #7)', () => {
  const NOW_MS = 1_700_000_000_000;
  const NOW_S = 1_700_000_000;
  const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

  function launchingRespawn(): { seam: RespawnSeam; exit(eventId: string, code: number): void } {
    const ends = new Map<string, (exit: { code: number }) => void>();
    const seam: RespawnSeam = async (event) => ({
      result: 'spawned',
      exited: new Promise<{ code: number }>((resolve) => ends.set(event.event_id, resolve)),
    });
    return { seam, exit: (eventId, code) => ends.get(eventId)!({ code }) };
  }

  /** A failed row WITH v9 attribution, so the watcher knows which run it launched. */
  function seedFailedAttributed(eventId: string, runId: string): void {
    db.acceptIngressEvent(eventId, 1000, { flowId: 'flowA', flowPath: '/flows/a.yaml', runId, substrateJson: '{}' });
    db.incrementSpawnAttempts(eventId);
    db.markIngressFailed(eventId);
  }

  function parkRun(runId: string, releaseAt: number): void {
    db.insertRun({ run_id: runId, flow: '/flows/a.yaml', input_fingerprint: 'fp', status: 'halted', outcome: 'parked' });
    db.insertCard({ run_id: runId, id: 'c1', parent_id: null, lane: 'narrate', status: 'ready', attempt: 0, wave: 0, owned_paths: [], rework_count: 0 });
    db.getStateDb().prepare('UPDATE cards SET release_at = $at WHERE run_id = $r').run({ $at: releaseAt, $r: runId });
    // A real park also records WHY the card is gated: `release_at` alone cannot
    // distinguish a provider cap from the fan-out cache-warming stagger, which
    // stamps the same column.
    db.appendCardLog({
      runId,
      kind: 'entered_lane',
      cardId: 'c1',
      station: 'narrate',
      attempt: 0,
      sourceLane: 'narrate',
      destLane: 'narrate',
      reasonClass: 'rate_limited',
    });
  }

  function alerting(): { alerts: RedriveAlerting; seen: SpawnFailedAlert[] } {
    const seen: SpawnFailedAlert[] = [];
    return { seen, alerts: { alert: async (a) => { seen.push(a); }, channels: { flowA: '#a' }, globalAlertChannel: '#ops' } };
  }

  it('leaves the row spawned, logs parked, alerts informationally, and spends no further attempt', async () => {
    seedFailedAttributed('e-park', 'run-park');
    const slots = createRunSlots({ capacity: 1 });
    const respawn = launchingRespawn();
    const { alerts, seen } = alerting();

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, slots, alerts, now: () => NOW_MS });
    parkRun('run-park', NOW_S + 600);
    respawn.exit('e-park', 1);
    await settle();

    // The incident's ingress_log: "spawn_failed — re-driven run exited with code 1".
    expect(db.getIngressEvent('e-park')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 2 });
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['redriven', 'parked']);
    expect(db.getIngressLog({ outcome: 'parked' })[0]!.reason).toContain(new Date((NOW_S + 600) * 1000).toISOString());
    expect(seen).toHaveLength(1);
    expect(seen[0]!.reason).toMatch(/^parked/);
    expect(seen[0]).toMatchObject({ flowId: 'flowA', channel: '#a', eventId: 'e-park' });
    // Not redrivable: a parked run is resumed, never re-driven.
    expect(db.listRedrivable(CAP)).toEqual([]);
    expect(slots.inFlightCount()).toBe(0);
  });

  it('does not hold the run slot on a park alert that never settles', async () => {
    // The failure path already refuses to await its alert (see the exit-watcher
    // test above) because the watcher releases the slot in a finally AFTER it.
    // The park path runs through the same finally, so a stalled transport there
    // would pin the slot for the life of the listener and defer every later
    // launch — the durable ingress_log entry is written before the alert, so
    // waiting for the send buys nothing.
    seedFailedAttributed('e-park-hang', 'run-park-hang');
    const slots = createRunSlots({ capacity: 1 });
    const respawn = launchingRespawn();
    const neverSettling: RedriveAlerting = {
      alert: async () =>
        new Promise<void>(() => {
          /* never settles — simulates a stalled alert transport */
        }),
      channels: { flowA: '#a' },
      globalAlertChannel: '#ops',
    };

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, slots, alerts: neverSettling, now: () => NOW_MS });
    parkRun('run-park-hang', NOW_S + 600);
    respawn.exit('e-park-hang', 1);

    await Promise.race([
      settle(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('exit watcher hung on a pending park alert')), 250)),
    ]);

    // The park is recorded durably and the slot is free for the next launch.
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['redriven', 'parked']);
    expect(db.getIngressEvent('e-park-hang')!.spawn_state).toBe('spawned');
    expect(slots.inFlightCount()).toBe(0);
  });

  it('keeps the failure path for a pre-v9 row with no run attribution, even if some run is parked', async () => {
    seedFailed('e-legacy', 1); // no run_id
    const respawn = launchingRespawn();
    const { alerts, seen } = alerting();

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, alerts, now: () => NOW_MS });
    respawn.exit('e-legacy', 1);
    await settle();

    expect(db.getIngressEvent('e-legacy')!.spawn_state).toBe('failed');
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['redriven', 'spawn_failed']);
    expect(seen[0]!.reason).toBe('re-driven run exited with code 1');
  });

  it('keeps the failure path for a genuine nonzero exit of an attributed run that is not parked', async () => {
    seedFailedAttributed('e-dies', 'run-dies');
    db.insertRun({ run_id: 'run-dies', flow: '/flows/a.yaml', input_fingerprint: 'fp', status: 'halted', outcome: 'halted' });
    const respawn = launchingRespawn();
    const { alerts, seen } = alerting();

    await redriveOnBoot({ db, respawn: respawn.seam, cap: CAP, alerts, now: () => NOW_MS });
    respawn.exit('e-dies', 1);
    await settle();

    expect(db.getIngressEvent('e-dies')).toMatchObject({ spawn_state: 'failed', spawn_attempts: 2 });
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['redriven', 'spawn_failed']);
    expect(seen[0]!.reason).toBe('re-driven run exited with code 1');
  });
});
