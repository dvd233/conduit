/**
 * Tests for the core accept-dedup-spawn path (WI-406, D1/FR-1/FR-2/FR-3/FR-7/NFR-2/NFR-3).
 *
 * runSpawnPath is the listener's core path for a valid event: it atomically
 * records the event_id as 'accepted' BEFORE spawning, counts the attempt, builds
 * + projects the substrate envelope, spawns `conduit run` through an injected
 * seam, marks 'spawned' on success or 'failed' + alerts on failure, and logs
 * every outcome to ingress_log. It composes the REAL WI-401 (ingress_events
 * state machine), WI-403 (buildEnvelope/projectSubstrate) and WI-404
 * (appendIngressLog/getIngressLog) — only the two outermost I/O effects are
 * stubbed: the spawn (Bun.spawn) seam and the alert (egressSend/Slack) seam.
 *
 * Contract this file pins for src/ingress/spawn.ts:
 *
 *   export interface SpawnInvocation { flowPath: string; inputInline: string }
 *   export interface SpawnExit { code: number }
 *   export interface SpawnSeamResult { ok: boolean; error?: string; exited?: Promise<SpawnExit> }
 *   export type SpawnSeam = (invocation: SpawnInvocation) => Promise<SpawnSeamResult>;
 *
 *   export interface SpawnFailedAlert { flowId: string; channel: string; eventId: string; reason: string }
 *   export type AlertSeam = (alert: SpawnFailedAlert) => Promise<void>;
 *
 *   export interface SpawnPathDeps {
 *     db: ConduitDB;
 *     spawn: SpawnSeam;
 *     alert: AlertSeam;
 *     globalAlertChannel: string;   // listener-global fallback alert target
 *   }
 *
 *   export interface SpawnPathInput {
 *     source: string; eventId: string; receivedAt: number; authVerified: boolean;
 *     headers: Record<string, unknown>; body: unknown; attachments?: unknown[];
 *     flowId: string; flowPath: string; flow: FlowConfig;
 *     substrateMapping?: Record<string, string>;   // binding's JSON-path projection (WI-402)
 *   }
 *
 *   export type SpawnPathResult =
 *     | { outcome: 'accepted'; runId: string } | { outcome: 'duplicate' }
 *     | { outcome: 'spawn_failed' } | { outcome: 'queued'; runId: string };
 *
 *   export function runSpawnPath(deps: SpawnPathDeps, input: SpawnPathInput): Promise<SpawnPathResult>;
 *
 * Sequence (happy): acceptIngressEvent → (if won) incrementSpawnAttempts →
 * buildEnvelope+projectSubstrate → spawn seam(flowPath, JSON envelope) →
 * markIngressSpawned + appendIngressLog('accepted'). On a lost accept: log
 * 'duplicate', NO spawn. On spawn failure: markIngressFailed (attempts already
 * counted, no double-count) → resolve alert channel (failing flow's
 * channels.egress target first, else globalAlertChannel) → alert seam →
 * appendIngressLog('spawn_failed'). The attempt is counted BEFORE the spawn so a
 * crash mid-spawn leaves a recoverable 'accepted' row (WI-406 AC8 / WI-401 listRedrivable).
 *
 * The original acknowledgement-on-accept work — the seam resolves on LAUNCH, not on exit. runSpawnPath returns
 * (and its callers ack) as soon as the child is live; a seam that reports an
 * `exited` promise keeps its run slot until the child exits, and a non-zero exit
 * runs the same FR-7 treatment asynchronously. A seam that omits `exited` is
 * treated exactly as before — the launch is over when the seam resolves.
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openConduitDB, type ConduitDB, type IngressEventRecord } from '../persistence/db';
import { buildEnvelope, projectSubstrate } from './envelope';
import { deriveIngressRunId } from './run-id';
import { createRunSlots } from './run-slots';
import type { FlowConfig } from '../types/kernel';
import {
  runSpawnPath,
  type SpawnPathInput,
  type SpawnSeam,
  type SpawnInvocation,
  type SpawnSeamResult,
  type AlertSeam,
  type SpawnFailedAlert,
} from './spawn';

let dir: string;
let db: ConduitDB;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'conduit-spawn-'));
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

function makeFlow(egressTarget?: string): FlowConfig {
  const flow: FlowConfig = { version: 1, stations: {} };
  if (egressTarget !== undefined) {
    flow.channels = { egress: [{ type: 'slack', target: egressTarget }] };
  }
  return flow;
}

function baseInput(over: Partial<SpawnPathInput> = {}): SpawnPathInput {
  return {
    source: 'github',
    eventId: 'evt-1',
    receivedAt: 1000,
    authVerified: true,
    headers: { 'x-delivery': 'd1' },
    body: { action: 'opened', number: 7 },
    flowId: 'github-sync',
    flowPath: '/flows/github-sync.yaml',
    flow: makeFlow(),
    ...over,
  };
}

/** A spawn seam that records its invocations and the DB state captured AT call time. */
interface SpawnRecorder {
  seam: SpawnSeam;
  calls: SpawnInvocation[];
  stateAtCall: Array<IngressEventRecord | null>;
  redrivableAtCall: IngressEventRecord[][];
}
function recordingSpawn(
  result: SpawnSeamResult,
  eventId = 'evt-1',
): SpawnRecorder {
  const calls: SpawnInvocation[] = [];
  const stateAtCall: Array<IngressEventRecord | null> = [];
  const redrivableAtCall: IngressEventRecord[][] = [];
  const seam: SpawnSeam = async (inv) => {
    calls.push(inv);
    // Capture the durable state at the instant of spawn — this is what a crash
    // "right here" would leave behind for boot recovery.
    stateAtCall.push(db.getIngressEvent(eventId));
    redrivableAtCall.push(db.listRedrivable(100));
    return result;
  };
  return { seam, calls, stateAtCall, redrivableAtCall };
}

function recordingAlert(): { seam: AlertSeam; alerts: SpawnFailedAlert[] } {
  const alerts: SpawnFailedAlert[] = [];
  const seam: AlertSeam = async (a) => {
    alerts.push(a);
  };
  return { seam, alerts };
}

const SPAWN_OK: SpawnSeamResult = { ok: true };
const SPAWN_FAIL: SpawnSeamResult = { ok: false, error: 'missing binary' };

// ===========================================================================
// AC1 / AC3 — happy path: accept-before-spawn, attempt counted, mark spawned, log accepted
// ===========================================================================

describe('happy path (AC1, AC3)', () => {
  it('spawns exactly once, marks spawned, logs accepted, with spawn_attempts === 1', async () => {
    const spawn = recordingSpawn(SPAWN_OK);
    const alert = recordingAlert();

    const result = await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    expect(result).toEqual({ outcome: 'accepted', runId: deriveIngressRunId('evt-1') });
    expect(spawn.calls).toHaveLength(1);

    // Accept + increment happened BEFORE the spawn seam ran (ordering + Q2 counting).
    expect(spawn.stateAtCall[0]).toMatchObject({ spawn_state: 'accepted', spawn_attempts: 1 });

    // Terminal state after a successful spawn.
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });

    const log = db.getIngressLog();
    expect(log).toHaveLength(1);
    expect(log[0]).toMatchObject({ outcome: 'accepted', eventId: 'evt-1', source: 'github' });
  });

  it('spawning is the only external effect on success — no alert is fired', async () => {
    const spawn = recordingSpawn(SPAWN_OK);
    const alert = recordingAlert();

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    expect(spawn.calls).toHaveLength(1);
    expect(alert.alerts).toHaveLength(0);
  });
});

// ===========================================================================
// AC7 — the spawned run is seeded with the serialized substrate envelope
// ===========================================================================

describe('substrate envelope seeding (AC7, FR-2, FR-10)', () => {
  it('passes the flow path and the serialized canonical envelope as --input-inline', async () => {
    const spawn = recordingSpawn(SPAWN_OK);
    const input = baseInput();

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 },
      input,
    );

    const inv = spawn.calls[0]!;
    expect(inv.flowPath).toBe('/flows/github-sync.yaml');

    // No mapping → the inlined payload is exactly the canonical envelope (real WI-403).
    const expected = projectSubstrate(
      buildEnvelope({
        source: 'github',
        eventId: 'evt-1',
        receivedAt: 1000,
        authVerified: true,
        headers: { 'x-delivery': 'd1' },
        body: { action: 'opened', number: 7 },
      }),
      undefined,
    );
    expect(JSON.parse(inv.inputInline)).toEqual(expected);
  });

  it('applies the binding substrate mapping when projecting the envelope', async () => {
    const spawn = recordingSpawn(SPAWN_OK);
    const input = baseInput({ substrateMapping: { subject: '$.body.action', id: '$.event_id' } });

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 },
      input,
    );

    expect(JSON.parse(spawn.calls[0]!.inputInline)).toEqual({ subject: 'opened', id: 'evt-1' });
  });

  it('drops a secret header from the inlined substrate (envelope filtering, NFR-5)', async () => {
    const SECRET = 'sk-live-SPAWN-SECRET-aabbccddeeff';
    const spawn = recordingSpawn(SPAWN_OK);

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput({ headers: { authorization: `Bearer ${SECRET}`, 'x-delivery': 'd1' } }),
    );

    expect(spawn.calls[0]!.inputInline).not.toContain(SECRET);
  });
});

// ===========================================================================
// The original ingress-attribution work — derived run id + attribution persisted with the accept
// ===========================================================================

describe('run-id derivation + attribution (the original ingress-attribution work)', () => {
  it('passes the event-derived run id to the spawn seam', async () => {
    const spawn = recordingSpawn(SPAWN_OK);

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    expect(spawn.calls[0]!.runId).toBe(deriveIngressRunId('evt-1'));
  });

  it('derives distinct run ids for distinct events (the ingress event-isolation work collision fix)', async () => {
    const spawn = recordingSpawn(SPAWN_OK);
    const deps = { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 };

    await runSpawnPath(deps, baseInput({ eventId: 'photo-1' }));
    await runSpawnPath(deps, baseInput({ eventId: 'photo-2' }));

    expect(spawn.calls).toHaveLength(2);
    expect(spawn.calls[0]!.runId).not.toBe(spawn.calls[1]!.runId);
  });

  it('persists flow attribution + payload + run id atomically with the accept (visible at spawn time)', async () => {
    const spawn = recordingSpawn(SPAWN_OK);

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    // At the instant of spawn — i.e. what a crash "right here" leaves behind —
    // the row already carries everything a re-drive needs.
    const atSpawn = spawn.stateAtCall[0]!;
    expect(atSpawn.flow_id).toBe('github-sync');
    expect(atSpawn.flow_path).toBe('/flows/github-sync.yaml');
    expect(atSpawn.run_id).toBe(deriveIngressRunId('evt-1'));
    expect(atSpawn.substrate_json).toBe(spawn.calls[0]!.inputInline);
  });

  it('a failed spawn leaves a fully-attributed row for the re-drive sweep', async () => {
    const spawn = recordingSpawn(SPAWN_FAIL);

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    const rows = db.listRedrivable(100);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      event_id: 'evt-1',
      spawn_state: 'failed',
      flow_id: 'github-sync',
      flow_path: '/flows/github-sync.yaml',
      run_id: deriveIngressRunId('evt-1'),
    });
    expect(rows[0]!.substrate_json).toBe(spawn.calls[0]!.inputInline);
  });
});

// ===========================================================================
// AC2 — duplicate suppression (exactly-once; 0 second billed runs)
// ===========================================================================

describe('duplicate suppression (AC2, NFR-2)', () => {
  it('does not spawn a second run for an already-spawned event; logs duplicate', async () => {
    const spawn = recordingSpawn(SPAWN_OK);
    const deps = { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 };

    const first = await runSpawnPath(deps, baseInput());
    const second = await runSpawnPath(deps, baseInput()); // same event_id re-delivered

    expect(first).toEqual({ outcome: 'accepted', runId: deriveIngressRunId('evt-1') });
    expect(second).toEqual({ outcome: 'duplicate' });
    expect(spawn.calls).toHaveLength(1); // NOT 2 — the second delivery never spawns
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['accepted', 'duplicate']);
    // State is untouched by the duplicate.
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
  });

  it('treats an already-accepted (not yet spawned) event as a duplicate without spawning', async () => {
    // A prior attempt accepted the event but had not yet spawned (e.g. concurrent delivery).
    db.acceptIngressEvent('evt-1', 500);
    const spawn = recordingSpawn(SPAWN_OK);

    const result = await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    expect(result).toEqual({ outcome: 'duplicate' });
    expect(spawn.calls).toHaveLength(0);
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['duplicate']);
  });
});

// ===========================================================================
// AC4 / AC5 — spawn failure: mark failed (no double-count), alert, log, never drop
// ===========================================================================

describe('spawn failure handling (AC4, FR-7)', () => {
  it('marks failed with attempts === 1, fires an alert, logs spawn_failed, and reports the failure', async () => {
    const spawn = recordingSpawn(SPAWN_FAIL);
    const alert = recordingAlert();

    const result = await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    expect(result).toEqual({ outcome: 'spawn_failed' });
    // Attempt was counted once before the spawn; markIngressFailed does NOT double-count.
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'failed', spawn_attempts: 1 });
    expect(alert.alerts).toHaveLength(1);
    expect(alert.alerts[0]).toMatchObject({ flowId: 'github-sync', eventId: 'evt-1' });
    // The event is never silently dropped — its failure is durably logged.
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['spawn_failed']);
  });
});

describe('spawn_failed alert routing (AC5, Q1)', () => {
  it("routes the alert to the failing flow's own egress target when it declares one", async () => {
    const spawn = recordingSpawn(SPAWN_FAIL);
    const alert = recordingAlert();

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#listener-global', redriveCap: 100 },
      baseInput({ flow: makeFlow('#github-flow-alerts') }),
    );

    expect(alert.alerts[0]!.channel).toBe('#github-flow-alerts');
  });

  it('falls back to the listener-global alert channel when the flow declares no egress', async () => {
    const spawn = recordingSpawn(SPAWN_FAIL);
    const alert = recordingAlert();

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#listener-global', redriveCap: 100 },
      baseInput({ flow: makeFlow() }), // no channels.egress
    );

    expect(alert.alerts[0]!.channel).toBe('#listener-global');
  });
});

// ===========================================================================
// Seam throw — spawn seam throws instead of returning an error object
// ===========================================================================

describe('spawn seam throws (PR-4 review comment)', () => {
  it('treats a thrown spawn seam as a spawn failure: marks failed, alerts with thrown message, logs spawn_failed, returns spawn_failed', async () => {
    const thrownError = new Error('ENOENT: bun binary not found');
    const calls: SpawnInvocation[] = [];
    const throwingSeam: SpawnSeam = async (inv) => {
      calls.push(inv);
      throw thrownError;
    };
    const alert = recordingAlert();

    const result = await runSpawnPath(
      { db, spawn: throwingSeam, alert: alert.seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    // Returns spawn_failed instead of throwing.
    expect(result).toEqual({ outcome: 'spawn_failed' });

    // Row is marked failed — not left in 'accepted'.
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'failed', spawn_attempts: 1 });

    // Alert fired with the thrown error message as reason.
    expect(alert.alerts).toHaveLength(1);
    expect(alert.alerts[0]).toMatchObject({ eventId: 'evt-1', reason: thrownError.message });

    // Failure is durably logged — event is never silently dropped.
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['spawn_failed']);
  });

  it('uses String(thrown value) as the reason when the thrown value is not an Error', async () => {
    const throwingSeam: SpawnSeam = async () => {
      // eslint-disable-next-line @typescript-eslint/only-throw-error
      throw 'raw string throw';
    };
    const alert = recordingAlert();

    const result = await runSpawnPath(
      { db, spawn: throwingSeam, alert: alert.seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    expect(result).toEqual({ outcome: 'spawn_failed' });
    expect(alert.alerts[0]).toMatchObject({ reason: 'raw string throw' });
  });
});

// ===========================================================================
// AC8 / FR-1 — accept-before-spawn recoverability; never blind-retry a spawned event
// ===========================================================================

describe('recoverability and listener boundary (AC8, FR-1)', () => {
  it('leaves an attempt-counted accepted row that boot re-drive could recover at the instant of spawn', async () => {
    const spawn = recordingSpawn(SPAWN_OK);

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 },
      baseInput(),
    );

    // At spawn-time the row is still 'accepted' (mark-spawned happens only after a
    // successful return) with the attempt already counted — so a crash right here
    // leaves a recoverable row, and listRedrivable surfaces it.
    expect(spawn.stateAtCall[0]).toMatchObject({ spawn_state: 'accepted', spawn_attempts: 1 });
    expect(spawn.redrivableAtCall[0]!.map((r) => r.event_id)).toContain('evt-1');
  });

  it('never re-spawns an event that already reached spawned (no blind retry)', async () => {
    const spawn = recordingSpawn(SPAWN_OK);
    const deps = { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#listener', redriveCap: 100 };

    await runSpawnPath(deps, baseInput());
    await runSpawnPath(deps, baseInput());
    await runSpawnPath(deps, baseInput());

    expect(spawn.calls).toHaveLength(1);
  });
});

// ===========================================================================
// The original listener-backpressure work — run-slot backpressure on the hot path
// ===========================================================================

describe('run-slot backpressure (the original listener-backpressure work)', () => {
  it('queues an accepted event when all slots are busy: no spawn, no attempt, row stays accepted', async () => {
    const slots = createRunSlots({ capacity: 1 });
    slots.tryAcquire('other-run'); // capacity consumed by another event's run
    const spawn = recordingSpawn(SPAWN_OK);

    const result = await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );

    expect(result).toEqual({ outcome: 'queued', runId: deriveIngressRunId('evt-1') });
    expect(spawn.calls).toHaveLength(0);
    // Queueing is NOT a spawn attempt — the full redrive-cap budget remains.
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'accepted', spawn_attempts: 0 });
    // The sweep can see it: fully attributed and redrivable.
    const rows = db.listRedrivable(3);
    expect(rows.map((r) => r.event_id)).toContain('evt-1');
    expect(rows[0]!.substrate_json).not.toBeNull();
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['queued']);
  });

  it('holds the slot for the whole launch and releases only after the final state is marked', async () => {
    const statesAtRelease: string[] = [];
    const slots = createRunSlots({
      capacity: 1,
      onRelease: () => statesAtRelease.push(db.getIngressEvent('evt-1')!.spawn_state),
    });
    let inFlightDuringSpawn = false;
    const seam: SpawnSeam = async () => {
      inFlightDuringSpawn = slots.inFlight('evt-1');
      return SPAWN_OK;
    };

    await runSpawnPath(
      { db, spawn: seam, alert: recordingAlert().seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );

    expect(inFlightDuringSpawn).toBe(true);
    expect(slots.inFlightCount()).toBe(0);
    // Release fired AFTER markIngressSpawned — the kicked sweep never sees a
    // freed slot next to a stale 'accepted' row.
    expect(statesAtRelease).toEqual(['spawned']);
  });

  it('releases the slot on spawn failure, after the row is marked failed', async () => {
    const statesAtRelease: string[] = [];
    const slots = createRunSlots({
      capacity: 1,
      onRelease: () => statesAtRelease.push(db.getIngressEvent('evt-1')!.spawn_state),
    });

    await runSpawnPath(
      { db, spawn: recordingSpawn(SPAWN_FAIL).seam, alert: recordingAlert().seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );

    expect(slots.inFlightCount()).toBe(0);
    expect(statesAtRelease).toEqual(['failed']);
  });

  it('releases the slot when the spawn seam throws', async () => {
    const slots = createRunSlots({ capacity: 1 });
    const throwingSeam: SpawnSeam = async () => {
      throw new Error('boom');
    };

    await runSpawnPath(
      { db, spawn: throwingSeam, alert: recordingAlert().seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );

    expect(slots.inFlightCount()).toBe(0);
  });

  it('suppresses a redelivery whose event is already in flight (sweep is re-driving it)', async () => {
    // A prior attempt failed; the periodic sweep is mid-redrive (slot held for
    // evt-1) when the provider redelivers the same event.
    db.acceptIngressEvent('evt-1', 500);
    db.incrementSpawnAttempts('evt-1');
    db.markIngressFailed('evt-1');
    const slots = createRunSlots({ capacity: 5 });
    expect(slots.tryAcquire('evt-1')).toBe('acquired'); // the sweep's claim
    const spawn = recordingSpawn(SPAWN_OK);

    const result = await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );

    expect(result).toEqual({ outcome: 'duplicate' });
    expect(spawn.calls).toHaveLength(0);
    // The redelivery must NOT free the sweep's slot.
    expect(slots.inFlight('evt-1')).toBe(true);
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['duplicate']);
  });

  it('caps a concurrent burst: capacity K spawns K runs, queues the rest', async () => {
    const slots = createRunSlots({ capacity: 2 });
    const started: string[] = [];
    let releaseAll: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      releaseAll = resolve;
    });
    const seam: SpawnSeam = async (inv) => {
      started.push(inv.runId);
      await gate; // hold the slot — a real run takes minutes
      return SPAWN_OK;
    };
    const deps = { db, spawn: seam, alert: recordingAlert().seam, globalAlertChannel: '#l', redriveCap: 3, slots };

    const burst = ['p1', 'p2', 'p3', 'p4'].map((id) =>
      runSpawnPath(deps, baseInput({ eventId: id })),
    );
    // Let the first two claim slots and reach their (blocked) spawn.
    await Promise.resolve();
    const outcomes = await Promise.all([burst[2]!, burst[3]!]);

    expect(started).toHaveLength(2);
    expect(outcomes).toEqual([
      { outcome: 'queued', runId: deriveIngressRunId('p3') },
      { outcome: 'queued', runId: deriveIngressRunId('p4') },
    ]);
    expect(db.getIngressEvent('p3')).toMatchObject({ spawn_state: 'accepted', spawn_attempts: 0 });

    releaseAll();
    await Promise.all([burst[0]!, burst[1]!]);
    expect(slots.inFlightCount()).toBe(0);
    // The queued events are NOT spawned by the hot path — the sweep owns them.
    expect(started).toHaveLength(2);
  });

  it('runs ungated when no slot gate is provided (legacy single-seam callers)', async () => {
    const spawn = recordingSpawn(SPAWN_OK);

    const result = await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#l', redriveCap: 3 },
      baseInput(),
    );

    expect(result).toEqual({ outcome: 'accepted', runId: deriveIngressRunId('evt-1') });
    expect(spawn.calls).toHaveLength(1);
  });
});

// ===========================================================================
// The original acknowledgement-on-accept work — ack on accept: the path resolves at LAUNCH, the exit is
// supervised afterwards
// ===========================================================================

/** Let the detached exit watcher's continuations run to completion. */
const settleExitWatcher = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 0));

/** A seam that launches immediately and lets the test decide when the child ends. */
function launchingSpawn(): {
  seam: SpawnSeam;
  calls: SpawnInvocation[];
  /** End the child with an exit code. */
  exit(code: number): void;
  /** Make the child's terminal state unobservable (the exit promise rejects). */
  loseChild(err: unknown): void;
} {
  const calls: SpawnInvocation[] = [];
  let end: (exit: { code: number }) => void = () => {};
  let lose: (err: unknown) => void = () => {};
  const exited = new Promise<{ code: number }>((resolve, reject) => {
    end = resolve;
    lose = reject;
  });
  const seam: SpawnSeam = async (inv) => {
    calls.push(inv);
    return { ok: true, exited };
  };
  return { seam, calls, exit: (code) => end({ code }), loseChild: (err) => lose(err) };
}

describe('ack on accept (the original acknowledgement-on-accept work)', () => {
  it('resolves as soon as the child is LAUNCHED, while the run is still executing', async () => {
    const spawn = launchingSpawn();
    const alert = recordingAlert();
    const slots = createRunSlots({ capacity: 1 });

    // The child never exits during this test — before acknowledgement-on-accept this call could not
    // resolve at all, and the webhook response waited out the whole run.
    const result = await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );

    expect(result).toEqual({ outcome: 'accepted', runId: deriveIngressRunId('evt-1') });
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['accepted']);
    expect(alert.alerts).toHaveLength(0);
    // The run is live, so its slot is still held.
    expect(slots.inFlight('evt-1')).toBe(true);
  });

  it('holds the run slot until the child exits — a second event queues while the first run is live', async () => {
    const spawn = launchingSpawn();
    const slots = createRunSlots({ capacity: 1 });
    const deps = {
      db,
      spawn: spawn.seam,
      alert: recordingAlert().seam,
      globalAlertChannel: '#l',
      redriveCap: 3,
      slots,
    };

    await runSpawnPath(deps, baseInput({ eventId: 'evt-1' }));
    const second = await runSpawnPath(deps, baseInput({ eventId: 'evt-2' }));

    // max_concurrent_runs still means concurrent RUNS, not concurrent acks.
    expect(second).toEqual({ outcome: 'queued', runId: deriveIngressRunId('evt-2') });
    expect(spawn.calls).toHaveLength(1);

    spawn.exit(0);
    await settleExitWatcher();

    expect(slots.inFlightCount()).toBe(0);
  });

  it('marks failed, alerts, and logs spawn_failed when the child exits non-zero after the ack', async () => {
    const spawn = launchingSpawn();
    const alert = recordingAlert();
    const slots = createRunSlots({ capacity: 1 });

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput({ flow: makeFlow('#github-flow-alerts') }),
    );

    // Nothing has failed yet — the run is live.
    expect(alert.alerts).toHaveLength(0);

    spawn.exit(2);
    await settleExitWatcher();

    // Same FR-7 treatment as a failed launch, just asynchronous — and the
    // attempt counted at launch is NOT double-counted.
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'failed', spawn_attempts: 1 });
    expect(alert.alerts).toHaveLength(1);
    expect(alert.alerts[0]).toMatchObject({
      flowId: 'github-sync',
      eventId: 'evt-1',
      channel: '#github-flow-alerts',
    });
    expect(alert.alerts[0]!.reason).toContain('code 2');
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['accepted', 'spawn_failed']);
    // The row is recoverable again — the sweep can re-drive it within cap.
    expect(db.listRedrivable(3).map((r) => r.event_id)).toContain('evt-1');
    expect(slots.inFlightCount()).toBe(0);
  });

  it('leaves the row spawned and fires nothing when the child exits cleanly', async () => {
    const spawn = launchingSpawn();
    const alert = recordingAlert();
    const slots = createRunSlots({ capacity: 1 });

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );
    spawn.exit(0);
    await settleExitWatcher();

    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    expect(alert.alerts).toHaveLength(0);
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['accepted']);
    expect(slots.inFlightCount()).toBe(0);
  });

  it('releases the slot only after the failed row is marked (the kicked sweep sees no stale row)', async () => {
    const spawn = launchingSpawn();
    const statesAtRelease: string[] = [];
    const slots = createRunSlots({
      capacity: 1,
      onRelease: () => statesAtRelease.push(db.getIngressEvent('evt-1')!.spawn_state),
    });

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: recordingAlert().seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );
    // No release at ack time — that would let a queued event start a second run.
    expect(statesAtRelease).toEqual([]);

    spawn.exit(1);
    await settleExitWatcher();

    expect(statesAtRelease).toEqual(['failed']);
  });

  it('treats an unobservable exit as a run failure instead of leaving the row spawned forever', async () => {
    const spawn = launchingSpawn();
    const alert = recordingAlert();
    const slots = createRunSlots({ capacity: 1 });

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );
    spawn.loseChild(new Error('lost track of the child process'));
    await settleExitWatcher();

    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'failed' });
    expect(alert.alerts[0]!.reason).toBe('lost track of the child process');
    expect(slots.inFlightCount()).toBe(0);
  });

  it('survives an alert seam that throws after the ack — the failure is still recorded', async () => {
    const spawn = launchingSpawn();
    const slots = createRunSlots({ capacity: 1 });
    const throwingAlert: AlertSeam = async () => {
      throw new Error('slack is down');
    };

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: throwingAlert, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );
    spawn.exit(3);
    await settleExitWatcher();

    // No caller is left to receive the throw, so the durable record is the log.
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'failed' });
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['accepted', 'spawn_failed']);
    expect(slots.inFlightCount()).toBe(0);
  });
});

// ===========================================================================
// Issue #7 — a PARKED run is not a failed run
// ===========================================================================

describe('exit watcher × a failure alert that never settles (#16 review)', () => {
  it('releases the run slot and writes spawn_failed without waiting for the transport', async () => {
    // The watcher releases the slot in a finally AFTER the alert, and writes the
    // durable ingress_log entry after it too. Awaiting a transport that never
    // SETTLES (not merely one that throws) therefore pinned the slot for the
    // life of the listener and lost the log row — at max_concurrent_runs 1,
    // one hung Slack post is a listener that never launches again, with nothing
    // recorded to say why. The two sibling watchers (recovery.ts's re-driven
    // exit, parked.ts's recordPark) already refuse to await; this is the third.
    const slots = createRunSlots({ capacity: 1 });
    const spawn = launchingSpawn();
    const neverSettles = async (): Promise<void> =>
      new Promise<void>(() => {
        /* never settles — a stalled alert transport */
      });

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: neverSettles, globalAlertChannel: '#l', redriveCap: 3, slots },
      baseInput(),
    );
    expect(slots.inFlightCount()).toBe(1);

    spawn.exit(1);
    await Promise.race([
      settleExitWatcher(),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('exit watcher hung on a pending failure alert')), 250),
      ),
    ]);

    // The durable record exists and the slot is free for the next launch.
    expect(db.getIngressEvent('evt-1')!.spawn_state).toBe('failed');
    expect(db.getIngressLog().map((e) => e.outcome)).toContain('spawn_failed');
    expect(slots.inFlightCount()).toBe(0);
  });
});

describe('exit watcher × parked run (issue #7)', () => {
  const NOW_MS = 1_700_000_000_000;
  const NOW_S = 1_700_000_000;

  /** Leave the run exactly as cmdRun leaves a parked halt: the row, and a ready card behind its gate. */
  function parkRun(runId: string, releaseAt: number): void {
    db.insertRun({ run_id: runId, flow: '/flows/github-sync.yaml', input_fingerprint: 'fp', status: 'halted', outcome: 'parked' });
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

  it('leaves a parked run spawned, logs parked (run id + ISO gate), and alerts informationally — never spawn_failed', async () => {
    const spawn = launchingSpawn();
    const alert = recordingAlert();
    const slots = createRunSlots({ capacity: 1 });
    const runId = deriveIngressRunId('evt-1');

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#l', redriveCap: 3, slots, now: () => NOW_MS },
      baseInput({ flow: makeFlow('#gh') }),
    );
    // The run parks and its process exits 1 — the documented "did not complete".
    parkRun(runId, NOW_S + 600);
    spawn.exit(1);
    await settleExitWatcher();

    // The incident: this row went 'failed', the alert said "exited with code 1",
    // and the boot sweep re-drove a run that only needed resuming.
    expect(db.getIngressEvent('evt-1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['accepted', 'parked']);
    const parked = db.getIngressLog({ outcome: 'parked' })[0]!;
    expect(parked.reason).toContain(runId);
    expect(parked.reason).toContain(new Date((NOW_S + 600) * 1000).toISOString());
    expect(alert.alerts).toHaveLength(1);
    expect(alert.alerts[0]!.reason).toMatch(/^parked/);
    expect(alert.alerts[0]).toMatchObject({ flowId: 'github-sync', channel: '#gh', eventId: 'evt-1' });
    expect(slots.inFlightCount()).toBe(0);
  });

  it('still fails a nonzero exit whose run is NOT parked (a row that only claims to be, with cards working)', async () => {
    const spawn = launchingSpawn();
    const alert = recordingAlert();
    const runId = deriveIngressRunId('evt-1');

    await runSpawnPath(
      { db, spawn: spawn.seam, alert: alert.seam, globalAlertChannel: '#l', redriveCap: 3, now: () => NOW_MS },
      baseInput(),
    );
    parkRun(runId, NOW_S + 600);
    db.getStateDb().prepare("UPDATE cards SET status = 'working' WHERE run_id = $r").run({ $r: runId });
    spawn.exit(1);
    await settleExitWatcher();

    expect(db.getIngressEvent('evt-1')!.spawn_state).toBe('failed');
    expect(db.getIngressLog().map((e) => e.outcome)).toEqual(['accepted', 'spawn_failed']);
    expect(alert.alerts[0]!.reason).toBe('conduit run exited with code 1');
  });
});
