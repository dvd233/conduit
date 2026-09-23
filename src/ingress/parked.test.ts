/**
 * Issue #7, pass two — the ingress listener resumes a parked run instead of
 * burying it.
 *
 * A parked run exits 1 (documented: did not complete). Both child-exit
 * watchers read that as a failure, and the boot sweep then "re-drove" it with
 * `conduit run --run-id <same>` — a no-op that prints the run state and exits
 * 0, so the run was never resumed. This module owns the two halves the
 * listener needs: recognising a park (and telling the channel ONCE), and
 * resuming what is due through the slot gate on every sweep.
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openConduitDB, type ConduitDB } from '../persistence/db';
import { createRunSlots } from './run-slots';
import type { SpawnFailedAlert } from './spawn';
import type { HitlResumeSpawn } from './adapters/slack-events';
import {
  inspectParkedRun,
  listDueParkedRuns,
  recordPark,
  resumeDueParkedRuns,
} from './parked';

/** Listener clock (milliseconds) and the run clock it maps to (seconds). */
const NOW_MS = 1_700_000_000_000;
const NOW_S = 1_700_000_000;

let dir: string;
let db: ConduitDB;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'conduit-parked-'));
  db = openConduitDB({ stateDbPath: join(dir, 'state.sqlite'), journalDbPath: join(dir, 'journal.sqlite') });
});

afterEach(() => {
  try {
    db.close();
  } catch {
    /* already closed */
  }
  rmSync(dir, { recursive: true, force: true });
});

// ── Seed helpers — the real persistence layer, composed the way the listener
// and the CLI leave a parked ingress run behind. ─────────────────────────────

/** An ingress event that launched (spawned) run `runId` of flow `flowPath`. */
function seedIngressRun(eventId: string, runId: string, flowPath = '/flows/a.yaml', flowId = 'flowA'): void {
  db.acceptIngressEvent(eventId, NOW_MS - 60_000, { flowId, flowPath, runId, substrateJson: '{}' });
  db.incrementSpawnAttempts(eventId);
  db.markIngressSpawned(eventId);
  db.insertRun({ run_id: runId, flow: flowPath, input_fingerprint: `fp-${runId}`, status: 'running' });
}

/** What cmdRun leaves when the run parks: the row, and a ready card behind its gate. */
function parkRun(runId: string, releaseAt: number, cardId = 'c1'): void {
  db.getStateDb()
    .prepare("UPDATE runs SET status = 'halted', outcome = 'parked' WHERE run_id = $r")
    .run({ $r: runId });
  db.insertCard({
    run_id: runId, id: cardId, parent_id: null, lane: 'narrate', status: 'ready',
    attempt: 0, wave: 0, owned_paths: [], rework_count: 0,
  });
  db.getStateDb()
    .prepare('UPDATE cards SET release_at = $at WHERE run_id = $r AND id = $c')
    .run({ $at: releaseAt, $r: runId, $c: cardId });
  // A real park also records WHY the card is gated: `release_at` alone cannot
  // distinguish a provider cap from the fan-out cache-warming stagger, which
  // stamps the same column.
  db.appendCardLog({
    runId,
    kind: 'entered_lane',
    cardId,
    station: 'narrate',
    attempt: 0,
    sourceLane: 'narrate',
    destLane: 'narrate',
    reasonClass: 'rate_limited',
  });
}

/** Drive `runId` to the completed terminal — cards at `done`, run row `complete`. */
function completeRun(runId: string): void {
  db.getStateDb().prepare("UPDATE cards SET lane = 'done', status = 'complete', release_at = NULL WHERE run_id = $r").run({ $r: runId });
  db.getStateDb().prepare("UPDATE runs SET status = 'done', outcome = 'complete' WHERE run_id = $r").run({ $r: runId });
}

/**
 * Drive `runId` to a genuinely halted terminal (scrapped cards, `outcome`
 * NOT 'parked') — the case that must never be mistaken for a park.
 */
function scrapRun(runId: string): void {
  db.getStateDb().prepare("UPDATE cards SET lane = 'scrap', status = 'scrapped', release_at = NULL WHERE run_id = $r").run({ $r: runId });
  db.getStateDb().prepare("UPDATE runs SET status = 'halted', outcome = 'halted' WHERE run_id = $r").run({ $r: runId });
}

/** An alert seam that records what the channel was told, in order. */
function recordingAlert(): { seam: (a: SpawnFailedAlert) => Promise<void>; alerts: SpawnFailedAlert[] } {
  const alerts: SpawnFailedAlert[] = [];
  return { alerts, seam: async (a) => { alerts.push(a); } };
}

/** A resume seam that records requests and lets the test decide when each child exits. */
function launchingResume(): {
  seam: HitlResumeSpawn;
  calls: Array<{ flowPath: string; runId: string }>;
  finish(runId: string, result?: { ok: boolean; error?: string }): void;
} {
  const calls: Array<{ flowPath: string; runId: string }> = [];
  const ends = new Map<string, (r: { ok: boolean; error?: string }) => void>();
  const seam: HitlResumeSpawn = (req) => {
    calls.push(req);
    return new Promise((resolve) => ends.set(req.runId, resolve));
  };
  return {
    seam,
    calls,
    finish: (runId, result = { ok: true }) => ends.get(runId)!(result),
  };
}

/** Yield to the microtask/timer queue so a detached supervisor can finish. */
const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

// ===========================================================================
// inspectParkedRun — the runs row must say parked AND the cards must agree
// ===========================================================================

describe('inspectParkedRun', () => {
  it('reports the gate when the row is halted/parked and the cards confirm it', () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S + 600);
    expect(inspectParkedRun(db, 'run-1', NOW_S)).toEqual({ releaseAt: NOW_S + 600 });
  });

  it('is null for a row that says parked while the cards disagree (a resume is driving it)', () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S + 600);
    db.getStateDb().prepare("UPDATE cards SET status = 'working' WHERE run_id = 'run-1'").run();
    expect(inspectParkedRun(db, 'run-1', NOW_S)).toBeNull();
  });

  it('is null for a plain halt, a complete run, and an unknown run', () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S + 600);
    scrapRun('run-1');
    expect(inspectParkedRun(db, 'run-1', NOW_S)).toBeNull();
    seedIngressRun('e2', 'run-2');
    parkRun('run-2', NOW_S + 600);
    completeRun('run-2');
    expect(inspectParkedRun(db, 'run-2', NOW_S)).toBeNull();
    expect(inspectParkedRun(db, 'run-none', NOW_S)).toBeNull();
  });
});

// ===========================================================================
// recordPark — log every park, alert ONCE per event
// ===========================================================================

describe('recordPark — the channel hears about a park once, the log every time', () => {
  const notice = (releaseAt: number) => ({
    source: 'webhook', eventId: 'e1', flowId: 'flowA', channel: '#a', runId: 'run-1', releaseAt,
  });

  it('logs a parked entry naming the run and the gate as ISO-8601 UTC, and alerts', async () => {
    seedIngressRun('e1', 'run-1');
    const alert = recordingAlert();

    await recordPark(db, alert.seam, notice(NOW_S + 600));

    const [entry] = db.getIngressLog({ outcome: 'parked' });
    expect(entry).toMatchObject({ source: 'webhook', eventId: 'e1', outcome: 'parked' });
    expect(entry!.reason).toContain('run-1');
    expect(entry!.reason).toContain(new Date((NOW_S + 600) * 1000).toISOString());
    expect(alert.alerts).toHaveLength(1);
    expect(alert.alerts[0]!.reason).toMatch(/^parked/);
    expect(alert.alerts[0]!.reason).toContain('run-1');
    expect(alert.alerts[0]!.reason).toContain(new Date((NOW_S + 600) * 1000).toISOString());
    expect(alert.alerts[0]).toMatchObject({ flowId: 'flowA', channel: '#a', eventId: 'e1' });
  });

  it('does NOT repeat the alert on a later re-park of the same event, but still logs it', async () => {
    // The MAX park cap chunks a long reset into several resume/park cycles;
    // the channel should not hear "parked" once per chunk.
    seedIngressRun('e1', 'run-1');
    const alert = recordingAlert();

    await recordPark(db, alert.seam, notice(NOW_S + 600));
    await recordPark(db, alert.seam, notice(NOW_S + 4200));

    expect(db.getIngressLog({ outcome: 'parked' })).toHaveLength(2);
    expect(alert.alerts).toHaveLength(1);
  });

  it('keys the once-rule per EVENT — another event still gets its own alert', async () => {
    seedIngressRun('e1', 'run-1');
    seedIngressRun('e2', 'run-2');
    const alert = recordingAlert();

    await recordPark(db, alert.seam, notice(NOW_S + 600));
    await recordPark(db, alert.seam, { ...notice(NOW_S + 600), eventId: 'e2', runId: 'run-2' });

    expect(alert.alerts.map((a) => a.eventId)).toEqual(['e1', 'e2']);
  });

  it('never marks the event failed, and never throws when the alert transport does', async () => {
    seedIngressRun('e1', 'run-1');
    await recordPark(db, async () => { throw new Error('transport down'); }, notice(NOW_S + 600));
    expect(db.getIngressEvent('e1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    expect(db.getIngressLog({ outcome: 'parked' })).toHaveLength(1);
  });
});

// ===========================================================================
// listDueParkedRuns — the pure "what is due" query
// ===========================================================================

describe('listDueParkedRuns', () => {
  it('lists a parked ingress run whose gate has passed, with its attribution', () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    expect(listDueParkedRuns(db, NOW_S)).toEqual([
      { runId: 'run-1', eventId: 'e1', flowId: 'flowA', flowPath: '/flows/a.yaml', releaseAt: NOW_S - 1 },
    ]);
  });

  it('is due exactly AT the gate, not before', () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S);
    expect(listDueParkedRuns(db, NOW_S).map((r) => r.runId)).toEqual(['run-1']);
    expect(listDueParkedRuns(db, NOW_S - 1)).toEqual([]);
  });

  it('skips a run that is not parked at all', () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    scrapRun('run-1');
    expect(listDueParkedRuns(db, NOW_S)).toEqual([]);
  });

  it('skips a parked run with no ingress attribution — a CLI run is not the listener\'s to resume', () => {
    db.insertRun({ run_id: 'cli-run', flow: '/flows/a.yaml', input_fingerprint: 'fp', status: 'running' });
    parkRun('cli-run', NOW_S - 1);
    expect(listDueParkedRuns(db, NOW_S)).toEqual([]);
  });

  it('skips a row that says parked while its cards disagree', () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    db.getStateDb().prepare("UPDATE cards SET status = 'working' WHERE run_id = 'run-1'").run();
    expect(listDueParkedRuns(db, NOW_S)).toEqual([]);
  });

  it("skips an event that is no longer 'spawned' — a failed row belongs to the attempt-capped re-drive sweep", () => {
    // Otherwise a resume that fails BEFORE driving the run (lease conflict,
    // preflight, missing flow file) leaves the run due forever: the row goes
    // 'failed', the gate stays in the past, and every tick would spawn another
    // `conduit resume` while consuming no attempts.
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    db.markIngressFailed('e1');
    expect(listDueParkedRuns(db, NOW_S)).toEqual([]);
  });

  it('orders by gate time, soonest first', () => {
    seedIngressRun('e-late', 'run-late');
    seedIngressRun('e-early', 'run-early');
    parkRun('run-late', NOW_S - 1);
    parkRun('run-early', NOW_S - 100);
    expect(listDueParkedRuns(db, NOW_S).map((r) => r.runId)).toEqual(['run-early', 'run-late']);
  });
});

// ===========================================================================
// resumeDueParkedRuns — the slot-gated driver
// ===========================================================================

describe('resumeDueParkedRuns', () => {
  function deps(over: Partial<Parameters<typeof resumeDueParkedRuns>[0]> = {}) {
    const alert = recordingAlert();
    const resume = launchingResume();
    return {
      alert,
      resume,
      deps: {
        db,
        slots: createRunSlots({ capacity: 2 }),
        resumeSpawn: resume.seam,
        alerts: { alert: alert.seam, channels: { flowA: '#a' }, globalAlertChannel: '#ops' },
        now: () => NOW_MS,
        ...over,
      },
    };
  }

  it('resumes each due run through the seam with its recorded flow path, holding a slot until it exits', async () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    const { deps: d, resume } = deps();

    const report = await resumeDueParkedRuns(d);

    expect(resume.calls).toEqual([{ flowPath: '/flows/a.yaml', runId: 'run-1' }]);
    expect(report.resumed).toEqual(['run-1']);
    // The resumed run is a real process against the model box: it owns a slot
    // for as long as it runs.
    expect(d.slots.inFlightCount()).toBe(1);
    // A park is not a launch failure — the attempt cap is untouched.
    expect(db.getIngressEvent('e1')!.spawn_attempts).toBe(1);
  });

  it('leaves a not-yet-due run alone', async () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S + 60);
    const { deps: d, resume } = deps();
    await resumeDueParkedRuns(d);
    expect(resume.calls).toEqual([]);
  });

  it('does NOT bypass a saturated slot gate — a rate limit is the wrong moment to stampede', async () => {
    seedIngressRun('e1', 'run-1');
    seedIngressRun('e2', 'run-2');
    parkRun('run-1', NOW_S - 10);
    parkRun('run-2', NOW_S - 5);
    const slots = createRunSlots({ capacity: 1 });
    const { deps: d, resume } = deps({ slots });

    const report = await resumeDueParkedRuns(d);

    expect(resume.calls.map((c) => c.runId)).toEqual(['run-1']);
    expect(report.deferred).toEqual(['run-2']);
    // No attempt spent on the deferral, nothing logged as failed.
    expect(db.getIngressLog({ outcome: 'spawn_failed' })).toHaveLength(0);

    // The slot frees when run-1 exits; the next sweep picks up run-2.
    completeRun('run-1');
    resume.finish('run-1');
    await settle();
    expect(slots.inFlightCount()).toBe(0);
    await resumeDueParkedRuns(d);
    expect(resume.calls.map((c) => c.runId)).toEqual(['run-1', 'run-2']);
  });

  it('skips a run whose EVENT already has a launch in flight — never two drivers for one run', async () => {
    // redriveOnBoot resolves on LAUNCH and marks the row 'spawned' before the
    // parked sweep runs, so a row it just re-drove is immediately a resume
    // candidate here. The re-drive sweep guards with the EVENT id while a
    // resume registers `parked-resume:<runId>`, so that guard cannot see this
    // one. Unguarded, the resume loses the run lease and exits 1 — and because
    // its gate is in the past (that is why it was due) the run reads as neither
    // complete nor parked, so a run that is succeeding collects a false
    // 'did not complete' alert and a burned re-drive attempt.
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 10);
    const slots = createRunSlots({ capacity: 4 });
    // Stand in for the re-driven child: its launch holds the EVENT id.
    expect(slots.tryAcquire('e1')).toBe('acquired');
    const { deps: d, resume } = deps({ slots });

    const report = await resumeDueParkedRuns(d);

    expect(resume.calls).toHaveLength(0);
    expect(report.resumed).toEqual([]);
    expect(report.deferred).toEqual(['run-1']);
    // Deferred, not failed: the row keeps its state and spends no attempt.
    expect(db.getIngressEvent('e1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    expect(db.getIngressLog({ outcome: 'spawn_failed' })).toHaveLength(0);

    // Once that child is done, the next sweep resumes normally.
    slots.release('e1');
    await resumeDueParkedRuns(d);
    expect(resume.calls.map((c) => c.runId)).toEqual(['run-1']);
  });

  it('suppresses a second resume of a run whose resume is already in flight', async () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    const { deps: d, resume } = deps();

    await resumeDueParkedRuns(d);
    // The row still reads parked until the child exits — a second sweep must
    // not start a second driver for the same run.
    const second = await resumeDueParkedRuns(d);

    expect(resume.calls).toHaveLength(1);
    expect(second.deferred).toEqual(['run-1']);
    expect(db.getIngressLog({ outcome: 'duplicate' }).map((e) => e.reason)).toEqual([
      expect.stringContaining('run-1'),
    ]);
  });

  it('after the resume: a run parked AGAIN is left for the next due sweep, logged, not re-alerted', async () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    const { deps: d, resume, alert } = deps();
    // The channel already heard about this park sequence.
    await recordPark(db, alert.seam, { source: 'webhook', eventId: 'e1', flowId: 'flowA', channel: '#a', runId: 'run-1', releaseAt: NOW_S - 1 });
    expect(alert.alerts).toHaveLength(1);

    await resumeDueParkedRuns(d);
    // The resumed process hit the cap again and exited 1, re-parked further out.
    db.getStateDb().prepare('UPDATE cards SET release_at = $at WHERE run_id = $r').run({ $at: NOW_S + 3600, $r: 'run-1' });
    resume.finish('run-1', { ok: false, error: 'conduit resume exited with code 1' });
    await settle();

    expect(db.getIngressEvent('e1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    expect(db.getIngressLog({ outcome: 'spawn_failed' })).toHaveLength(0);
    expect(db.getIngressLog({ outcome: 'parked' })).toHaveLength(2);
    expect(alert.alerts).toHaveLength(1);
    expect(d.slots.inFlightCount()).toBe(0);
    // Not due yet: the next sweep leaves it alone.
    await resumeDueParkedRuns(d);
    expect(resume.calls).toHaveLength(1);
  });

  it('after the resume: a run that neither completed nor parked is NOW a failure', async () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    const { deps: d, resume, alert } = deps();

    await resumeDueParkedRuns(d);
    scrapRun('run-1');
    resume.finish('run-1', { ok: false, error: 'conduit resume exited with code 1' });
    await settle();

    expect(db.getIngressEvent('e1')!.spawn_state).toBe('failed');
    const [failed] = db.getIngressLog({ outcome: 'spawn_failed' });
    expect(failed!.reason).toContain('conduit resume exited with code 1');
    expect(alert.alerts).toHaveLength(1);
    expect(alert.alerts[0]).toMatchObject({ flowId: 'flowA', channel: '#a', eventId: 'e1' });
    expect(alert.alerts[0]!.reason).toContain('conduit resume exited with code 1');
    expect(d.slots.inFlightCount()).toBe(0);
  });

  it('does not hold the run slot on a resume-failure alert that never settles', async () => {
    // Mirrors recovery.test.ts's "does not hold the run slot on a park alert
    // that never settles": the failure branch here awaited its alert until the
    // #16 review flagged it, and at capacity 1 a stalled transport would pin
    // `parked-resume:<runId>` for the life of the listener — no later parked
    // run could ever resume, and the durable 'spawn_failed' log entry (written
    // AFTER the alert in the old code) would never land either. A throwing
    // seam already passes today (the try/catch swallows it); only a seam that
    // never SETTLES proves the await itself is gone.
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    const slots = createRunSlots({ capacity: 1 });
    const neverSettling = { alert: () => new Promise<void>(() => {}), channels: {}, globalAlertChannel: '#ops' };
    const resume = launchingResume();
    const d = {
      db,
      slots,
      resumeSpawn: resume.seam,
      alerts: neverSettling,
      now: () => NOW_MS,
    };

    await resumeDueParkedRuns(d);
    // Neither complete nor re-parked: the failure branch.
    scrapRun('run-1');
    resume.finish('run-1', { ok: false, error: 'conduit resume exited with code 1' });

    await Promise.race([
      settle(),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('superviseResume hung on a pending resume-failure alert')), 250),
      ),
    ]);

    expect(db.getIngressEvent('e1')!.spawn_state).toBe('failed');
    const [failed] = db.getIngressLog({ outcome: 'spawn_failed' });
    expect(failed).toMatchObject({ eventId: 'e1', outcome: 'spawn_failed' });
    expect(slots.inFlightCount()).toBe(0);
  });

  it('after a resume that failed WITHOUT driving the run, the next sweep resumes nothing — no unbounded loop', async () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    const { deps: d, resume } = deps();

    await resumeDueParkedRuns(d);
    // A lease conflict / preflight failure: the child exits nonzero having
    // touched nothing — the run still reads parked with its gate in the past.
    resume.finish('run-1', { ok: false, error: 'conduit resume exited with code 1' });
    await settle();
    expect(db.getIngressEvent('e1')!.spawn_state).toBe('failed');

    const second = await resumeDueParkedRuns(d);

    expect(resume.calls).toHaveLength(1);
    expect(second).toEqual({ resumed: [], deferred: [] });
  });

  it('after the resume: a re-park is logged even when no alert seam is wired, like the exit watchers', async () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    const { deps: d, resume } = deps({ alerts: undefined });

    await resumeDueParkedRuns(d);
    db.getStateDb().prepare('UPDATE cards SET release_at = $at WHERE run_id = $r').run({ $at: NOW_S + 3600, $r: 'run-1' });
    resume.finish('run-1', { ok: false, error: 'conduit resume exited with code 1' });
    await settle();

    expect(db.getIngressLog({ outcome: 'parked' })).toHaveLength(1);
    expect(db.getIngressEvent('e1')!.spawn_state).toBe('spawned');
  });

  it('after the resume: a completed run is logged as accepted, the way a HITL resume is', async () => {
    seedIngressRun('e1', 'run-1');
    parkRun('run-1', NOW_S - 1);
    const { deps: d, resume, alert } = deps();

    await resumeDueParkedRuns(d);
    completeRun('run-1');
    resume.finish('run-1', { ok: true });
    await settle();

    expect(db.getIngressEvent('e1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    const accepted = db.getIngressLog({ outcome: 'accepted' });
    expect(accepted).toHaveLength(1);
    expect(accepted[0]!.reason).toContain('run-1');
    expect(alert.alerts).toHaveLength(0);
    expect(d.slots.inFlightCount()).toBe(0);
  });
});
