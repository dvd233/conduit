/**
 * Concurrent transform fan-out + the cache-warming stagger (v10).
 *
 * Two new kernel behaviors, both gated so default (concurrency===1, no stagger)
 * runs are unaffected:
 *
 *   1. CONCURRENT TRANSFORM EXECUTION. Under `concurrency > 1`, transform siblings
 *      that are all ready in one tick run as OVERLAPPING in-process adapter calls
 *      (the fan-out reviewer case) instead of one-at-a-time. Proven by an adapter
 *      that records peak in-flight concurrency: >1 under K>1, exactly 1 at K=1.
 *
 *   2. RELEASE-GATE STAGGER. A fan-out station's `child_stagger_seconds` holds every
 *      child EXCEPT the first (lowest id) behind `cards.release_at = now + stagger`,
 *      so the first warms a shared prompt-prefix cache before the rest fire. Proven
 *      by inspecting the stamped release_at and that the gated siblings still run.
 *
 * These drive the REAL runExecutor (CLAUDE.md: point integration tests at the real
 * executor path); only the ModelAdapter is a stub. The release-gate wait uses an
 * injected fast `sleep` so a fake advancing clock re-ticks without real delay.
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { join } from 'node:path';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';

import type { ModelAdapter, ModelCall, ModelResponse } from '../worker/adapter';
import type { RunEngineArgs } from '../cli/main';
import type { Card, FlowConfig } from '../types/kernel';
import { openConduitDB, type ConduitDB, DEFAULT_RUN_ID } from '../persistence/db';
import { ensureCheckpointSchema } from '../checkpoint/checkpoint';
import { loadFlow } from '../flow/load';
import { runExecutor } from './executor';

const SECONDS = (n: number) => () => n;
/** A monotonically advancing injected clock so a release-gated run re-ticks to completion. */
function advancingClock(start: number): () => number {
  let t = start;
  return () => t++;
}
/** Fast sleep: the release-gate wait resolves immediately so tests don't burn real time. */
const FAST_SLEEP = async (): Promise<void> => {};

function makeIO(): { io: { out(l: string): void; err(l: string): void }; lines: string[] } {
  const lines: string[] = [];
  return { io: { out: (l) => lines.push(l), err: (l) => lines.push(l) }, lines };
}

function seedCard(db: ConduitDB, over: Partial<Card> & { id: string; lane: string }): void {
  db.insertCard({
    run_id: DEFAULT_RUN_ID,
    id: over.id,
    parent_id: over.parent_id ?? null,
    lane: over.lane,
    status: over.status ?? 'ready',
    attempt: over.attempt ?? 0,
    wave: over.wave ?? 0,
    owned_paths: over.owned_paths ?? [],
    rework_count: over.rework_count ?? 0,
  });
}

function makeOwnedDirWithSeed(parent: string, name: string): string {
  const dir = join(parent, name);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, 'seed.json'), JSON.stringify({ lens: name }), 'utf-8');
  return dir;
}

function openDb(): ConduitDB {
  const db = openConduitDB({ stateDbPath: ':memory:', journalDbPath: ':memory:' });
  ensureCheckpointSchema(db.getStateDb());
  return db;
}

// ---------------------------------------------------------------------------
// (1) Concurrent transform execution — direct-seeded siblings at a wip:3 station
// ---------------------------------------------------------------------------

/**
 * A single transform station with wip 3 — three siblings can run at once.
 * `stationExtra` injects extra station-level YAML (e.g. `effectful: true`, a
 * `check:` gate) so the exclusion-predicate tests can prove those classes stay
 * serial even at concurrency > 1.
 */
function setupReviewFlow(dir: string, stationExtra = ''): FlowConfig {
  mkdirSync(join(dir, 'prompts'), { recursive: true });
  writeFileSync(join(dir, 'prompts', 'review.md'), 'Review for: {{seed.json}}');
  writeFileSync(
    join(dir, 'flow.yaml'),
    `
flow: concurrent-review
project_root: .
flow_version: 1
budgets:
  run: { wall_clock_minutes: 10, max_tokens: 100000 }
  per_card: { max_execution_attempts: 4 }
  liveness: { no_progress_minutes: 3 }
defaults: { cap_policy: scrap, on_dep_scrap: hold }
terminal_lanes: [done, scrap, hold]
stations:
  - id: review
    worker:
      kind: transform
      model: gpt-4o-mini
      prompt_file: prompts/review.md
      prompt_version: "1"
      output_schema: { fields: [{ name: ok, type: string, required: true }] }
    inputs: [seed.json]
    outputs: [out.json]
    wip: 3
${stationExtra}    next: done
`,
  );
  const loaded = loadFlow(join(dir, 'flow.yaml'));
  if (!loaded.ok) throw new Error(`review fixture invalid: ${JSON.stringify(loaded.errors)}`);
  return loaded.flow;
}

/** Adapter that records PEAK simultaneous in-flight calls (overlap) via a delay window. */
function makeOverlapAdapter(): { adapter: ModelAdapter; calls: ModelCall[]; peak: () => number } {
  const calls: ModelCall[] = [];
  let inFlight = 0;
  let peak = 0;
  const adapter: ModelAdapter = {
    async call(req: ModelCall): Promise<ModelResponse> {
      calls.push(req);
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, 15)); // hold the slot open so siblings can overlap
      inFlight--;
      return { text: JSON.stringify({ ok: 'y' }), inputTokens: 5, outputTokens: 3, costUsd: 0.001 };
    },
  };
  return { adapter, calls, peak: () => peak };
}

let projectDir: string;
let originalCwd: string;
let db: ConduitDB | null;

beforeEach(() => {
  originalCwd = process.cwd();
  projectDir = mkdtempSync(join(tmpdir(), 'conduit-stagger-'));
  process.chdir(projectDir);
  db = null;
});

afterEach(() => {
  if (db) {
    db.close();
    db = null;
  }
  process.chdir(originalCwd);
  rmSync(projectDir, { recursive: true, force: true });
});

async function runReview(concurrency: number, stationExtra = ''): Promise<{ peak: number; calls: number }> {
  const flow = setupReviewFlow(projectDir, stationExtra);
  db = openDb();
  for (const id of ['c1', 'c2', 'c3']) {
    seedCard(db, { id, lane: 'review', owned_paths: [makeOwnedDirWithSeed(projectDir, id)] });
  }
  const { adapter, calls, peak } = makeOverlapAdapter();
  const { io } = makeIO();
  await runExecutor({ db, flow, now: SECONDS(1000), adapter, io, concurrency } as RunEngineArgs);
  for (const id of ['c1', 'c2', 'c3']) {
    expect(db.getCard(DEFAULT_RUN_ID, id)?.lane).toBe('done'); // every sibling completed
  }
  return { peak: peak(), calls: calls.length };
}

describe('concurrent transform fan-out (v10)', () => {
  it('runs transform siblings CONCURRENTLY under concurrency > 1 (peak overlap > 1)', async () => {
    const { peak, calls } = await runReview(3);
    expect(calls).toBe(3);
    expect(peak).toBeGreaterThan(1); // the load-bearing proof: their adapter calls overlapped
  });

  it('runs transform siblings SERIALLY at concurrency 1 (peak overlap == 1) — default unchanged', async () => {
    const { peak, calls } = await runReview(1);
    expect(calls).toBe(3);
    expect(peak).toBe(1); // the concurrent path is gated on concurrency>1
  });

  it('EXCLUDES an effectful transform from the concurrent batch — stays serial at concurrency 3', async () => {
    // The exclusion predicate (`!stationConfig.effectful`, plus the gated/deliver/
    // rank/fan-out guards beside it) is the safety boundary of this whole change: an
    // effectful transform must NOT enter the Promise.all batch, or its side effects
    // (outbox writes / idempotency keys) would overlap and break exactly-once. This
    // pins that guard — three effectful siblings at concurrency 3 run one-at-a-time.
    const { peak, calls } = await runReview(3, '    effectful: true\n');
    expect(calls).toBe(3); // all three ran — through the SERIAL in-process path
    expect(peak).toBe(1); // never overlapped: the effectful guard kept them out of the batch
  });
});

// ---------------------------------------------------------------------------
// (2) Release-gate stagger — a fan-out station with child_stagger_seconds
// ---------------------------------------------------------------------------

function setupStaggeredFanOutFlow(dir: string, staggerSeconds: number, wallClockMinutes = 10): FlowConfig {
  mkdirSync(join(dir, 'prompts'), { recursive: true });
  writeFileSync(join(dir, 'prompts', 'plan.md'), 'Propose the children.');
  writeFileSync(
    join(dir, 'flow.yaml'),
    `
flow: staggered-fanout
project_root: .
flow_version: 1
budgets:
  run: { wall_clock_minutes: ${wallClockMinutes}, max_tokens: 100000 }
  per_card: { max_execution_attempts: 4 }
  liveness: { no_progress_minutes: 3 }
defaults: { cap_policy: scrap, on_dep_scrap: hold }
terminal_lanes: [done, scrap, hold]
security:
  bash:
    allow: ["true"]
stations:
  - id: plan
    worker:
      kind: transform
      model: gpt-4o-mini
      prompt_file: prompts/plan.md
      prompt_version: "1"
      output_schema: { fields: [{ name: children, type: object, required: true }] }
    inputs: []
    outputs: [children.json]
    fan_out: 3
    child_entry: cwork
    child_terminal: done
    resume_at: merge
    child_stagger_seconds: ${staggerSeconds}
    next: merge
  - id: cwork
    worker: { kind: deterministic, command: "true" }
    wip: 3
    next: done
  - id: merge
    worker: { kind: deterministic, command: "true" }
    next: done
`,
  );
  const loaded = loadFlow(join(dir, 'flow.yaml'));
  if (!loaded.ok) throw new Error(`stagger fixture invalid: ${JSON.stringify(loaded.errors)}`);
  return loaded.flow;
}

function makeProposalAdapter(proposal: unknown): ModelAdapter {
  return {
    async call(): Promise<ModelResponse> {
      return { text: JSON.stringify(proposal), inputTokens: 10, outputTokens: 6, costUsd: 0.002 };
    },
  };
}

describe('fan-out cache-warming stagger — release_at stamping (v10)', () => {
  it('stamps every child EXCEPT the first behind a future release_at, and the gated siblings still run', async () => {
    const flow = setupStaggeredFanOutFlow(projectDir, 3);
    db = openDb();
    seedCard(db, { id: 'root', lane: 'plan', owned_paths: ['children.json'] });

    const proposal = {
      children: [
        { id: 'c1', depends_on: [] as string[], owned_paths: ['out/c1.json'] },
        { id: 'c2', depends_on: [] as string[], owned_paths: ['out/c2.json'] },
        { id: 'c3', depends_on: [] as string[], owned_paths: ['out/c3.json'] },
      ],
    };
    const { io } = makeIO();
    // The gate has to be observed WHILE it is live: dispatching a card clears
    // `release_at` (the gate it describes has just been passed), so by the time
    // the run finishes there is no stamp left to assert on. The executor sleeps
    // to the soonest gate, so the first sleep that finds children is the moment
    // the stagger is in force.
    let gated: Array<{ id: string; release_at: number | null }> = [];
    const tick = advancingClock(1000);
    const snapshotWhileGated = (): number => {
      // Sampled every tick; kept the FIRST time a sibling is actually holding a
      // gate, which is the only window in which the stamp exists.
      if (gated.length === 0) {
        const rows = db!
          .getStateDb()
          .prepare("SELECT id, release_at FROM cards WHERE parent_id = 'root' ORDER BY id")
          .all() as Array<{ id: string; release_at: number | null }>;
        if (rows.some((r) => r.release_at !== null)) gated = rows;
      }
      return tick();
    };
    // Advancing clock + fast sleep so the stagger elapses and the run completes.
    await runExecutor({
      db,
      flow,
      now: snapshotWhileGated,
      adapter: makeProposalAdapter(proposal),
      io,
      sleep: FAST_SLEEP,
    } as RunEngineArgs);

    expect(gated.map((r) => r.id)).toEqual(['c1', 'c2', 'c3']);
    // The first child (lowest id) is un-gated; it dispatches immediately to warm the cache.
    expect(gated.find((r) => r.id === 'c1')!.release_at).toBeNull();
    // Its siblings were held behind a future release gate.
    expect(gated.find((r) => r.id === 'c2')!.release_at).not.toBeNull();
    expect(gated.find((r) => r.id === 'c3')!.release_at).not.toBeNull();

    const rows = db
      .getStateDb()
      .prepare("SELECT id, release_at, lane FROM cards WHERE parent_id = 'root' ORDER BY id")
      .all() as Array<{ id: string; release_at: number | null; lane: string }>;

    expect(rows.map((r) => r.id)).toEqual(['c1', 'c2', 'c3']);
    // ...and the gate is genuinely NOT a deadlock: every child ran to terminal.
    for (const r of rows) expect(r.lane).toBe('done');
    // A spent gate does not outlive its dispatch: the claim clears release_at,
    // so no later reader of the column sees a stale past value (the ingress
    // parked sweep reads MIN(release_at) to decide when a run is due).
    for (const r of rows) expect(r.release_at).toBeNull();
  });

  it('does NOT stamp release_at when child_stagger_seconds is 0 (all children un-gated)', async () => {
    const flow = setupStaggeredFanOutFlow(projectDir, 0);
    db = openDb();
    seedCard(db, { id: 'root', lane: 'plan', owned_paths: ['children.json'] });
    const proposal = {
      children: [
        { id: 'c1', depends_on: [] as string[], owned_paths: ['out/c1.json'] },
        { id: 'c2', depends_on: [] as string[], owned_paths: ['out/c2.json'] },
      ],
    };
    const { io } = makeIO();
    await runExecutor({ db, flow, now: SECONDS(1000), adapter: makeProposalAdapter(proposal), io } as RunEngineArgs);

    const gated = db
      .getStateDb()
      .prepare("SELECT COUNT(*) AS n FROM cards WHERE parent_id = 'root' AND release_at IS NOT NULL")
      .get() as { n: number };
    expect(gated.n).toBe(0); // stagger 0 → no gate stamped anywhere
  });

  it('honors the consumption andon DURING a stagger wait instead of sleeping past the budget', async () => {
    // The wall-clock budget (1 min) is SHORTER than the stagger (100s): the gated
    // siblings would otherwise sleep to the gate at ~+100s. The andon must trip
    // mid-wait and halt. The advancing clock crosses the 60s budget during the wait
    // (before the 100s gate opens), so c2/c3 never run.
    const flow = setupStaggeredFanOutFlow(projectDir, 100, 1);
    db = openDb();
    seedCard(db, { id: 'root', lane: 'plan', owned_paths: ['children.json'] });
    const proposal = {
      children: [
        { id: 'c1', depends_on: [] as string[], owned_paths: ['out/c1.json'] },
        { id: 'c2', depends_on: [] as string[], owned_paths: ['out/c2.json'] },
        { id: 'c3', depends_on: [] as string[], owned_paths: ['out/c3.json'] },
      ],
    };
    const { io, lines } = makeIO();
    await runExecutor({
      db,
      flow,
      now: advancingClock(1000),
      adapter: makeProposalAdapter(proposal),
      io,
      sleep: FAST_SLEEP,
    } as RunEngineArgs);

    // The first (un-gated) child ran; the gated siblings did NOT — the andon halted
    // the run mid-wait rather than sleeping through the budget to the gate.
    expect(db.getCard(DEFAULT_RUN_ID, 'c1')?.lane).toBe('done');
    expect(db.getCard(DEFAULT_RUN_ID, 'c2')?.lane).not.toBe('done');
    expect(db.getCard(DEFAULT_RUN_ID, 'c3')?.lane).not.toBe('done');
    expect(lines.some((l) => /andon/i.test(l) && /budget exceeded/i.test(l))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// (3) output_scope: owned_dir — fan-out children write DISTINCT artifacts
// ---------------------------------------------------------------------------

/**
 * Fan-out plan (transform) → 2 `review` children with `output_scope: owned_dir`.
 * Without the scope, both children write the SAME `<projectRoot>/findings.json`
 * (the declared output name is static per station) and the second clobbers the
 * first. With it, each child writes `<owned_paths[0]>/findings.json`.
 */
function setupOwnedDirFanOutFlow(dir: string, opts: { seed?: boolean } = {}): FlowConfig {
  const useSeed = opts.seed !== false;
  mkdirSync(join(dir, 'prompts'), { recursive: true });
  writeFileSync(join(dir, 'prompts', 'plan.md'), 'Propose the children.');
  // The seed-less variant lets the fail-closed test reach the WRITE-time guard:
  // with {{seed.json}} in the template, a dir-less child dies earlier at render.
  writeFileSync(join(dir, 'prompts', 'review.md'), useSeed ? 'Review for: {{seed.json}}' : 'Review.');
  writeFileSync(
    join(dir, 'flow.yaml'),
    `
flow: owned-dir-outputs
project_root: .
flow_version: 1
budgets:
  run: { wall_clock_minutes: 10, max_tokens: 100000 }
  per_card: { max_execution_attempts: 4 }
  liveness: { no_progress_minutes: 3 }
defaults: { cap_policy: scrap, on_dep_scrap: hold }
terminal_lanes: [done, scrap, hold]
security:
  bash:
    allow: ["true"]
stations:
  - id: plan
    worker:
      kind: transform
      model: gpt-4o-mini
      prompt_file: prompts/plan.md
      prompt_version: "1"
      output_schema: { fields: [{ name: children, type: object, required: true }] }
    inputs: []
    outputs: [children.json]
    fan_out: 2
    child_entry: review
    child_terminal: done
    resume_at: merge
    next: merge
  - id: review
    worker:
      kind: transform
      model: gpt-4o-mini
      prompt_file: prompts/review.md
      prompt_version: "1"
      output_schema: { fields: [{ name: ok, type: string, required: true }] }
    inputs: [${useSeed ? 'seed.json' : ''}]
    outputs: [findings.json]
    output_scope: owned_dir
    wip: 2
    next: done
  - id: merge
    worker: { kind: deterministic, command: "true" }
    fan_in: { policy: all }
    next: done
`,
  );
  const loaded = loadFlow(join(dir, 'flow.yaml'));
  if (!loaded.ok) throw new Error(`owned-dir fixture invalid: ${JSON.stringify(loaded.errors)}`);
  return loaded.flow;
}

/**
 * Routing stub: the plan station gets the proposal; a review child gets an
 * answer that ECHOES its seed's lens, so each child's written artifact is
 * distinguishable on disk (proving no clobber, not just two writes).
 */
function makeRoutingAdapter(proposal: unknown): ModelAdapter {
  return {
    async call(req: ModelCall): Promise<ModelResponse> {
      if (req.prompt.includes('Propose')) {
        return { text: JSON.stringify(proposal), inputTokens: 10, outputTokens: 6, costUsd: 0.002 };
      }
      const lens = /"lens"\s*:\s*"([^"]+)"/.exec(req.prompt)?.[1] ?? 'unknown';
      return { text: JSON.stringify({ ok: lens }), inputTokens: 5, outputTokens: 3, costUsd: 0.001 };
    },
  };
}

describe('output_scope: owned_dir — per-child artifacts (v10)', () => {
  it('two fan-out children write DISTINCT findings.json into their own owned dirs (no clobber)', async () => {
    const flow = setupOwnedDirFanOutFlow(projectDir);
    db = openDb();
    seedCard(db, { id: 'root', lane: 'plan', owned_paths: ['children.json'] });

    // Pre-create each child's owned dir with its seed (commitFanOut writes the
    // seed itself in production; dirs must exist either way — same rule).
    const dirA = makeOwnedDirWithSeed(projectDir, 'rev-a');
    const dirB = makeOwnedDirWithSeed(projectDir, 'rev-b');
    const proposal = {
      children: [
        { id: 'rev-a', depends_on: [] as string[], owned_paths: [dirA], seed: { lens: 'rev-a' } },
        { id: 'rev-b', depends_on: [] as string[], owned_paths: [dirB], seed: { lens: 'rev-b' } },
      ],
    };
    const { io } = makeIO();
    await runExecutor({
      db,
      flow,
      now: SECONDS(1000),
      adapter: makeRoutingAdapter(proposal),
      io,
      concurrency: 2,
    } as RunEngineArgs);

    // Both children completed and each wrote ITS OWN artifact into its owned dir.
    expect(db.getCard(DEFAULT_RUN_ID, 'rev-a')?.lane).toBe('done');
    expect(db.getCard(DEFAULT_RUN_ID, 'rev-b')?.lane).toBe('done');
    const a = JSON.parse(readFileSync(join(dirA, 'findings.json'), 'utf-8')) as { ok: string };
    const b = JSON.parse(readFileSync(join(dirB, 'findings.json'), 'utf-8')) as { ok: string };
    expect(a.ok).toBe('rev-a'); // each artifact carries its own child's answer —
    expect(b.ok).toBe('rev-b'); // proof of no clobber, not merely two files
    // ...and nothing landed at the un-scoped legacy location.
    expect(existsSync(join(projectDir, 'findings.json'))).toBe(false);
  });

  it('fails CLOSED when a scoped child has no directory-shaped owned path', async () => {
    const flow = setupOwnedDirFanOutFlow(projectDir, { seed: false });
    db = openDb();
    seedCard(db, { id: 'root', lane: 'plan', owned_paths: ['children.json'] });
    // owned_paths[0] is a FILE path that does not exist as a directory.
    const proposal = {
      children: [{ id: 'c1', depends_on: [] as string[], owned_paths: [join(projectDir, 'out-c1.json')] }],
    };
    const { io } = makeIO();
    // The write-time guard throws a config violation (matching the escape-guard
    // convention) rather than silently writing to project root.
    await expect(
      runExecutor({ db, flow, now: SECONDS(1000), adapter: makeRoutingAdapter(proposal), io } as RunEngineArgs),
    ).rejects.toThrow(/output_scope: owned_dir/);
    expect(existsSync(join(projectDir, 'findings.json'))).toBe(false);
  });
});
