/**
 * Issue #3 — a provider rate limit parks the card; it does not scrap the run.
 *
 * THE DEFECT: a rate limit and a segfault both surfaced as
 * 'harness-nonzero-exit', and the retry loop had no delay. So a session cap with
 * a multi-hour reset burned a card's entire remaining attempt budget in about
 * three seconds, scrapped the card, halted the run, and discarded the paid work
 * from earlier attempts — recoverable only by a fresh run_id that could not
 * reuse the halted run's checkpoints.
 *
 * THE FIX, end to end: the adapter tags a 429 as 'harness-rate-limited', and the
 * executor parks the card behind cards.release_at (the gate the v10 fan-out
 * stagger already established) WITHOUT consuming an execution attempt. These
 * tests drive the REAL runExecutor, because the bug lived in what the executor
 * did with the adapter's throw — the FSM and the adapter were each fine on their
 * own.
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { join } from 'node:path';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { openConduitDB, type ConduitDB, DEFAULT_RUN_ID } from '../persistence/db';
import { ensureCheckpointSchema, readCheckpoint } from '../checkpoint/checkpoint';
import { loadFlow } from '../flow/load';
import type { FlowConfig } from '../types/kernel';
import { runExecutor, releaseAtForRateLimit } from './executor';
import type { RunEngineArgs } from '../cli/main';
import type { ModelAdapter } from '../worker/adapter';
import {
  createHarnessRegistry,
  type HarnessAdapter,
  type HarnessInvocation,
  type HarnessRegistry,
} from '../worker/harness-adapter';
import { createClaudeHarnessAdapter } from '../worker/harness-adapter-claude';
import type { HarnessSpawnResult } from '../worker/harness-runner';
import { getRunParkedRelease } from '../run/run-state';

// ---------------------------------------------------------------------------
// Shared executor-test recipe.
// ---------------------------------------------------------------------------

function openDb(): ConduitDB {
  const db = openConduitDB({ stateDbPath: ':memory:', journalDbPath: ':memory:' });
  ensureCheckpointSchema(db.getStateDb());
  return db;
}

const io = { out: (_l: string) => {}, err: (_l: string) => {} };

/** A ModelAdapter that must never be touched by a harness maker under no gate. */
const throwingModel: ModelAdapter = {
  async call() {
    throw new Error('ModelAdapter.call must not be used by a harness maker');
  },
};

// ---------------------------------------------------------------------------
// Scriptable fake harness adapter. Each invoke consumes the next behavior (the
// last one repeats), so a persistent failure retries to the cap and a
// fail-then-recover sequence advances.
// ---------------------------------------------------------------------------

type Behavior =
  | { kind: 'ok' }
  | { kind: 'missing' }
  | { kind: 'unparseable' }
  | { kind: 'invalid' }
  | { kind: 'throw'; code?: string; message: string };

/**
 * A harness adapter that replays `behaviors` in order, holding the last one
 * once the script runs out — so a test can say "rate-limit the first call, then
 * succeed" and let the executor drive as many calls as it needs. Every
 * invocation is recorded for assertions about what the station was asked to do.
 */
function makeScriptedHarness(
  behaviors: Behavior | Behavior[],
  opts: { name?: string; outputName?: string } = {},
): { adapter: HarnessAdapter; calls: HarnessInvocation[] } {
  const name = opts.name ?? 'fake-harness';
  const outputName = opts.outputName ?? 'result.json';
  const seq = Array.isArray(behaviors) ? behaviors : [behaviors];
  const calls: HarnessInvocation[] = [];
  let i = 0;
  const adapter: HarnessAdapter = {
    name,
    reportsUsage: true,
    canRestrictTools: true,
    async probeBinary() {
      return { present: true };
    },
    async invoke(call: HarnessInvocation) {
      calls.push(call);
      const b = seq[Math.min(i, seq.length - 1)]!;
      i++;
      // process.cwd() === the project root (each test chdirs into its temp dir and
      // the flow declares project_root: .), so this is where the executor collects
      // declared outputs: join(projectRoot, outputName).
      const abs = join(process.cwd(), outputName);
      switch (b.kind) {
        case 'throw':
          throw Object.assign(new Error(b.message), b.code ? { code: b.code } : {});
        case 'missing':
          break; // writes nothing — the missing declared-output case
        case 'unparseable':
          writeFileSync(abs, 'this is not json <<<', 'utf-8');
          break;
        case 'invalid':
          writeFileSync(abs, JSON.stringify({ notSummary: 'x' }), 'utf-8');
          break;
        case 'ok':
          writeFileSync(abs, JSON.stringify({ summary: 'ok' }), 'utf-8');
          break;
      }
      return { outputs: [], usage: { tokens: 10, cost: 0.01 } };
    },
  };
  return { adapter, calls };
}

// ---------------------------------------------------------------------------
// Flow fixture — one `coder` harness station, configurable attempt cap.
// ---------------------------------------------------------------------------

const CRITIC_MODEL = 'critic-model';

/**
 * Write a one-harness-station flow to `dir`, optionally `gated` (a check with a
 * back-edge, so a rate limit can land on a REWORK invocation — the case issue
 * #7 is about) and with the attempt/wall-clock budgets under test.
 */
function writeHarnessFlow(
  dir: string,
  registry: HarnessRegistry,
  opts: {
    maxAttempts?: number;
    wallClockMinutes?: number;
    harness?: string;
    gated?: boolean;
    effectful?: boolean;
  } = {},
): FlowConfig {
  const maxAttempts = opts.maxAttempts ?? 2;
  const wallClockMinutes = opts.wallClockMinutes ?? 10;
  const harness = opts.harness ?? 'fake-harness';
  mkdirSync(join(dir, 'prompts'), { recursive: true });
  // The loader rejects a {{feedback}} reference on a station nothing rejects
  // back to, so the feedback input exists only on the gated shape.
  writeFileSync(join(dir, 'prompts', 'coder.md'), opts.gated ? 'TASK: {{task.json}}\n{{feedback}}' : 'TASK: {{task.json}}');
  writeFileSync(join(dir, 'prompts', 'verify.md'), 'Check {{result.json}}');
  writeFileSync(join(dir, 'task.json'), '{"task":"build the widget"}');

  // Issue #7's shape needs a critic gate whose back-edge returns to the maker:
  // the cap landed on the REWORK invocation, after the station had already
  // succeeded once.
  const gate = opts.gated
    ? `
    check:
      kind: gate
      critic: { role: critic, model: ${CRITIC_MODEL}, prompt_file: prompts/verify.md, prompt_version: "1" }
      on_reject: coder
      rework_cap: 3`
    : '';

  const flowYaml = `
flow: harness-failures
project_root: .
flow_version: 1
budgets:
  run: { wall_clock_minutes: ${wallClockMinutes}, max_tokens: 100000 }
  per_card: { max_execution_attempts: ${maxAttempts} }
  liveness: { no_progress_minutes: 3 }
terminal_lanes: [done, scrap, hold]
stations:
  - id: coder
    effectful: ${opts.effectful ?? false}
    worker:
      kind: harness
      harness: ${harness}
      model: sonnet
      prompt_file: prompts/coder.md
      prompt_version: "1"
      tools: [Read, Write, Bash]
      output_schema:
        fields:
          - { name: summary, type: string, required: true }
    inputs: [task.json${opts.gated ? ', feedback' : ''}]
    outputs: [result.json]
    next: done${gate}
`;
  writeFileSync(join(dir, 'flow.yaml'), flowYaml);
  const loaded = loadFlow(join(dir, 'flow.yaml'), { harnessRegistry: registry });
  if (!loaded.ok) throw new Error(`fixture flow invalid: ${JSON.stringify(loaded.errors)}`);
  return loaded.flow;
}

function seedCoderCard(db: ConduitDB): void {
  db.insertCard({
    run_id: DEFAULT_RUN_ID,
    id: 'entry',
    parent_id: null,
    lane: 'coder',
    status: 'ready',
    attempt: 0,
    wave: 0,
    owned_paths: ['task.json', 'result.json'],
    rework_count: 0,
  });
}

const getCard = (db: ConduitDB) => db.getCard(DEFAULT_RUN_ID, 'entry');

/** getCard() does not select release_at, so read the gate column directly. */
function releaseAtOf(db: ConduitDB): number | null {
  const row = db
    .getStateDb()
    .prepare('SELECT release_at FROM cards WHERE run_id = $r AND id = $i')
    .get({ $r: DEFAULT_RUN_ID, $i: 'entry' }) as { release_at: number | null } | undefined;
  return row?.release_at ?? null;
}

const coderCheckpoint = (db: ConduitDB) =>
  readCheckpoint(db.getStateDb(), {
    run: DEFAULT_RUN_ID, flow: '1', card: 'entry', station: 'coder', attempt: 0,
  });

/** The scrap/hold reasons recorded on the card's terminal card_log entries. */
function terminalReasons(db: ConduitDB): string[] {
  return db
    .getCardLog('entry')
    .filter((e): e is Extract<typeof e, { kind: 'terminal' }> => e.kind === 'terminal')
    .map((e) => e.reason);
}

// ---------------------------------------------------------------------------
// Lifecycle — each test runs inside its own temp project dir (project_root: .).
// ---------------------------------------------------------------------------

let originalCwd: string;
let projectDir: string;
let db: ConduitDB | null;

beforeEach(() => {
  originalCwd = process.cwd();
  projectDir = mkdtempSync(join(tmpdir(), 'conduit-harness-fail-'));
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

/**
 * A VIRTUAL clock, advanced by the injected sleep — the idiom executor.ts's
 * release-gate wait documents ("a test injects a fast sleep so its advancing
 * clock re-ticks without burning real time").
 *
 * Without it these tests hang: parking a card is SUPPOSED to make the run loop
 * sleep until the gate opens, and with a frozen clock that gate never arrives.
 */
function virtualClock(startSeconds = 1000): {
  now: () => number;
  sleep: (ms: number) => Promise<void>;
  read: () => number;
  advance: (seconds: number) => void;
} {
  let clock = startSeconds;
  return {
    now: () => clock,
    sleep: async (ms: number) => {
      clock += Math.max(1, Math.ceil(ms / 1000));
    },
    read: () => clock,
    advance: (seconds: number) => {
      clock += seconds;
    },
  };
}

/** Drive the real executor over `flow` on a virtual clock. */
async function run(
  flow: FlowConfig,
  registry: HarnessRegistry,
  clock = virtualClock(),
  opts: { adapter?: ModelAdapter; io?: typeof io } = {},
): Promise<void> {
  await runExecutor({
    db: db!, flow, now: clock.now, sleep: clock.sleep,
    adapter: opts.adapter ?? throwingModel, io: opts.io ?? io, harnessRegistry: registry,
  } as unknown as RunEngineArgs);
}


// ---------------------------------------------------------------------------
// A rate limit is not a failed attempt.
// ---------------------------------------------------------------------------

/** What the claude adapter throws on a 429, including the reported reset. */
function rateLimited(resetAtMs?: number): Behavior {
  return {
    kind: 'throw',
    code: 'harness-rate-limited',
    message: "claude-headless: provider rate limit: You've hit your session limit",
    ...(resetAtMs !== undefined ? { resetAtMs } : {}),
  } as Behavior;
}

/**
 * A wall-clock budget small enough that the consumption andon trips at the
 * FIRST release-gate check. That halts the run while the card is still
 * parked, which is both what we need in order to observe the parked state and
 * a real guarantee worth pinning: a cap longer than the run's remaining
 * budget must halt the run, not idle it until the gate opens.
 */
const HALT_AT_FIRST_GATE = { maxAttempts: 5, wallClockMinutes: 0.001 };

describe('issue #3 — a 429 parks the card rather than scrapping it', () => {
  it('does NOT scrap: the card stays at its lane, ready to run again', async () => {
    db = openDb();
    const { adapter } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, HALT_AT_FIRST_GATE);
    seedCoderCard(db);

    await run(flow, registry);

    const card = getCard(db);
    // The destructive outcome this issue is about: scrap is terminal, so
    // `conduit resume` had nothing left to dispatch.
    expect(card?.lane).not.toBe('scrap');
    expect(card?.status).not.toBe('scrapped');
    expect(card?.lane).toBe('coder');
    expect(card?.status).toBe('ready');
  });

  it('never consumes an execution attempt, however often it is capped', async () => {
    db = openDb();
    const { adapter, calls } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, HALT_AT_FIRST_GATE);
    seedCoderCard(db);
    const before = getCard(db)?.attempt;

    await run(flow, registry);

    // The card was capped and STILL owes nothing: with max_execution_attempts
    // 5, the old code spent all five in about three seconds and scrapped. Here
    // the attempt counter never moves, so the card keeps every chance the flow
    // promised it.
    expect(calls.length).toBeGreaterThanOrEqual(1);
    expect(getCard(db)?.attempt).toBe(before!);
  });

  it('is bounded by the run BUDGET, not by burning the attempt cap', async () => {
    db = openDb();
    const { adapter, calls } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, HALT_AT_FIRST_GATE);
    seedCoderCard(db);

    await run(flow, registry);

    // The wall-clock andon is checked BEFORE the loop sleeps toward a gate, so
    // a cap the run cannot afford to wait out halts it — rather than either
    // idling forever or scrapping the card. Retries stay under the attempt cap
    // of 5 because each waits for the window instead of racing through it.
    expect(calls.length).toBeLessThan(5);
    expect(getCard(db)?.lane).toBe('coder');
  });

  it('spaces retries by the park interval instead of firing them in seconds', async () => {
    db = openDb();
    const clock = virtualClock(1000);
    const { adapter } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, HALT_AT_FIRST_GATE);
    seedCoderCard(db);

    await run(flow, registry, clock);

    // The issue's journal showed attempts 2, 3 and 4 one second apart. A park
    // moves the clock by the full window before anything is retried.
    expect(clock.read() - 1000).toBeGreaterThanOrEqual(300);
  });

  it('writes release_at in the INJECTED seconds frame, not a raw epoch', async () => {
    db = openDb();
    const { adapter } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, HALT_AT_FIRST_GATE);
    seedCoderCard(db);

    await run(flow, registry, virtualClock(1000));

    // Injected now starts at 1000 SECONDS. Writing the provider's absolute
    // epoch-MILLISECONDS reset here instead would be ~1.7e12 — about fifty
    // thousand years out — and the card would never be dispatched again.
    // (releaseAtForRateLimit's arithmetic is pinned exactly in its own tests.)
    const releaseAt = releaseAtOf(db) ?? 0;
    expect(releaseAt).toBeGreaterThan(1000);
    expect(releaseAt).toBeLessThan(100_000);
  });

  it('releases the worker slot, so siblings are not blocked at a wip:1 station', async () => {
    db = openDb();
    const { adapter } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, HALT_AT_FIRST_GATE);
    seedCoderCard(db);

    await run(flow, registry);

    const active = db
      .getStateDb()
      .prepare('SELECT COUNT(*) AS n FROM active_workers WHERE card_id = $id')
      .get({ $id: 'entry' }) as { n: number };
    expect(active.n).toBe(0);
  });

  it('journals the parked attempt as usage-unknown, NAMING the rate limit', async () => {
    db = openDb();
    const { adapter } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, HALT_AT_FIRST_GATE);
    seedCoderCard(db);

    await run(flow, registry);

    // Issue #5 ask (4): a usage_unknown row that says WHY. Before this, a cap
    // was only diagnosable from the $0.00 / 1s timing signature, because the
    // recorded reason was "exited with code 1:" with nothing after the colon.
    const spans = db.getJournalSpansForRun(DEFAULT_RUN_ID, 'entry');
    const parked = spans.find((s) => s.name === 'coder.harness');
    expect(parked?.usageUnknown).toBe(true);
    expect(JSON.stringify(parked?.attributes)).toMatch(/harness-rate-limited/);
  });

  it('RECOVERS: once the gate opens the card runs and completes', async () => {
    db = openDb();
    // Capped once, then the window resets and the same card succeeds — the
    // whole point of parking rather than scrapping.
    const { adapter, calls } = makeScriptedHarness([rateLimited(), { kind: 'ok' }]);
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, { maxAttempts: 5 });
    seedCoderCard(db);

    await run(flow, registry);

    expect(calls).toHaveLength(2);
    expect(getCard(db)?.lane).toBe('done');
  });

  it('CONTRAST: the same failure classed as a crash still scraps', async () => {
    // Proves the CLASSIFICATION is what saves the card, not some unrelated
    // slackening of the retry loop.
    db = openDb();
    const { adapter, calls } = makeScriptedHarness({
      kind: 'throw', code: 'harness-nonzero-exit', message: 'exited with code 1',
    });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, { maxAttempts: 2 });
    seedCoderCard(db);

    await run(flow, registry);

    expect(getCard(db)?.lane).toBe('scrap');
    expect(calls).toHaveLength(2);
  });
});

// ---------------------------------------------------------------------------
// Issue #7 — the cap lands on a REWORK invocation, through the REAL adapter.
// ---------------------------------------------------------------------------

/**
 * The claude-headless adapter driven by a scripted SPAWN, so the classification
 * under test is the adapter's own (`isRateLimited`), not a stubbed throw. Each
 * spawn consumes the next scripted result (the last one repeats); a success
 * result also writes the declared output, since the executor collects it from
 * disk.
 */
function makeScriptedClaudeSpawn(
  results: Array<Partial<HarnessSpawnResult> & { stdout: string }>,
  onSpawn: (n: number) => void = () => {},
): { adapter: HarnessAdapter; spawns: number } {
  const state = { spawns: 0 };
  const adapter = createClaudeHarnessAdapter({
    projectRoot: process.cwd(),
    envAllowlist: [],
    probe: async () => ({ present: true }),
    run: async () => {
      const scripted = results[Math.min(state.spawns, results.length - 1)]!;
      state.spawns++;
      onSpawn(state.spawns);
      if ((scripted.exitCode ?? 0) === 0) {
        writeFileSync(join(process.cwd(), 'result.json'), JSON.stringify({ summary: 'narrated' }), 'utf-8');
      }
      return { exitCode: 0, stderr: '', durationMs: 1, timedOut: false, ...scripted };
    },
  });
  return {
    adapter,
    get spawns() {
      return state.spawns;
    },
  };
}

/** The CLI's warning event at 0.99 of the five-hour window, reset `resetsAt` (epoch SECONDS). */
function warningEvent(resetsAt: number): string {
  return JSON.stringify({
    type: 'rate_limit_event',
    rate_limit_info: {
      status: 'allowed_warning', rateLimitType: 'five_hour', utilization: 0.99, resetsAt, isUsingOverage: false,
    },
  });
}

const SUCCESS_RESULT = JSON.stringify({
  type: 'result', subtype: 'success', is_error: false, result: 'done', total_cost_usd: 0.01,
  usage: { input_tokens: 10, output_tokens: 5 },
});

/** A critic that rejects its first verdict back to the maker, then passes. */
function makeRejectOnceCritic(): ModelAdapter {
  let rejected = false;
  return {
    async call(req) {
      if (req.model !== CRITIC_MODEL) {
        throw new Error(`unexpected model call for '${req.model}' — a harness maker must not call the model`);
      }
      const verdict = rejected
        ? { verdict: 'pass', findings: [] }
        : { verdict: 'reject', findings: ['tighten the second paragraph'], return_to: 'coder' };
      rejected = true;
      return { text: JSON.stringify(verdict), inputTokens: 5, outputTokens: 5, costUsd: 0.001 };
    },
  };
}

describe('issue #7 — a cap on the REWORK invocation parks the card (real adapter classification)', () => {
  /** Thirty minutes out in REAL time, so the park lands on the provider's reset, not the default. */
  const RESET_IN_SECONDS = 1800;

  it('parks at the same lane with no attempt consumed, instead of scrapping the run', async () => {
    db = openDb();
    const clock = virtualClock(1000);
    const resetsAt = Math.floor(Date.now() / 1000) + RESET_IN_SECONDS;
    const claude = makeScriptedClaudeSpawn(
      [
        // First invocation succeeds at 0.99 utilization (the journal's allowed_warning).
        { stdout: [warningEvent(resetsAt), SUCCESS_RESULT].join('\n') },
        // The rework invocation dies on the cap: the kept warning line is all stdout
        // holds, there is NO result event, and the cap is named only on stderr.
        { stdout: warningEvent(resetsAt), exitCode: 1, stderr: "You've hit your usage limit · resets 8pm (UTC)" },
      ],
      // Real invocations take time. Moving the clock past the tiny wall-clock
      // budget here makes the andon trip at the FIRST gate check, so the state
      // under inspection is the first park — not a re-park after sleeping to it.
      () => clock.advance(10),
    );
    const registry = createHarnessRegistry([claude.adapter]);
    const flow = writeHarnessFlow(projectDir, registry, {
      ...HALT_AT_FIRST_GATE, harness: 'claude-headless', gated: true,
    });
    seedCoderCard(db);

    await run(flow, registry, clock, { adapter: makeRejectOnceCritic() });

    const card = getCard(db);
    // The reported end state was lane=scrap, status=scrapped — the whole run
    // lost to a valid rework with no headroom.
    expect(card?.lane).toBe('coder');
    expect(card?.status).toBe('ready');
    // attempt=1 is the REWORK's own bump (advanceCard keys the next checkpoint
    // on it); the park added nothing on top, and the rework counter shows the
    // one legitimate reject.
    expect(card?.attempt).toBe(1);
    expect(card?.rework_count).toBe(1);
    expect(terminalReasons(db)).toEqual([]);
    // Parked, and on the provider's reset: 1800s ahead in the injected frame,
    // not the 300s default a missing reset would fall back to. The park samples
    // now() after the invoke, so its base lands inside the 10s the spawn advanced.
    const releaseAt = releaseAtOf(db) ?? 0;
    expect(releaseAt).toBeGreaterThanOrEqual(1000 + RESET_IN_SECONDS - 1);
    expect(releaseAt).toBeLessThanOrEqual(1010 + RESET_IN_SECONDS + 2);
    // One real run, one capped rework — never a retry loop burning the cap.
    expect(claude.spawns).toBe(2);
  });

  it('CONTRAST: the same rework crash with an unrelated stderr still scraps', async () => {
    db = openDb();
    const resetsAt = Math.floor(Date.now() / 1000) + RESET_IN_SECONDS;
    const claude = makeScriptedClaudeSpawn([
      { stdout: [warningEvent(resetsAt), SUCCESS_RESULT].join('\n') },
      { stdout: warningEvent(resetsAt), exitCode: 1, stderr: 'segfault' },
    ]);
    const registry = createHarnessRegistry([claude.adapter]);
    const flow = writeHarnessFlow(projectDir, registry, { maxAttempts: 2, harness: 'claude-headless', gated: true });
    seedCoderCard(db);

    await run(flow, registry, virtualClock(1000), { adapter: makeRejectOnceCritic() });

    expect(getCard(db)?.lane).toBe('scrap');
    expect(terminalReasons(db)).toContain('harness-nonzero-exit');
  });
});

// ---------------------------------------------------------------------------
// Issue #7 — a run that cannot afford to wait out the cap halts as PARKED.
// ---------------------------------------------------------------------------

describe('issue #7 — the andon halt during a rate-limit park is reported as parked', () => {
  it('names the gate on stderr as ISO-8601 UTC and leaves the card exactly as parked', async () => {
    db = openDb();
    const clock = virtualClock(1000);
    const { adapter } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, HALT_AT_FIRST_GATE);
    seedCoderCard(db);
    const errors: string[] = [];

    await run(flow, registry, clock, { io: { out: () => {}, err: (l) => errors.push(l) } });

    // The old line — "andon: run halted — wall_clock budget exceeded" — read as
    // a runaway. This run did no work at all: it was told to wait, could not
    // afford to, and stopped with every card intact.
    const releaseAt = releaseAtOf(db);
    expect(releaseAt).not.toBeNull();
    const andon = errors.find((l) => /^andon:/.test(l));
    expect(andon).toMatch(/parked behind a provider rate limit/);
    expect(andon).toContain(new Date(releaseAt! * 1000).toISOString());
    const card = getCard(db);
    expect(card?.lane).toBe('coder');
    expect(card?.status).toBe('ready');
    expect(card?.attempt).toBe(0);
    expect(releaseAt!).toBeGreaterThan(clock.read());
  });
});

// ---------------------------------------------------------------------------
// releaseAtForRateLimit — the units/clocks conversion, pinned exactly.
// ---------------------------------------------------------------------------

describe('issue #16 review — an endless cap escalates instead of parking forever', () => {
  it('holds the card once the consecutive parks reach the cap, sparing no attempt on the way', async () => {
    // A park spends no execution attempt, so NONE of the four rework guards
    // bounds it: guard #1 skips a 'rate_limited' entry by design, guard #2's
    // counter never moves, guard #3 has no findings to compare, and guard #4
    // halts the RUN — which the ingress listener now resumes unattended with a
    // fresh per-process budget. Without a bound on the parks themselves, a
    // station whose failure is misread as a cap parks, wakes, fails and parks
    // again forever, alerting once. So it escalates to `hold` instead.
    db = openDb();
    const { adapter, calls } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    // A budget generous enough that the andon does not halt the run first: the
    // bound under test is the park count, not the wall clock.
    const flow = writeHarnessFlow(projectDir, registry, { maxAttempts: 5, wallClockMinutes: 10_000 });
    seedCoderCard(db);
    const attemptBefore = getCard(db)?.attempt;

    await run(flow, registry, virtualClock(1000));

    const card = getCard(db);
    // Exactly the cap: eleven parks, and the twelfth cap holds instead.
    const parks = db
      .getJournalSpansForRun(DEFAULT_RUN_ID, 'entry')
      .filter((x) => x.attributes.outcome === 'harness-rate-limited');
    expect(parks.length).toBe(12);
    expect(card?.lane).toBe('hold');
    // Held, NOT scrapped: nothing the card produced is thrown away, and a
    // human decides what a cap that will not clear means.
    expect(card?.status).toBe('held');
    expect(card?.lane).not.toBe('scrap');
    // The escalation names the repetition rather than the last 429, so the
    // operator can tell "still capped" from "this is not really a cap".
    const held = db
      .getCardLogForRun(DEFAULT_RUN_ID, 'entry')
      .filter((e) => e.kind === 'terminal');
    expect(held.some((e) => e.kind === 'terminal' && /times in a row/.test(e.reason))).toBe(true);
    // Every one of those cycles was a real park: the attempt counter is
    // untouched, which is issue #3's guarantee and must survive this bound.
    expect(card?.attempt).toBe(attemptBefore!);
    expect(calls.length).toBeGreaterThan(1);
  });

  it('resets the streak when the station actually runs, so an intermittent cap never escalates', async () => {
    db = openDb();
    // Cap, cap, then succeed. The success writes its own <station>.harness span
    // and breaks the streak, so the card finishes instead of accumulating
    // toward the hold.
    const { adapter } = makeScriptedHarness([
      rateLimited(),
      rateLimited(),
      { kind: 'ok', output: { summary: 'built the widget' } } as Behavior,
    ]);
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, { maxAttempts: 5, wallClockMinutes: 10_000 });
    seedCoderCard(db);

    await run(flow, registry, virtualClock(1000));

    const card = getCard(db);
    expect(card?.lane).not.toBe('hold');
    expect(card?.lane).toBe('done');
  });
});

// ---------------------------------------------------------------------------
// An effectful harness station cannot park through a cap: the invoke IS the
// billed/irreversible side effect, so a pending outbox intent of unknown
// outcome must escalate rather than resume at the same idempotency key.
//
// A wall-clock budget of EXACTLY zero (not HALT_AT_FIRST_GATE's 0.001
// minutes) is deliberate here: the release-gate andon check runs on the
// injected clock BEFORE any sleep, so with any nonzero budget elapsed==0
// still clears it and the run loop takes one more internal tick — which, for
// an effectful station, is enough for reconcileOnResume to self-correct the
// dangling intent to 'hold' within the SAME process before ever halting.
// That masks the bug this test is for: a real deployment's budget is minutes
// or hours, so the run halts on the FIRST park, is reported 'parked', and
// only a SEPARATE resumed process ever reaches the reconcile check. Zero
// forces the halt at that same first park, so the test observes exactly the
// state a real halt-then-resume would leave behind.
// ---------------------------------------------------------------------------

const HALT_ON_FIRST_PARK = { maxAttempts: 5, wallClockMinutes: 0 };

describe('effectful harness station + rate limit — a pending intent cannot be parked through', () => {
  it('holds the card instead of parking, and the run does not read as resumable', async () => {
    db = openDb();
    const { adapter } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, { ...HALT_ON_FIRST_PARK, effectful: true });
    seedCoderCard(db);

    await run(flow, registry);

    const card = getCard(db);
    // Not parked: parking would resume the card at the SAME idempotency key
    // (flow.version:card:station:attempt, since a park spends no attempt),
    // and reconcileOnResume can only ever answer 'escalate_hold' for a
    // pending intent of unknown outcome — so parking here would just relabel
    // this same hold one dispatch later, while advertising the run as
    // resumable in between.
    expect(card?.lane).toBe('hold');
    expect(card?.status).toBe('held');
    expect(releaseAtOf(db)).toBeNull();

    const reasons = terminalReasons(db);
    expect(reasons.some((r) => /outbox intent/.test(r) && /manual reconciliation/.test(r))).toBe(true);

    // The run must not be reported 'parked' — that is what puts a run on the
    // ingress listener's unattended resume path, and resuming here would
    // dispatch straight back into the same escalate_hold.
    expect(getRunParkedRelease(db, DEFAULT_RUN_ID, 100_000)).toBeNull();
  });

  it('still journals the rate-limit span even though it escalates instead of parking', async () => {
    db = openDb();
    const { adapter } = makeScriptedHarness(rateLimited());
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(projectDir, registry, { ...HALT_ON_FIRST_PARK, effectful: true });
    seedCoderCard(db);

    await run(flow, registry);

    // The journal span is the record of what happened at the provider and is
    // written the same way regardless of what the executor does next.
    const spans = db.getJournalSpansForRun(DEFAULT_RUN_ID, 'entry');
    const parked = spans.find((s) => s.name === 'coder.harness');
    expect(parked?.usageUnknown).toBe(true);
    expect(JSON.stringify(parked?.attributes)).toMatch(/harness-rate-limited/);
  });
});

describe('releaseAtForRateLimit', () => {
  const REAL_NOW_MS = 1_700_000_000_000;

  it('adds the provider reset as a DURATION to the injected clock', () => {
    // 90s of real time from now → 90 injected seconds later. Never the raw
    // epoch: `release_at` is compared against the injected clock in SECONDS.
    expect(releaseAtForRateLimit(1000, REAL_NOW_MS + 90_000, REAL_NOW_MS)).toBe(1090);
  });

  it('rounds a partial second UP, so it never wakes just before the reset', () => {
    expect(releaseAtForRateLimit(1000, REAL_NOW_MS + 1_500, REAL_NOW_MS)).toBe(1002);
  });

  it('falls back to the default park when the provider reported no reset', () => {
    expect(releaseAtForRateLimit(1000, undefined, REAL_NOW_MS)).toBe(1300);
  });

  it('falls back rather than releasing instantly into the same cap', () => {
    // A reset already in the past means a stale or skewed reading, not "you may
    // retry now" — retrying immediately would just earn another 429.
    expect(releaseAtForRateLimit(1000, REAL_NOW_MS - 60_000, REAL_NOW_MS)).toBe(1300);
  });

  it('CAPS a single park, so a hours-away reset is not one long blocking sleep', () => {
    // A real session cap can reset many hours out, and the release-gate wait is
    // a blocking sleep. Capping at an hour re-checks the consumption andon and
    // refreshes the reset estimate instead of trusting one reading for a whole
    // afternoon; the card simply re-parks if it is still capped.
    const FIFTY_FOUR_HOURS_MS = 54 * 60 * 60 * 1000;
    expect(releaseAtForRateLimit(1000, REAL_NOW_MS + FIFTY_FOUR_HOURS_MS, REAL_NOW_MS)).toBe(1000 + 3600);
  });

  it('leaves a reset INSIDE the cap exactly as reported', () => {
    expect(releaseAtForRateLimit(1000, REAL_NOW_MS + 600_000, REAL_NOW_MS)).toBe(1600);
  });

  it('never returns an epoch-scale value from an epoch-scale input', () => {
    // The bug this guards: writing resetAtMs straight through parked cards
    // roughly fifty thousand years out, and they were never dispatched again.
    const out = releaseAtForRateLimit(1000, REAL_NOW_MS + 90_000, REAL_NOW_MS);
    expect(out).toBeLessThan(1_000_000);
  });
});
