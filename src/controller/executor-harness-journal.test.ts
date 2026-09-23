/**
 * Per-attempt harness journaling + usage-to-budget attribution (WI-567).
 *
 * Puts the most expensive calls in the flow ON THE BOOKS (FR-5, NFR-Op-2). Every
 * harness attempt — success OR the WI-566 named failures — writes a journal row
 * carrying adapter/harness identity, model id, measured wall-clock duration,
 * produced-artifact hashes, and reported usage; that usage folds into the same
 * card/wave/run accumulators the consumption andon reads. A harness that reports
 * no usage journals it as explicitly UNKNOWN (never a silent zero).
 *
 * Read seams (from db.ts): usage is read via getStationUsage (the OTel dict,
 * summed per (card,station,attempt)); the span identity/duration/unknown-sentinel
 * are new ADDITIVE journal columns exposed on StoredJournalSpan
 * (adapter/durationMs/usageUnknown), read via getJournalSpansForRun; produced
 * artifact hashes live in the span attributes. Budget attribution is observed
 * through the consumption andon: harness usage must reach `tokensSpent` (else the
 * andon never trips).
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { join } from 'node:path';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { openConduitDB, type ConduitDB, DEFAULT_RUN_ID } from '../persistence/db';
import { ensureCheckpointSchema } from '../checkpoint/checkpoint';
import { loadFlow } from '../flow/load';
import type { FlowConfig } from '../types/kernel';
import { runExecutor } from './executor';
import type { RunEngineArgs } from '../cli/main';
import type { ModelAdapter } from '../worker/adapter';
import {
  createHarnessRegistry,
  type HarnessAdapter,
  type HarnessInvocation,
  type HarnessRegistry,
  type UsageReport,
} from '../worker/harness-adapter';

const SECONDS = (n: number) => () => n;

function openDb(): ConduitDB {
  const db = openConduitDB({ stateDbPath: ':memory:', journalDbPath: ':memory:' });
  ensureCheckpointSchema(db.getStateDb());
  return db;
}

const throwingModel: ModelAdapter = {
  async call() {
    throw new Error('ModelAdapter.call must not be used by a harness maker');
  },
};

// ---------------------------------------------------------------------------
// Scriptable fake harness — controls output validity, per-call usage (or the
// explicit unknown signal), and can throw a WI-566-classified failure.
// ---------------------------------------------------------------------------

type Behavior = 'ok' | 'invalid' | 'throw-nonzero';

function makeHarness(
  opts: { name?: string; behavior?: Behavior; usage?: UsageReport; outputName?: string } = {},
): { adapter: HarnessAdapter; calls: HarnessInvocation[] } {
  const name = opts.name ?? 'fake-harness';
  const behavior = opts.behavior ?? 'ok';
  const usage: UsageReport = opts.usage ?? { tokens: 120, cost: 0.02 };
  const outputName = opts.outputName ?? 'result.json';
  const calls: HarnessInvocation[] = [];
  const adapter: HarnessAdapter = {
    name,
    reportsUsage: true,
    canRestrictTools: true,
    async probeBinary() {
      return { present: true };
    },
    async invoke(call: HarnessInvocation) {
      calls.push(call);
      if (behavior === 'throw-nonzero') {
        throw Object.assign(new Error('claude-headless: exited with code 1'), { code: 'harness-nonzero-exit' });
      }
      const abs = join(process.cwd(), outputName);
      // 'invalid' writes a schema-violating output (a PAID-but-failed attempt).
      writeFileSync(abs, JSON.stringify(behavior === 'invalid' ? { notSummary: 'x' } : { summary: 'ok' }), 'utf-8');
      return { outputs: [], usage };
    },
  };
  return { adapter, calls };
}

// ---------------------------------------------------------------------------
// issue #26 AC5 — a harness adapter that THROWS but still attaches usage to
// the error (a `BilledHarnessError`), mirroring what harness-adapter-claude.ts
// / harness-adapter-codex.ts now do on a recoverable classified failure
// ('harness-nonzero-exit', 'harness-rate-limited', 'harness-timeout' when
// recoverable). `usage: undefined` reproduces the pre-#26 shape (a throw that
// genuinely cannot recover a figure, e.g. a claude wall-clock timeout).
// ---------------------------------------------------------------------------

function makeHarnessThrowingWithUsage(
  usage: UsageReport | undefined,
  opts: { code?: string; name?: string } = {},
): { adapter: HarnessAdapter; calls: HarnessInvocation[] } {
  const calls: HarnessInvocation[] = [];
  const adapter: HarnessAdapter = {
    name: opts.name ?? 'fake-harness',
    reportsUsage: true,
    canRestrictTools: true,
    async probeBinary() {
      return { present: true };
    },
    async invoke(call: HarnessInvocation) {
      calls.push(call);
      throw Object.assign(
        new Error('claude-headless: exited with code 1'),
        { code: opts.code ?? 'harness-nonzero-exit', ...(usage !== undefined ? { usage } : {}) },
      );
    },
  };
  return { adapter, calls };
}

// ---------------------------------------------------------------------------
// Flow fixture — one `coder` harness station, configurable budgets.
// ---------------------------------------------------------------------------

function writeHarnessFlow(
  dir: string,
  registry: HarnessRegistry,
  opts: { maxAttempts?: number; runMaxTokens?: number } = {},
): FlowConfig {
  const maxAttempts = opts.maxAttempts ?? 2;
  const runMaxTokens = opts.runMaxTokens ?? 100000;
  mkdirSync(join(dir, 'prompts'), { recursive: true });
  writeFileSync(join(dir, 'prompts', 'coder.md'), 'TASK: {{task.json}}');
  writeFileSync(join(dir, 'task.json'), '{"task":"build the widget"}');

  const flowYaml = `
flow: harness-journal
project_root: .
flow_version: 1
budgets:
  run: { wall_clock_minutes: 10, max_tokens: ${runMaxTokens} }
  per_card: { max_execution_attempts: ${maxAttempts} }
  liveness: { no_progress_minutes: 3 }
terminal_lanes: [done, scrap, hold]
stations:
  - id: coder
    worker:
      kind: harness
      harness: fake-harness
      model: sonnet
      prompt_file: prompts/coder.md
      prompt_version: "1"
      tools: [Read, Write, Bash]
      output_schema:
        fields:
          - { name: summary, type: string, required: true }
    inputs: [task.json]
    outputs: [result.json]
    next: done
`;
  writeFileSync(join(dir, 'flow.yaml'), flowYaml);
  const loaded = loadFlow(join(dir, 'flow.yaml'), { harnessRegistry: registry });
  if (!loaded.ok) throw new Error(`fixture flow invalid: ${JSON.stringify(loaded.errors)}`);
  return loaded.flow;
}

function seedCard(db: ConduitDB, id: string): void {
  db.insertCard({
    run_id: DEFAULT_RUN_ID, id, parent_id: null, lane: 'coder', status: 'ready',
    attempt: 0, wave: 0, owned_paths: ['task.json', 'result.json'], rework_count: 0,
  });
}

const getCard = (db: ConduitDB, id = 'entry') => db.getCard(DEFAULT_RUN_ID, id);

/** The harness journal span(s) for a card (name carries the '.harness' suffix). */
function harnessSpans(db: ConduitDB, cardId = 'entry') {
  return db.getJournalSpansForRun(DEFAULT_RUN_ID, cardId).filter((s) => s.name.includes('harness'));
}

let originalCwd: string;
let dir: string;
let db: ConduitDB | null;

beforeEach(() => {
  originalCwd = process.cwd();
  dir = mkdtempSync(join(tmpdir(), 'conduit-harness-journal-'));
  process.chdir(dir);
  db = null;
});

afterEach(() => {
  if (db) { db.close(); db = null; }
  process.chdir(originalCwd);
  rmSync(dir, { recursive: true, force: true });
});

function makeIO(): { io: { out: (l: string) => void; err: (l: string) => void }; out: string[]; err: string[] } {
  const out: string[] = [];
  const err: string[] = [];
  return { io: { out: (l) => out.push(l), err: (l) => err.push(l) }, out, err };
}

async function run(flow: FlowConfig, registry: HarnessRegistry, io: { out: (l: string) => void; err: (l: string) => void }, at = 1000): Promise<void> {
  await runExecutor({ db: db!, flow, now: SECONDS(at), adapter: throwingModel, io, harnessRegistry: registry } as RunEngineArgs);
}

/**
 * Drive the executor on a VIRTUAL clock advanced by the injected sleep, for
 * the one test here that PARKS a card.
 *
 * The frozen-clock `run()` above is right for every other test in this file,
 * but a park makes the run loop sleep until the release gate opens, and a
 * frozen clock means that gate never arrives. Without this the park test does
 * not fail cleanly when the fold regresses: it HANGS to the 5s test timeout,
 * which is weaker evidence than an assertion and slow to diagnose. Same idiom
 * as executor-harness-rate-limit.test.ts's virtualClock.
 */
async function runVirtual(
  flow: FlowConfig,
  registry: HarnessRegistry,
  io: { out: (l: string) => void; err: (l: string) => void },
  startSeconds = 1000,
): Promise<void> {
  let clock = startSeconds;
  await runExecutor({
    db: db!,
    flow,
    now: () => clock,
    sleep: async (ms: number) => {
      clock += Math.max(1, Math.ceil(ms / 1000));
    },
    adapter: throwingModel,
    io,
    harnessRegistry: registry,
  } as unknown as RunEngineArgs);
}

// ---------------------------------------------------------------------------
// AC1 — a successful attempt journals identity, model, duration, artifact
// hashes, and usage.
// ---------------------------------------------------------------------------

describe('WI-567 AC1 — a successful harness attempt is fully journaled', () => {
  it('writes a journal row with adapter identity, model, duration, artifact hashes, and usage', async () => {
    db = openDb();
    const { adapter } = makeHarness({ name: 'fake-harness', usage: { tokens: 120, cost: 0.02 } });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry);
    seedCard(db, 'entry');
    const { io } = makeIO();

    await run(flow, registry, io);

    expect(getCard(db)?.lane).toBe('done');

    // Usage is journaled (read via the OTel usage seam), from the STRUCTURED
    // adapter result — tokens sum to the reported total, cost + model recorded.
    const usage = db.getStationUsage('entry', 'coder', 0);
    expect(usage).not.toBeNull();
    const u = usage as Record<string, number | string>;
    expect(Number(u['gen_ai.usage.input_tokens']) + Number(u['gen_ai.usage.output_tokens'])).toBe(120);
    expect(u['cost_usd']).toBe(0.02);
    expect(u['gen_ai.request.model']).toBe('sonnet');

    // The span carries the harness/adapter identity, a measured duration, and the
    // produced-artifact hashes (naming the declared output).
    const spans = harnessSpans(db);
    expect(spans).toHaveLength(1);
    const span = spans[0]!;
    expect(span.adapter).toBe('fake-harness');
    expect(typeof span.durationMs).toBe('number');
    expect(span.usageUnknown).toBe(false);
    expect(JSON.stringify(span.attributes)).toContain('result.json');
  });
});

// ---------------------------------------------------------------------------
// AC2 — a failed attempt is on the books too (a journal row per attempt with
// its failure state).
// ---------------------------------------------------------------------------

describe('WI-567 AC2 — failed harness attempts are journaled', () => {
  it('journals every failed (paid) attempt with its failure state before scrapping', async () => {
    db = openDb();
    // Schema-invalid output: the harness ran and reported usage, but the attempt
    // fails — a PAID failure that must still be on the books (WI-566 scraps it).
    const { adapter } = makeHarness({ behavior: 'invalid', usage: { tokens: 50, cost: 0.01 } });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry, { maxAttempts: 2 });
    seedCard(db, 'entry');
    const { io } = makeIO();

    await run(flow, registry, io);

    // WI-566: exhausted the cap → scrapped.
    expect(getCard(db)?.lane).toBe('scrap');

    // One journal span PER attempt (2), each naming the failure state; the paid
    // usage of each failed attempt is on the books.
    const spans = harnessSpans(db);
    expect(spans).toHaveLength(2);
    for (const span of spans) {
      expect(JSON.stringify(span.attributes)).toMatch(/harness-output-invalid/i);
    }
    expect(db.getStationUsage('entry', 'coder', 0)).not.toBeNull();
    expect(db.getStationUsage('entry', 'coder', 1)).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// AC3 — reported usage folds into the run budget accumulator the consumption
// andon reads.
// ---------------------------------------------------------------------------

describe('WI-567 AC3 — harness usage is attributed to the run budget', () => {
  it('folds harness usage into tokensSpent so the consumption andon trips', async () => {
    db = openDb();
    // Each harness attempt burns 100 tokens; the run budget is 50. Once card1's
    // usage folds into tokensSpent (100 >= 50) the andon trips and halts the run.
    // With the gateless-maker andon check (Finding 9, mirroring the transform
    // path's check-before-advance), the busting call halts BEFORE card1 itself
    // advances — so NEITHER card completes. If harness usage did NOT fold,
    // tokensSpent stays 0, the andon never trips, and BOTH cards complete.
    const { adapter } = makeHarness({ usage: { tokens: 100, cost: 0.1 } });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry, { runMaxTokens: 50 });
    seedCard(db, 'entry');
    seedCard(db, 'entry2');
    const { io, err } = makeIO();

    await run(flow, registry, io);

    const doneCount = [getCard(db, 'entry'), getCard(db, 'entry2')].filter((c) => c?.lane === 'done').length;
    // No card completed: the folded harness usage tripped the consumption andon
    // and halted the run before the budget-busting card advanced (the discriminator
    // from the no-fold case, where both cards would complete).
    expect(doneCount).toBe(0);
    const errText = err.join('\n');
    expect(errText).toMatch(/andon/i);
    expect(errText).toMatch(/token/i);
  });
});

// ---------------------------------------------------------------------------
// The original per-run usage-attribution work — harness usage spans are attributed to the OWNING run, not the
// global DEFAULT_RUN_ID sweep. Regression: before the fix, executeHarnessStation
// journaled its usage span without a runId, so getRunUsageTotals(realRun) was 0
// and the spend collapsed under 'default'.
// ---------------------------------------------------------------------------

describe('the original per-run usage-attribution work — harness usage attributed to the owning run', () => {
  it('records harness token/cost under the run that made the call, not DEFAULT_RUN_ID', async () => {
    db = openDb();
    const REAL_RUN = 'run-harness-attrib';
    const { adapter } = makeHarness({ name: 'fake-harness', usage: { tokens: 120, cost: 0.02 } });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry);
    // Seed + run under a NON-default run id (mirrors seedCard, but run-scoped).
    db.insertCard({
      run_id: REAL_RUN, id: 'entry', parent_id: null, lane: 'coder', status: 'ready',
      attempt: 0, wave: 0, owned_paths: ['task.json', 'result.json'], rework_count: 0,
    });
    const { io } = makeIO();

    await runExecutor({
      db, flow, now: SECONDS(1000), adapter: throwingModel, io,
      harnessRegistry: registry, runId: REAL_RUN,
    } as RunEngineArgs);

    expect(db.getCard(REAL_RUN, 'entry')?.lane).toBe('done');
    // The harness spend lands under the owning run...
    expect(db.getRunUsageTotals(REAL_RUN)).toEqual({ tokens: 120, costUsd: 0.02 });
    // ...and NOT under the global default sweep (the exact per-run usage-attribution work mis-attribution).
    expect(db.getRunUsageTotals(DEFAULT_RUN_ID)).toEqual({ tokens: 0, costUsd: 0 });
  });
});

// ---------------------------------------------------------------------------
// AC4 — a usage-blind attempt records usage as explicitly UNKNOWN, never zero,
// and is still bounded (completes).
// ---------------------------------------------------------------------------

describe('WI-567 AC4 — unknown usage is recorded explicitly, never a silent zero', () => {
  it('journals an explicit usage-unknown sentinel and does not fabricate a zero-usage row', async () => {
    db = openDb();
    const { adapter } = makeHarness({ usage: { unknown: true } });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry);
    seedCard(db, 'entry');
    const { io } = makeIO();

    await run(flow, registry, io);

    // The attempt is still bounded and completes normally.
    expect(getCard(db)?.lane).toBe('done');

    const spans = harnessSpans(db);
    expect(spans).toHaveLength(1);
    // Explicitly flagged unknown — NOT silently recorded as tokens=0.
    expect(spans[0]!.usageUnknown).toBe(true);
    // No fabricated zero-usage row: getStationUsage has no token/cost figures to sum.
    expect(db.getStationUsage('entry', 'coder', 0)).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// AC5 — the new journal columns are ADDITIVE: an existing-style span that omits
// them still writes and reads, and its new columns default to null (not masked).
// ---------------------------------------------------------------------------

describe('WI-567 AC5 — new journal columns are additive (existing rows survive)', () => {
  it('accepts a pre-harness-style usage span and leaves its harness columns null', () => {
    db = openDb();
    // A transform-style span carrying ONLY the existing usage fields — no adapter,
    // duration, or unknown sentinel. It must still insert and read back unchanged.
    db.appendJournalSpan({
      runId: DEFAULT_RUN_ID, cardId: 'legacy', station: 'brief', attempt: 0, name: 'brief.transform',
      usage: { model: 'gpt-4o-mini', inputTokens: 42, outputTokens: 7, costUsd: 0.0123 },
    });

    // Existing usage read is intact — the new columns did not mask it.
    expect(db.getStationUsage('legacy', 'brief', 0)).toEqual({
      'gen_ai.usage.input_tokens': 42,
      'gen_ai.usage.output_tokens': 7,
      // Issue #5's cache columns are ABSENT on this pre-#5 row, and the
      // aggregate reports 0 rather than null — the row is still readable
      // and its original numbers are unmasked, which is what AC5 asserts.
      'gen_ai.usage.cache_read_input_tokens': 0,
      'gen_ai.usage.cache_creation_input_tokens': 0,
      'gen_ai.request.model': 'gpt-4o-mini',
      cost_usd: 0.0123,
    });

    // The new additive columns default to null/absent for a non-harness span.
    const span = db.getJournalSpansForRun(DEFAULT_RUN_ID, 'legacy')[0]!;
    expect(span.adapter ?? null).toBeNull();
    expect(span.usageUnknown ?? false).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// issue #26 AC5 (maker path parity) — a thrown invocation was still BILLED for
// whatever it did before it died. Before this fix, executeHarnessStation's
// catch unconditionally journaled usageUnknown:true and folded nothing, even
// when the adapter attached a real figure to the throw (harness-adapter-claude
// / -codex now do this for a recoverable classified failure).
// ---------------------------------------------------------------------------

describe('issue #26 AC5 (maker path) — a thrown invocation that carried usage is billed, not written off', () => {
  it('folds the thrown usage into the run budget and journals the real figure instead of usageUnknown', async () => {
    db = openDb();
    const { adapter } = makeHarnessThrowingWithUsage({ tokens: 77, cost: 0.03 });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry, { maxAttempts: 1 });
    seedCard(db, 'entry');
    const { io } = makeIO();

    await run(flow, registry, io);

    // Cap exhausted at 1 attempt — scrapped, same as any other named failure.
    expect(getCard(db)?.lane).toBe('scrap');

    const spans = harnessSpans(db);
    expect(spans).toHaveLength(1);
    expect(spans[0]!.usageUnknown).toBe(false);

    const usage = db.getStationUsage('entry', 'coder', 0);
    expect(usage).not.toBeNull();
    const u = usage as Record<string, number | string>;
    expect(Number(u['gen_ai.usage.input_tokens']) + Number(u['gen_ai.usage.output_tokens'])).toBe(77);
    expect(u['cost_usd']).toBe(0.03);

    // NOTE: getRunUsageTotals aggregates the JOURNAL, so it is NOT a
    // discriminator for the fold — it reports the same figure whether or not
    // foldHarnessUsage ever ran. Kept as a journal assertion only; the
    // budget fold is proven by the andon test below, which is the one that
    // fails when the fold is removed.
    expect(db.getRunUsageTotals(DEFAULT_RUN_ID)).toEqual({ tokens: 77, costUsd: 0.03 });
  });

  it('the thrown usage reaches tokensSpent — a second card never dispatches because the andon tripped', async () => {
    db = openDb();
    // The real discriminator for the FOLD, mirroring the gateless-maker andon
    // test above. Each attempt throws after being billed 100 tokens against a
    // 50-token run budget, so card 'entry' alone busts it. If the thrown usage
    // folds, the andon trips and halts the run before 'entry2' is ever
    // dispatched; if it does NOT fold, tokensSpent stays 0, no andon fires,
    // and BOTH cards run to a scrap. The adapter's own call log is what
    // separates the two — it cannot be satisfied by a journal row.
    const { adapter, calls } = makeHarnessThrowingWithUsage({ tokens: 100, cost: 0.1 });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry, { maxAttempts: 1, runMaxTokens: 50 });
    seedCard(db, 'entry');
    seedCard(db, 'entry2');
    const { io, err } = makeIO();

    await run(flow, registry, io);

    const errText = err.join('\n');
    expect(errText).toMatch(/andon/i);
    expect(errText).toMatch(/token/i);
    // Exactly one invocation: the run halted before the second card's turn.
    expect(calls).toHaveLength(1);
    expect(getCard(db, 'entry2')?.lane).not.toBe('scrap');
  });

  it('a rate-limit park folds the usage the capped call was already billed for', async () => {
    db = openDb();
    // A provider cap is not a free call: the invocation can run for minutes
    // and be billed before the cap is reported, and the park deliberately
    // spends NO execution attempt — so without the fold that spend is
    // invisible to every budget AND the card keeps re-dispatching. Same
    // andon discriminator as the throw test above: 100 billed tokens against
    // a 50-token run budget, with a second card that must never get its turn.
    const { adapter, calls } = makeHarnessThrowingWithUsage(
      { tokens: 100, cost: 0.1 },
      { code: 'harness-rate-limited' },
    );
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry, { maxAttempts: 1, runMaxTokens: 50 });
    seedCard(db, 'entry');
    seedCard(db, 'entry2');
    const { io, err } = makeIO();

    // Virtual clock: a park sleeps toward its release gate, so a frozen clock
    // would hang instead of letting the regression surface as an assertion.
    await runVirtual(flow, registry, io);

    const errText = err.join('\n');
    expect(errText).toMatch(/andon/i);
    expect(errText).toMatch(/token/i);
    expect(calls).toHaveLength(1);
    // The park's journal row keeps its outcome attribute intact, so
    // countConsecutiveRateLimitParks still sees the streak it keys on.
    const spans = harnessSpans(db);
    expect(spans).toHaveLength(1);
    expect(spans[0]!.attributes.outcome).toBe('harness-rate-limited');
    expect(spans[0]!.usageUnknown).toBe(false);
  });

  it('regression: a thrown invocation with no usage attached still journals usageUnknown:true and folds nothing', async () => {
    db = openDb();
    // makeHarness's 'throw-nonzero' behavior attaches NO usage — the pre-#26
    // shape, and still the correct one for a throw that genuinely cannot
    // recover a figure (e.g. a claude wall-clock timeout).
    const { adapter } = makeHarness({ behavior: 'throw-nonzero' });
    const registry = createHarnessRegistry([adapter]);
    const flow = writeHarnessFlow(dir, registry, { maxAttempts: 1 });
    seedCard(db, 'entry');
    const { io } = makeIO();

    await run(flow, registry, io);

    expect(getCard(db)?.lane).toBe('scrap');
    const spans = harnessSpans(db);
    expect(spans).toHaveLength(1);
    expect(spans[0]!.usageUnknown).toBe(true);
    expect(db.getStationUsage('entry', 'coder', 0)).toBeNull();
    expect(db.getRunUsageTotals(DEFAULT_RUN_ID)).toEqual({ tokens: 0, costUsd: 0 });
  });
});
