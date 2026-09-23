/**
 * Harness stations compose with check: gates in BOTH roles (WI-570, Phase 2).
 *
 * Role (a) — a harness MAKER behind a critic gate: composes with the SAME
 * runGateCheckOrAdvance machinery a transform/deterministic maker uses (WI-565
 * AC4 proved the pass case). This file adds the reject→rework→feedback→pass loop,
 * the four rework guards (per-card rework cap → scrap, no-progress findings-hash
 * monotonicity → scrap), and the pass-advances parity.
 *
 * Role (b) — a harness station as the CRITIC gate (the adversarial research gate,
 * the driving use case): a `check: { critic: { harness: <adapter> } }` critic
 * invokes the harness adapter and parses its structured output as the gate
 * verdict (verdict/findings), routing pass/reject through the identical gate path.
 * This is NEW surface — today the critic always resolves to a model call — so the
 * role (b) test is RED until the criticHarness config + gate-path wiring lands.
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
import type { ModelAdapter, ModelCall } from '../worker/adapter';
import {
  createHarnessRegistry,
  type HarnessAdapter,
  type HarnessInvocation,
  type HarnessRegistry,
  type UsageReport,
} from '../worker/harness-adapter';

const SECONDS = (n: number) => () => n;
const CRITIC_MODEL = 'critic-model';

function openDb(): ConduitDB {
  const db = openConduitDB({ stateDbPath: ':memory:', journalDbPath: ':memory:' });
  ensureCheckpointSchema(db.getStateDb());
  return db;
}

const io = { out: (_l: string) => {}, err: (_l: string) => {} };

// ---------------------------------------------------------------------------
// A harness MAKER that writes result.json each invoke and records its prompts.
// ---------------------------------------------------------------------------

function makeHarnessMaker(): { adapter: HarnessAdapter; calls: HarnessInvocation[] } {
  const calls: HarnessInvocation[] = [];
  const adapter: HarnessAdapter = {
    name: 'fake-harness',
    reportsUsage: true,
    canRestrictTools: true,
    async probeBinary() {
      return { present: true };
    },
    async invoke(call: HarnessInvocation) {
      calls.push(call);
      writeFileSync(join(process.cwd(), 'result.json'), JSON.stringify({ summary: 'implemented' }), 'utf-8');
      return { outputs: [], usage: { tokens: 10, cost: 0.01 } };
    },
  };
  return { adapter, calls };
}

// ---------------------------------------------------------------------------
// issue #26 — a harness maker that reports NO usage, so a test isolating a
// harness CRITIC's own spend does not have to net out the maker's.
// ---------------------------------------------------------------------------

function makeZeroUsageHarnessMaker(): { adapter: HarnessAdapter; calls: HarnessInvocation[] } {
  const calls: HarnessInvocation[] = [];
  const adapter: HarnessAdapter = {
    name: 'fake-harness',
    reportsUsage: true,
    canRestrictTools: true,
    async probeBinary() {
      return { present: true };
    },
    async invoke(call: HarnessInvocation) {
      calls.push(call);
      writeFileSync(join(process.cwd(), 'result.json'), JSON.stringify({ summary: 'implemented' }), 'utf-8');
      return { outputs: [], usage: { tokens: 0, cost: 0 } };
    },
  };
  return { adapter, calls };
}

// ---------------------------------------------------------------------------
// issue #26 — a configurable AGENTIC (harness) critic: fixed verdict, fixed or
// caller-reported usage, and an optional adapter-configured default model (so
// AC4's station-over-adapter precedence test can tell which one won). Each
// reject carries a DISTINCT finding by default so guard #3 (no-progress) never
// trips ahead of whatever guard a given test is isolating; pass `sameFindings`
// to invert that when a test specifically wants guard #3.
// ---------------------------------------------------------------------------

function makeConfigurableHarnessCritic(
  opts: {
    verdict?: 'pass' | 'reject';
    usage?: UsageReport;
    name?: string;
    adapterModel?: string;
    sameFindings?: boolean;
  } = {},
): { adapter: HarnessAdapter; calls: HarnessInvocation[] } {
  const verdict = opts.verdict ?? 'reject';
  const usage: UsageReport = opts.usage ?? { tokens: 8, cost: 0.008 };
  const calls: HarnessInvocation[] = [];
  let n = 0;
  const adapter: HarnessAdapter = {
    name: opts.name ?? 'claude-critic',
    reportsUsage: true,
    canRestrictTools: true,
    ...(opts.adapterModel !== undefined ? { model: opts.adapterModel } : {}),
    async probeBinary() {
      return { present: true };
    },
    async invoke(call: HarnessInvocation) {
      calls.push(call);
      n++;
      const findings = verdict === 'reject' ? [opts.sameFindings === true ? 'same-finding' : `finding-${n}`] : [];
      writeFileSync(join(process.cwd(), 'verdict.json'), JSON.stringify({ verdict, findings, return_to: 'coder' }), 'utf-8');
      return { outputs: [], usage };
    },
  };
  return { adapter, calls };
}

// ---------------------------------------------------------------------------
// A TRANSFORM critic: rejects the first `rejectsBeforePass` times then passes.
// `sameFindings` reuses one finding set so the no-progress guard (#3) trips.
// ---------------------------------------------------------------------------

function makeCritic(opts: { rejectsBeforePass?: number; sameFindings?: boolean } = {}): {
  adapter: ModelAdapter;
  calls: ModelCall[];
} {
  let rejectsLeft = opts.rejectsBeforePass ?? 0;
  let n = 0;
  const calls: ModelCall[] = [];
  const adapter: ModelAdapter = {
    async call(req: ModelCall) {
      calls.push(req);
      // Only the gate critic may reach the model — a harness maker never does.
      if (req.model !== CRITIC_MODEL) {
        throw new Error(`unexpected model call for '${req.model}' — a harness maker must not call the model`);
      }
      n++;
      if (rejectsLeft > 0) {
        rejectsLeft--;
        const findings = opts.sameFindings ? ['unchanged finding'] : [`finding-${n}`];
        return { text: JSON.stringify({ verdict: 'reject', findings, return_to: 'coder' }), inputTokens: 5, outputTokens: 5, costUsd: 0.001 };
      }
      return { text: JSON.stringify({ verdict: 'pass', findings: [] }), inputTokens: 5, outputTokens: 5, costUsd: 0.001 };
    },
  };
  return { adapter, calls };
}

// ---------------------------------------------------------------------------
// Flow fixture — a `coder` harness maker gated by a transform critic (role a),
// OR gated by a harness critic (role b, via `criticHarness`).
// ---------------------------------------------------------------------------

function writeGatedFlow(
  dir: string,
  registry: HarnessRegistry,
  opts: {
    reworkCap?: number;
    harnessCritic?: string;
    criticTools?: string[];
    /** issue #26 AC4: an explicit `check.critic.model` override for a harness critic. */
    criticModel?: string;
    /** issue #26 AC3: the run's token budget, so a test can size it below/above a critic's known spend. */
    runMaxTokens?: number;
  } = {},
): FlowConfig {
  const reworkCap = opts.reworkCap ?? 2;
  const runMaxTokens = opts.runMaxTokens ?? 100000;
  mkdirSync(join(dir, 'prompts'), { recursive: true });
  writeFileSync(join(dir, 'prompts', 'coder.md'), 'TASK: {{task.json}}\n[[FB]]{{feedback}}[[/FB]]\nEND');
  writeFileSync(join(dir, 'prompts', 'verify.md'), 'Check {{result.json}}');
  writeFileSync(join(dir, 'task.json'), '{"task":"build"}');

  // Role (b): the critic is a harness adapter; role (a): a transform model critic.
  // A declared critic `tools` allowlist (WI-595) is threaded into the harness
  // critic's invocation (gate.ts) — only meaningful on the harness branch.
  const criticToolsPart = opts.criticTools !== undefined ? `tools: [${opts.criticTools.join(', ')}], ` : '';
  const criticModelPart = opts.criticModel !== undefined ? `model: ${opts.criticModel}, ` : '';
  const criticBlock = opts.harnessCritic !== undefined
    ? `critic: { role: critic, harness: ${opts.harnessCritic}, ${criticModelPart}${criticToolsPart}prompt_file: prompts/verify.md, prompt_version: "1" }`
    : `critic: { role: critic, model: ${CRITIC_MODEL}, prompt_file: prompts/verify.md, prompt_version: "1" }`;

  const flowYaml = `
flow: harness-gate
project_root: .
flow_version: 1
budgets:
  run: { wall_clock_minutes: 10, max_tokens: ${runMaxTokens} }
  per_card: { max_execution_attempts: 4 }
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
    inputs: [task.json, feedback]
    outputs: [result.json]
    next: done
    check:
      kind: gate
      ${criticBlock}
      on_reject: coder
      rework_cap: ${reworkCap}
`;
  writeFileSync(join(dir, 'flow.yaml'), flowYaml);
  const loaded = loadFlow(join(dir, 'flow.yaml'), { harnessRegistry: registry });
  if (!loaded.ok) throw new Error(`fixture flow invalid: ${JSON.stringify(loaded.errors)}`);
  return loaded.flow;
}

function seedCard(db: ConduitDB): void {
  db.insertCard({
    run_id: DEFAULT_RUN_ID, id: 'entry', parent_id: null, lane: 'coder', status: 'ready',
    attempt: 0, wave: 0, owned_paths: ['task.json', 'result.json'], rework_count: 0,
  });
}

const getCard = (db: ConduitDB) => db.getCard(DEFAULT_RUN_ID, 'entry');

function gateVerdicts(db: ConduitDB) {
  return db.getCardLog('entry').filter((e): e is Extract<typeof e, { kind: 'gate_verdict' }> => e.kind === 'gate_verdict');
}

function terminalReasons(db: ConduitDB): string[] {
  return db.getCardLog('entry')
    .filter((e): e is Extract<typeof e, { kind: 'terminal' }> => e.kind === 'terminal')
    .map((e) => e.reason);
}

let originalCwd: string;
let dir: string;
let db: ConduitDB | null;

beforeEach(() => {
  originalCwd = process.cwd();
  dir = mkdtempSync(join(tmpdir(), 'conduit-harness-gate-'));
  process.chdir(dir);
  db = null;
});

afterEach(() => {
  if (db) { db.close(); db = null; }
  process.chdir(originalCwd);
  rmSync(dir, { recursive: true, force: true });
});

async function run(
  flow: FlowConfig,
  registry: HarnessRegistry,
  adapter: ModelAdapter,
  ioOverride: { out: (l: string) => void; err: (l: string) => void } = io,
): Promise<void> {
  await runExecutor({ db: db!, flow, now: SECONDS(1000), adapter, io: ioOverride, harnessRegistry: registry } as RunEngineArgs);
}

/** Captures stderr lines so a test can assert on (or rule out) the andon message. */
function makeIO(): { io: { out: (l: string) => void; err: (l: string) => void }; out: string[]; err: string[] } {
  const out: string[] = [];
  const err: string[] = [];
  return { io: { out: (l) => out.push(l), err: (l) => err.push(l) }, out, err };
}

// ---------------------------------------------------------------------------
// Role (a) AC1 — harness maker behind a rejecting critic gate reworks with the
// critic's {{feedback}} threaded, journaling a gate_verdict reject row.
// ---------------------------------------------------------------------------

describe('WI-570 role (a) — harness maker under a critic gate that rejects (AC1)', () => {
  it('journals a gate_verdict reject, reworks with feedback threaded, then advances on pass', async () => {
    db = openDb();
    const maker = makeHarnessMaker();
    const registry = createHarnessRegistry([maker.adapter]);
    const flow = writeGatedFlow(dir, registry);
    seedCard(db);
    const critic = makeCritic({ rejectsBeforePass: 1 });

    await run(flow, registry, critic.adapter);

    // A gate_verdict REJECT row was journaled with the critic's findings.
    const reject = gateVerdicts(db).find((v) => v.verdict === 'reject');
    expect(reject).toBeDefined();
    expect(reject!.findings).toEqual(['finding-1']);
    expect(reject!.returnTo).toBe('coder');
    // The card reworked (back-edge to the harness maker), then the pass advanced it.
    expect(getCard(db)?.rework_count ?? 0).toBeGreaterThanOrEqual(1);
    expect(getCard(db)?.lane).toBe('done');
    // The critic's finding was threaded into the harness maker's SECOND invocation.
    expect(maker.calls.length).toBeGreaterThanOrEqual(2);
    const fb = maker.calls[1]!.prompt.match(/\[\[FB\]\]([\s\S]*?)\[\[\/FB\]\]/);
    expect(fb).not.toBeNull();
    expect(fb![1]).toContain('finding-1');
  });
});

// ---------------------------------------------------------------------------
// Role (a) AC4 — a harness maker whose gate passes advances exactly like a gated
// transform.
// ---------------------------------------------------------------------------

describe('WI-570 role (a) — a passing gate advances the harness maker (AC4)', () => {
  it('advances to done on a first-attempt pass with no rework', async () => {
    db = openDb();
    const maker = makeHarnessMaker();
    const registry = createHarnessRegistry([maker.adapter]);
    const flow = writeGatedFlow(dir, registry);
    seedCard(db);
    const critic = makeCritic({ rejectsBeforePass: 0 });

    await run(flow, registry, critic.adapter);

    expect(getCard(db)?.lane).toBe('done');
    expect(getCard(db)?.rework_count ?? 0).toBe(0);
    expect(gateVerdicts(db).some((v) => v.verdict === 'pass')).toBe(true);
    expect(maker.calls).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Role (a) AC3 — all four rework guards remain intact on a harness-gated loop.
// ---------------------------------------------------------------------------

describe('WI-570 role (a) — the four rework guards hold on a harness-gated loop (AC3)', () => {
  it('Guard#1: a persistent reject with changing findings scraps at the per-card rework cap', async () => {
    db = openDb();
    const maker = makeHarnessMaker();
    const registry = createHarnessRegistry([maker.adapter]);
    const flow = writeGatedFlow(dir, registry, { reworkCap: 2 });
    seedCard(db);
    // Always rejects with a DISTINCT finding each time so guard #3 never trips first,
    // isolating the per-card rework cap (guard #1).
    const critic = makeCritic({ rejectsBeforePass: 99 });

    await run(flow, registry, critic.adapter);

    expect(getCard(db)?.lane).toBe('scrap');
    expect(terminalReasons(db)).toContain('rework_cap');
  });

  it('Guard#3: a reject repeating the SAME findings scraps for no-progress', async () => {
    db = openDb();
    const maker = makeHarnessMaker();
    const registry = createHarnessRegistry([maker.adapter]);
    const flow = writeGatedFlow(dir, registry, { reworkCap: 5 });
    seedCard(db);
    // Same findings every reject → the findings-hash monotonicity guard trips
    // before the rework cap.
    const critic = makeCritic({ rejectsBeforePass: 99, sameFindings: true });

    await run(flow, registry, critic.adapter);

    expect(getCard(db)?.lane).toBe('scrap');
    expect(terminalReasons(db)).toContain('no_progress');
  });
});

// ---------------------------------------------------------------------------
// Role (b) AC2 — a harness station AS the critic gate (agentic critic). NEW
// surface: the critic invokes a harness adapter and parses its structured output
// as the gate verdict. RED until the criticHarness config + gate-path wiring land.
// ---------------------------------------------------------------------------

/** A harness CRITIC: writes its verdict (verdict/findings) as its structured output. */
function makeHarnessCritic(verdict: 'pass' | 'reject', findings: string[]): {
  adapter: HarnessAdapter;
  calls: HarnessInvocation[];
} {
  const calls: HarnessInvocation[] = [];
  const adapter: HarnessAdapter = {
    name: 'claude-critic',
    reportsUsage: true,
    canRestrictTools: true,
    async probeBinary() {
      return { present: true };
    },
    async invoke(call: HarnessInvocation) {
      calls.push(call);
      // The agentic critic emits its verdict as the structured gate-critic output.
      writeFileSync(join(process.cwd(), 'verdict.json'), JSON.stringify({ verdict, findings, return_to: 'coder' }), 'utf-8');
      return { outputs: [], usage: { tokens: 8, cost: 0.008 } };
    },
  };
  return { adapter, calls };
}

/** A harness CRITIC that resolves successfully but writes NO verdict.json
 *  (a bug / wrong cwd / forgotten Write) — used for the stale-verdict test. */
function makeNonWritingHarnessCritic(): { adapter: HarnessAdapter; calls: HarnessInvocation[] } {
  const calls: HarnessInvocation[] = [];
  const adapter: HarnessAdapter = {
    name: 'claude-critic',
    reportsUsage: true,
    canRestrictTools: true,
    async probeBinary() {
      return { present: true };
    },
    async invoke(call: HarnessInvocation) {
      calls.push(call);
      // Deliberately writes NO verdict.json this attempt.
      return { outputs: [], usage: { tokens: 8, cost: 0.008 } };
    },
  };
  return { adapter, calls };
}

describe('WI-570 role (b) — a harness station acts as the critic gate (AC2)', () => {
  it('invokes the harness critic, emits a reject verdict, and reworks through the gate path', async () => {
    db = openDb();
    const maker = makeHarnessMaker();
    // The critic is a HARNESS adapter (adversarial research gate); it rejects once.
    const critic = makeHarnessCritic('reject', ['agentic critic finding']);
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    // A harness critic MUST declare a non-empty tools allowlist to load
    // (HARNESS_CRITIC_TOOLS_REQUIRED); this test exercises the runtime gate
    // path, so it declares tools rather than asserting the load error.
    const flow = writeGatedFlow(dir, registry, { harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'] });
    seedCard(db);

    // No model critic — the gate critic is the harness. A ModelAdapter that throws
    // proves the verdict came from the harness critic, not a model call.
    await run(flow, registry, { async call() { throw new Error('gate critic must be the harness, not the model'); } });

    // The harness critic ran and its verdict routed through the SAME gate path.
    expect(critic.calls.length).toBeGreaterThanOrEqual(1);
    const reject = gateVerdicts(db).find((v) => v.verdict === 'reject');
    expect(reject).toBeDefined();
    expect(reject!.findings).toEqual(['agentic critic finding']);
    expect(getCard(db)?.rework_count ?? 0).toBeGreaterThanOrEqual(1);
  });

  it('advances the maker when the harness critic emits a pass verdict', async () => {
    db = openDb();
    const maker = makeHarnessMaker();
    const critic = makeHarnessCritic('pass', []);
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    // Non-empty criticTools is mandatory for a harness critic to load (WI-595 /
    // HARNESS_CRITIC_TOOLS_REQUIRED); this test exercises the pass path.
    const flow = writeGatedFlow(dir, registry, { harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'] });
    seedCard(db);

    await run(flow, registry, { async call() { throw new Error('gate critic must be the harness, not the model'); } });

    expect(critic.calls.length).toBeGreaterThanOrEqual(1);
    expect(gateVerdicts(db).some((v) => v.verdict === 'pass')).toBe(true);
    expect(getCard(db)?.lane).toBe('done');
  });

  it('threads the declared gate-critic tools allowlist into the harness critic invocation (WI-595)', async () => {
    db = openDb();
    const maker = makeHarnessMaker();
    const critic = makeHarnessCritic('pass', []);
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    // The critic declares a tools allowlist; claude-critic canRestrictTools=true,
    // so it loads, and gate.ts must thread that allowlist into the invoke (the
    // WI-595 fix — previously hardcoded []). Without threading, the real claude
    // critic gets NO --allowed-tools and cannot write its verdict file.
    const flow = writeGatedFlow(dir, registry, { harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'] });
    seedCard(db);

    await run(flow, registry, { async call() { throw new Error('gate critic must be the harness, not the model'); } });

    expect(critic.calls.length).toBeGreaterThanOrEqual(1);
    expect(critic.calls[0]!.tools).toEqual(['Read', 'Write']);
  });

  it('does NOT leak a pre-existing stale verdict.json — a critic that fails to (re)write scraps harness-critic-verdict-missing', async () => {
    db = openDb();
    const maker = makeHarnessMaker();
    // The critic resolves successfully but never writes a verdict this attempt.
    const critic = makeNonWritingHarnessCritic();
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    // Non-empty criticTools is mandatory for a harness critic to load (WI-595 /
    // HARNESS_CRITIC_TOOLS_REQUIRED); this test exercises the stale-verdict path.
    const flow = writeGatedFlow(dir, registry, { harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'] });
    seedCard(db);
    // A STALE passing verdict left over from an earlier gate check. It must NOT be
    // read as THIS attempt's judgment — the critic path clears it before invoke,
    // so a critic that fails to (re)write falls through to the fail-closed scrap.
    writeFileSync(join(process.cwd(), 'verdict.json'), JSON.stringify({ verdict: 'pass', findings: [] }), 'utf-8');

    await run(flow, registry, { async call() { throw new Error('gate critic must be the harness, not the model'); } });

    expect(critic.calls.length).toBeGreaterThanOrEqual(1);
    // The stale pass did NOT leak through — fail-closed scrap, never an advance.
    expect(getCard(db)?.lane).not.toBe('done');
    expect(getCard(db)?.lane).toBe('scrap');
    // a pre-public engine review (@queso finding 1): a missing verdict.json now names its own
    // reason distinctly from invoke-failed/unparseable/invalid — previously this
    // collapsed to the same 'model-incompatible' label as every other critic
    // failure, which is what made a model bisect point at the wrong suspect.
    expect(terminalReasons(db).some((r) => /^harness-critic-verdict-missing:/.test(r))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Review #1 — the harness registry reaches runGateCheckOrAdvance for EVERY
// maker kind, not just a harness maker. Before the fix, executeTransformStation
// and executeDeterministicStation never received harnessRegistry, so a
// transform/deterministic maker gated by an AGENTIC critic (check.critic.harness)
// always threw 'no harness adapter registry configured' inside runGateRework
// and silently escalated the card to hold.
// ---------------------------------------------------------------------------

const MAKER_MODEL = 'maker-model';

/** A `kind: transform` (or deterministic) maker gated by a HARNESS critic. */
function writeNonHarnessMakerGatedFlow(
  dir: string,
  registry: HarnessRegistry,
  makerKind: 'transform' | 'deterministic',
  opts: { runMaxTokens?: number } = {},
): FlowConfig {
  const runMaxTokens = opts.runMaxTokens ?? 100000;
  mkdirSync(join(dir, 'prompts'), { recursive: true });
  writeFileSync(join(dir, 'prompts', 'coder.md'), 'TASK: {{task.json}}');
  // The deterministic maker (`command: "true"`) writes no artifact, so its
  // variant declares no outputs and its critic prompt carries no {{refs}} —
  // otherwise the output-missing safety net holds the card before the gate.
  writeFileSync(
    join(dir, 'prompts', 'verify.md'),
    makerKind === 'transform' ? 'Check {{result.json}}' : 'Judge the artifact.',
  );
  writeFileSync(join(dir, 'task.json'), '{"task":"build"}');

  const workerBlock =
    makerKind === 'transform'
      ? `worker:
      kind: transform
      model: ${MAKER_MODEL}
      prompt_file: prompts/coder.md
      prompt_version: "1"
      output_schema:
        fields:
          - { name: summary, type: string, required: true }`
      : `worker: { kind: deterministic, command: "true" }`;
  const outputsLine = makerKind === 'transform' ? 'outputs: [result.json]' : '';

  const flowYaml = `
flow: harness-critic-${makerKind}-maker
project_root: .
flow_version: 1
budgets:
  run: { wall_clock_minutes: 10, max_tokens: ${runMaxTokens} }
  per_card: { max_execution_attempts: 4 }
  liveness: { no_progress_minutes: 3 }
terminal_lanes: [done, scrap, hold]
security:
  bash:
    allow: ["true"]
stations:
  - id: coder
    ${workerBlock}
    inputs: [task.json]
    ${outputsLine}
    next: done
    check:
      kind: gate
      critic: { role: critic, harness: claude-critic, tools: [Read, Write], prompt_file: prompts/verify.md, prompt_version: "1" }
      on_reject: coder
      rework_cap: 2
`;
  writeFileSync(join(dir, 'flow.yaml'), flowYaml);
  const loaded = loadFlow(join(dir, 'flow.yaml'), { harnessRegistry: registry });
  if (!loaded.ok) throw new Error(`fixture flow invalid: ${JSON.stringify(loaded.errors)}`);
  return loaded.flow;
}

/** Answers the transform MAKER's model call only; any other model is a wiring bug. */
function makerOnlyModel(): ModelAdapter {
  return {
    async call(req: ModelCall) {
      if (req.model !== MAKER_MODEL) {
        throw new Error(`unexpected model call for '${req.model}' — the gate critic must be the harness, not a model`);
      }
      return { text: JSON.stringify({ summary: 'implemented' }), inputTokens: 5, outputTokens: 5, costUsd: 0.001 };
    },
  };
}

describe('review #1 — an agentic (harness) critic gates a NON-harness maker', () => {
  it('transform maker: the harness critic runs and a pass advances to done (never hold)', async () => {
    db = openDb();
    const critic = makeHarnessCritic('pass', []);
    const registry = createHarnessRegistry([critic.adapter]);
    const flow = writeNonHarnessMakerGatedFlow(dir, registry, 'transform');
    seedCard(db);

    await run(flow, registry, makerOnlyModel());

    // Before the fix: critic never invoked, card escalated to hold with
    // "no harness adapter registry configured".
    expect(critic.calls.length).toBeGreaterThanOrEqual(1);
    expect(gateVerdicts(db).some((v) => v.verdict === 'pass')).toBe(true);
    expect(getCard(db)?.lane).toBe('done');
  });

  it('transform maker: a harness-critic reject routes through the rework back-edge', async () => {
    db = openDb();
    const critic = makeHarnessCritic('reject', ['agentic finding on a transform maker']);
    const registry = createHarnessRegistry([critic.adapter]);
    const flow = writeNonHarnessMakerGatedFlow(dir, registry, 'transform');
    seedCard(db);

    await run(flow, registry, makerOnlyModel());

    const reject = gateVerdicts(db).find((v) => v.verdict === 'reject');
    expect(reject).toBeDefined();
    expect(reject!.findings).toEqual(['agentic finding on a transform maker']);
    expect(getCard(db)?.rework_count ?? 0).toBeGreaterThanOrEqual(1);
    // Never the silent escalate-to-hold the missing registry produced.
    expect(getCard(db)?.status).not.toBe('held');
  });

  it('deterministic maker: the harness critic runs and a pass advances to done (never hold)', async () => {
    db = openDb();
    const critic = makeHarnessCritic('pass', []);
    const registry = createHarnessRegistry([critic.adapter]);
    const flow = writeNonHarnessMakerGatedFlow(dir, registry, 'deterministic');
    seedCard(db);

    // No model at all for a deterministic maker — ANY model call is a wiring bug.
    await run(flow, registry, { async call() { throw new Error('no model call expected for a deterministic maker with a harness critic'); } });

    expect(critic.calls.length).toBeGreaterThanOrEqual(1);
    expect(gateVerdicts(db).some((v) => v.verdict === 'pass')).toBe(true);
    expect(getCard(db)?.lane).toBe('done');
  });
});

// ---------------------------------------------------------------------------
// issue #26 — a harness gate critic's usage/cost/model was discarded: hardcoded
// to { tokens: 0, cost: 0 } on both verdict branches, and never threaded a
// `check.critic.model` override into the invocation. AC3 makes the critic's
// billed spend reach the SAME run/wave accumulators the maker path feeds; AC4
// makes `check.critic.model` reach `HarnessInvocation.model` with
// station-over-adapter precedence (FR-10).
// ---------------------------------------------------------------------------

/** A ModelAdapter that must never be touched — the gate critic here is a harness. */
const neverModel: ModelAdapter = {
  async call() {
    throw new Error('gate critic must be the harness, not the model');
  },
};

describe('issue #26 AC3 — a harness gate critic\'s own spend reaches the run budget', () => {
  it('folds a rejecting critic\'s real usage into tokensSpent, tripping the andon on a tight budget', async () => {
    db = openDb();
    const maker = makeZeroUsageHarnessMaker();
    // A critic that keeps rejecting (distinct findings each time, so guard #3
    // never trips first) and reports 5000 tokens per call — large next to a
    // tight run budget.
    const critic = makeConfigurableHarnessCritic({ verdict: 'reject', usage: { tokens: 5000, cost: 5 } });
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    // Budget of 100 tokens: the maker itself spends 0, so ONLY the critic's
    // fold can cross it. A generous rework cap so the run would otherwise keep
    // reworking rather than scrapping — isolating the andon as the halt cause.
    const flow = writeGatedFlow(dir, registry, {
      harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'], reworkCap: 10, runMaxTokens: 100,
    });
    seedCard(db);
    const { io: capturedIo, err } = makeIO();

    await run(flow, registry, neverModel, capturedIo);

    // Halted mid-flight: neither done (the gate never passed) nor scrap (the
    // rework cap was never reached) — the andon stopped it first.
    expect(getCard(db)?.lane).toBe('coder');
    expect(getCard(db)?.status).not.toBe('scrapped');
    const errText = err.join('\n');
    expect(errText).toMatch(/andon/i);
    expect(errText).toMatch(/token/i);
  });

  it('does NOT halt the same flow when the budget is generous — it runs to a rework-cap scrap instead', async () => {
    db = openDb();
    const maker = makeZeroUsageHarnessMaker();
    const critic = makeConfigurableHarnessCritic({ verdict: 'reject', usage: { tokens: 5000, cost: 5 } });
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    // Same critic and same per-call spend as the halting test above; only the
    // budget changes — generous enough that several rejections fit inside it,
    // so the run reaches the rework-cap scrap rather than the andon.
    const flow = writeGatedFlow(dir, registry, {
      harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'], reworkCap: 1, runMaxTokens: 1_000_000,
    });
    seedCard(db);
    const { io: capturedIo, err } = makeIO();

    await run(flow, registry, neverModel, capturedIo);

    expect(getCard(db)?.lane).toBe('scrap');
    expect(terminalReasons(db)).toContain('rework_cap');
    expect(err.join('\n')).not.toMatch(/andon/i);
  });

  it('journals a <station>.harness-critic span with the critic\'s real tokens, cost, and adapter name', async () => {
    db = openDb();
    const maker = makeZeroUsageHarnessMaker();
    // rework_cap: 0 — the FIRST rejection is already at cap, so exactly one
    // critic call happens (a clean, single-span assertion).
    const critic = makeConfigurableHarnessCritic({ verdict: 'reject', usage: { tokens: 42, cost: 0.42 }, name: 'claude-critic' });
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    const flow = writeGatedFlow(dir, registry, { harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'], reworkCap: 0 });
    seedCard(db);

    await run(flow, registry, neverModel);

    expect(critic.calls).toHaveLength(1);
    const spans = db.getJournalSpansForRun(DEFAULT_RUN_ID, 'entry');
    const criticSpans = spans.filter((s) => s.name === 'coder.harness-critic');
    expect(criticSpans).toHaveLength(1);
    expect(criticSpans[0]!.adapter).toBe('claude-critic');
    expect(criticSpans[0]!.usageUnknown).toBe(false);

    const usage = db.getStationUsage('entry', 'coder', 0);
    expect(usage).not.toBeNull();
    const u = usage as Record<string, number | string>;
    expect(Number(u['gen_ai.usage.input_tokens']) + Number(u['gen_ai.usage.output_tokens'])).toBe(42);
    expect(u['cost_usd']).toBe(0.42);
  });

  it('an unknown-usage critic journals usageUnknown:true and folds NOTHING into the budget', async () => {
    db = openDb();
    const maker = makeZeroUsageHarnessMaker();
    const critic = makeConfigurableHarnessCritic({ verdict: 'reject', usage: { unknown: true } });
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    // A budget that WOULD be tripped by nearly any nonzero folded figure, so a
    // silent halt here would prove the unknown usage got folded as "some number".
    const flow = writeGatedFlow(dir, registry, {
      harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'], reworkCap: 0, runMaxTokens: 10,
    });
    seedCard(db);
    const { io: capturedIo, err } = makeIO();

    await run(flow, registry, neverModel, capturedIo);

    // Scrapped via rework_cap (the guard, not the andon) proves nothing folded.
    expect(getCard(db)?.lane).toBe('scrap');
    expect(terminalReasons(db)).toContain('rework_cap');
    expect(err.join('\n')).not.toMatch(/andon/i);

    const spans = db.getJournalSpansForRun(DEFAULT_RUN_ID, 'entry');
    const criticSpans = spans.filter((s) => s.name === 'coder.harness-critic');
    expect(criticSpans).toHaveLength(1);
    expect(criticSpans[0]!.usageUnknown).toBe(true);
  });

  it('a gated kind:deterministic maker\'s harness critic usage reaches the run budget (halts on a tight budget)', async () => {
    db = openDb();
    // Reuses the review #1 fixture: a `kind: deterministic` maker (`command:
    // "true"`, bills nothing itself) gated by a harness critic. A PASS
    // verdict still runs the fold+andon check inside runGateCheckOrAdvance
    // (that check is unconditional on verdict) — 5000 tokens against a
    // 100-token budget, so a halt here can ONLY be explained by the critic's
    // usage reaching tokensSpent via foldHarnessUsage: the deterministic
    // command touches neither a ModelAdapter nor a harness, so nothing else
    // in this path could move the budget. Mirrors the AC3 headline test's
    // halt/no-halt discriminator, applied to the deterministic maker kind
    // specifically (the one path that would NOT notice a broken fold, since
    // it has no adapter call of its own to leak the failure some other way).
    const critic = makeConfigurableHarnessCritic({ verdict: 'pass', usage: { tokens: 5000, cost: 5 }, name: 'claude-critic' });
    const registry = createHarnessRegistry([critic.adapter]);
    const flow = writeNonHarnessMakerGatedFlow(dir, registry, 'deterministic', { runMaxTokens: 100 });
    seedCard(db);
    const { io: capturedIo, err } = makeIO();

    await run(flow, registry, neverModel, capturedIo);

    // Halted before advancing: the andon trips on the critic's own folded
    // spend, so the card never reaches 'done' despite the pass verdict.
    expect(getCard(db)?.lane).not.toBe('done');
    const errText = err.join('\n');
    expect(errText).toMatch(/andon/i);
    expect(errText).toMatch(/token/i);
  });

  it('...does NOT halt the same deterministic-maker flow when the budget is generous (negative control)', async () => {
    db = openDb();
    const critic = makeConfigurableHarnessCritic({ verdict: 'pass', usage: { tokens: 5000, cost: 5 }, name: 'claude-critic' });
    const registry = createHarnessRegistry([critic.adapter]);
    const flow = writeNonHarnessMakerGatedFlow(dir, registry, 'deterministic', { runMaxTokens: 100000 });
    seedCard(db);
    const { io: capturedIo, err } = makeIO();

    await run(flow, registry, neverModel, capturedIo);

    expect(getCard(db)?.lane).toBe('done');
    expect(err.join('\n')).not.toMatch(/andon/i);
  });

  it('journals the harness critic\'s usage on the deterministic maker path (journal only — the fold is proven separately above)', async () => {
    db = openDb();
    const critic = makeHarnessCritic('pass', []); // usage: { tokens: 8, cost: 0.008 }
    const registry = createHarnessRegistry([critic.adapter]);
    const flow = writeNonHarnessMakerGatedFlow(dir, registry, 'deterministic');
    seedCard(db);

    await run(flow, registry, neverModel);

    expect(getCard(db)?.lane).toBe('done');
    const usage = db.getStationUsage('entry', 'coder', 0);
    expect(usage).not.toBeNull();
    const u = usage as Record<string, number | string>;
    expect(Number(u['gen_ai.usage.input_tokens']) + Number(u['gen_ai.usage.output_tokens'])).toBe(8);
    expect(u['cost_usd']).toBe(0.008);
  });

  it('journals the critic under a name distinct from the maker\'s <station>.harness span — never the same name', async () => {
    db = openDb();
    const maker = makeHarnessMaker(); // writes 'coder.harness' spans, usage {tokens:10, cost:0.01}
    const critic = makeConfigurableHarnessCritic({ verdict: 'pass' });
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    const flow = writeGatedFlow(dir, registry, { harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'] });
    seedCard(db);

    await run(flow, registry, neverModel);

    expect(getCard(db)?.lane).toBe('done');
    const spans = db.getJournalSpansForRun(DEFAULT_RUN_ID, 'entry');
    const makerSpans = spans.filter((s) => s.name === 'coder.harness');
    const criticSpans = spans.filter((s) => s.name === 'coder.harness-critic');
    // The maker's own span keeps its ORIGINAL name — proves the critic wiring
    // didn't rename or absorb it.
    expect(makerSpans.length).toBeGreaterThanOrEqual(1);
    expect(criticSpans.length).toBeGreaterThanOrEqual(1);
    // Nothing the critic wrote collides with the exact name
    // countConsecutiveRateLimitParks keys its streak on.
    expect(spans.every((s) => s.name !== 'coder.harness' || makerSpans.includes(s))).toBe(true);
  });
});

describe('issue #26 AC4 — check.critic.model reaches the harness critic\'s HarnessInvocation.model (FR-10)', () => {
  it('threads an explicit check.critic.model into the invocation, winning over the adapter\'s own default', async () => {
    db = openDb();
    const maker = makeZeroUsageHarnessMaker();
    const critic = makeConfigurableHarnessCritic({ verdict: 'pass', adapterModel: 'adapter-default-model' });
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    const flow = writeGatedFlow(dir, registry, {
      harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'], criticModel: 'station-critic-model',
    });
    seedCard(db);

    await run(flow, registry, neverModel);

    expect(critic.calls).toHaveLength(1);
    expect(critic.calls[0]!.model).toBe('station-critic-model');
  });

  it('falls back to the adapter\'s own configured default when check.critic.model is absent', async () => {
    db = openDb();
    const maker = makeZeroUsageHarnessMaker();
    const critic = makeConfigurableHarnessCritic({ verdict: 'pass', adapterModel: 'adapter-default-model' });
    const registry = createHarnessRegistry([maker.adapter, critic.adapter]);
    // No criticModel declared at all.
    const flow = writeGatedFlow(dir, registry, { harnessCritic: 'claude-critic', criticTools: ['Read', 'Write'] });
    seedCard(db);

    await run(flow, registry, neverModel);

    expect(critic.calls).toHaveLength(1);
    expect(critic.calls[0]!.model).toBe('adapter-default-model');
  });
});
