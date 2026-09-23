/**
 * Tests for the two MVP QC check archetypes (WI-300):
 *   - gate (converge)  → src/quality/gate.ts
 *   - rank (curate)    → src/quality/rank.ts
 *
 * SPEC §6, FR-6/6a, FR-14, SPEC §4A. Both are PURE transform QC stations: the
 * only external interaction is the kernel-mediated critic model call (injected
 * adapter, WI-296) — no tools, no filesystem; the verdict is a function of
 * input. Each verdict carries a findings_hash (authoritative WI-289 envelope)
 * that feeds the rework progress-monotonicity guard (WI-299).
 *
 *   - gate: pass → advance happy path; reject → route to a return_to lane that
 *     is VALIDATED against the flow's back-edges (invalid return_to is rejected).
 *   - rank: scores N candidates into an ordered short-list and NEVER auto-picks
 *     — it routes to a HITL hold, or (no HITL) applies proceed_with_findings or
 *     scrap per config. There is no code path that silently selects a candidate.
 *
 * ---------------------------------------------------------------------------
 * Contract this file pins
 * ---------------------------------------------------------------------------
 *
 * src/quality/gate.ts:
 *   export interface GateCriticVerdict { verdict: 'pass' | 'reject'; findings: string[]; return_to?: string }
 *   export interface GateConfig {
 *     cardId: string; station: string; attempt: number; maxExecutionAttempts: number;
 *     model: string; prompt: string; params: Record<string, unknown>;
 *     adapter: ModelAdapter; db: ConduitDB;
 *     onReject: Lane;                                              // default back-edge if the critic omits return_to
 *     validBackEdges: ReadonlyArray<{ from: string; to: string }>;// from the WI-292 loader
 *   }
 *   export type GateDecision =
 *     | { action: 'pass';           output: StationOutput<GateCriticVerdict> }
 *     | { action: 'reject';         returnTo: Lane; output: StationOutput<GateCriticVerdict> }
 *     | { action: 'invalid_verdict'; reason: 'invalid_return_to' }
 *     | { action: 'scrapped';       reason: string };  // 'model-incompatible' (transform critic,
 *                                                       // runGateCheck) or a distinct
 *                                                       // 'harness-critic-*' reason (agentic
 *                                                       // critic, runHarnessGateCheck) — a pre-public engine review
 *                                                       // review, @queso findings 1 + 2a.
 *   export function computeFindingsHash(findings: string[]): string;  // deterministic, order-insensitive
 *   export function runGateCheck(config: GateConfig): Promise<GateDecision>;
 *
 * src/quality/rank.ts:
 *   export interface RankCriticVerdict { ranking: string[]; findings: string[] }
 *   export interface RankConfig {
 *     cardId: string; station: string; attempt: number; maxExecutionAttempts: number;
 *     model: string; prompt: string; params: Record<string, unknown>;
 *     adapter: ModelAdapter; db: ConduitDB;
 *     candidateIds: string[];
 *     hitlEnabled: boolean;                                  // is a HITL hold wired (egress)?
 *     noSelectionPolicy: 'proceed_with_findings' | 'scrap';  // applied ONLY when no HITL is available
 *   }
 *   export type RankDecision =
 *     | { action: 'await_selection';        shortList: string[]; output: StationOutput<RankCriticVerdict> }
 *     | { action: 'proceed_with_findings';  shortList: string[]; output: StationOutput<RankCriticVerdict> }
 *     | { action: 'scrap';                  reason: 'no_selection' }
 *     | { action: 'scrapped';               reason: 'model-incompatible' };
 *   export function runRankCheck(config: RankConfig): Promise<RankDecision>;
 *   // NOTE: RankDecision deliberately has NO "selected"/"auto_pick" variant —
 *   // rank never picks a candidate; selection is always deferred to a human
 *   // (await_selection) or a fallback policy.
 *
 * Both gate.ts and rank.ts build the critic call on runTransformStation (WI-296)
 * and rebuild the StationOutput with the authoritative findings_hash + return_to.
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { join } from 'node:path';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import type { Lane, StationOutput } from '../types/kernel';
import { openConduitDB, DEFAULT_RUN_ID, type ConduitDB } from '../persistence/db';
import type { ModelAdapter, ModelCall, ModelResponse } from '../worker/adapter';
import type { HarnessAdapter, HarnessInvocation, HarnessResult } from '../worker/harness-adapter';
import {
  transition,
  type FsmState,
  type TransitionContext,
} from '../statemachine/transitions';
import { runGateCheck, runHarnessGateCheck, computeFindingsHash } from './gate';
import type { GateConfig, GateDecision, GateCriticVerdict, HarnessGateConfig } from './gate';
import { runRankCheck } from './rank';
import type { RankConfig, RankDecision, RankCriticVerdict } from './rank';

// ---------------------------------------------------------------------------
// Stub kernel adapter — records calls, returns queued responses.
// ---------------------------------------------------------------------------

function makeStubAdapter(responses: ModelResponse[]): {
  adapter: ModelAdapter;
  calls: ModelCall[];
} {
  const calls: ModelCall[] = [];
  let i = 0;
  const adapter: ModelAdapter = {
    async call(req) {
      calls.push(req);
      if (i >= responses.length) {
        throw new Error(`stub adapter over-called: no response for call #${i + 1}`);
      }
      return responses[i++];
    },
  };
  return { adapter, calls };
}

function resp(text: string, overrides: Partial<ModelResponse> = {}): ModelResponse {
  return { text, inputTokens: 10, outputTokens: 5, costUsd: 0.001, ...overrides };
}

let db: ConduitDB;

beforeEach(() => {
  db = openConduitDB({ stateDbPath: ':memory:', journalDbPath: ':memory:' });
});

afterEach(() => {
  db.close();
});

const BACK_EDGES = [
  { from: 'gate', to: 'draft' },
  { from: 'gate', to: 'refine' },
];

function gateConfig(over: Partial<GateConfig> & { adapter: ModelAdapter }): GateConfig {
  return {
    cardId: 'card-1',
    station: 'gate',
    attempt: 0,
    maxExecutionAttempts: 3,
    model: 'gpt-4o-mini',
    prompt: 'Critique the draft.',
    params: { temperature: 0 },
    db,
    runId: DEFAULT_RUN_ID,
    onReject: 'draft',
    validBackEdges: BACK_EDGES,
    ...over,
  };
}

function rankConfig(over: Partial<RankConfig> & { adapter: ModelAdapter }): RankConfig {
  return {
    cardId: 'card-1',
    station: 'rank',
    attempt: 0,
    maxExecutionAttempts: 3,
    model: 'gpt-4o',
    prompt: 'Rank the candidates.',
    params: { temperature: 0 },
    db,
    runId: DEFAULT_RUN_ID,
    candidateIds: ['v1', 'v2', 'v3'],
    hitlEnabled: true,
    noSelectionPolicy: 'scrap',
    ...over,
  };
}

function expectGate<A extends GateDecision['action']>(
  d: GateDecision,
  action: A,
): Extract<GateDecision, { action: A }> {
  if (d.action !== action) throw new Error(`expected gate action=${action}, got ${JSON.stringify(d)}`);
  return d as Extract<GateDecision, { action: A }>;
}

function expectRank<A extends RankDecision['action']>(
  d: RankDecision,
  action: A,
): Extract<RankDecision, { action: A }> {
  if (d.action !== action) throw new Error(`expected rank action=${action}, got ${JSON.stringify(d)}`);
  return d as Extract<RankDecision, { action: A }>;
}

// ===========================================================================
// computeFindingsHash — deterministic, order-insensitive (feeds WI-299) (AC3)
// ===========================================================================

describe('computeFindingsHash (AC3)', () => {
  it('is deterministic and non-empty', () => {
    expect(computeFindingsHash(['missing null check', 'typo'])).toBe(
      computeFindingsHash(['missing null check', 'typo']),
    );
    expect(computeFindingsHash(['x']).length).toBeGreaterThan(0);
  });

  it('is identical for the same findings in a different order (cosmetic reordering = no progress)', () => {
    expect(computeFindingsHash(['a', 'b', 'c'])).toBe(computeFindingsHash(['c', 'a', 'b']));
  });

  it('differs when the set of findings differs (real progress)', () => {
    expect(computeFindingsHash(['a', 'b'])).not.toBe(computeFindingsHash(['a', 'c']));
  });
});

// ===========================================================================
// GATE — converge via bounded back-edge rework (AC1, AC2, AC3)
// ===========================================================================

describe('runGateCheck (gate / converge)', () => {
  it('passes → advances the happy path (return_to is null) (AC1)', async () => {
    const { adapter, calls } = makeStubAdapter([resp('{"verdict":"pass","findings":[]}')]);

    const decision = await runGateCheck(gateConfig({ adapter }));

    const pass = expectGate(decision, 'pass');
    expect(pass.output.payload.verdict).toBe('pass');
    expect(pass.output.return_to).toBeNull();
    // The critic call went through the injected kernel adapter (pure station).
    expect(calls).toHaveLength(1);
  });

  it('rejects → routes to the critic-named return_to lane, validated against the back-edges (AC1, AC2)', async () => {
    const { adapter } = makeStubAdapter([
      resp('{"verdict":"reject","findings":["unsafe cast"],"return_to":"refine"}'),
    ]);

    const decision = await runGateCheck(gateConfig({ adapter }));

    const reject = expectGate(decision, 'reject');
    expect(reject.returnTo).toBe('refine');
    expect(reject.output.return_to).toBe('refine');
    expect(reject.output.payload.verdict).toBe('reject');
  });

  it('rejects an invalid return_to that is not a validated back-edge (AC2)', async () => {
    const { adapter } = makeStubAdapter([
      resp('{"verdict":"reject","findings":["x"],"return_to":"probing"}'), // not in BACK_EDGES from gate
    ]);

    const decision = await runGateCheck(gateConfig({ adapter }));

    expect(decision).toEqual({ action: 'invalid_verdict', reason: 'invalid_return_to' });
  });

  it('falls back to the configured onReject lane when the critic omits return_to (AC2)', async () => {
    const { adapter } = makeStubAdapter([resp('{"verdict":"reject","findings":["x"]}')]);

    const decision = await runGateCheck(gateConfig({ adapter, onReject: 'draft' }));

    const reject = expectGate(decision, 'reject');
    expect(reject.returnTo).toBe('draft');
  });

  it('carries a findings_hash derived from the verdict findings (feeds the rework guard) (AC3)', async () => {
    const { adapter } = makeStubAdapter([
      resp('{"verdict":"reject","findings":["unsafe cast","no bounds check"],"return_to":"draft"}'),
    ]);

    const decision = await runGateCheck(gateConfig({ adapter }));

    const reject = expectGate(decision, 'reject');
    expect(reject.output.findings_hash).toBe(
      computeFindingsHash(['unsafe cast', 'no bounds check']),
    );
  });

  it('two rejects with identical findings produce identical findings_hash (no-progress signal)', async () => {
    const stubA = makeStubAdapter([resp('{"verdict":"reject","findings":["bug A"],"return_to":"draft"}')]);
    const stubB = makeStubAdapter([resp('{"verdict":"reject","findings":["bug A"],"return_to":"draft"}')]);

    const a = expectGate(await runGateCheck(gateConfig({ adapter: stubA.adapter })), 'reject');
    const b = expectGate(await runGateCheck(gateConfig({ adapter: stubB.adapter })), 'reject');

    expect(a.output.findings_hash).toBe(b.output.findings_hash);
  });

  it('propagates a model-incompatible scrap when the critic call cannot be parsed', async () => {
    const { adapter } = makeStubAdapter([resp('not json'), resp('still not json'), resp('nope')]);

    const decision = await runGateCheck(gateConfig({ adapter, maxExecutionAttempts: 3 }));

    expect(decision).toEqual({ action: 'scrapped', reason: 'model-incompatible' });
  });

  it('is a function of its input: identical critic responses yield identical decisions (purity) (AC6)', async () => {
    const text = '{"verdict":"reject","findings":["dup"],"return_to":"refine"}';
    const first = expectGate(await runGateCheck(gateConfig({ adapter: makeStubAdapter([resp(text)]).adapter })), 'reject');
    const second = expectGate(await runGateCheck(gateConfig({ adapter: makeStubAdapter([resp(text)]).adapter })), 'reject');

    expect(second.returnTo).toBe(first.returnTo);
    expect(second.output.findings_hash).toBe(first.output.findings_hash);
  });
});

// ===========================================================================
// runHarnessGateCheck (agentic critic) — a pre-public engine review consumer smoke-test review
// (@queso). Findings 1 + 2a.
//
// Finding 1: a single 'model-incompatible' catch-all covered invoke-throw,
// missing verdict.json, unparseable verdict.json, AND schema-invalid verdict
// — @queso burned four runs bisecting models because the reason named the
// wrong suspect. Each path now gets its own distinct, greppable reason.
//
// Finding 2a: gateCriticSchema silently accepted a reject verdict with
// missing/empty findings (e.g. the critic mis-emitted `reasons` instead of
// `findings`) as a legal findings-less reject — unactionable by definition.
// That case now schema-rejects with a dedicated named reason.
// ===========================================================================

describe('runHarnessGateCheck (agentic critic, a pre-public engine review @queso findings 1 + 2a)', () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'conduit-harness-gate-check-'));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  function harnessAdapter(
    onInvoke: (call: HarnessInvocation) => Promise<HarnessResult> | HarnessResult,
  ): HarnessAdapter {
    return {
      name: 'fake-critic',
      reportsUsage: false,
      canRestrictTools: true,
      async probeBinary() {
        return { present: true };
      },
      async invoke(call) {
        return onInvoke(call);
      },
    };
  }

  function harnessGateConfig(
    over: Partial<HarnessGateConfig> & { harnessAdapter: HarnessAdapter },
  ): HarnessGateConfig {
    return {
      cardId: 'card-1',
      station: 'gate',
      attempt: 0,
      prompt: 'Critique the draft.',
      criticInputScope: [],
      projectRoot: dir,
      timeoutMs: 5000,
      onReject: 'draft',
      validBackEdges: BACK_EDGES,
      ...over,
    };
  }

  const NOOP_RESULT: HarnessResult = { outputs: [], usage: { unknown: true } };

  function writeVerdict(text: string): void {
    writeFileSync(join(dir, 'verdict.json'), text, 'utf-8');
  }

  it('names harness-critic-invoke-failed when invoke() throws — distinct from every other path (finding 1)', async () => {
    const adapter = harnessAdapter(() => {
      throw new Error('agent CLI crashed');
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-invoke-failed:/);
    expect(scrapped.reason).toContain('agent CLI crashed');
  });

  it('names harness-critic-verdict-missing when no verdict.json is produced — distinct from unparseable/invalid (finding 1)', async () => {
    const adapter = harnessAdapter(() => NOOP_RESULT); // never writes verdict.json

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-verdict-missing:/);
    expect(scrapped.reason).toContain('verdict.json');
  });

  it('names harness-critic-verdict-unparseable when verdict.json exists but is not valid JSON — distinct from missing/invalid (finding 1)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict('the critic wrote prose instead of JSON, no braces here');
      return NOOP_RESULT;
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-verdict-unparseable:/);
    expect(scrapped.reason).toContain('verdict.json');
  });

  it('names harness-critic-verdict-invalid when verdict.json is valid JSON but fails schema — distinct from missing/unparseable (finding 1)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'maybe', findings: [] }));
      return NOOP_RESULT;
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-verdict-invalid:/);
    expect(scrapped.reason).toContain('verdict.json');
  });

  it('never collapses harness-critic-* scraps back to the transform model-incompatible label (finding 1)', async () => {
    const adapter = harnessAdapter(() => {
      throw new Error('boom');
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).not.toBe('model-incompatible');
  });

  it('schema-rejects a reject verdict with empty findings, naming the verdict file and the expected contract (finding 2a)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'reject', findings: [] }));
      return NOOP_RESULT;
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-reject-without-findings:/);
    expect(scrapped.reason).toContain('verdict.json');
    expect(scrapped.reason).toContain('findings');
    expect(scrapped.reason).toContain('return_to');
  });

  it('schema-rejects the reported wrong-key case — {verdict:"reject",reasons:[...]} has no findings key at all (finding 2a)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'reject', reasons: ['unsafe cast'] }));
      return NOOP_RESULT;
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-reject-without-findings:/);
  });

  it('does NOT scrap the card silently — a findings-less reject never reaches the reject/rework branch (finding 2a)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'reject', findings: [] }));
      return NOOP_RESULT;
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    expect(decision.action).toBe('scrapped');
    expect(decision.action).not.toBe('reject');
  });

  it('a PASS verdict with empty findings stays legal (unchanged) — the finding 2a fix targets reject only', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'pass', findings: [] }));
      return NOOP_RESULT;
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const pass = expectGate(decision, 'pass');
    expect(pass.output.payload.verdict).toBe('pass');
  });

  it('a reject verdict WITH findings still routes to reject/rework, unaffected by the finding 2a schema check', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'reject', findings: ['unsafe cast'], return_to: 'draft' }));
      return NOOP_RESULT;
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const rejected = expectGate(decision, 'reject');
    expect(rejected.output.payload.findings).toEqual(['unsafe cast']);
    expect(rejected.returnTo).toBe('draft');
  });
});

// ===========================================================================
// runHarnessGateCheck — issue #26: a harness critic is a billed agentic
// invocation, and the real usage/cost the adapter reported (or, on a throw,
// whatever usageFromThrow recovers) was being dropped on the floor and every
// StationOutput hardcoded usage: {tokens:0, cost:0} — invisible to every
// budget meant to bound it. Separately, config.model never reached
// HarnessInvocation.model, so a station override silently had no effect.
// ===========================================================================

describe('runHarnessGateCheck (issue #26 — usage, cost, and model threading)', () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'conduit-harness-gate-usage-'));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  function harnessAdapter(
    onInvoke: (call: HarnessInvocation) => Promise<HarnessResult> | HarnessResult,
    name = 'fake-critic',
  ): HarnessAdapter {
    return {
      name,
      reportsUsage: true,
      canRestrictTools: true,
      async probeBinary() {
        return { present: true };
      },
      async invoke(call) {
        return onInvoke(call);
      },
    };
  }

  function harnessAdapterCapturing(
    onInvoke: (call: HarnessInvocation) => Promise<HarnessResult> | HarnessResult,
  ): { adapter: HarnessAdapter; calls: HarnessInvocation[] } {
    const calls: HarnessInvocation[] = [];
    const adapter = harnessAdapter((call) => {
      calls.push(call);
      return onInvoke(call);
    });
    return { adapter, calls };
  }

  function harnessGateConfig(
    over: Partial<HarnessGateConfig> & { harnessAdapter: HarnessAdapter },
  ): HarnessGateConfig {
    return {
      cardId: 'card-1',
      station: 'gate',
      attempt: 0,
      prompt: 'Critique the draft.',
      criticInputScope: [],
      projectRoot: dir,
      timeoutMs: 5000,
      onReject: 'draft',
      validBackEdges: BACK_EDGES,
      ...over,
    };
  }

  function writeVerdict(text: string): void {
    writeFileSync(join(dir, 'verdict.json'), text, 'utf-8');
  }

  it('a PASS verdict carries the adapter\'s real usage.tokens/usage.cost into the StationOutput, not zero (AC1)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'pass', findings: [] }));
      return { outputs: [], usage: { tokens: 12345, cost: 0.42 } };
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const pass = expectGate(decision, 'pass');
    expect(pass.output.usage).toEqual({ tokens: 12345, cost: 0.42 });
  });

  it('a REJECT verdict likewise carries the adapter\'s real usage into the StationOutput (AC1)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'reject', findings: ['unsafe cast'], return_to: 'draft' }));
      return { outputs: [], usage: { tokens: 777, cost: 0.05 } };
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const rejected = expectGate(decision, 'reject');
    expect(rejected.output.usage).toEqual({ tokens: 777, cost: 0.05 });
  });

  it('an adapter reporting { unknown: true } propagates as unknown, never flattened to a fabricated zero (AC2)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'pass', findings: [] }));
      return { outputs: [], usage: { unknown: true } };
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const pass = expectGate(decision, 'pass');
    expect('unknown' in pass.output.usage).toBe(true);
  });

  it('config.model reaches HarnessInvocation.model verbatim (AC4)', async () => {
    const { adapter, calls } = harnessAdapterCapturing(() => {
      writeVerdict(JSON.stringify({ verdict: 'pass', findings: [] }));
      return { outputs: [], usage: { unknown: true } };
    });

    await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter, model: 'claude-opus-4' }));

    expect(calls).toHaveLength(1);
    expect(calls[0].model).toBe('claude-opus-4');
  });

  it('omitting config.model leaves HarnessInvocation.model absent, so the adapter falls through to its own default (AC4)', async () => {
    const { adapter, calls } = harnessAdapterCapturing(() => {
      writeVerdict(JSON.stringify({ verdict: 'pass', findings: [] }));
      return { outputs: [], usage: { unknown: true } };
    });

    await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    expect(calls[0].model).toBeUndefined();
  });

  it('attaches criticUsage (adapterName + a non-negative durationMs) on a pass (AC5)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'pass', findings: [] }));
      return { outputs: [], usage: { tokens: 1, cost: 0.001 } };
    }, 'critic-x');

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const pass = expectGate(decision, 'pass');
    expect(pass.criticUsage?.adapterName).toBe('critic-x');
    expect(typeof pass.criticUsage?.durationMs).toBe('number');
    expect(pass.criticUsage!.durationMs).toBeGreaterThanOrEqual(0);
  });

  it('attaches criticUsage on a reject (AC5)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'reject', findings: ['unsafe cast'], return_to: 'draft' }));
      return { outputs: [], usage: { tokens: 1, cost: 0.001 } };
    }, 'critic-x');

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const rejected = expectGate(decision, 'reject');
    expect(rejected.criticUsage?.adapterName).toBe('critic-x');
    expect(typeof rejected.criticUsage?.durationMs).toBe('number');
    expect(rejected.criticUsage!.durationMs).toBeGreaterThanOrEqual(0);
  });

  it('attaches criticUsage on harness-critic-verdict-missing — the invoke still ran and was billed (AC5)', async () => {
    const adapter = harnessAdapter(() => ({ outputs: [], usage: { tokens: 5, cost: 0.0001 } })); // never writes verdict.json

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-verdict-missing:/);
    expect(scrapped.criticUsage?.adapterName).toBe('fake-critic');
    expect(typeof scrapped.criticUsage?.durationMs).toBe('number');
  });

  it('attaches criticUsage on harness-critic-verdict-unparseable (AC5)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict('the critic wrote prose instead of JSON, no braces here');
      return { outputs: [], usage: { tokens: 5, cost: 0.0001 } };
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-verdict-unparseable:/);
    expect(scrapped.criticUsage?.adapterName).toBe('fake-critic');
    expect(typeof scrapped.criticUsage?.durationMs).toBe('number');
  });

  it('attaches criticUsage on a schema-invalid verdict (AC5)', async () => {
    const adapter = harnessAdapter(() => {
      writeVerdict(JSON.stringify({ verdict: 'maybe', findings: [] }));
      return { outputs: [], usage: { tokens: 5, cost: 0.0001 } };
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-verdict-invalid:/);
    expect(scrapped.criticUsage?.adapterName).toBe('fake-critic');
    expect(typeof scrapped.criticUsage?.durationMs).toBe('number');
  });

  it('an adapter that throws a plain error still scraps harness-critic-invoke-failed, with unknown criticUsage (AC5)', async () => {
    const adapter = harnessAdapter(() => {
      throw new Error('agent CLI crashed');
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-invoke-failed:/);
    expect(scrapped.criticUsage?.usage).toEqual({ unknown: true });
  });

  it('an adapter that throws with billed usage attached still scraps but folds the real numbers into criticUsage (AC5)', async () => {
    const billedErr = Object.assign(new Error('boom'), {
      code: 'harness-nonzero-exit',
      usage: { tokens: 900, cost: 0.03 },
    });
    const adapter = harnessAdapter(() => {
      throw billedErr;
    });

    const decision = await runHarnessGateCheck(harnessGateConfig({ harnessAdapter: adapter }));

    const scrapped = expectGate(decision, 'scrapped');
    expect(scrapped.reason).toMatch(/^harness-critic-invoke-failed:/);
    expect(scrapped.criticUsage?.usage).toEqual({ tokens: 900, cost: 0.03 });
  });

  it(
    'runGateCheck (the TRANSFORM critic) never populates criticUsage — its usage is already folded into the ' +
      'run/wave budgets via runTransformStation\'s trackingAdapter, so a criticUsage here would double-count ' +
      'the same spend the maker-path accumulators already saw (regression)',
    async () => {
      const passStub = makeStubAdapter([resp('{"verdict":"pass","findings":[]}')]);
      const passDecision = expectGate(await runGateCheck(gateConfig({ adapter: passStub.adapter })), 'pass');
      expect(passDecision.criticUsage).toBeUndefined();

      const rejectStub = makeStubAdapter([
        resp('{"verdict":"reject","findings":["unsafe cast"],"return_to":"draft"}'),
      ]);
      const rejectDecision = expectGate(await runGateCheck(gateConfig({ adapter: rejectStub.adapter })), 'reject');
      expect(rejectDecision.criticUsage).toBeUndefined();
    },
  );
});

// ===========================================================================
// RANK — curate into a selection, NEVER auto-pick (AC4, AC5, AC6)
// ===========================================================================

describe('runRankCheck (rank / curate)', () => {
  it('scores N candidates into an ordered short-list and routes to a HITL hold (AC4)', async () => {
    const { adapter, calls } = makeStubAdapter([
      resp('{"ranking":["v3","v1","v2"],"findings":["v2 off-brand"]}'),
    ]);

    const decision = await runRankCheck(rankConfig({ adapter, hitlEnabled: true }));

    const awaiting = expectRank(decision, 'await_selection');
    // The ordered short-list is the critic ranking — the full curated set, not a single pick.
    expect(awaiting.shortList).toEqual(['v3', 'v1', 'v2']);
    expect(calls).toHaveLength(1);
  });

  it('NEVER auto-selects the top candidate — with a HITL hold it defers to a human (AC5)', async () => {
    const { adapter } = makeStubAdapter([
      resp('{"ranking":["v3","v1","v2"],"findings":[]}'), // v3 clearly top-scored
    ]);

    const decision = await runRankCheck(rankConfig({ adapter, hitlEnabled: true }));

    // It must defer, NOT pick v3. The decision type has no "selected" variant.
    expect(decision.action).toBe('await_selection');
    expect(decision).not.toHaveProperty('selected');
  });

  it('with no HITL available, applies proceed_with_findings per config — never a silent auto-pick (AC5)', async () => {
    const { adapter } = makeStubAdapter([resp('{"ranking":["v1","v2","v3"],"findings":["weak"]}')]);

    const decision = await runRankCheck(
      rankConfig({ adapter, hitlEnabled: false, noSelectionPolicy: 'proceed_with_findings' }),
    );

    const proceed = expectRank(decision, 'proceed_with_findings');
    expect(proceed.shortList).toEqual(['v1', 'v2', 'v3']);
    expect(decision).not.toHaveProperty('selected');
  });

  it('with no HITL available and scrap policy, scraps with no_selection — never a silent auto-pick (AC5)', async () => {
    const { adapter } = makeStubAdapter([resp('{"ranking":["v1","v2","v3"],"findings":[]}')]);

    const decision = await runRankCheck(
      rankConfig({ adapter, hitlEnabled: false, noSelectionPolicy: 'scrap' }),
    );

    expect(decision).toEqual({ action: 'scrap', reason: 'no_selection' });
  });

  it('carries a findings_hash on its verdict (feeds the rework guard) (AC3/AC4)', async () => {
    const { adapter } = makeStubAdapter([resp('{"ranking":["v1","v2"],"findings":["a","b"]}')]);

    const decision = await runRankCheck(rankConfig({ adapter, hitlEnabled: true }));

    const awaiting = expectRank(decision, 'await_selection');
    expect(awaiting.output.findings_hash).toBe(computeFindingsHash(['a', 'b']));
  });

  it('propagates a model-incompatible scrap when the critic call cannot be parsed', async () => {
    const { adapter } = makeStubAdapter([resp('garbage'), resp('garbage'), resp('garbage')]);

    const decision = await runRankCheck(rankConfig({ adapter, maxExecutionAttempts: 3 }));

    expect(decision).toEqual({ action: 'scrapped', reason: 'model-incompatible' });
  });

  it('journals the critic usage under config.runId, not DEFAULT_RUN_ID (the original per-run usage-attribution work)', async () => {
    const { adapter } = makeStubAdapter([
      resp('{"ranking":["v1","v2"],"findings":[]}', { inputTokens: 20, outputTokens: 8, costUsd: 0.02 }),
    ]);

    await runRankCheck(rankConfig({ adapter, runId: 'run-beta', hitlEnabled: true }));

    // The rank critic's tokens/cost belong to the run that ran the check...
    expect(db.getRunUsageTotals('run-beta')).toEqual({ tokens: 28, costUsd: 0.02 });
    // ...not the 'default' sweep (the before per-run usage attribution mis-attribution).
    expect(db.getRunUsageTotals(DEFAULT_RUN_ID)).toEqual({ tokens: 0, costUsd: 0 });
  });
});

// ===========================================================================
// FSM integration (WI-293) — a gate reject is a legal QC_REJECT that routes to
// the back-edge AND increments the rework generation (AC1).
// ===========================================================================

describe('gate reject ↔ FSM back-edge + rework increment (AC1)', () => {
  const ctx: TransitionContext = {
    happyPathNext: { gate: 'render', render: null, draft: 'gate', refine: 'gate' },
    terminalLanes: ['done', 'scrap', 'hold'],
    reworkCap: 3,
    maxExecutionAttempts: 5,
    capPolicy: 'scrap',
    onDepScrap: 'hold',
    validBackEdges: BACK_EDGES,
  };

  it('applies the gate-named return_to via QC_REJECT, incrementing reworkCount', async () => {
    const { adapter } = makeStubAdapter([
      resp('{"verdict":"reject","findings":["x"],"return_to":"draft"}'),
    ]);
    const reject = expectGate(await runGateCheck(gateConfig({ adapter })), 'reject');

    const state: FsmState = { lane: 'gate', status: 'done_pending_ack', executionAttempt: 0, reworkCount: 1 };
    const result = transition(state, { type: 'QC_REJECT', returnTo: reject.returnTo }, ctx);

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error('expected legal transition');
    expect(result.next.lane).toBe('draft');
    expect(result.next.reworkCount).toBe(2); // rework generation incremented
  });
});
