/**
 * Smoke test for the canonical kernel domain types (WI-289).
 *
 * This module is PURE TYPES — there are no runtime values to import from
 * `./kernel`, so the load-bearing gate for this item is `bun run typecheck`
 * (`tsc --noEmit`), NOT `bun test` alone. The contract this file enforces:
 *
 *   1. Exhaustive `switch` over `Status` — a missing/renamed/added variant is a
 *      COMPILE error (the `assertNever` default + per-variant cases). (AC1, AC5)
 *   2. Exhaustive `switch` over `StationConfig['kind']` — same guarantee for the
 *      four execution kinds, including the agent-CLI `harness` kind (WI-559). (AC4)
 *   3. `Lane` accepts the four kernel terminals AND arbitrary station-id strings. (AC2)
 *   4. `Card` carries every state-DB column the kernel reads. (AC3, SPEC §11)
 *   5. The `StationOutput` envelope declares `findings_hash: string`,
 *      `return_to: Lane | null`, a typed payload, and per-call token/cost —
 *      the authoritative inputs to the rework guard (WI-299) and gate (WI-300). (AC6)
 *
 * The runtime `bun test` assertions below additionally prove the discriminant
 * switches are wired correctly (not dead code) and that the nullable/typed
 * envelope contract behaves as the downstream guards expect.
 */
import { describe, it, expect } from 'bun:test';
import { DEFAULT_RUN_ID } from '../persistence/db';
import type {
  Status,
  Lane,
  Card,
  StationConfig,
  FlowConfig,
  StationOutput,
} from './kernel';

/** Compile-time exhaustiveness sentinel: reaching this with a non-`never` is a type error. */
function assertNever(x: never): never {
  throw new Error(`Unhandled variant: ${String(x)}`);
}

// ---------------------------------------------------------------------------
// AC1 / AC5 — Status is EXACTLY the ten SPEC §3 variants.
// ---------------------------------------------------------------------------

// `satisfies` catches a typo'd or removed variant (an element not in the union
// fails to compile). The exhaustive switch below catches an ADDED variant.
const ALL_STATUSES = [
  'waiting',
  'ready',
  'claimed',
  'working',
  'done_pending_ack',
  'interrupted',
  'held',
  'awaiting_children',
  'complete',
  'scrapped',
] as const satisfies readonly Status[];

type StatusPhase = 'pending' | 'active' | 'terminal';

/**
 * Exhaustive switch over Status. If a kernel author adds a Status variant and
 * forgets to handle it here, `status` is no longer `never` at the `default`
 * branch and this file fails to typecheck — exactly the AC5 guarantee.
 */
function statusPhase(status: Status): StatusPhase {
  switch (status) {
    case 'waiting':
    case 'ready':
      return 'pending';
    case 'claimed':
    case 'working':
    case 'done_pending_ack':
    case 'interrupted':
    case 'held':
    case 'awaiting_children':
      return 'active';
    case 'complete':
    case 'scrapped':
      return 'terminal';
    default:
      return assertNever(status);
  }
}

describe('Status FSM (SPEC §3)', () => {
  it('maps every status to a phase with no fall-through', () => {
    const phases = ALL_STATUSES.map(statusPhase);
    // Every status resolved to a real phase (the switch never hit assertNever).
    expect(phases).toHaveLength(ALL_STATUSES.length);
    expect(phases.every((p) => p === 'pending' || p === 'active' || p === 'terminal')).toBe(true);
  });

  it('classifies the boundary statuses correctly', () => {
    expect(statusPhase('waiting')).toBe('pending');
    expect(statusPhase('ready')).toBe('pending');
    expect(statusPhase('working')).toBe('active');
    expect(statusPhase('awaiting_children')).toBe('active');
    expect(statusPhase('complete')).toBe('terminal');
    expect(statusPhase('scrapped')).toBe('terminal');
  });
});

// ---------------------------------------------------------------------------
// AC4 — StationConfig: kind union + effectful/wip/inputs/outputs + optional fields.
// ---------------------------------------------------------------------------

/**
 * Exhaustive switch over the execution `kind` axis (SPEC §4). Adding a fifth
 * kind without handling it here is a compile error. The `harness` case is the
 * agent-CLI worker introduced by WI-559 — a case label the switch cannot carry
 * until `harness` is a member of the `StationKind` union (AC1).
 */
function dispatchClass(
  kind: StationConfig['kind'],
): 'no-llm' | 'single-call' | 'tool-loop' | 'agent-cli' | 'child-flow' {
  switch (kind) {
    case 'deterministic':
      return 'no-llm';
    case 'transform':
      return 'single-call';
    case 'agentic':
      return 'tool-loop';
    case 'harness':
      return 'agent-cli';
    case 'subflow':
      return 'child-flow';
    default:
      return assertNever(kind);
  }
}

describe('StationConfig (SPEC §4)', () => {
  it('classifies all five execution kinds', () => {
    expect(dispatchClass('deterministic')).toBe('no-llm');
    expect(dispatchClass('transform')).toBe('single-call');
    expect(dispatchClass('agentic')).toBe('tool-loop');
    expect(dispatchClass('harness')).toBe('agent-cli');
    expect(dispatchClass('subflow')).toBe('child-flow');
  });

  it('models effectful, wip, inputs, outputs and the optional check / fan fields', () => {
    // Pure transform critic WITHOUT optional fields present.
    const critic: StationConfig = {
      kind: 'transform',
      effectful: false,
      wip: 1,
      inputs: ['draft'],
      outputs: ['verdict'],
    };

    // Agentic, effectful worker WITH the optional check + fan_in/fan_out present.
    const worker: StationConfig = {
      kind: 'agentic',
      effectful: true,
      wip: 4,
      inputs: ['task'],
      outputs: ['artifact'],
      check: 'qc',
      fan_in: 2,
      fan_out: 3,
    };

    expect(dispatchClass(critic.kind)).toBe('single-call');
    expect(critic.effectful).toBe(false);
    expect(critic.wip).toBe(1);
    expect(critic.inputs).toEqual(['draft']);
    // `check` is optional — absent on the critic, present on the worker.
    expect(critic.check).toBeUndefined();
    expect(worker.check).toBe('qc');
    expect(worker.effectful).toBe(true);
    expect(worker.fan_in).toBe(2);
    expect(worker.fan_out).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// WI-559 — the agent-CLI `harness` station kind + its config fields.
//   AC1: `harness` is a member of the StationKind union (proven by the switch
//        case above + the literals below).
//   AC2: StationConfig carries the harness fields — a required `harness`
//        adapter-name string, a `tools` string[] allowlist, and an optional
//        `unrestricted_tools` boolean waiver — while REUSING the shared
//        model/prompt/output_schema/inputs/outputs/effectful fields rather than
//        declaring harness-specific duplicates.
//   AC3: the whole module still typechecks and no non-harness station literal
//        is forced to carry the new fields (they are optional on the flat
//        StationConfig, mirroring `command`/`model`).
// ---------------------------------------------------------------------------

describe('harness station kind (WI-559)', () => {
  it('is a StationKind the dispatcher classifies as an agent-CLI worker', () => {
    // A minimal harness station: kind is 'harness', adapter-name + tools present.
    const minimal: StationConfig = {
      kind: 'harness',
      effectful: true,
      wip: 1,
      inputs: ['task'],
      outputs: ['artifact'],
      harness: 'claude-code',
      tools: ['Read', 'Write', 'Bash'],
    };

    expect(dispatchClass(minimal.kind)).toBe('agent-cli');
    // The adapter-name is the required `harness` string field.
    expect(minimal.harness).toBe('claude-code');
    // `tools` is the string[] allowlist.
    expect(minimal.tools).toEqual(['Read', 'Write', 'Bash']);
    // `unrestricted_tools` is an OPTIONAL waiver — absent on the minimal station.
    expect(minimal.unrestricted_tools).toBeUndefined();
  });

  it('accepts the unrestricted_tools waiver when a station opts out of the allowlist', () => {
    const waived: StationConfig = {
      kind: 'harness',
      effectful: true,
      wip: 1,
      inputs: ['task'],
      outputs: ['artifact'],
      harness: 'claude-code',
      tools: [],
      unrestricted_tools: true,
    };

    expect(waived.unrestricted_tools).toBe(true);
    // The allowlist can be empty when the waiver is set — the type does not require both.
    expect(waived.tools).toEqual([]);
  });

  it('reuses the shared model/prompt/output_schema fields, not harness-specific duplicates', () => {
    const station: StationConfig = {
      kind: 'harness',
      effectful: false,
      wip: 1,
      inputs: ['spec'],
      outputs: ['code'],
      harness: 'codex',
      tools: ['Read'],
      // Same model/prompt/output_schema surface a transform station uses (AC2:
      // "reused, not duplicated"). A duplicated harness_model / harness_prompt_file
      // would make this literal fail the "no duplicate fields" contract.
      model: 'claude-sonnet-5',
      prompt_file: 'prompts/agent.md',
      prompt_version: 'v1',
      output_schema: { fields: [{ name: 'summary', type: 'string', required: true }] },
    };

    expect(station.model).toBe('claude-sonnet-5');
    expect(station.prompt_file).toBe('prompts/agent.md');
    expect(station.output_schema?.fields[0]?.name).toBe('summary');
  });

  it('does not force the harness fields onto a non-harness station (fields are optional)', () => {
    // A plain transform critic constructs with NONE of the harness fields — the
    // AC3 guarantee that existing consumers are not broken by the additions.
    const critic: StationConfig = {
      kind: 'transform',
      effectful: false,
      wip: 1,
      inputs: ['draft'],
      outputs: ['verdict'],
    };

    expect(critic.harness).toBeUndefined();
    expect(critic.tools).toBeUndefined();
    expect(critic.unrestricted_tools).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// AC2 — Lane is a station id (string) OR one of the four kernel terminals.
// ---------------------------------------------------------------------------

const KERNEL_TERMINALS = ['intake', 'done', 'scrap', 'hold'] as const satisfies readonly Lane[];

function isKernelTerminal(lane: Lane): boolean {
  return (KERNEL_TERMINALS as readonly string[]).includes(lane);
}

describe('Lane (SPEC §3)', () => {
  it('accepts each kernel terminal', () => {
    for (const terminal of KERNEL_TERMINALS) {
      expect(isKernelTerminal(terminal)).toBe(true);
    }
  });

  it('accepts an arbitrary station id and does not treat it as a terminal', () => {
    const station: Lane = 'brief'; // a flow.yaml station id
    expect(isKernelTerminal(station)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// AC3 — Card carries the state-DB columns (SPEC §11).
// ---------------------------------------------------------------------------

describe('Card (SPEC §11)', () => {
  it('carries id, nullable parent_id, lane, status, attempt, wave and owned_paths', () => {
    const epic: Card = {
      run_id: DEFAULT_RUN_ID,
      id: 'card_epic',
      parent_id: null, // top-level card
      lane: 'intake',
      status: 'waiting',
      attempt: 0,
      wave: 0,
      owned_paths: [],
      rework_count: 0,
    };

    const child: Card = {
      run_id: DEFAULT_RUN_ID,
      id: 'card_child',
      parent_id: 'card_epic', // child references its parent
      lane: 'brief',
      status: 'working',
      attempt: 2,
      wave: 1,
      owned_paths: ['src/feature/a.ts', 'src/feature/b.ts'],
      rework_count: 0,
    };

    expect(epic.parent_id).toBeNull();
    expect(child.parent_id).toBe('card_epic');
    expect(statusPhase(child.status)).toBe('active');
    expect(isKernelTerminal(epic.lane)).toBe(true);
    expect(Array.isArray(child.owned_paths)).toBe(true);
    expect(child.owned_paths).toHaveLength(2);
    expect(child.attempt).toBe(2);
    expect(child.wave).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// AC6 — StationOutput / QC-verdict envelope: typed payload, findings_hash,
//        return_to (lane | null), and per-call token/cost attribution.
// ---------------------------------------------------------------------------

interface ReviewPayload {
  approved: boolean;
  notes: string;
}

describe('StationOutput / QC-verdict envelope', () => {
  it('carries a typed payload, findings_hash, null return_to, and token/cost on a passing verdict', () => {
    const passing: StationOutput<ReviewPayload> = {
      payload: { approved: true, notes: 'looks good' },
      findings_hash: 'sha256:da39a3ee',
      return_to: null, // no rework — proceed downstream
      usage: { tokens: 1280, cost: 0.0192 },
    };

    // findings_hash is a string (authoritative input to the WI-299 progress guard).
    expect(typeof passing.findings_hash).toBe('string');
    expect(passing.findings_hash.length).toBeGreaterThan(0);
    // return_to null means "no back-edge".
    expect(passing.return_to).toBeNull();
    // payload is the parameterized type, not `any`/`unknown` erased away.
    expect(passing.payload.approved).toBe(true);
    expect(passing.payload.notes).toBe('looks good');
    // per-call attribution is present and numeric. Narrowed off the
    // unknown arm first (issue #26): a KNOWN usage and an unreported one are
    // different facts, so reading tokens/cost requires proving which it is.
    expect('unknown' in passing.usage).toBe(false);
    if ('unknown' in passing.usage) throw new Error('expected known usage');
    expect(passing.usage.tokens).toBe(1280);
    expect(passing.usage.cost).toBeCloseTo(0.0192);
  });

  it('routes a failing verdict back to an earlier lane via return_to', () => {
    const failing: StationOutput<ReviewPayload> = {
      payload: { approved: false, notes: 'missing edge case' },
      findings_hash: 'sha256:bd17a1f0',
      return_to: 'brief', // back-edge to an earlier station (the WI-300 gate verdict)
      usage: { tokens: 980, cost: 0.0147 },
    };

    expect(failing.return_to).toBe('brief');
    expect(failing.payload.approved).toBe(false);
    // The gate (WI-300) compares findings_hash across attempts — it must be a stable string.
    expect(typeof failing.findings_hash).toBe('string');
    expect(failing.findings_hash).not.toBe('');
  });

  // Issue #26 AC2: an adapter that cannot report usage for a call must stay
  // distinguishable from one that reported a genuine zero. Before the union,
  // the only representable answer was `{ tokens: 0, cost: 0 }` — so an
  // unmeasured harness critic and a free call read identically, and every
  // budget that folds this number silently under-counted the unmeasured one.
  it('represents unreported usage as unknown, distinctly from a measured zero', () => {
    const unmeasured: StationOutput<ReviewPayload> = {
      payload: { approved: true, notes: 'critic adapter reports no usage' },
      findings_hash: 'sha256:da39a3ee',
      return_to: null,
      usage: { unknown: true },
    };
    const measuredZero: StationOutput<ReviewPayload> = {
      payload: { approved: true, notes: 'a genuinely free call' },
      findings_hash: 'sha256:da39a3ee',
      return_to: null,
      usage: { tokens: 0, cost: 0 },
    };

    expect('unknown' in unmeasured.usage).toBe(true);
    expect('unknown' in measuredZero.usage).toBe(false);

    // And the two are not merely differently-shaped: a consumer narrowing on
    // the discriminant reaches a different branch for each, which is the whole
    // point — one folds into a budget, the other cannot.
    const fold = (u: StationOutput<ReviewPayload>['usage']): number | null =>
      'unknown' in u ? null : u.tokens;
    expect(fold(unmeasured.usage)).toBeNull();
    expect(fold(measuredZero.usage)).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// FlowConfig — the parsed flow.yaml: a set of stations keyed by id (SPEC §4).
// ---------------------------------------------------------------------------

describe('FlowConfig (flow.yaml schema)', () => {
  it('holds a versioned set of stations keyed by station id', () => {
    const flow: FlowConfig = {
      version: 1,
      stations: {
        brief: { kind: 'transform', effectful: false, wip: 1, inputs: ['intake'], outputs: ['draft'] },
        build: {
          kind: 'agentic',
          effectful: true,
          wip: 4,
          inputs: ['draft'],
          outputs: ['artifact'],
          check: 'qc',
        },
      },
    };

    expect(flow.version).toBe(1);
    // Each station id resolves to a StationConfig the dispatcher can classify.
    expect(dispatchClass(flow.stations.brief.kind)).toBe('single-call');
    expect(dispatchClass(flow.stations.build.kind)).toBe('tool-loop');
    expect(flow.stations.build.check).toBe('qc');
  });
});
