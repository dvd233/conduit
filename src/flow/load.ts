/**
 * flow.yaml loader and validator (WI-292, WI-351).
 *
 * Implements FR-1 / Principle 10 — "config is validated, not trusted."
 * A valid flow.yaml is loaded synchronously into a frozen FlowConfig.
 * Any semantic error returns a structured LoadFlowResult with no FlowConfig.
 *
 * Validations (fail-closed, all errors collected before returning):
 *   1. Cyclic depends_on (SPEC §9 — DAG invariant)
 *   2. on_reject target references an unknown lane
 *   3. Overlapping owned_paths (outputs) across stations (SPEC §9)
 *   4. hold_timeout_seconds set without on_timeout (SPEC §4A)
 *   5. Worker kind must be valid when a worker is present (worker-kind validation work)
 *   6. Numeric config fields: wip ≥ 1, fan_out ≥ 1, rework_cap ≥ 0 (#15)
 *   7. Quorum fan-in k: integer 1 <= k <= fan_out (#4)
 *   8. depends_on references only known stations (#13)
 *
 * WI-351 additions (only for stations with a declared `next` field):
 *   9.  UNKNOWN_NEXT_TARGET — next names a non-station, non-terminal
 *   10. MISSING_DETERMINISTIC_COMMAND — deterministic station has no command
 *   11. COMMAND_NOT_ALLOWLISTED — command absent from security.bash.allow
 *   12. MISSING_PROMPT_TEMPLATE — model station has no prompt_file
 *   13. MISSING_PROMPT_VERSION — model station has no prompt_version
 *   14. Prompt file not on disk (worker and gate-critic), named in error
 *   15. MISSING_OUTPUT_SCHEMA — model station has no output_schema
 *   16. UNDECLARED_PROMPT_INPUT — prompt {{ref}} not in station's declared inputs
 */

import { readFileSync, existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname, join, isAbsolute, resolve } from 'node:path';
import { parse } from 'yaml';
import type {
  FlowConfig,
  FlowDefaults,
  FlowBudgets,
  FlowChannels,
  FlowEgressChannel,
  StationConfig,
  StationGateConfig,
  StationKind,
  FanInPolicyConfig,
  StationRankConfig,
  ImageInputDeclaration,
  StationDeliverConfig,
} from '../types/kernel';
import { findCycleNodes } from './dag-utils';
import { resolveSkill, type ResolveSkillResult } from '../skills/resolve';
import type { ParsedSkill } from '../skills/parse';
import { detectExecutionSurface, type ExecutionSurfaceWarning } from '../skills/detect-surface';
import { adapterCanExpressTools, type HarnessRegistry } from '../worker/harness-adapter';

// ---------------------------------------------------------------------------
// Public contract
// ---------------------------------------------------------------------------

export interface FlowValidationError {
  /** Stable machine-readable code for programmatic handling. */
  code: string;
  /** Human-readable description naming the offending entity. */
  message: string;
}

export type LoadFlowResult =
  | { ok: true; flow: FlowConfig; warnings?: ExecutionSurfaceWarning[] }
  | { ok: false; errors: FlowValidationError[] };

/** Default skills directory, next to the flow.yaml — matches src/skills/resolve.ts. */
const DEFAULT_SKILLS_DIR = 'skills';

/** Default per-station injected skill-content cap in bytes (WI-556) — 64 KiB. */
const DEFAULT_SKILL_CONTENT_MAX_BYTES = 65536;

/**
 * Maximum number of image inputs a transform/gate station may declare per call
 * (WI-414, AC3). Shared with the WI-420 call-time payload guard — a single
 * configured ceiling enforced at both load time and runtime.
 */
export const MAX_IMAGE_INPUTS_PER_CALL = 5;

// ---------------------------------------------------------------------------
// Raw YAML shapes (internal — not exported)
// ---------------------------------------------------------------------------

/** Parsed worker block from a station in flow.yaml. */
interface RawWorker {
  kind?: string;
  role?: string;
  model?: string;
  /** Deterministic station executable. */
  command?: string;
  /** Argument vector for the command. */
  args?: string[];
  /**
   * Per-station wall-clock timeout in seconds for a deterministic worker
   * (punch-list #8). Untrusted: validated to a positive integer (>= 1) in
   * collectErrors before buildStationConfig copies it onto the StationConfig.
   */
  timeout_seconds?: unknown;
  /** Path to the model station's prompt template (relative to flow.yaml dir). */
  prompt_file?: string;
  /** Version stamp of the prompt template. */
  prompt_version?: string | number;
  /** Inference parameters for the model call. */
  params?: Record<string, unknown>;
  /** Declared output schema for a model station's typed result. */
  output_schema?: RawOutputSchema;
  /** Names of SKILL.md bundles (skills/<name>) this worker consumes (WI-554/555). */
  uses?: string[];
  /** Adapter name for a `kind: harness` station (WI-559/563). */
  harness?: string;
  /** Child flow.yaml path for a `kind: subflow` station (the original multi-flow engine work). */
  flow?: string;
  /** Tools allowlist for a harness station's underlying agent-CLI. */
  tools?: string[];
  /** Optional waiver opting a harness station out of the `tools` allowlist. */
  unrestricted_tools?: boolean;
}

interface RawOutputSchema {
  fields?: RawOutputField[];
}

interface RawOutputField {
  name?: string;
  type?: string;
  required?: boolean;
}

/** Parsed critic block inside a station's check config. */
interface RawCritic {
  role?: string;
  model?: string;
  prompt_file?: string;
  prompt_version?: string | number;
  /** Adapter name for an agentic (harness) critic (WI-570). */
  harness?: string;
  /** Tools allowlist for an agentic (harness) critic's invocation (WI-595). */
  tools?: string[];
  /**
   * Wall-clock bound for an agentic (harness) critic's invocation, in seconds.
   * Absent -> the engine's DEFAULT_HARNESS_CRITIC_TIMEOUT_MS (5 minutes). A
   * critic re-reading long-form inputs (e.g. a full podcast transcript) needs
   * more than the default; this mirrors worker.timeout_seconds for makers.
   */
  timeout_seconds?: unknown;
}

interface RawCheckConfig {
  kind?: string;
  class?: string;
  critic?: RawCritic;
  on_reject?: string;
  /** Untrusted: validated to an integer >= 0 in collectErrors. */
  rework_cap?: unknown;
  progress_signal?: string;
  /** WI-398: noSelectionPolicy for rank stations. */
  no_selection_policy?: string;
  /** WI-398: optional hold-timeout policy for rank stations. */
  on_timeout?: string;
  /** The original HITL reply-and-resume work: station artifact holding flow-computed candidates (rank only). */
  candidates_from?: unknown;
  /** The original HITL reply-and-resume work: ask-message template path, rendered with the station's inputs (rank only). */
  ask_template?: unknown;
  /** The original HITL reply-and-resume work: file(s) uploaded with the ask (rank only). */
  ask_attach?: unknown;
  /** The original HITL reply-and-resume work: artifact path the kernel writes the recorded selection to (rank only). */
  selection_out?: unknown;
}

interface RawStation {
  id: string;
  worker?: RawWorker;
  effectful?: boolean;
  /** Untrusted: validated to an integer >= 1 in collectErrors. */
  wip?: unknown;
  inputs?: string[];
  outputs?: string[];
  check?: RawCheckConfig;
  /** fan_in can be a plain count (e.g. 2), a bare policy name string, or a policy object {policy, k}. */
  fan_in?: number | string | { policy?: string; k?: unknown };
  /** Untrusted: validated to an integer >= 1 in collectErrors. */
  fan_out?: unknown;
  depends_on?: string[];
  /**
   * Declared happy-path successor lane for this station (WI-351, FR-2).
   * When set, used to build FlowConfig.happyPathNext — never inferred from
   * station insertion order.
   */
  next?: string;
  /**
   * Branching topology fields (WI-393). Declared on a fan-out station.
   * Validated fail-closed in collectErrors before the FlowConfig is assembled.
   */
  child_entry?: string;
  child_terminal?: string;
  resume_at?: string;
  /** Fan-out cache-warming stagger, in seconds. Validated in collectErrors. */
  child_stagger_seconds?: unknown;
  /** Output write scope ('project_root' | 'owned_dir'). Validated in collectErrors. */
  output_scope?: unknown;
  /**
   * YAML image_inputs list (WI-414). Typed as `unknown[]` so collectErrors can
   * validate each entry's shape before buildStationConfig casts them safely.
   */
  image_inputs?: unknown[];
  /**
   * YAML deliver block (WI-596). Typed as `unknown` so collectErrors can
   * validate its shape (non-empty files list, delivery-capable egress channel)
   * before buildStationConfig casts it onto StationConfig.deliver.
   */
  deliver?: unknown;
}

/** Parsed security block from flow.yaml. */
interface RawSecurity {
  bash?: {
    allow?: string[];
    deny_shell_metachars?: boolean;
  };
  network_egress?: string;
}

interface RawYaml {
  flow?: string;
  project_root?: string;
  flow_version?: number;
  budgets?: FlowBudgets;
  defaults?: {
    cap_policy?: string;
    on_dep_scrap?: string;
    enforce_owned_paths?: boolean;
    concurrency?: unknown;
    /** Per-station aggregate injected skill-content cap in bytes (WI-556); default 65536 (64 KiB). */
    skill_content_max_bytes?: unknown;
    /** Per-run filesystem workspace (the original ingress-attribution work step 2): 'per_run' | 'shared'. */
    workspace?: unknown;
  };
  terminal_lanes?: string[];
  stations?: RawStation[];
  channels?: FlowChannels;
  security?: RawSecurity;
  prerequisites?: unknown;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const VALID_STATION_KINDS = ['deterministic', 'transform', 'agentic', 'harness', 'subflow'] as const;

/**
 * Legal values for a rank station's check.no_selection_policy (WI-398).
 * Mirrors VALID_ON_TIMEOUT's fail-closed pattern: an unrecognised value is
 * rejected at load (a typo must NOT silently coerce to the 'scrap' default).
 * Absent is legal and defaults to 'scrap' (see buildStationConfig).
 */
const VALID_NO_SELECTION_POLICY = ['scrap', 'proceed_with_findings'] as const;

/**
 * Resolve a station's execution kind.
 *
 * - No worker key present → check-only station, defaults to 'transform'.
 * - Worker present with a valid kind → that kind.
 * - Worker present with an INVALID kind → null (caller emits a load error;
 *   a typo like `tranform` must NOT silently become `transform`, per worker-kind validation work).
 */
function resolveStationKind(raw: RawStation): StationKind | null {
  if (raw.worker === undefined) {
    // Check-only station (no worker) — legitimately defaults to transform.
    return 'transform';
  }
  const kind = raw.worker.kind;
  if (kind === undefined) {
    // Worker block present but no explicit kind — treat as transform.
    return 'transform';
  }
  if ((VALID_STATION_KINDS as readonly string[]).includes(kind)) {
    return kind as StationKind;
  }
  return null;
}

/**
 * Normalise a raw fan_in (number | {policy, k}) into a structured FanInConfig.
 * Called only after collectErrors has passed, so a quorum's k is a valid count.
 */
function buildFanIn(raw: RawStation['fan_in']): FanInPolicyConfig | number | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw === 'number') return raw;
  // Bare string shorthand: `fan_in: best_effort` or `fan_in: all`.
  if (typeof raw === 'string') {
    if (raw === 'all') return { policy: 'all' };
    if (raw === 'best_effort') return { policy: 'best_effort' };
    return undefined;
  }
  const policy = raw.policy;
  if (policy === 'quorum') {
    return { policy: 'quorum', k: raw.k as number };
  }
  if (policy === 'all') return { policy: 'all' };
  if (policy === 'best_effort') return { policy: 'best_effort' };
  // Unknown/absent policy with a k → treat k as a bare count.
  return raw.k as number | undefined;
}

/**
 * Per-loadFlow-call memoization for resolveSkill. A single flow load resolves
 * each worker.uses skill up to three times (validation in collectErrors,
 * composition in buildStationConfig, execution-surface warnings in loadFlow)
 * with an identical (name, flowDir, skillsDir) input each time — flowDir and
 * skillsDir are constant across one loadFlow call, so caching by name alone is
 * sound within a single call and avoids re-reading + re-parsing bundles (and
 * large reference corpora) from disk redundantly.
 */
type SkillResolutionCache = Map<string, ResolveSkillResult>;

function resolveSkillCached(
  cache: SkillResolutionCache,
  name: string,
  flowDir: string,
  skillsDir: string,
): ResolveSkillResult {
  const cached = cache.get(name);
  if (cached !== undefined) return cached;
  const resolved = resolveSkill(name, flowDir, { skillsDir });
  cache.set(name, resolved);
  return resolved;
}

/**
 * Build a StationConfig from a raw station.
 *
 * Precondition: collectErrors has already validated this station (valid worker
 * kind, integer wip/fan_out, valid quorum k), so the casts below are sound.
 *
 * `flowDir` is the directory containing the flow.yaml, used to resolve
 * prompt_file paths to absolute paths for runtime use.
 */
function buildStationConfig(
  raw: RawStation,
  flowDir: string,
  hasHitlEgress: boolean = false,
  skillCache: SkillResolutionCache = new Map(),
): StationConfig {
  const config: StationConfig = {
    kind: resolveStationKind(raw) ?? 'transform',
    effectful: raw.effectful ?? false,
    wip: (raw.wip as number | undefined) ?? 1,
    inputs: raw.inputs ?? [],
    outputs: raw.outputs ?? [],
    fan_in: buildFanIn(raw.fan_in),
    fan_out: raw.fan_out as number | undefined,
  };

  // WI-351: real-run config surface (populated when present in the YAML).
  if (raw.next !== undefined) config.next = raw.next;

  const w = raw.worker;
  if (w?.role !== undefined) config.role = w.role;
  if (w?.model !== undefined) config.model = w.model;
  if (w?.command !== undefined) config.command = w.command;
  if (w?.args !== undefined) config.args = w.args;
  // Punch-list #8 — copy the (already-validated) deterministic timeout. Cast is
  // sound: collectErrors rejected any non-positive-integer value before here.
  if (w?.timeout_seconds !== undefined) config.timeout_seconds = w.timeout_seconds as number;
  if (w?.prompt_file !== undefined) config.prompt_file = join(flowDir, w.prompt_file);
  if (w?.prompt_version !== undefined) config.prompt_version = String(w.prompt_version);
  if (w?.params !== undefined) config.params = w.params;
  // WI-563: lift harness fields (adapter name, tools allowlist, waiver) onto the
  // flat StationConfig — mirrors the model/prompt fields above.
  if (w?.harness !== undefined) config.harness = w.harness;
  // The original multi-flow engine work: lift the subflow child-flow reference, resolved to an absolute
  // path against the parent flow's directory (mirrors prompt_file resolution).
  if (w?.flow !== undefined) config.flow = isAbsolute(w.flow) ? w.flow : resolve(flowDir, w.flow);
  if (w?.tools !== undefined) config.tools = w.tools;
  if (w?.unrestricted_tools !== undefined) config.unrestricted_tools = w.unrestricted_tools;

  // WI-555: compose worker.uses into prompt_content — each resolved skill's
  // injected content in declared order, then the station's local prompt last
  // (the station-local override layer). Precondition: collectErrors has
  // already validated every entry resolves (UNRESOLVED_USES) and rejected
  // duplicates/non-LLM stations before buildStationConfig ever runs.
  if (w?.uses !== undefined && w.uses.length > 0) {
    const skillContents = w.uses.map((name) => {
      const resolved = resolveSkillCached(skillCache, name, flowDir, DEFAULT_SKILLS_DIR);
      return resolved.ok ? resolved.skill.injectedContent : '';
    });
    const localPrompt = w.prompt_file !== undefined ? readFileSync(join(flowDir, w.prompt_file), 'utf-8') : '';
    config.prompt_content = [...skillContents, localPrompt].filter((s) => s.length > 0).join('\n\n');
    // WI-557: per-skill content hashes, in worker.uses order (order matters —
    // NOT sorted), so the executor can fold them into the binding stamp
    // without re-reading bundles at dispatch time.
    config.skill_content_hashes = skillContents.map((content) => createHash('sha256').update(content).digest('hex'));
  }
  if (w?.output_schema?.fields !== undefined) {
    config.output_schema = {
      fields: w.output_schema.fields.map((f) => ({
        name: f.name ?? '',
        type: f.type ?? '',
        required: f.required ?? false,
      })),
    };
  }

  // WI-393: branching topology routing targets (optional — present only on fan-out stations).
  if (raw.child_entry !== undefined) config.child_entry = raw.child_entry;
  if (raw.child_terminal !== undefined) config.child_terminal = raw.child_terminal;
  if (raw.resume_at !== undefined) config.resume_at = raw.resume_at;
  if (raw.child_stagger_seconds !== undefined) config.child_stagger_seconds = raw.child_stagger_seconds as number;
  // v10: output write scope (collectErrors has validated the enum + transform-only rule).
  if (raw.output_scope !== undefined) config.output_scope = raw.output_scope as 'project_root' | 'owned_dir';

  // WI-414: image inputs (optional — ABSENT when not declared, never an empty array).
  // collectErrors has already validated each entry is a non-null object with a string
  // path, so the cast below is sound. Paths are stored verbatim (no join/resolution —
  // unlike prompt_file, resolution is deferred to WI-419).
  if (Array.isArray(raw.image_inputs) && raw.image_inputs.length > 0) {
    config.image_inputs = raw.image_inputs as ImageInputDeclaration[];
  }

  // WI-596: deliver block (optional — ABSENT when not declared, never an empty
  // object). collectErrors has already validated a non-empty files list and a
  // delivery-capable egress channel, so the cast below is sound. Declared file
  // paths are stored VERBATIM — unlike prompt_file, they are not resolved to
  // absolute here (the executor, WI-599, resolves them against the card's
  // owned paths at delivery time).
  if (raw.deliver !== null && typeof raw.deliver === 'object') {
    const rawDeliver = raw.deliver as Record<string, unknown>;
    const deliver: StationDeliverConfig = { files: rawDeliver.files as string[] };
    if (rawDeliver.thread_from !== undefined) deliver.thread_from = rawDeliver.thread_from as string;
    if (rawDeliver.caption !== undefined) deliver.caption = rawDeliver.caption as string;
    config.deliver = deliver;
  }

  const chk = raw.check;

  // WI-398: rank-check config surface (check.kind === 'rank').
  // Parsed before gateCheck so a rank station's check block is never treated
  // as a pass/fail quality gate — the two are mutually exclusive in semantics.
  if (chk?.kind === 'rank' && (chk.critic || typeof chk.candidates_from === 'string')) {
    const noSelectionPolicy = chk.no_selection_policy;
    const rankCheck: StationRankConfig = {
      // The original HITL reply-and-resume work: in candidates_from mode there is no critic — the model/
      // prompt fields stay '' (their absence is validated in collectErrors;
      // the executor discriminates on candidatesFrom, never on '' strings).
      criticModel: chk.critic?.model ?? '',
      criticPromptFile: chk.critic?.prompt_file ? join(flowDir, chk.critic.prompt_file) : '',
      criticPromptVersion: String(chk.critic?.prompt_version ?? ''),
      // hitlEnabled iff the flow declares an egress channel that `uses: [hitl]`
      // — a human gate, not merely any egress channel.
      hitlEnabled: hasHitlEgress,
      // Default to 'scrap' when no_selection_policy is absent or unrecognised.
      noSelectionPolicy:
        noSelectionPolicy === 'proceed_with_findings' ? 'proceed_with_findings' : 'scrap',
    };
    // The original HITL reply-and-resume work surfaces — collectErrors has already validated the shapes, so
    // the casts below are sound. candidates_from/selection_out are stored
    // VERBATIM (project-root-relative, resolved by the executor at runtime);
    // ask_template resolves against the flow dir like criticPromptFile.
    if (typeof chk.candidates_from === 'string') {
      rankCheck.candidatesFrom = chk.candidates_from;
    }
    if (typeof chk.ask_template === 'string') {
      rankCheck.askTemplateFile = join(flowDir, chk.ask_template);
    }
    if (Array.isArray(chk.ask_attach) && chk.ask_attach.length > 0) {
      rankCheck.askAttach = chk.ask_attach as string[];
    }
    if (typeof chk.selection_out === 'string') {
      rankCheck.selectionOut = chk.selection_out;
    }
    config.rankCheck = rankCheck;
    // A rank station is NOT a gate station — do not also populate gateCheck.
    return config;
  }

  // WI-356: gate-check config surface — resolved so the executor does not re-read the YAML.
  if (chk?.critic) {
    const gateCheck: StationGateConfig = {
      criticModel: chk.critic.model ?? '',
      criticPromptFile: chk.critic.prompt_file ? join(flowDir, chk.critic.prompt_file) : '',
      criticPromptVersion: String(chk.critic.prompt_version ?? ''),
      onReject: chk.on_reject ?? '',
      reworkCap: (chk.rework_cap as number | undefined) ?? 0,
      criticInputScope: [...(raw.inputs ?? []), ...(raw.outputs ?? [])],
    };
    // WI-442: preserve the critic's human-readable role label (omit when absent).
    if (chk.critic.role !== undefined) gateCheck.criticRole = chk.critic.role;
    // WI-570: an agentic (harness) critic (omit when absent, never a false '').
    //
    // criticModel is NOT mutually exclusive with this, despite what this
    // comment used to claim. Issue #26 AC4 made a harness critic's model
    // meaningful: check.critic.model now reaches HarnessInvocation.model with
    // station-over-adapter precedence (FR-10), exactly as the maker path's
    // `stationConfig.model ?? harnessAdapter.model` does. Declaring both is
    // the SUPPORTED way to pin a critic to a specific model while letting the
    // adapter supply the deployment default everywhere else.
    //
    // Note the '' sentinel above: an absent critic model is the EMPTY STRING,
    // not undefined, so the precedence check downstream must treat '' as
    // absent — `criticModel ?? adapter.model` would wrongly pick ''.
    if (chk.critic.harness !== undefined) gateCheck.criticHarness = chk.critic.harness;
    // WI-595: the critic's tools allowlist, threaded to its harness invocation.
    if (chk.critic.tools !== undefined) gateCheck.criticTools = chk.critic.tools;
    // Harness-critic wall-clock bound (validated positive integer in
    // collectErrors) — threaded to runHarnessGateCheck in ms; absent falls
    // back to the engine default there.
    if (chk.critic.timeout_seconds !== undefined) {
      gateCheck.criticTimeoutMs = (chk.critic.timeout_seconds as number) * 1000;
    }
    config.gateCheck = gateCheck;
  }

  return config;
}

// ---------------------------------------------------------------------------
// Template scanning (WI-351)
// ---------------------------------------------------------------------------

/**
 * Extract all `{{artifact}}` references from a prompt template.
 * Returns a deduplicated set of artifact names.
 */
function extractTemplateRefs(template: string): Set<string> {
  const refs = new Set<string>();
  const TEMPLATE_REF_REGEX = /\{\{([^}]+)\}\}/g;
  let match: RegExpExecArray | null;
  while ((match = TEMPLATE_REF_REGEX.exec(template)) !== null) {
    refs.add(match[1]!.trim());
  }
  return refs;
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

function collectErrors(
  yaml: RawYaml,
  flowDir: string,
  skillCache: SkillResolutionCache = new Map(),
  harnessRegistry?: HarnessRegistry,
): FlowValidationError[] {
  const errors: FlowValidationError[] = [];
  const stations = yaml.stations ?? [];
  const stationIds = new Set(stations.map((s) => s.id));
  const terminalLanes = new Set(yaml.terminal_lanes ?? []);

  // terminal_lanes, when explicitly declared, must be non-empty. A flow with no
  // terminal lanes has no exits (cards can never reach done/scrap/hold), and an
  // empty list also breaks the runnable-card query downstream (`lane NOT IN ()`
  // is a SQL syntax error). Absent is fine — consumers default to done/scrap/hold.
  if (Array.isArray(yaml.terminal_lanes) && yaml.terminal_lanes.length === 0) {
    errors.push({
      code: 'EMPTY_TERMINAL_LANES',
      message:
        'terminal_lanes is declared but empty — a flow must declare at least one ' +
        'terminal lane (or omit terminal_lanes to default to done/scrap/hold)',
    });
  }

  // Pre-compute the set of station ids that are targets of at least one back-edge
  // (i.e. some station's check.on_reject === that id, including self-loops).
  // Used by the FEEDBACK_WITHOUT_BACK_EDGE check below (NFR-6).
  const backEdgeTargets = new Set<string>();
  for (const station of stations) {
    const onReject = station.check?.on_reject;
    if (onReject !== undefined) {
      backEdgeTargets.add(onReject);
    }
  }

  // ── 1. Cycle detection on depends_on (SPEC §9) ───────────────────────────
  const dependsOnMap = new Map<string, readonly string[]>();
  for (const station of stations) {
    dependsOnMap.set(station.id, station.depends_on ?? []);
  }

  const cycleNodes = findCycleNodes(Array.from(stationIds), dependsOnMap);
  if (cycleNodes.size > 0) {
    const names = Array.from(cycleNodes).sort().join(', ');
    errors.push({
      code: 'CYCLIC_DEPENDS_ON',
      message: `Cyclic dependency detected among stations: ${names}`,
    });
  }

  // ── 1a. Cycle detection on forward `next` topology ───────────────────────
  // Only runs when at least one station declares `next` (WI-351 flows). A cycle
  // in happyPathNext means every station in the cycle is someone else's successor
  // — no entry station exists, and run seeding from lane 'intake' silently
  // dispatches nothing. Back-edges (check.on_reject) are declared separately and
  // are NOT part of this forward graph, so valid rework loops are never flagged.
  const stationsWithNext = stations.filter((s) => s.next !== undefined);
  if (stationsWithNext.length > 0) {
    const nextMap = new Map<string, string>(
      stationsWithNext.map((s) => [s.id, s.next!]),
    );
    const cycleNextNodes = findNextCycleNodes(nextMap, stationIds, terminalLanes);
    if (cycleNextNodes.size > 0) {
      const names = Array.from(cycleNextNodes).sort().join(', ');
      errors.push({
        code: 'CYCLIC_NEXT',
        message: `Cycle detected in forward next topology among stations: ${names}`,
      });
    }
  }

  // ── 2. on_reject target must be a known station or terminal lane ──────────
  for (const station of stations) {
    const target = station.check?.on_reject;
    if (target !== undefined) {
      if (!stationIds.has(target) && !terminalLanes.has(target)) {
        errors.push({
          code: 'UNKNOWN_ON_REJECT_TARGET',
          message:
            `Station '${station.id}' references unknown on_reject target '${target}' — ` +
            `it is neither a station id nor a terminal lane`,
        });
      }
    }
  }

  // ── 3. Overlapping owned_paths (outputs must be disjoint) ─────────────────
  const outputToOwner = new Map<string, string>();
  for (const station of stations) {
    for (const output of (station.outputs ?? [])) {
      const existing = outputToOwner.get(output);
      if (existing !== undefined) {
        errors.push({
          code: 'OVERLAPPING_OWNED_PATHS',
          message:
            `Stations '${existing}' and '${station.id}' both claim output path '${output}' — ` +
            `owned paths must be disjoint (SPEC §9)`,
        });
      } else {
        outputToOwner.set(output, station.id);
      }
    }
  }

  // ── 4. hold_timeout_seconds requires on_timeout (SPEC §4A) ────────────────
  const VALID_ON_TIMEOUT = ['scrap', 'proceed_with_findings', 'escalate'] as const;
  // `?? []` only guards null/undefined; a non-array egress (e.g. a YAML mapping)
  // would make `for...of` throw, escaping loadFlow's {ok,errors} contract.
  // Treat a malformed egress as "no channels" (config validated, not trusted).
  const rawEgressForValidation = yaml.channels?.egress;
  for (const channel of (Array.isArray(rawEgressForValidation) ? rawEgressForValidation : [])) {
    if (channel.hold_timeout_seconds !== undefined && !channel.on_timeout) {
      errors.push({
        code: 'HOLD_TIMEOUT_WITHOUT_ON_TIMEOUT',
        message:
          `Egress channel '${channel.type ?? 'unknown'}' sets hold_timeout_seconds ` +
          `but is missing the required on_timeout field (SPEC §4A)`,
      });
    }
    // Fail-closed: an unrecognised on_timeout must be rejected at load, not
    // silently no-op'd at timeout (the executor only honours the three policies).
    if (channel.on_timeout !== undefined && !(VALID_ON_TIMEOUT as readonly string[]).includes(channel.on_timeout)) {
      errors.push({
        code: 'INVALID_ON_TIMEOUT',
        message:
          `Egress channel '${channel.type ?? 'unknown'}' has an invalid on_timeout ` +
          `'${channel.on_timeout}' — must be one of: ${VALID_ON_TIMEOUT.join(', ')} (SPEC §4A)`,
      });
    }
  }

  // ── 5. Worker kind must be valid when a worker is present (worker-kind validation work) ────────────
  // A typo (kind: tranform) must NOT silently default to transform.
  for (const station of stations) {
    if (station.worker !== undefined && station.worker.kind !== undefined) {
      const kind = station.worker.kind;
      if (!(VALID_STATION_KINDS as readonly string[]).includes(kind)) {
        errors.push({
          code: 'INVALID_WORKER_KIND',
          message:
            `Station '${station.id}' has an invalid worker.kind '${kind}' — ` +
            `must be one of: ${VALID_STATION_KINDS.join(', ')}`,
        });
      }
    }
  }

  // ── 6. Numeric config fields must be sane (#15) ───────────────────────────
  // wip ≥ 1 (wip:0 deadlocks every routed card), fan_out ≥ 1, rework_cap ≥ 0,
  // each an integer (YAML strings / floats are rejected).
  for (const station of stations) {
    if (station.wip !== undefined && !isIntInRange(station.wip, 1)) {
      errors.push({
        code: 'INVALID_WIP',
        message:
          `Station '${station.id}' has invalid wip '${String(station.wip)}' — ` +
          `must be an integer >= 1`,
      });
    }
    if (station.fan_out !== undefined && !isIntInRange(station.fan_out, 1)) {
      errors.push({
        code: 'INVALID_FAN_OUT',
        message:
          `Station '${station.id}' has invalid fan_out '${String(station.fan_out)}' — ` +
          `must be an integer >= 1`,
      });
    }
    // Fan-out cache-warming stagger (optional): a non-negative integer (seconds),
    // and only meaningful on a fan-out station (needs child_entry to have siblings
    // to stagger). 0 is allowed as an explicit "no stagger".
    if (station.child_stagger_seconds !== undefined) {
      if (!isIntInRange(station.child_stagger_seconds, 0)) {
        errors.push({
          code: 'INVALID_CHILD_STAGGER_SECONDS',
          message:
            `Station '${station.id}' has invalid child_stagger_seconds '${String(station.child_stagger_seconds)}' — ` +
            `must be an integer >= 0 (seconds)`,
        });
      } else if (station.child_entry === undefined) {
        errors.push({
          code: 'INVALID_CHILD_STAGGER_SECONDS',
          message:
            `Station '${station.id}' sets child_stagger_seconds but is not a fan-out station ` +
            `(no child_entry) — the stagger only applies to spawned children`,
        });
      }
    }
    // v10: output_scope — enum-valued, transform-only (the engine only writes
    // declared outputs for transform stations; deterministic/harness stations
    // write their own files), and meaningless without declared outputs.
    if (station.output_scope !== undefined) {
      if (station.output_scope !== 'project_root' && station.output_scope !== 'owned_dir') {
        errors.push({
          code: 'INVALID_OUTPUT_SCOPE',
          message:
            `Station '${station.id}' has invalid output_scope '${String(station.output_scope)}' — ` +
            `must be 'project_root' or 'owned_dir'`,
        });
      } else if (station.worker?.kind !== 'transform') {
        errors.push({
          code: 'INVALID_OUTPUT_SCOPE',
          message:
            `Station '${station.id}' sets output_scope but its worker kind is ` +
            `'${String(station.worker?.kind)}' — output_scope applies only to transform stations ` +
            `(the engine writes declared outputs only for transforms)`,
        });
      } else if (!Array.isArray(station.outputs) || station.outputs.length === 0) {
        errors.push({
          code: 'INVALID_OUTPUT_SCOPE',
          message:
            `Station '${station.id}' sets output_scope but declares no outputs — ` +
            `the scope only governs where declared outputs are written`,
        });
      }
    }
    const reworkCap = station.check?.rework_cap;
    if (reworkCap !== undefined && !isIntInRange(reworkCap, 0)) {
      errors.push({
        code: 'INVALID_REWORK_CAP',
        message:
          `Station '${station.id}' has invalid rework_cap '${String(reworkCap)}' — ` +
          `must be an integer >= 0`,
      });
    }
    // Punch-list #8 — deterministic timeout_seconds, when present, must be a
    // positive integer (>= 1). A value of 0 / negative / float / non-number is a
    // foot-gun (0 = "kill instantly") and is rejected fail-closed at load.
    const timeoutSeconds = station.worker?.timeout_seconds;
    if (timeoutSeconds !== undefined && !isIntInRange(timeoutSeconds, 1)) {
      errors.push({
        code: 'INVALID_TIMEOUT_SECONDS',
        message:
          `Station '${station.id}' has invalid timeout_seconds '${String(timeoutSeconds)}' — ` +
          `must be an integer >= 1`,
      });
    }
    // Same foot-gun guard for the harness critic's bound (mirrors the worker
    // timeout: 0 = "kill instantly" is rejected fail-closed at load).
    const criticTimeoutSeconds = station.check?.critic?.timeout_seconds;
    if (criticTimeoutSeconds !== undefined && !isIntInRange(criticTimeoutSeconds, 1)) {
      errors.push({
        code: 'INVALID_TIMEOUT_SECONDS',
        message:
          `Station '${station.id}' has invalid check.critic.timeout_seconds ` +
          `'${String(criticTimeoutSeconds)}' — must be an integer >= 1`,
      });
    }
    // The bound is consumed ONLY by a harness critic's invocation — a
    // model-based critic call has no wall-clock surface, so a declared
    // timeout would be silently dropped. Config is validated, not trusted:
    // declared-but-ignored config is rejected at load (same posture as the
    // image_inputs non-transform rejection below).
    if (criticTimeoutSeconds !== undefined && station.check?.critic?.harness === undefined) {
      errors.push({
        code: 'CRITIC_TIMEOUT_REQUIRES_HARNESS',
        message:
          `Station '${station.id}' sets check.critic.timeout_seconds but its critic ` +
          `declares no harness — the bound applies only to harness-critic invocations ` +
          `and would be silently ignored; remove it or set check.critic.harness`,
      });
    }
  }

  // ── 7. Quorum fan-in: k is a COUNT, integer 1 <= k <= fan_out (#4) ────────
  for (const station of stations) {
    const fanIn = station.fan_in;
    if (fanIn !== undefined && typeof fanIn === 'object' && fanIn.policy === 'quorum') {
      const k = fanIn.k;
      // fan_out bounds the survivor count. Absent fan_out → unbounded upper.
      const upper = isIntInRange(station.fan_out, 1) ? (station.fan_out as number) : undefined;
      const kIsCount = isIntInRange(k, 1);
      const withinUpper = upper === undefined || (kIsCount && (k as number) <= upper);
      if (!kIsCount || !withinUpper) {
        errors.push({
          code: 'INVALID_QUORUM_K',
          message:
            `Station '${station.id}' has invalid quorum k '${String(k)}' — ` +
            `must be an integer count with 1 <= k <= fan_out` +
            (upper !== undefined ? ` (fan_out=${upper})` : ''),
        });
      }
    }
  }

  // ── 7a. Unknown bare-string fan_in values ────────────────────────────────────
  // 'all' and 'best_effort' are the only valid bare-string shorthand values.
  // An unrecognised string (e.g. a typo) silently returns undefined in buildFanIn,
  // so we fail-closed here — same pattern as INVALID_FAN_OUT.
  for (const station of stations) {
    const fanIn = station.fan_in;
    if (typeof fanIn === 'string' && fanIn !== 'all' && fanIn !== 'best_effort') {
      errors.push({
        code: 'INVALID_FAN_IN',
        message:
          `Station '${station.id}' has unknown fan_in shorthand '${fanIn}' — ` +
          `valid bare-string values are 'all' and 'best_effort'`,
      });
    }
  }

  // ── 8. Station-level depends_on must reference a known station (#13) ───────
  // Mirrors the on_reject validation: a station depending on a non-existent
  // station id is rejected rather than silently parking the card in waiting.
  for (const station of stations) {
    for (const dep of (station.depends_on ?? [])) {
      if (!stationIds.has(dep)) {
        errors.push({
          code: 'UNKNOWN_DEPENDS_ON',
          message:
            `Station '${station.id}' depends_on unknown station '${dep}' — ` +
            `it is not a defined station id`,
        });
      }
    }
  }

  // ── 8d. worker.uses composition validation (WI-555, FR-2/FR-8) ────────────
  // Unconditional — unlike the WI-351 block below, this does NOT require the
  // station to declare `next`; worker.uses is validated on every station.
  for (const station of stations) {
    const uses = station.worker?.uses;
    if (uses === undefined || uses.length === 0) continue;

    const kind = resolveStationKind(station);

    // FR-8: a deterministic station has no prompt to inject skill content into.
    if (kind === 'deterministic') {
      errors.push({
        code: 'USES_ON_NON_LLM',
        message:
          `Station '${station.id}' is kind=deterministic but declares worker.uses — ` +
          `skills are instruction content and a deterministic station has no prompt to inject them into`,
      });
      continue;
    }

    // DUPLICATE_USES — the same skill named twice in one station would double-inject.
    const seenUses = new Set<string>();
    for (const name of uses) {
      if (seenUses.has(name)) {
        errors.push({
          code: 'DUPLICATE_USES',
          message: `Station '${station.id}' declares skill '${name}' more than once in worker.uses`,
        });
      }
      seenUses.add(name);
    }

    // UNRESOLVED_USES — "config is validated, not trusted": composition treats
    // an unresolvable/typo'd skill name as a hard load error, unlike WI-554's
    // execution-surface-warning pass (which silently skips it — a warning, not
    // a composition input).
    const resolvedSkills: Array<{ name: string; skill: ParsedSkill }> = [];
    for (const name of uses) {
      const resolved = resolveSkillCached(skillCache, name, flowDir, DEFAULT_SKILLS_DIR);
      if (!resolved.ok) {
        errors.push({
          code: 'UNRESOLVED_USES',
          message:
            `Station '${station.id}' worker.uses names skill '${name}' which could not be resolved: ` +
            resolved.error.message,
        });
      } else {
        resolvedSkills.push({ name, skill: resolved.skill });
      }
    }

    // UNDECLARED_PROMPT_INPUT on skill-injected content — a skill's body or
    // references/ may itself contain '{{...}}'-looking prose that has nothing
    // to do with Conduit's template dialect (a third-party skill author
    // documenting an unrelated templating syntax, e.g. "write {{user_name}}").
    // Left unchecked, that text passes load cleanly and only surfaces as an
    // opaque runtime crash from renderPrompt()'s scope guard, once the
    // composed prompt_content reaches the real executor. Scanning it here
    // converts that into the same load-time error every other declared-input
    // mismatch already gets — "config is validated, not trusted" extends to
    // skill-injected content, not just the station's own local prompt.
    const declaredInputs = new Set(station.inputs ?? []);
    for (const { name, skill } of resolvedSkills) {
      for (const ref of extractTemplateRefs(skill.injectedContent)) {
        if (!declaredInputs.has(ref)) {
          errors.push({
            code: 'UNDECLARED_PROMPT_INPUT',
            message:
              `Station '${station.id}' worker.uses skill '${name}' content references placeholder ` +
              `'{{${ref}}}' which is not in the station's declared inputs ` +
              `([${(station.inputs ?? []).join(', ')}]) — likely unrelated syntax in the skill's own ` +
              `prose rather than a Conduit artifact reference; declare '${ref}' as a station input if intended`,
          });
        }
      }
    }

    // SKILL_CONTENT_CAP_EXCEEDED (WI-556, FR-5) — a per-station AGGREGATE cap
    // on total injected skill content (bodies + references, summed across every
    // skill in worker.uses; description does not count, matching §2.4). Default
    // 64 KiB, overridable via defaults.skill_content_max_bytes. Measured on the
    // assembled injected content (byte length, not UTF-16 code units) so an
    // eager-concat transform station can never be silently bloated.
    const capBytes =
      typeof yaml.defaults?.skill_content_max_bytes === 'number'
        ? yaml.defaults.skill_content_max_bytes
        : DEFAULT_SKILL_CONTENT_MAX_BYTES;
    const totalBytes = resolvedSkills.reduce(
      (sum, { skill }) => sum + Buffer.byteLength(skill.injectedContent, 'utf8'),
      0,
    );
    if (resolvedSkills.length > 0 && totalBytes > capBytes) {
      const perSkillBreakdown = resolvedSkills
        .map(({ name, skill }) => {
          const fileSizes = [
            `body: ${Buffer.byteLength(skill.body, 'utf8')} bytes`,
            ...skill.references.map(
              (ref) => `references/${ref.name}: ${Buffer.byteLength(ref.content, 'utf8')} bytes`,
            ),
          ];
          return `skill '${name}' (${fileSizes.join(', ')})`;
        })
        .join('; ');
      errors.push({
        code: 'SKILL_CONTENT_CAP_EXCEEDED',
        message:
          `Station '${station.id}' worker.uses injects ${totalBytes} bytes of skill content, exceeding the ` +
          `${capBytes}-byte cap (defaults.skill_content_max_bytes) — per-skill breakdown: ${perSkillBreakdown}`,
      });
    }
  }

  // ── WI-393: branching topology routing targets ────────────────────────────
  // Only fires when the field is present (linear flows declare none; the new
  // validations must NOT fire on absent fields).
  //   child_entry   → must name a known station id
  //   resume_at     → must name a known station id OR a terminal lane
  //   child_terminal → must name a terminal lane (a station id is rejected)
  for (const station of stations) {
    if (station.child_entry !== undefined && !stationIds.has(station.child_entry)) {
      errors.push({
        code: 'UNKNOWN_CHILD_ENTRY',
        message:
          `Station '${station.id}' child_entry '${station.child_entry}' is not a known station id`,
      });
    }

    if (
      station.resume_at !== undefined &&
      !stationIds.has(station.resume_at) &&
      !terminalLanes.has(station.resume_at)
    ) {
      errors.push({
        code: 'UNKNOWN_RESUME_AT',
        message:
          `Station '${station.id}' resume_at '${station.resume_at}' is neither a known station id nor a terminal lane`,
      });
    }

    if (station.child_terminal !== undefined && !terminalLanes.has(station.child_terminal)) {
      errors.push({
        code: 'INVALID_CHILD_TERMINAL',
        message:
          `Station '${station.id}' child_terminal '${station.child_terminal}' is not a terminal lane — must name a terminal lane, not a station id`,
      });
    }
  }

  // ── 8a. defaults.cap_policy / defaults.on_dep_scrap — legal value check ───
  // Only the two documented string values are accepted for each field.  A typo
  // (e.g. `cap_policy: scrapp`) is rejected at load so the executor never sees
  // an unrecognised policy string.
  const LEGAL_CAP_POLICIES = ['scrap', 'proceed_with_findings'] as const;
  const LEGAL_ON_DEP_SCRAP = ['scrap', 'hold'] as const;

  const rawCapPolicy = yaml.defaults?.cap_policy;
  if (rawCapPolicy !== undefined && !(LEGAL_CAP_POLICIES as readonly string[]).includes(rawCapPolicy)) {
    errors.push({
      code: 'INVALID_CAP_POLICY',
      message:
        `defaults.cap_policy '${rawCapPolicy}' is not a valid cap policy — ` +
        `must be one of: ${LEGAL_CAP_POLICIES.join(', ')}`,
    });
  }

  const rawOnDepScrap = yaml.defaults?.on_dep_scrap;
  if (rawOnDepScrap !== undefined && !(LEGAL_ON_DEP_SCRAP as readonly string[]).includes(rawOnDepScrap)) {
    errors.push({
      code: 'INVALID_ON_DEP_SCRAP',
      message:
        `defaults.on_dep_scrap '${rawOnDepScrap}' is not a valid dep-scrap policy — ` +
        `must be one of: ${LEGAL_ON_DEP_SCRAP.join(', ')}`,
    });
  }

  // ── 8a-ext. defaults.concurrency — integer >= 1 ───────────────────────────
  const rawConcurrency = yaml.defaults?.concurrency;
  if (rawConcurrency !== undefined && !isIntInRange(rawConcurrency, 1)) {
    errors.push({
      code: 'INVALID_CONCURRENCY',
      message:
        `defaults.concurrency '${String(rawConcurrency)}' is invalid — ` +
        `must be an integer >= 1`,
    });
  }

  // ── 8a-ext2. defaults.skill_content_max_bytes — integer >= 1 (WI-556) ────
  const rawSkillContentMaxBytes = yaml.defaults?.skill_content_max_bytes;
  if (rawSkillContentMaxBytes !== undefined && !isIntInRange(rawSkillContentMaxBytes, 1)) {
    errors.push({
      code: 'INVALID_SKILL_CONTENT_MAX_BYTES',
      message:
        `defaults.skill_content_max_bytes '${String(rawSkillContentMaxBytes)}' is invalid — ` +
        `must be an integer >= 1`,
    });
  }

  // ── 8a-ext3. defaults.workspace — legal value check (the original ingress-attribution work step 2) ───
  const LEGAL_WORKSPACE = ['per_run', 'shared'] as const;
  const rawWorkspace = yaml.defaults?.workspace;
  if (
    rawWorkspace !== undefined &&
    !(LEGAL_WORKSPACE as readonly unknown[]).includes(rawWorkspace)
  ) {
    errors.push({
      code: 'INVALID_WORKSPACE',
      message:
        `defaults.workspace '${String(rawWorkspace)}' is not a valid workspace mode — ` +
        `must be one of: ${LEGAL_WORKSPACE.join(', ')}`,
    });
  }

  // ── 8b. prerequisites — fail-closed type check ───────────────────────────────
  // A present `prerequisites` must be an array of strings. Any other shape
  // (bare string, number, mapping) is rejected at load so the executor and
  // build tooling never receive a silently coerced value.  Every entry is
  // checked so a valid leading string cannot mask a later non-string.
  const rawPrerequisites = yaml.prerequisites;
  if (rawPrerequisites !== undefined) {
    if (!Array.isArray(rawPrerequisites)) {
      errors.push({
        code: 'INVALID_PREREQUISITES',
        message:
          `'prerequisites' must be an array of strings, got ${JSON.stringify(rawPrerequisites)}`,
      });
    } else {
      // Each entry is interpolated into `RUN apt-get install -y <entry>` in the
      // generated Dockerfile (conduit build), so an entry carrying shell
      // metacharacters, whitespace, a leading dash (apt option injection), or an
      // empty value is a command-injection vector. Fail closed: every entry must
      // be a plausible Debian package name — starts with [a-z0-9], then only
      // [a-z0-9 + . _ : -] (none of which are shell metacharacters). Mirrors the
      // INVALID_CAP_POLICY fail-closed pattern; rejected at load so the build
      // tooling never renders a hostile value.
      const SAFE_PACKAGE_NAME = /^[a-z0-9][a-z0-9+._:-]*$/;
      for (const entry of rawPrerequisites) {
        if (typeof entry !== 'string') {
          errors.push({
            code: 'INVALID_PREREQUISITES',
            message:
              `'prerequisites' entries must be strings — found non-string entry: ${JSON.stringify(entry)}`,
          });
        } else if (!SAFE_PACKAGE_NAME.test(entry)) {
          errors.push({
            code: 'INVALID_PREREQUISITES',
            message:
              `'prerequisites' entries must be valid package names matching ${SAFE_PACKAGE_NAME} ` +
              `(no shell metacharacters, spaces, leading dash, or empty values) — found: ${JSON.stringify(entry)}`,
          });
        }
      }
    }
  }

  // ── 8c. Rank station no_selection_policy — fail-closed value check ─────────
  // Mirrors INVALID_ON_TIMEOUT: an unrecognised no_selection_policy is rejected
  // at load rather than silently coerced to 'scrap'. Absent is legal (defaults
  // to 'scrap' in buildStationConfig); only a present-but-unknown value errors.
  for (const station of stations) {
    const noSelectionPolicy = station.check?.no_selection_policy;
    if (
      noSelectionPolicy !== undefined &&
      !(VALID_NO_SELECTION_POLICY as readonly string[]).includes(noSelectionPolicy)
    ) {
      errors.push({
        code: 'INVALID_NO_SELECTION_POLICY',
        message:
          `Station '${station.id}' has an invalid no_selection_policy ` +
          `'${noSelectionPolicy}' — must be one of: ${VALID_NO_SELECTION_POLICY.join(', ')}`,
      });
    }
  }

  // ── 8d. Rank station candidate source — the original HITL reply-and-resume work, fail-closed ─────────────
  // A rank check has exactly ONE candidate source: a critic block OR a
  // flow-computed `candidates_from` artifact. Both is ambiguous (whose ranking
  // wins?); the HITL work surfaces on a non-rank check are configuration noise that
  // must not silently no-op.
  for (const station of stations) {
    const chk = station.check;
    if (chk === undefined) continue;

    if (chk.kind === 'rank') {
      if (chk.critic !== undefined && chk.candidates_from !== undefined) {
        errors.push({
          code: 'RANK_CANDIDATES_AND_CRITIC',
          message:
            `Station '${station.id}' declares BOTH check.critic and ` +
            `check.candidates_from — a rank station has exactly one candidate ` +
            `source (the original HITL reply-and-resume work)`,
        });
      }
      if (
        chk.candidates_from !== undefined &&
        (typeof chk.candidates_from !== 'string' || chk.candidates_from.trim().length === 0)
      ) {
        errors.push({
          code: 'INVALID_RANK_CANDIDATES_FROM',
          message:
            `Station '${station.id}' check.candidates_from must be a non-empty ` +
            `artifact path string`,
        });
      }
      if (chk.ask_template !== undefined) {
        if (typeof chk.ask_template !== 'string' || chk.ask_template.trim().length === 0) {
          errors.push({
            code: 'INVALID_RANK_ASK_TEMPLATE',
            message: `Station '${station.id}' check.ask_template must be a non-empty path string`,
          });
        } else {
          const resolvedAskPath = join(flowDir, chk.ask_template);
          if (!existsSync(resolvedAskPath)) {
            errors.push({
              code: 'RANK_ASK_TEMPLATE_NOT_FOUND',
              message:
                `Station '${station.id}' check.ask_template '${chk.ask_template}' ` +
                `does not exist on disk at '${resolvedAskPath}'`,
            });
          } else {
            // Same scope rule as a worker prompt: the ask may reference only
            // the station's declared inputs — reject undeclared placeholders
            // at load, not at ask time in front of a waiting human.
            const askTemplate = readFileSync(resolvedAskPath, 'utf-8');
            const askRefs = extractTemplateRefs(askTemplate);
            const declared = new Set(station.inputs ?? []);
            for (const ref of askRefs) {
              if (!declared.has(ref)) {
                errors.push({
                  code: 'RANK_ASK_UNDECLARED_INPUT',
                  message:
                    `Station '${station.id}' check.ask_template references artifact ` +
                    `'${ref}' which is not in the station's declared inputs ` +
                    `([${(station.inputs ?? []).join(', ')}])`,
                });
              }
            }
          }
        }
      }
      if (chk.ask_attach !== undefined) {
        const attach = chk.ask_attach;
        const entriesValid =
          Array.isArray(attach) &&
          attach.length > 0 &&
          attach.every((f) => typeof f === 'string' && f.trim().length > 0);
        if (!entriesValid) {
          errors.push({
            code: 'INVALID_RANK_ASK_ATTACH',
            message:
              `Station '${station.id}' check.ask_attach must be a non-empty ` +
              `list of file path strings`,
          });
        }
      }
      if (
        chk.selection_out !== undefined &&
        (typeof chk.selection_out !== 'string' || chk.selection_out.trim().length === 0)
      ) {
        errors.push({
          code: 'INVALID_RANK_SELECTION_OUT',
          message:
            `Station '${station.id}' check.selection_out must be a non-empty ` +
            `artifact path string`,
        });
      }
    } else {
      // HITL work keys on a non-rank check: reject rather than silently ignore.
      for (const key of ['candidates_from', 'ask_template', 'ask_attach', 'selection_out'] as const) {
        if (chk[key] !== undefined) {
          errors.push({
            code: 'RANK_SURFACE_ON_NON_RANK_CHECK',
            message:
              `Station '${station.id}' declares check.${key} but check.kind is ` +
              `'${chk.kind ?? 'gate'}' — ${key} is a rank-station surface (the original HITL reply-and-resume work)`,
          });
        }
      }
    }
  }

  // ── WI-351: real-run config surface (only for stations with `next` declared) ──
  //
  // Stations without a `next` field are not yet wired into the declared topology
  // and therefore skip these validations (preserves pre-WI-351 fixture compatibility).
  const bashAllowList = yaml.security?.bash?.allow;

  for (const station of stations) {
    if (station.next === undefined) continue;

    // 9. UNKNOWN_NEXT_TARGET ─────────────────────────────────────────────────
    const allLanes = new Set([...stationIds, ...(yaml.terminal_lanes ?? [])]);
    if (!allLanes.has(station.next)) {
      errors.push({
        code: 'UNKNOWN_NEXT_TARGET',
        message:
          `Station '${station.id}' declares next '${station.next}' which is ` +
          `neither a known station id nor a terminal lane`,
      });
    }

    const kind = resolveStationKind(station);

    if (kind === 'deterministic') {
      const command = station.worker?.command;

      // 10. MISSING_DETERMINISTIC_COMMAND ──────────────────────────────────
      if (!command) {
        errors.push({
          code: 'MISSING_DETERMINISTIC_COMMAND',
          message:
            `Station '${station.id}' is kind=deterministic but has no worker.command ` +
            `(required before any spawn)`,
        });
      } else if (bashAllowList !== undefined && !bashAllowList.includes(command)) {
        // 11. COMMAND_NOT_ALLOWLISTED ────────────────────────────────────────
        errors.push({
          code: 'COMMAND_NOT_ALLOWLISTED',
          message:
            `Station '${station.id}' command '${command}' is not in security.bash.allow ` +
            `(required before any spawn)`,
        });
      }
    } else if (kind === 'subflow') {
      // The original multi-flow engine work: a subflow station's "worker" is a child flow, not a model —
      // no prompt/schema surface applies. Its own requirement (worker.flow,
      // resolvable, acyclic, depth-capped) is validated transitively by
      // validateSubflowReferences in loadFlow.
    } else {
      // Model station (transform / agentic)
      // WI-398: rank stations have no worker prompt — the critic is rankCheck, not
      // a worker. Skip the MISSING_PROMPT_TEMPLATE check for rank stations.
      if (station.check?.kind === 'rank') {
        // Rank check-only station: no worker prompt required. Skip validations
        // 12–16 (all worker-prompt checks) so a rank station with `next` doesn't
        // trigger MISSING_PROMPT_TEMPLATE (the rank critic is a separate concern).
      } else {
      const promptFile = station.worker?.prompt_file;

      // 12. MISSING_PROMPT_TEMPLATE ────────────────────────────────────────
      if (!promptFile) {
        errors.push({
          code: 'MISSING_PROMPT_TEMPLATE',
          message: `Station '${station.id}' is kind=${kind} but has no worker.prompt_file`,
        });
      } else {
        const resolvedPromptPath = join(flowDir, promptFile);

        // 14. Prompt file must exist on disk at load time ─────────────────
        if (!existsSync(resolvedPromptPath)) {
          errors.push({
            code: 'PROMPT_FILE_NOT_FOUND',
            message:
              `Station '${station.id}' worker.prompt_file '${promptFile}' does not exist ` +
              `on disk at '${resolvedPromptPath}'`,
          });
        } else {
          // 16. UNDECLARED_PROMPT_INPUT (worker scope = station inputs only) ─
          const template = readFileSync(resolvedPromptPath, 'utf-8');
          const refs = extractTemplateRefs(template);
          const declaredInputs = new Set(station.inputs ?? []);
          for (const ref of refs) {
            if (!declaredInputs.has(ref)) {
              errors.push({
                code: 'UNDECLARED_PROMPT_INPUT',
                message:
                  `Station '${station.id}' worker prompt_file references artifact '${ref}' ` +
                  `which is not in the station's declared inputs ` +
                  `([${(station.inputs ?? []).join(', ')}])`,
              });
            }
          }

          // NFR-6: FEEDBACK_WITHOUT_BACK_EDGE ──────────────────────────────────
          // The 'feedback' input is synthetic — it can ONLY be supplied when a
          // back-edge (check.on_reject) routes a rejected card back to this
          // station. A maker that declares {{feedback}} but is never reachable
          // via a back-edge will always see feedback as absent; that is a config
          // error caught here, not silently at runtime.
          //
          // 'feedback' is the reserved synthetic input name (see render.ts
          // FEEDBACK_INPUT / WI-379). Self-loops (on_reject: self → self) count
          // as a valid back-edge and are accepted (dogfood / ideate shape).
          const FEEDBACK_RESERVED = 'feedback';
          if (refs.has(FEEDBACK_RESERVED) && !backEdgeTargets.has(station.id)) {
            errors.push({
              code: 'FEEDBACK_WITHOUT_BACK_EDGE',
              message:
                `Station '${station.id}' prompt template references the feedback input ` +
                `but station '${station.id}' is not a back-edge target ` +
                `(no check.on_reject points to it) — ` +
                `the feedback input can never be supplied`,
            });
          }
        }
      }

      // 13. MISSING_PROMPT_VERSION ─────────────────────────────────────────
      if (!station.worker?.prompt_version) {
        errors.push({
          code: 'MISSING_PROMPT_VERSION',
          message: `Station '${station.id}' is kind=${kind} but has no worker.prompt_version`,
        });
      }

      // 15. MISSING_OUTPUT_SCHEMA ──────────────────────────────────────────
      if (!station.worker?.output_schema) {
        errors.push({
          code: 'MISSING_OUTPUT_SCHEMA',
          message: `Station '${station.id}' is kind=${kind} but has no worker.output_schema`,
        });
      }
      } // end of !rank-station else block
    }

    // Gate-critic prompt validation (station's check.critic.prompt_file) ────
    const criticPromptFile = station.check?.critic?.prompt_file;
    if (criticPromptFile) {
      const resolvedCriticPath = join(flowDir, criticPromptFile);

      if (!existsSync(resolvedCriticPath)) {
        errors.push({
          code: 'PROMPT_FILE_NOT_FOUND',
          message:
            `Station '${station.id}' gate-critic prompt_file '${criticPromptFile}' does not ` +
            `exist on disk at '${resolvedCriticPath}'`,
        });
      } else {
        // 16. UNDECLARED_PROMPT_INPUT (critic scope = station inputs + outputs) ─
        const template = readFileSync(resolvedCriticPath, 'utf-8');
        const refs = extractTemplateRefs(template);
        const criticScope = new Set([...(station.inputs ?? []), ...(station.outputs ?? [])]);
        for (const ref of refs) {
          if (!criticScope.has(ref)) {
            errors.push({
              code: 'UNDECLARED_PROMPT_INPUT',
              message:
                `Station '${station.id}' gate-critic prompt_file references artifact '${ref}' ` +
                `which is not in the station's declared inputs+outputs`,
            });
          }
        }
      }
    }
  }

  // ── WI-563: harness station validation ───────────────────────────────────
  // Review #3: runs unconditionally for EVERY kind:harness station, regardless
  // of whether the station declares `next` (load-time config validation, like
  // the image_inputs block below). Previously this lived inside the
  // `next !== undefined` loop above, so a harness station without `next`
  // skipped fail-closed load validation and instead escalated to hold at
  // first dispatch — inconsistent with the fail-closed-at-load contract.
  for (const station of stations) {
    if (resolveStationKind(station) !== 'harness') continue;

    // Review #5: a harness maker's typed result is read from its FIRST
    // declared outputs entry (the executor's `stationConfig.outputs[0]`).
    // With no outputs entry the payload can never be read, so every attempt
    // scraps under a misleading 'harness-output-unparseable … (none declared)'
    // reason — reject the config at load instead.
    if ((station.outputs ?? []).length === 0) {
      errors.push({
        code: 'HARNESS_MISSING_OUTPUTS',
        message:
          `Station '${station.id}' is kind=harness but declares no outputs — ` +
          `a harness station must declare at least one outputs entry (the file ` +
          `it writes its typed result to)`,
      });
    }

    // Adapter + tools-expressibility validation: only judged against an
    // injected registry (WI-560) — callers that don't pass one skip these
    // checks (loadFlow's registry param is optional).
    if (harnessRegistry === undefined) continue;

    const adapterName = station.worker?.harness;
    if (adapterName === undefined) {
      errors.push({
        code: 'UNKNOWN_HARNESS_ADAPTER',
        message: `Station '${station.id}' is kind=harness but declares no worker.harness adapter name`,
      });
    } else {
      const resolved = harnessRegistry.resolve(adapterName);
      if (!resolved.ok) {
        // resolved.error already names the requested adapter (WI-560 registry contract).
        errors.push({
          code: 'UNKNOWN_HARNESS_ADAPTER',
          message: `Station '${station.id}': ${resolved.error}`,
        });
      } else {
        const tools = station.worker?.tools ?? [];
        const waived = station.worker?.unrestricted_tools === true;
        // The original per-list tool-expression work: judged per-list (adapterCanExpressTools) — an adapter whose
        // containment is a capability lattice (codex-exec sandbox envelopes)
        // expresses SOME lists; boolean-only adapters keep today's behavior.
        if (tools.length > 0 && !adapterCanExpressTools(resolved.adapter, tools) && !waived) {
          errors.push({
            code: 'HARNESS_TOOLS_UNEXPRESSIBLE',
            message:
              `Station '${station.id}' declares a tools allowlist that adapter ` +
              `'${resolved.adapter.name}' cannot narrow/express — ` +
              `set unrestricted_tools: true to waive this check`,
          });
        }
      }
    }
  }

  // ── WI-595: harness CRITIC tools-expressibility validation ──────────────
  // Mirrors the maker's HARNESS_TOOLS_UNEXPRESSIBLE guard above: a gate
  // critic's declared tools allowlist is threaded into its harness
  // invocation (quality/gate.ts), so an adapter that cannot restrict/narrow
  // tools must fail this at LOAD, never silently receive an unrestricted
  // invocation at runtime. Unlike the maker, the critic's harness NAME is
  // resolved lazily at dispatch (gate-rework.ts) rather than validated here
  // for UNKNOWN_HARNESS_ADAPTER (WI-590) — this check only fires when the
  // name IS resolvable, layering on top without changing that boundary.
  //
  // DELIBERATE ASYMMETRY (decided, not an omission): unlike the maker's
  // check, this one has NO `unrestricted_tools`-style waiver. The maker's
  // waiver is a flow-author escape hatch for a WORK station; a harness
  // critic IS the quality gate itself — its verdict must be produced under
  // known containment. "Can't express tool restriction" + "run unrestricted
  // anyway" would hand an adversarial critic arbitrary tools on an adapter
  // that cannot constrain them. Fail-closed at load (operator picks a
  // tool-restricting adapter for critics, e.g. claude-headless) is the
  // correct posture. A concrete need for a critic waiver is a future PRD
  // decision, not something to add speculatively here.
  //
  // RULING (RetroLearning row 16, operator-ruled): a harness gate critic MUST
  // declare a NON-EMPTY criticTools allowlist — an omitted/empty list is a
  // LOAD-TIME error (HARNESS_CRITIC_TOOLS_REQUIRED), never a silently-
  // unrestricted critic. Rationale: the critic IS the quality gate itself, so
  // its verdict must be produced under KNOWN containment. The explicit-waiver
  // path was already ruled out (no unrestricted_tools waiver for critics — see
  // the DELIBERATE ASYMMETRY note above), so the omission path must not grant
  // what the waiver path forbids. Consequence (intentional): harness critics
  // require an adapter that can express the declared list — per-tool-name
  // (claude-headless) or, since the original per-list tool-expression work, a capability-lattice adapter whose
  // OS sandbox envelope matches the list exactly (codex-exec via
  // planCodexSandbox). Lists no envelope matches still fail with
  // HARNESS_TOOLS_UNEXPRESSIBLE below.
  if (harnessRegistry !== undefined) {
    for (const station of stations) {
      const criticHarnessName = station.check?.critic?.harness;
      if (criticHarnessName === undefined) continue;
      const criticTools = station.check?.critic?.tools ?? [];

      if (criticTools.length === 0) {
        errors.push({
          code: 'HARNESS_CRITIC_TOOLS_REQUIRED',
          message:
            `Station '${station.id}' configures a gate critic via harness adapter ` +
            `'${criticHarnessName}' but declares no criticTools — a harness critic ` +
            `must declare an explicit, non-empty tool allowlist so its verdict is ` +
            `produced under known containment (there is no unrestricted-tools waiver ` +
            `for critics; pick a canRestrictTools adapter such as claude-headless)`,
        });
        continue;
      }

      const resolved = harnessRegistry.resolve(criticHarnessName);
      if (!resolved.ok) continue; // unresolvable name: a runtime concern for critics (WI-590), not re-validated here

      // The original per-list tool-expression work: per-list judgment. The critic containment guarantee is
      // unchanged — the verdict is still produced under KNOWN containment;
      // for a lattice adapter that containment is an OS-enforced sandbox
      // envelope matching the declared list's capability classes exactly.
      if (!adapterCanExpressTools(resolved.adapter, criticTools)) {
        errors.push({
          code: 'HARNESS_TOOLS_UNEXPRESSIBLE',
          message:
            `Station '${station.id}' declares a gate-critic tools allowlist that adapter ` +
            `'${resolved.adapter.name}' cannot narrow/express`,
        });
      }
    }
  }

  // ── WI-414: image_inputs validation (AC1, AC3, AC4) ─────────────────────
  // Runs unconditionally for every station that declares image_inputs, regardless
  // of whether the station has a `next` field (load-time config validation, not
  // a real-run surface check). Capability is NOT checked here (Resolved Q1).
  for (const station of stations) {
    const rawImageInputs = station.image_inputs;
    if (rawImageInputs === undefined) continue;

    // Fail-closed: image_inputs are consumed ONLY by the transform worker
    // (executeTransformStation attaches them to the model call). A station with
    // no worker block (check-only / gate critic) or a non-transform worker
    // (deterministic = no model call; agentic image attach is not wired yet)
    // would silently drop a declared image — a config foot-gun. Reject it at
    // load so the author sees the gap instead of debugging missing images.
    // (An invalid worker.kind resolves to null and is reported separately, so
    // we skip the unsupported-station error in that case to avoid piling on.)
    const resolvedKind = resolveStationKind(station);
    const consumesImages = station.worker !== undefined && resolvedKind === 'transform';
    if (!consumesImages && resolvedKind !== null) {
      const where =
        station.worker === undefined
          ? 'is a check-only station (no worker)'
          : `is kind=${resolvedKind}`;
      errors.push({
        code: 'UNSUPPORTED_IMAGE_INPUTS_ON_STATION',
        message:
          `Station '${station.id}' declares image_inputs but ${where} — ` +
          `image inputs are only consumed by a transform worker and would be ` +
          `silently dropped here; remove image_inputs or change the station to a transform worker`,
      });
      continue;
    }

    // Guard: image_inputs MUST be a list when present. Any non-array value —
    // including scalar null (`image_inputs: null` in YAML) and empty
    // `image_inputs:` (also parses to null) — is a malformed declaration.
    // This guard MUST precede the for...of loop: iterating over null throws
    // TypeError, breaking the {ok, errors} contract of loadFlow.
    if (!Array.isArray(rawImageInputs)) {
      errors.push({
        code: 'INVALID_IMAGE_INPUTS',
        message:
          `Station '${station.id}' image_inputs must be a list — ` +
          `got a non-list value; omit the key entirely when no image inputs are declared`,
      });
      continue;
    }

    // AC1: each entry must be a non-null object carrying a `path` string.
    // Reject bare strings, null, numbers, and objects missing a string `path`.
    let allEntriesValid = true;
    for (const entry of rawImageInputs) {
      const isValidEntry =
        entry !== null &&
        typeof entry === 'object' &&
        typeof (entry as Record<string, unknown>).path === 'string';

      if (!isValidEntry) {
        errors.push({
          code: 'INVALID_IMAGE_INPUT_ENTRY',
          message:
            `Station '${station.id}' has an invalid image_inputs entry — ` +
            `each entry must be an object carrying a 'path' string ` +
            `(got: ${JSON.stringify(entry)})`,
        });
        allEntriesValid = false;
      }
    }

    // AC3: bounded list — count must not exceed the per-call maximum.
    if (rawImageInputs.length > MAX_IMAGE_INPUTS_PER_CALL) {
      errors.push({
        code: 'TOO_MANY_IMAGE_INPUTS',
        message:
          `Station '${station.id}' declares ${rawImageInputs.length} image inputs, ` +
          `which exceeds the per-call maximum of ${MAX_IMAGE_INPUTS_PER_CALL}`,
      });
    }

    // AC4: image path must not collide with a declared text input name.
    // Only check entries that passed AC1 (have a valid string path) to avoid
    // producing spurious collision errors for already-rejected entries.
    if (allEntriesValid) {
      const textInputNames = new Set(station.inputs ?? []);
      const seenPaths = new Set<string>();
      for (const entry of rawImageInputs) {
        const path = (entry as { path: string }).path;
        if (textInputNames.has(path)) {
          errors.push({
            code: 'IMAGE_INPUT_PATH_COLLISION',
            message:
              `Station '${station.id}' image input path '${path}' collides with ` +
              `a declared text input name — image inputs and text inputs must be distinct`,
          });
        }
        // A path listed twice would be loaded, hashed (double stamp
        // contribution), and uploaded twice — almost certainly a config typo.
        if (seenPaths.has(path)) {
          errors.push({
            code: 'DUPLICATE_IMAGE_INPUT',
            message:
              `Station '${station.id}' declares image input path '${path}' more than once — ` +
              `each image input path must be unique within a station`,
          });
        }
        seenPaths.add(path);
      }
    }
  }

  // ── WI-596: deliver block validation (AC2, AC5, FR-13) ───────────────────
  // A `deliver` block must name at least one file, and the flow must declare a
  // delivery-capable egress channel (resolveDeliveryChannel is the single
  // source of truth for what counts as delivery-capable — a hitl-only egress
  // set must not be silently hijacked). `thread_from` naming a field the
  // runtime substrate can't supply is deliberately NOT validated here — the
  // same flow may be Slack- or CLI-triggered (FR-8, decision 6).
  for (const station of stations) {
    const rawDeliver = station.deliver;
    if (rawDeliver === undefined) continue;

    const deliverObj =
      rawDeliver !== null && typeof rawDeliver === 'object'
        ? (rawDeliver as Record<string, unknown>)
        : undefined;
    const files = deliverObj?.files;

    if (!Array.isArray(files) || files.length === 0) {
      errors.push({
        code: 'EMPTY_DELIVER_FILES',
        message:
          `Station '${station.id}' declares a deliver block with no files — ` +
          `deliver.files must be a non-empty list`,
      });
    } else if (files.some((f) => typeof f !== 'string')) {
      // config-is-validated-not-trusted (mirrors INVALID_IMAGE_INPUT_ENTRY /
      // INVALID_PREREQUISITES): every declared file must be a string path —
      // buildStationConfig casts this list `as string[]` and WI-599's executor
      // reads each entry as a path, so a non-string here must be rejected at
      // load rather than shipped through to trip at runtime.
      errors.push({
        code: 'INVALID_DELIVER_FILE_ENTRY',
        message:
          `Station '${station.id}' deliver.files must contain only strings — ` +
          `found a non-string entry`,
      });
    } else {
      // Egress containment (SPEC §7): a delivered file is READ and shipped to
      // an external service, so a `deliver.files` entry must stay under the
      // project root. Reject an absolute path or a `..`-escape at LOAD — the
      // executor's runtime check (deliverPathWithinRoot) also resolves symlinks,
      // but a static literal escape should never reach dispatch. projectRoot is
      // per-run, so containment is checked lexically here (relative to '.') and
      // symlink-resolved at runtime against the real root.
      for (const f of files as string[]) {
        const escapes = isAbsolute(f) || !resolve('/root', f).startsWith('/root/');
        if (escapes) {
          errors.push({
            code: 'DELIVER_FILE_ESCAPES_ROOT',
            message:
              `Station '${station.id}' deliver.files entry '${f}' must be a ` +
              `project-root-relative path — absolute paths and '..' escapes are ` +
              `rejected (delivery reads and ships the file to an external service)`,
          });
        }
      }
    }

    const caption = deliverObj?.caption;
    if (caption !== undefined && typeof caption !== 'string') {
      errors.push({
        code: 'INVALID_DELIVER_CAPTION',
        message: `Station '${station.id}' deliver.caption must be a string when present`,
      });
    }

    const threadFrom = deliverObj?.thread_from;
    if (threadFrom !== undefined && typeof threadFrom !== 'string') {
      errors.push({
        code: 'INVALID_DELIVER_THREAD_FROM',
        message: `Station '${station.id}' deliver.thread_from must be a string when present`,
      });
    }

    if (resolveDeliveryChannel(yaml.channels?.egress) === undefined) {
      errors.push({
        code: 'NO_DELIVERY_CHANNEL',
        message:
          `Station '${station.id}' declares a deliver block but the flow has no ` +
          `delivery-capable egress channel — declare an egress channel with ` +
          `uses: [delivery] (or a single egress channel with no uses declared) ` +
          `so the delivery has a destination`,
      });
    }
  }

  return errors;
}

/**
 * Resolve the flow's delivery-capable egress channel (WI-596, FR-2). Pure —
 * mirrors the hitl `uses:` derivation at loadFlow's hasHitlEgress computation.
 *
 * Returns:
 *   - the channel whose `uses` includes 'delivery'; else
 *   - the FIRST channel when NO channel declares any `uses` (fallback — a
 *     flow with a single, unqualified egress channel is delivery-capable by
 *     default); else
 *   - undefined (some channel declares `uses` but none includes 'delivery' —
 *     e.g. a hitl-only egress set — or there is no egress at all).
 */
export function resolveDeliveryChannel(
  egress: FlowEgressChannel[] | undefined,
): FlowEgressChannel | undefined {
  if (!Array.isArray(egress) || egress.length === 0) return undefined;

  const delivery = egress.find((ch) => Array.isArray(ch.uses) && ch.uses.includes('delivery'));
  if (delivery !== undefined) return delivery;

  const anyChannelDeclaresUses = egress.some((ch) => Array.isArray(ch.uses) && ch.uses.length > 0);
  return anyChannelDeclaresUses ? undefined : egress[0];
}

/** True iff `value` is an integer >= `min`. Rejects strings, floats, NaN. */
function isIntInRange(value: unknown, min: number): boolean {
  return typeof value === 'number' && Number.isInteger(value) && value >= min;
}

/**
 * Detect nodes that participate in a cycle in the forward `next` topology.
 *
 * Each entry in `nextMap` is a directed edge `station → next`. Edges pointing
 * to terminal lanes are exits, not part of any cycle. Because the graph is
 * functional (at most one outgoing edge per node), we can detect cycles by
 * following chains from each unvisited node and checking for revisits within
 * the same traversal path.
 *
 * @param nextMap      - Map of station id → declared `next` target.
 * @param stationIds   - All station ids in the flow (the valid node set).
 * @param terminalLanes - Terminal lane names (treated as exits, not cycle nodes).
 * @returns Set of station ids that participate in at least one cycle.
 */
function findNextCycleNodes(
  nextMap: ReadonlyMap<string, string>,
  stationIds: ReadonlySet<string>,
  terminalLanes: ReadonlySet<string>,
): Set<string> {
  const cycleNodes = new Set<string>();
  // 'done': fully explored, confirmed acyclic path from here.
  // 'active': currently on the traversal stack (may be in a cycle).
  const state = new Map<string, 'active' | 'done'>();

  for (const startNode of nextMap.keys()) {
    if (state.has(startNode)) continue;

    // Walk the chain from startNode, collecting the path in order.
    const path: string[] = [];
    const pathIndex = new Map<string, number>(); // node → index in path

    let current: string | undefined = startNode;
    while (current !== undefined) {
      // Stop at terminals or nodes not in the station set.
      if (!stationIds.has(current) || terminalLanes.has(current)) break;

      if (state.get(current) === 'done') {
        // Already confirmed acyclic from here — safe to stop.
        break;
      }

      if (pathIndex.has(current)) {
        // We've revisited a node on the current path → cycle found.
        // Mark every node from the re-entry point to the end of the path.
        const cycleStart = pathIndex.get(current)!;
        for (let i = cycleStart; i < path.length; i++) {
          cycleNodes.add(path[i]!);
        }
        // The re-entry point itself is in the cycle.
        cycleNodes.add(current);
        break;
      }

      pathIndex.set(current, path.length);
      path.push(current);
      state.set(current, 'active');

      current = nextMap.get(current);
    }

    // Mark all nodes on this path as done (acyclic, or cycle already recorded).
    for (const node of path) {
      state.set(node, 'done');
    }
  }

  return cycleNodes;
}

// ---------------------------------------------------------------------------
// Public factory
// ---------------------------------------------------------------------------

/**
 * Load and validate a flow.yaml file synchronously.
 *
 * Returns `{ ok: true, flow }` with a frozen FlowConfig on success, or
 * `{ ok: false, errors }` (no `flow` property) if any semantic rule fails.
 */
/** Maximum subflow nesting depth (the original multi-flow engine work): a top-level flow plus this many
 * levels of children. Deep towers of flows are a smell — the cap keeps a
 * runaway reference chain a load error instead of a runtime surprise. */
const SUBFLOW_MAX_DEPTH = 3;

/**
 * Validate every `kind: subflow` station's child-flow reference (the original multi-flow engine work).
 *
 * Each reference is resolved against the parent flow's directory and loaded
 * TRANSITIVELY (the recursive loadFlow call carries the ancestor chain), so:
 *   - a missing `flow:` field, an unresolvable path, or an invalid child flow
 *     rejects the parent (SUBFLOW_MISSING_FLOW / SUBFLOW_INVALID);
 *   - a reference cycle (A→B→A at any depth, including self-reference) is
 *     named explicitly (SUBFLOW_CYCLE);
 *   - nesting deeper than SUBFLOW_MAX_DEPTH is rejected with the full chain
 *     (SUBFLOW_DEPTH_EXCEEDED).
 */
function validateSubflowReferences(
  yaml: RawYaml,
  absolutePath: string,
  flowDir: string,
  options?: { harnessRegistry?: HarnessRegistry; _subflowChain?: string[] },
): FlowValidationError[] {
  const errors: FlowValidationError[] = [];
  const chain = options?._subflowChain ?? [];

  for (const station of yaml.stations ?? []) {
    if (station.worker?.kind !== 'subflow') continue;

    const ref = station.worker.flow;
    if (typeof ref !== 'string' || ref === '') {
      errors.push({
        code: 'SUBFLOW_MISSING_FLOW',
        message:
          `Station '${station.id}' has kind 'subflow' but no worker.flow — ` +
          `a subflow station must name the child flow.yaml it invokes`,
      });
      continue;
    }
    const childPath = isAbsolute(ref) ? ref : resolve(flowDir, ref);

    if (childPath === absolutePath || chain.includes(childPath)) {
      const cycle = [...chain, absolutePath, childPath];
      errors.push({
        code: 'SUBFLOW_CYCLE',
        message:
          `Station '${station.id}' creates a subflow reference cycle: ` +
          cycle.join(' → '),
      });
      continue;
    }

    if (chain.length + 1 >= SUBFLOW_MAX_DEPTH) {
      errors.push({
        code: 'SUBFLOW_DEPTH_EXCEEDED',
        message:
          `Station '${station.id}' nests subflows deeper than ${SUBFLOW_MAX_DEPTH} ` +
          `(chain: ${[...chain, absolutePath, childPath].join(' → ')})`,
      });
      continue;
    }

    const childResult = loadFlow(childPath, {
      ...options,
      _subflowChain: [...chain, absolutePath],
    });
    if (!childResult.ok) {
      const detail = childResult.errors.map((e) => `[${e.code}] ${e.message}`).join('; ');
      // worker.flow is a path resolved against the parent flow's directory —
      // NOT an engine-manifest flow name, even though `conduit listen
      // --manifest` maps flow names to paths using the same key vocabulary.
      // A FILE_READ_ERROR among the child's errors means the child path
      // didn't resolve at all — the classic "I guessed a manifest name"
      // mistake (child-flow path-diagnostic work review) — so append a hint sentence explaining the
      // distinction. A child that resolved but failed its OWN validation
      // gets no hint: that's a different mistake and the message should say
      // so plainly, without the path-vs-name detour.
      const guessedManifestName = childResult.errors.some((e) => e.code === 'FILE_READ_ERROR');
      const hint = guessedManifestName
        ? ' (note: worker.flow is a path resolved against the parent flow\'s directory — e.g. ../pic-edit/flow.yaml — not an engine-manifest flow name)'
        : '';
      errors.push({
        code: 'SUBFLOW_INVALID',
        message:
          `Station '${station.id}' references child flow '${childPath}' which ` +
          `failed to load: ${detail}${hint}`,
      });
    }
  }

  return errors;
}

export function loadFlow(
  absolutePath: string,
  options?: { harnessRegistry?: HarnessRegistry; _subflowChain?: string[] },
): LoadFlowResult {
  const flowDir = dirname(absolutePath);

  // ── Read ──────────────────────────────────────────────────────────────────
  let content: string;
  try {
    content = readFileSync(absolutePath, 'utf-8');
  } catch (err) {
    return {
      ok: false,
      errors: [
        {
          code: 'FILE_READ_ERROR',
          message: `Cannot read flow file '${absolutePath}': ${String(err)}`,
        },
      ],
    };
  }

  // ── Parse ─────────────────────────────────────────────────────────────────
  let yaml: RawYaml;
  try {
    yaml = parse(content) as RawYaml;
  } catch (err) {
    return {
      ok: false,
      errors: [
        {
          code: 'YAML_PARSE_ERROR',
          message: `Failed to parse YAML at '${absolutePath}': ${String(err)}`,
        },
      ],
    };
  }

  // Resolved once per loadFlow call and threaded through validation,
  // composition, and the execution-surface warnings pass below — see
  // resolveSkillCached's doc comment for why this is sound (flowDir and
  // skillsDir are constant across a single call).
  const skillCache: SkillResolutionCache = new Map();

  // ── Validate ──────────────────────────────────────────────────────────────
  const errors = collectErrors(yaml, flowDir, skillCache, options?.harnessRegistry);

  // ── Subflow references (the original multi-flow engine work): resolve transitively, fail-closed ─────
  // Runs after collectErrors (which owns per-field shape checks) but folds its
  // errors into the same fail-closed result: an unresolvable child, an invalid
  // child flow, an A→…→A reference cycle, or nesting beyond the depth cap all
  // reject the PARENT at load — composition problems must die at flow load,
  // never at dispatch.
  errors.push(...validateSubflowReferences(yaml, absolutePath, flowDir, options));

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  // ── Build stations map ────────────────────────────────────────────────────
  // hitlEnabled keys off an egress channel that declares `uses: [hitl]` — a
  // human gate — NOT mere egress presence. A flow can declare a delivery-only
  // egress channel without turning every rank station into a human checkpoint.
  // `?? []` only guards null/undefined; a non-array egress (a YAML mapping) would
  // make `.some()` throw. Array.isArray restores the pre-change tolerance (the old
  // `.length > 0` read undefined → falsy on a non-array) — malformed → no hitl.
  const rawEgress = yaml.channels?.egress;
  const hasHitlEgress = Array.isArray(rawEgress) && rawEgress.some(
    (ch) => Array.isArray(ch.uses) && ch.uses.includes('hitl'),
  );
  const stations: Record<string, StationConfig> = {};
  for (const raw of (yaml.stations ?? [])) {
    stations[raw.id] = buildStationConfig(raw, flowDir, hasHitlEgress, skillCache);
  }

  // ── Build happyPathNext from declared `next` fields (WI-351, FR-2) ────────
  // Only populated when at least one station declares `next`; undefined otherwise
  // so flowToTransitionContext() can fall back to insertion order for pre-WI-351 flows.
  const rawStations = yaml.stations ?? [];
  const hasNextDeclarations = rawStations.some((s) => s.next !== undefined);
  const happyPathNext: Record<string, string | null> | undefined = hasNextDeclarations
    ? Object.fromEntries(rawStations.filter((s) => s.next !== undefined).map((s) => [s.id, s.next!]))
    : undefined;

  // ── Collect back-edges from check.on_reject ───────────────────────────────
  const backEdges: Array<{ from: string; to: string }> = [];
  for (const raw of rawStations) {
    const onReject = raw.check?.on_reject;
    if (onReject !== undefined) {
      backEdges.push({ from: raw.id, to: onReject });
    }
  }

  // ── Map defaults.cap_policy / defaults.on_dep_scrap (fail-closed defaults) ─
  // Both default to 'scrap' when absent — the safest, most conservative choice.
  // By this point collectErrors has already validated that the raw values (if
  // present) are legal strings, so the casts below are sound.
  const flowDefaults: FlowDefaults = {
    capPolicy: (yaml.defaults?.cap_policy as FlowDefaults['capPolicy'] | undefined) ?? 'scrap',
    onDepScrap: (yaml.defaults?.on_dep_scrap as FlowDefaults['onDepScrap'] | undefined) ?? 'scrap',
    // Opt-in owned-paths integrity gate (SPEC §5/§6) — default false; only an
    // explicit `true` enables it (a non-boolean value is treated as off).
    enforceOwnedPaths: yaml.defaults?.enforce_owned_paths === true,
    // Optional run-level concurrency cap (FR-2a). Absent when not declared in YAML.
    // collectErrors has already validated it is an integer >= 1; narrow by runtime
    // type (parse, not assert) so a non-number can never silently flow through.
    ...(typeof yaml.defaults?.concurrency === 'number'
      ? { concurrency: yaml.defaults.concurrency }
      : {}),
    // Optional per-run workspace mode (the original ingress-attribution work step 2). Absent = 'shared'
    // behavior; collectErrors already rejected any value outside the two
    // legal strings, so the narrowing below is sound.
    ...(yaml.defaults?.workspace === 'per_run' || yaml.defaults?.workspace === 'shared'
      ? { workspace: yaml.defaults.workspace }
      : {}),
  };

  // ── Resolve worker.uses skills + collect execution-surface warnings ──────
  // Consume-all-execute-none (FR-7): a skill bundle's scripts/allowed-tools/
  // hooks/command-block surface is never honored, only reported. Unresolvable
  // skills are not a load error here — WI-555 owns composing `uses` into the
  // station's prompt content (where a missing/invalid skill should surface).
  const executionSurfaceWarnings: ExecutionSurfaceWarning[] = [];
  for (const raw of rawStations) {
    for (const name of raw.worker?.uses ?? []) {
      const resolved = resolveSkillCached(skillCache, name, flowDir, DEFAULT_SKILLS_DIR);
      if (!resolved.ok) continue;
      const bundleDir = join(flowDir, DEFAULT_SKILLS_DIR, name);
      const warning = detectExecutionSurface(resolved.skill, bundleDir);
      if (warning !== null) executionSurfaceWarnings.push(warning);
    }
  }

  // ── Assemble and freeze ───────────────────────────────────────────────────
  const flow: FlowConfig = {
    version: yaml.flow_version ?? 1,
    // WI-442: preserve the human-readable flow name (omit when absent — no empty-string fallback).
    ...(yaml.flow !== undefined ? { name: yaml.flow } : {}),
    stations,
    terminal_lanes: yaml.terminal_lanes,
    budgets: yaml.budgets,
    channels: yaml.channels,
    back_edges: backEdges.length > 0 ? backEdges : undefined,
    happyPathNext,
    project_root: yaml.project_root !== undefined ? join(flowDir, yaml.project_root) : undefined,
    defaults: flowDefaults,
    prerequisites: Object.freeze(Array.isArray(yaml.prerequisites) ? (yaml.prerequisites as string[]) : []) as string[],
  };

  Object.freeze(flow);
  return executionSurfaceWarnings.length > 0
    ? { ok: true, flow, warnings: executionSurfaceWarnings }
    : { ok: true, flow };
}

// ---------------------------------------------------------------------------
// Harness binary presence probe (WI-563, FR-10)
// ---------------------------------------------------------------------------

/**
 * Probe every `kind: harness` station's configured binary for presence/
 * executability. A SEPARATE async pass over an already-loaded FlowConfig —
 * HarnessAdapter.probeBinary() is async (WI-560) and cannot run inside the
 * synchronous collectErrors/loadFlow. FR-10: caught at load/startup, never at
 * first dispatch. Never invokes the adapter — probe only. Non-harness
 * stations and stations whose adapter is unresolvable (already reported by
 * loadFlow's UNKNOWN_HARNESS_ADAPTER) are skipped.
 */
export async function probeHarnessBinaries(
  flow: FlowConfig,
  registry: HarnessRegistry,
): Promise<FlowValidationError[]> {
  const errors: FlowValidationError[] = [];
  for (const [stationId, station] of Object.entries(flow.stations)) {
    if (station.kind !== 'harness' || station.harness === undefined) continue;
    const resolved = registry.resolve(station.harness);
    if (!resolved.ok) continue;
    const probe = await resolved.adapter.probeBinary();
    if (!probe.present) {
      errors.push({
        code: 'HARNESS_BINARY_NOT_FOUND',
        message:
          `Station '${stationId}' harness adapter '${resolved.adapter.name}' binary not found` +
          (probe.detail !== undefined ? ` at '${probe.detail}'` : ''),
      });
    }
  }
  return errors;
}
