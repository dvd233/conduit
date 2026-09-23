/**
 * Canonical kernel domain types (WI-289).
 *
 * Encodes the (lane, status) state model from SPEC §3, the station/flow config
 * shapes from SPEC §4, the Card state-DB columns from SPEC §11, and the
 * StationOutput QC-verdict envelope consumed by the rework guard (WI-299)
 * and gate (WI-300).
 *
 * This module exports TYPES ONLY — no runtime values. The load-bearing gate
 * is `bun run typecheck` (tsc --noEmit), not bun test.
 */

// ---------------------------------------------------------------------------
// Status — the ten FSM sub-states (SPEC §3).
// ---------------------------------------------------------------------------

/**
 * Execution sub-state of a Card. Orthogonal to Lane (which is routing).
 *
 * Universal scheduling path: waiting → ready → claimed → working →
 *   done_pending_ack, plus the side-states interrupted, held,
 *   awaiting_children, complete, scrapped.
 */
export type Status =
  | 'waiting'
  | 'ready'
  | 'claimed'
  | 'working'
  | 'done_pending_ack'
  | 'interrupted'
  | 'held'
  | 'awaiting_children'
  | 'complete'
  | 'scrapped';

// ---------------------------------------------------------------------------
// Lane — routing address: a station id OR one of the four kernel terminals.
// ---------------------------------------------------------------------------

/**
 * Where a Card currently sits in the flow graph. Either a station id defined
 * in flow.yaml (arbitrary string) or one of the four kernel-reserved terminals:
 * 'intake', 'done', 'scrap', 'hold'.
 */
export type Lane = string;

// ---------------------------------------------------------------------------
// Card — the unit of work moving through the flow (SPEC §11).
// ---------------------------------------------------------------------------

/**
 * A single unit of work tracked by the kernel state machine. Maps 1-to-1 to a
 * row in the state-DB `cards` table. Epics (parent cards) have parent_id=null;
 * task cards reference their parent's id.
 */
export interface Card {
  /** Run this card belongs to. */
  run_id: string;
  /** Stable unique identifier for this card. */
  id: string;
  /** Parent epic's id, or null for top-level cards. */
  parent_id: string | null;
  /** Current routing position in the flow graph. */
  lane: Lane;
  /** Current execution sub-state within the lane. */
  status: Status;
  /** Number of execution attempts on the current lane (resets on lane change). */
  attempt: number;
  /** Dependency wave this card belongs to (used for fan-out scheduling). */
  wave: number;
  /** File paths this card is allowed to write (Law §7 path ownership). */
  owned_paths: string[];
  /**
   * Number of times this card has been sent back for rework (durable across restarts).
   * Optional so existing in-memory construction sites don't need to specify it;
   * the DB column carries DEFAULT 0 so it is always present on persisted cards.
   */
  rework_count?: number;
}

// ---------------------------------------------------------------------------
// StationConfig — per-station config parsed from flow.yaml (SPEC §4).
// ---------------------------------------------------------------------------

/**
 * Execution kind of a station — the first orthogonal axis.
 *
 * - deterministic: no LLM; a pure command or function
 * - transform:     one LLM call, typed data in/out, no tools, no loop
 * - agentic:       LLM with Read/Write/Bash in a multi-turn loop
 * - harness:       external agent-CLI (e.g. `claude -p`, `codex exec`) wrapped
 *                   with the transform contract but weaker containment (no
 *                   per-tool-call gating — see the adapter seam, later WIs)
 */
export type StationKind = 'deterministic' | 'transform' | 'agentic' | 'harness' | 'subflow';

/**
 * Full configuration for one station, as loaded from flow.yaml.
 * The `effectful` flag distinguishes pure (replayable) from effectful
 * (billed call or irreversible side-effect) stations.
 */
export interface StationConfig {
  /** Execution kind — determines which worker runtime handles this station. */
  kind: StationKind;
  /**
   * Human-readable worker role from the station's `worker.role` field in
   * flow.yaml (WI-442) — e.g. 'planner', 'drafter', 'assembler'. Preserved
   * verbatim for diagnostics and prompt/log attribution. Absent when the
   * station declares no worker, or a worker with no `role` — never an empty
   * string.
   */
  role?: string;
  /** True if the station performs a billed call or irreversible side-effect. */
  effectful: boolean;
  /** Maximum concurrent cards allowed in this station (WIP cap). */
  wip: number;
  /** Artifact names this station reads as input. */
  inputs: string[];
  /** Artifact names this station produces as output. */
  outputs: string[];
  /**
   * Where a transform station's declared outputs are written (v10, fan-out
   * children). Default 'project_root' — the historical behavior. 'owned_dir'
   * writes each output into the card's `owned_paths[0]` directory instead (the
   * same card-scoped location seed.json lives in), so N homogeneous fan-out
   * children each produce their OWN artifacts rather than clobbering one shared
   * name at project root (SPEC §9: outputs are disjoint across concurrent
   * cards). Validated at load: transform stations only, outputs declared.
   * Fail-closed at write: a card whose owned_paths[0] is not an existing
   * directory is a config violation.
   */
  output_scope?: 'project_root' | 'owned_dir';
  /** Optional id of the QC check station gating this station's output. */
  check?: string;
  /** Resolved gate check configuration (populated by the loader when a `check` block is present). */
  gateCheck?: StationGateConfig;
  /**
   * Resolved rank-check configuration (WI-398), populated by the loader when a
   * station declares `check: { kind: rank }`. Its presence is the discriminator
   * for a rank station: `isRankStation(s) === (s.rankCheck !== undefined)`.
   */
  rankCheck?: StationRankConfig;
  /**
   * Optional fan-in policy for merge stations.
   *
   * Either a bare survivor COUNT (e.g. `2`) or a structured policy object.
   * For `quorum`, `k` is a COUNT (integer ≥ 1): the parent proceeds iff at
   * least `k` children reach a non-scrap terminal lane (see evaluateFanIn).
   */
  fan_in?: number | FanInPolicyConfig;
  /** Optional fan-out count for split stations. */
  fan_out?: number;

  // -------------------------------------------------------------------------
  // Real-run config surface (WI-351). All optional so the WI-289 minimal
  // literal { kind, effectful, wip, inputs, outputs } still type-checks.
  // -------------------------------------------------------------------------

  /**
   * Declared happy-path successor lane for this station (a station id or a
   * terminal lane). The flow's happyPathNext map is built strictly from these
   * declarations — never from station insertion order (FR-2).
   */
  next?: string;
  /** Deterministic worker executable (e.g. `duckdb`). Required for kind=deterministic. */
  command?: string;
  /** Argument vector passed to `command` (metacharacter-free, per the Law-lite allowlist). */
  args?: string[];
  /**
   * Per-station wall-clock timeout in SECONDS (pre-launch punch-list #8). When
   * present (a positive integer, validated at load), a station that exceeds the
   * deadline is bounded rather than allowed to hang forever — closing the
   * deadlock where a stuck worker is an *active* worker the liveness watchdog
   * never trips on. Absent → the per-kind default (see below), backwards-compatible.
   *
   * Honoured for both runnable worker kinds, each with the right mechanism:
   *   - deterministic — Bun.spawn kills the subprocess (SIGKILL) at the deadline;
   *     absent here means genuinely unbounded (no SIGKILL).
   *   - transform — each gateway call is bounded by an AbortSignal.timeout, so a
   *     hung HTTP request is aborted (not a subprocess; no spawn on this path).
   *     Absent here falls back to the adapter's explicit engine-default timeout,
   *     because Bun's fetch has a hidden ~300s default anyway (the original transform-timeout work).
   * (agentic is not yet runnable; it will carry its own loop budget — SPEC §8.)
   */
  timeout_seconds?: number;
  /** Path to the model station's prompt template, resolved relative to the flow.yaml dir. */
  prompt_file?: string;
  /**
   * Composed instruction content for a `worker.uses:` station (WI-555): each
   * resolved skill's injected content in declared order, then the station's
   * local prompt (prompt_file text) last as the station-local override layer.
   * Absent for a station with no `worker.uses:`. When present, the executor's
   * prompt-render read prefers this over `prompt_file` (Skill Ingest PRD §2.2).
   */
  prompt_content?: string;
  /**
   * SHA-256 content hash of each resolved `worker.uses:` skill's injected
   * content, in declared order (WI-557). Absent for a station with no
   * `worker.uses:`. The executor combines these with `prompt_version` to form
   * the checkpoint binding stamp's promptTemplateVersion input, so editing a
   * skill's body or references/ invalidates and cascades — see
   * `computeSkillAwarePromptTemplateVersion` in checkpoint.ts.
   */
  skill_content_hashes?: string[];
  /** Version stamp of the prompt template (part of the checkpoint binding stamp). */
  prompt_version?: string;
  /**
   * Model identifier for transform/agentic stations (e.g. 'gpt-4o-mini').
   * Absent for deterministic stations.
   */
  model?: string;
  /** Inference parameters for the model call (e.g. { temperature: 0.7 }). */
  params?: Record<string, unknown>;
  /** Declared output schema for a model station's typed result. */
  output_schema?: StationOutputSchema;

  /**
   * Adapter name for a `kind: harness` station (e.g. `claude-code`, `codex`) —
   * required for harness stations, absent otherwise (WI-559).
   */
  harness?: string;
  /**
   * Absolute path to the child flow.yaml for a `kind: subflow` station
   * (the original multi-flow engine work, flow-as-station composition) — required for subflow stations,
   * absent otherwise. Resolved at load time relative to the parent flow's
   * directory; the loader validates the reference transitively (child loads,
   * no cycles, depth-capped).
   */
  flow?: string;
  /** Tools allowlist for a harness station's underlying agent-CLI. */
  tools?: string[];
  /** Optional waiver opting a harness station out of the `tools` allowlist. */
  unrestricted_tools?: boolean;

  /**
   * Declared image inputs for this station (WI-414, FR-1). Absent when no
   * image inputs are declared — never an empty array (follows the optional-field
   * pattern of `next`, `prompt_file`, `output_schema`). Each entry carries at
   * least a `path` (stored verbatim, not resolved at load time); extra keys are
   * tolerated for extensibility (Resolved Q4). Resolution is deferred to WI-419.
   */
  image_inputs?: ImageInputDeclaration[];

  /**
   * Station-level file-delivery declaration (WI-596, PRD "Slack Egress File
   * Delivery" FR-1/6/8/9). When present, on successful completion the executor
   * (WI-599) delivers the declared produced file(s) through the flow's
   * delivery-capable egress channel (resolveDeliveryChannel), optionally
   * threaded (thread_from) and/or captioned. Absent when the station declares
   * no `deliver:` block — never an empty object (follows the optional-field
   * pattern of `image_inputs`, `next`, `output_schema`). Parsing/validation
   * only lives in the loader; no side effect happens here.
   */
  deliver?: StationDeliverConfig;

  // -------------------------------------------------------------------------
  // Branching topology (WI-393). Declared on a fan-out station so the
  // fan-out/fan-in executor knows where children start, where each child
  // sub-path terminates, and where the parent resumes after fan-in. All
  // optional — a linear flow declares none of them. Each target is validated
  // fail-closed at load: child_entry/resume_at must name a known station (or,
  // for resume_at, a terminal lane); child_terminal must name a terminal lane.
  // -------------------------------------------------------------------------

  /**
   * For a fan-out station: the station each spawned child card enters first.
   * Must name a known station id (validated at load).
   */
  child_entry?: string;
  /**
   * For a fan-out station: the terminal lane each child sub-path ends at.
   * Must name a terminal lane — a non-terminal lane (station id) is rejected.
   */
  child_terminal?: string;
  /**
   * For a fan-out station: the lane the parent resumes at after fan-in.
   * Must name a known station id or a terminal lane (validated at load).
   */
  resume_at?: string;
  /**
   * For a fan-out station: the cache-warming stagger, in seconds. When set and
   * > 0, the first spawned child (lowest id) dispatches immediately while the
   * remaining siblings are held (cards.release_at = now + child_stagger_seconds)
   * so the first child can warm a shared prompt-prefix cache before the rest
   * fire. 0 or absent = no stagger (all children dispatch together). Validated
   * at load as a non-negative integer, only on a fan-out station.
   */
  child_stagger_seconds?: number;
}

/**
 * Resolved station-level `deliver` block (WI-596). Declares which produced
 * file(s) to deliver on successful completion, and how to address the send.
 *
 * Validated at load ("config is validated, not trusted"): `files` must be
 * non-empty (a zero-file deliver is rejected, FR-13), and the flow must declare
 * a delivery-capable egress channel — one whose `uses` includes `delivery`, or,
 * when no egress channel declares any `uses`, the first channel by fallback
 * (FR-2/FR-13). A `thread_from` the runtime substrate cannot supply is NOT a
 * load error: the same flow may be Slack- or CLI-triggered (FR-8, decision 6).
 */
export interface StationDeliverConfig {
  /** Produced file(s) to deliver, in declared order (FR-1/FR-10). Non-empty. */
  files: string[];
  /**
   * Optional substrate field name whose value addresses the target thread
   * (e.g. `thread_ts` captured at ingress). Absent → unthreaded delivery (FR-6/FR-8).
   */
  thread_from?: string;
  /**
   * Optional caption carried on the upload-completion call as Slack's
   * initial_comment (FR-9, decision 7) — never a separate send. Absent → bare file.
   */
  caption?: string;
}

/**
 * Resolved gate-check configuration for a station that has an inline `check` block.
 * Populated by the flow loader (WI-356) so the executor can run the gate critic
 * without re-reading the YAML.
 */
export interface StationGateConfig {
  /** Model identifier for the gate critic (e.g. 'gpt-4o'). */
  criticModel: string;
  /**
   * Human-readable critic role from the gate check's `critic.role` field in
   * flow.yaml (WI-442) — e.g. 'plan-critic'. Preserved verbatim for
   * diagnostics and prompt/log attribution. Absent when the critic declares
   * no `role` — never an empty string.
   */
  criticRole?: string;
  /** Absolute path to the critic's prompt template file. */
  criticPromptFile: string;
  /** Version stamp of the critic prompt template. */
  criticPromptVersion: string;
  /** The lane the card routes to on rejection (on_reject in YAML). */
  onReject: string;
  /** Maximum number of rework cycles before the cap policy fires (rework_cap). */
  reworkCap: number;
  /** Artifacts in scope for rendering the critic prompt (station inputs + outputs). */
  criticInputScope: string[];
  /**
   * Adapter name for an AGENTIC critic (WI-570, Phase 2) — a `kind: harness`
   * station used as the critic gate instead of a model call. Resolved against
   * the same engine-config harness registry a `kind: harness` maker uses
   * (WI-560).
   *
   * NOT mutually exclusive with `criticModel`, despite what this comment used
   * to claim. Issue #26 AC4 made a harness critic's model meaningful:
   * `check.critic.model` now reaches `HarnessInvocation.model` with
   * station-over-adapter precedence (FR-10), so declaring both is the
   * SUPPORTED way to pin a critic to a specific model while the adapter
   * supplies the deployment default elsewhere. Note that an absent critic
   * model is the EMPTY STRING, not undefined (flow/load.ts), so any
   * precedence check must treat `''` as absent.
   */
  criticHarness?: string;
  /**
   * Tools allowlist for an agentic (harness) critic's invocation (WI-595).
   * A harness critic MUST declare a NON-EMPTY list: an absent/empty allowlist
   * is REJECTED at flow load (HARNESS_CRITIC_TOOLS_REQUIRED, RetroLearning
   * row 16) so a critic's verdict is always produced under known containment —
   * there is no unrestricted-by-default path and no waiver for critics. A
   * non-empty list restricts the invocation to exactly those tools; load-time
   * validation additionally rejects it with HARNESS_TOOLS_UNEXPRESSIBLE when
   * the resolved adapter cannot express it (so a canRestrictTools:false adapter
   * cannot host a critic at all).
   */
  criticTools?: string[];
  /**
   * Wall-clock bound for an agentic (harness) critic's invocation, in ms
   * (check.critic.timeout_seconds * 1000). Absent -> the engine default
   * (DEFAULT_HARNESS_CRITIC_TIMEOUT_MS, 5 minutes). Mirrors the maker's
   * worker.timeout_seconds: a critic that must re-read long-form inputs
   * (full podcast transcript + draft + prior findings) cannot always fit
   * the default — a killed critic invoke otherwise fail-closes the card
   * to scrap (harness-critic-invoke-failed) on a healthy draft.
   */
  criticTimeoutMs?: number;
}

/**
 * Resolved rank-check configuration for a station with a `check: { kind: rank }`
 * block (WI-398). Populated by the flow loader so the executor can run the rank
 * critic and apply the HITL / no-selection policy without re-reading the YAML.
 *
 * A station is a RANK station iff `rankCheck !== undefined` (the discriminator
 * `isRankStation`). Distinct from `gateCheck` (a pass/reject quality gate): a
 * rank station ranks fan-in survivors and defers selection to a human (HITL) —
 * the kernel NEVER auto-picks a candidate (FR-14 / NFR-3).
 */
export interface StationRankConfig {
  /** Model identifier for the rank critic (e.g. 'gpt-4o'). */
  criticModel: string;
  /** Absolute path to the rank critic's prompt template file. */
  criticPromptFile: string;
  /** Version stamp of the rank critic prompt template. */
  criticPromptVersion: string;
  /**
   * True when a HITL egress channel is wired for this flow. When true the rank
   * station posts the short-list and parks the card as held (await_selection).
   * When false, `noSelectionPolicy` fires (no human in the loop).
   */
  hitlEnabled: boolean;
  /** Policy applied when no HITL is available (no auto-pick variant exists). */
  noSelectionPolicy: 'proceed_with_findings' | 'scrap';

  // -------------------------------------------------------------------------
  // Flow-computed candidates (the original HITL reply-and-resume work). A rank station may present a
  // shortlist the FLOW already produced (a deterministic merge, an upstream
  // station's artifact) instead of running a critic model call. The two
  // sources are mutually exclusive, validated at load:
  // `candidatesFrom` XOR a critic block.
  // -------------------------------------------------------------------------

  /**
   * Station artifact path (project-root-relative, stored verbatim) holding the
   * candidates to present: a JSON array of strings, or of {id, label} objects.
   * Present iff the flow declared `check.candidates_from` — the discriminator
   * for the no-critic rank mode (the original HITL reply-and-resume work FR-1).
   */
  candidatesFrom?: string;
  /**
   * Absolute path to an ask-message template (resolved against the flow dir,
   * like criticPromptFile). Rendered through the SAME renderPrompt pipeline as
   * a worker prompt — {{artifact}} placeholders resolve against the station's
   * declared `inputs` — so the flow shapes its own approval card text
   * (the original HITL reply-and-resume work FR-2). Absent → the kernel's default ask text.
   */
  askTemplateFile?: string;
  /**
   * Produced file(s) to upload WITH the ask (e.g. the edited image the human
   * is approving), delivered through the same outbox-guarded upload path as a
   * station `deliver:` block, threaded with the ask (the original HITL reply-and-resume work FR-2). Paths
   * stored verbatim. Absent → text-only ask.
   */
  askAttach?: string[];
  /**
   * Artifact path (project-root-relative, stored verbatim) the kernel writes
   * the recorded human selection to when the card resumes past the rank
   * station — {selection, correlation_id} — so downstream stations consume the
   * pick like any other input (the original HITL reply-and-resume work FR-4). The bytes are deterministic (no
   * wall-clock) so the artifact hashes stably into downstream binding stamps.
   * Complements (never replaces) the hitl.selection journal span.
   */
  selectionOut?: string;
}

/**
 * A single declared image input on a transform/gate station (WI-414, FR-1).
 *
 * The `path` is stored verbatim as declared in flow.yaml — resolution to an
 * absolute path is deferred to WI-419 (`loadImageInput(projectRoot, declaredPath)`).
 *
 * Extra keys beyond `path` are tolerated for forward-compatibility (Resolved Q4):
 * a future `detail` or resolution-hint field is additive and never rejected at load.
 */
export interface ImageInputDeclaration {
  /** Declared image file path, stored verbatim (not resolved at load time). */
  path: string;
  /** Extension point: future fields (e.g. `detail`) are additive. */
  [key: string]: unknown;
}

/** One declared field in a model station's output schema. */
export interface StationOutputField {
  name: string;
  type: string;
  required: boolean;
}

/** Declared output schema for a model (transform/agentic) station (WI-351). */
export interface StationOutputSchema {
  fields: StationOutputField[];
}

/** Structured fan-in policy as carried on a loaded StationConfig (SPEC §6). */
export type FanInPolicyConfig =
  | { policy: 'quorum'; k: number }   // k is a COUNT (integer ≥ 1)
  | { policy: 'all' }
  | { policy: 'best_effort' };

// ---------------------------------------------------------------------------
// StationOutput — the QC-verdict envelope (authoritative for WI-299 / WI-300).
// ---------------------------------------------------------------------------

/**
 * Output envelope returned by every station execution. The generic `TPayload`
 * carries the station-specific result; the remaining fields are fixed and
 * consumed by the kernel's rework guard (WI-299) and gate verdict (WI-300).
 */
export interface StationOutput<TPayload> {
  /** Station-specific result, typed by the caller. */
  payload: TPayload;
  /**
   * Stable hash of the QC findings for this execution attempt. The rework
   * progress guard (WI-299) compares this across attempts to detect loops.
   */
  findings_hash: string;
  /**
   * Back-edge target lane for rework, or null if the verdict is "proceed".
   * Consumed by the gate (WI-300) to route the card.
   */
  return_to: Lane | null;
  /**
   * Per-call token and cost attribution for budget tracking.
   *
   * A UNION, not a bare `{tokens, cost}`, because a MISSING measurement and a
   * MEASURED zero are different facts and must stay distinguishable (issue
   * #26 AC2). An adapter that cannot report usage for a call returns
   * `{ unknown: true }` (HarnessResult.usage, worker/harness-adapter.ts) and
   * that unknown must survive into the StationOutput rather than being
   * flattened to a fabricated `{ tokens: 0, cost: 0 }` — a fabricated zero is
   * indistinguishable from a free call and silently under-counts every budget
   * that folds this number.
   *
   * Structurally compatible with `UsageReport` from worker/harness-adapter.ts,
   * deliberately restated here rather than imported: types/kernel.ts is the
   * kernel's dependency-free type root and must not take an edge into worker/.
   */
  usage:
    | {
        tokens: number;
        cost: number;
      }
    | { unknown: true };
}

// ---------------------------------------------------------------------------
// FlowConfig — the parsed flow.yaml (SPEC §4).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// FlowConfig extended fields — populated by the flow loader (WI-292).
// All fields are optional so the WI-289 literal { version, stations } stays valid.
// ---------------------------------------------------------------------------

/** Budget caps for a run, wave, card, and the liveness watchdog. */
export interface FlowBudgets {
  run?: {
    wall_clock_minutes?: number;
    max_tokens?: number;
  };
  per_wave?: {
    max_tokens?: number;
    max_dispatches?: number;
  };
  per_card?: {
    max_execution_attempts?: number;
  };
  liveness?: {
    no_progress_minutes?: number;
  };
}

/** A single egress channel entry from flow.yaml. */
export interface FlowEgressChannel {
  type?: string;
  target?: string;
  uses?: string[];
  hold_timeout_seconds?: number;
  on_timeout?: string;
}

/** Ingress + egress channel config for a flow. */
export interface FlowChannels {
  ingress?: {
    type?: string;
  };
  egress?: FlowEgressChannel[];
}

/**
 * Flow-level policy defaults parsed from the `defaults:` block in flow.yaml.
 *
 * These govern behaviours that apply across all stations in the flow:
 *   - `capPolicy`:   what happens when a card exhausts the rework cap.
 *                    'scrap' (default) routes to the scrap terminal;
 *                    'proceed_with_findings' advances the card forward along
 *                    the happy path so it completes despite unresolved findings
 *                    (the gate_verdict card_log entry records those findings).
 *   - `onDepScrap`:  what happens when a dependency card is scrapped.
 *                    'scrap' routes the waiting parent to scrap;
 *                    'hold' routes it to the hold terminal for human review.
 *
 * Both values are validated at load time — only the two legal strings each are
 * accepted. Absent values default to 'scrap' / 'scrap' (fail-closed).
 */
export interface FlowDefaults {
  capPolicy: 'scrap' | 'proceed_with_findings';
  onDepScrap: 'scrap' | 'hold';
  /**
   * Opt-in MARK_DONE owned-paths integrity gate (SPEC §5/§6). When true, the
   * executor verifies that every file a station writes stays within the card's
   * `owned_paths` and hard-pauses to `hold` on a containment breach. Default
   * false: existing flows are not yet authored to keep all outputs within
   * owned_paths (e.g. fan-out children that share a static output name), so the
   * gate is enabled per-flow rather than globally (build-order dependency).
   */
  enforceOwnedPaths: boolean;
  /**
   * Run-level concurrency cap K (FR-2a). Bounds the maximum number of in-flight
   * worker processes. Effective per-station parallelism is min(K, station.wip).
   * Absent means the CLI default (1) applies. Only valid integers ≥ 1 are accepted.
   */
  concurrency?: number;
  /**
   * Per-run filesystem workspace (the original ingress-attribution work step 2). 'per_run' binds each
   * run's effective project root to `<project_root>/.conduit/runs/<run-id>/`
   * (materialized at run registration), so N concurrent runs of this flow are
   * filesystem-disjoint by construction — flow-declared paths are project-root
   * -relative and static, and run namespacing alone partitions only the DB.
   * Absent / 'shared' preserves today's behavior (all runs share the root).
   */
  workspace?: 'per_run' | 'shared';
}

/**
 * Top-level representation of a parsed flow.yaml file. The `stations` map
 * is keyed by station id as it appears in the YAML.
 *
 * The optional fields below are populated by the flow loader (WI-292).
 * They are absent on the minimal literal used by WI-289 tests.
 */
export interface FlowConfig {
  /** Schema version of this flow definition (from `flow_version` in YAML). */
  version: number;
  /**
   * Human-readable flow name from the top-level `flow:` field in flow.yaml
   * (WI-442). Preserved verbatim for diagnostics, logging, and egress labels.
   * Absent when `flow:` is not declared — never an empty string.
   */
  name?: string;
  /**
   * Absolute path to the project root directory (resolved from the `project_root`
   * field in flow.yaml relative to the flow file's directory). Absent when no
   * project_root was declared.
   */
  project_root?: string;
  /** All stations in this flow, keyed by their id. */
  stations: Record<string, StationConfig>;
  /**
   * Declared happy-path routing map (station id → successor lane, where the
   * successor is another station id or a terminal lane). Built strictly from
   * each station's `next` field by the flow loader (WI-351), so the executor
   * never derives routing from station insertion order (FR-2). The routing
   * seam flowToTransitionContext() reads this surface, not Object.keys order.
   */
  happyPathNext?: Record<string, string | null>;
  /** Kernel-reserved terminal lane ids (e.g. done, scrap, hold). */
  terminal_lanes?: string[];
  /** Budget caps — consumption andon + liveness watchdog (SPEC §8). */
  budgets?: FlowBudgets;
  /** Ingress listener and egress notification channels. */
  channels?: FlowChannels;
  /**
   * Back-edges in the lane graph: on_reject targets extracted from station
   * check configs. Consumed by the transition-matrix loader (WI-302) and
   * the rework gate (WI-300).
   */
  back_edges?: ReadonlyArray<{ from: string; to: string }>;
  /**
   * Flow-level policy defaults from the `defaults:` block in flow.yaml.
   * Absent on minimal WI-289 literals; always populated by the loader when
   * a real flow.yaml is loaded (defaults to 'scrap'/'scrap' when omitted).
   */
  defaults?: FlowDefaults;
  /**
   * System package names a flow's stations need at runtime, from the optional
   * top-level `prerequisites:` list in flow.yaml (WI-434). Validated fail-closed
   * at load (INVALID_PREREQUISITES) and defaulted to an empty array when absent.
   * This is the single source of truth consumed by `conduit build` (apt-get list)
   * and `conduit doctor` (prereq-present probe), so it must not drift.
   *
   * Optional on the type (like the other loader-enriched fields above) so minimal
   * WI-289 literals still type-check; the loader ALWAYS populates it (defaulting
   * to []), so the returned frozen FlowConfig always includes the field.
   */
  prerequisites?: string[];
}
