/**
 * Gate QC check — converge via bounded back-edge rework (WI-300, SPEC §6, FR-6).
 *
 * A gate check runs a critic as a pure transform station (no tools, no filesystem),
 * validates the returned verdict against the flow's back-edges, and routes the card:
 *   - pass   → advance the happy path (return_to: null)
 *   - reject → route to a validated on_reject lane, carrying a findings_hash that
 *              feeds the rework progress-monotonicity guard (WI-299)
 *
 * The findings_hash is computed ORDER-INSENSITIVELY from the critic's findings so
 * cosmetic reordering of the same findings = same hash = no-progress signal.
 */

import { createHash } from 'node:crypto';
import { readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import type { Lane, StationOutput } from '../types/kernel';
import { DEFAULT_RUN_ID, type ConduitDB } from '../persistence/db';
import type { ModelAdapter } from '../worker/adapter';
import { runTransformStation, coerciveParse, type OutputSchema } from '../worker/transform';
import type { HarnessAdapter, HarnessResult, MountedInput, UsageReport } from '../worker/harness-adapter';
import { usageFromThrow } from '../worker/harness-adapter';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** The structured verdict the critic model must return. */
export interface GateCriticVerdict {
  verdict: 'pass' | 'reject';
  findings: string[];
  /** The back-edge target lane; optional — falls back to config.onReject. */
  return_to?: string;
}

/** Everything the gate check needs at runtime. */
export interface GateConfig {
  cardId: string;
  station: string;
  /**
   * Run that owns this check — threaded onto the critic's usage span so its
   * token/cost is attributed to the run, not the DEFAULT_RUN_ID sweep (issue
   * per-run usage-attribution work). REQUIRED: a critic call is a billed model call, and an omitted run
   * id is exactly the silent mis-attribution per-run usage-attribution work removes — a compile error
   * beats a wrong journal row.
   */
  runId: string;
  attempt: number;
  maxExecutionAttempts: number;
  model: string;
  prompt: string;
  params: Record<string, unknown>;
  adapter: ModelAdapter;
  db: ConduitDB;
  /** Default back-edge lane used when the critic omits return_to. */
  onReject: Lane;
  /** Validated on_reject edges from the WI-292 flow loader. */
  validBackEdges: ReadonlyArray<{ from: string; to: string }>;
}

/**
 * What an AGENTIC (harness) critic invocation actually consumed.
 *
 * Carried on EVERY GateDecision branch — including the failure branches —
 * because a critic that timed out or returned a garbled verdict was still
 * billed for the work it did, and the executor must fold that spend into the
 * run and wave budgets regardless of whether the verdict was usable (issue
 * #26 AC5).
 *
 * ONLY `runHarnessGateCheck` (the agentic critic) populates this.
 * `runGateCheck` (the transform critic) never does, and that asymmetry is
 * deliberate, not an oversight to "fix" later: a transform critic's usage is
 * ALREADY folded into the run budget by the time this module sees it — it
 * flows through `runTransformStation`, which calls the adapter, which in
 * production is the executor's `trackingAdapter` wrapper that adds every call
 * to `tokensSpent`. Populating `criticUsage` there too would make the
 * executor count that same spend a second time. `criticUsage` is the
 * HARNESS-only channel precisely because that's the one path with no other
 * accumulator watching it.
 */
export interface CriticUsage {
  /** Adapter that ran the critic (the journal's `adapter` column). */
  adapterName: string;
  /** Effective model the invocation was told to use, when one was resolved. */
  model?: string;
  /** Wall-clock duration of the invoke(), in milliseconds. */
  durationMs: number;
  /** The adapter's own report. `{ unknown: true }` stays unknown, never zero. */
  usage: UsageReport;
}

export type GateDecision =
  | { action: 'pass'; output: StationOutput<GateCriticVerdict>; criticUsage?: CriticUsage }
  | { action: 'reject'; returnTo: Lane; output: StationOutput<GateCriticVerdict>; criticUsage?: CriticUsage }
  | { action: 'invalid_verdict'; reason: 'invalid_return_to'; criticUsage?: CriticUsage }
  /**
   * a pre-public engine review (@queso, findings 1 + 2a): `reason` is a NAMED failure class,
   * not a single catch-all. `runGateCheck` (transform critic) still always
   * reports the transform convention 'model-incompatible' (SPEC §7 rev-1 H5,
   * unchanged). `runHarnessGateCheck` (agentic critic) reports one of its own
   * distinct `harness-critic-*` reasons below, each carrying a `: <detail>`
   * suffix (mirrors the harness MAKER path's `harness-output-invalid: ...`
   * convention in executor.ts) so a failure names its own suspect instead of
   * forcing a blind bisect across models.
   */
  | { action: 'scrapped'; reason: string; criticUsage?: CriticUsage };

// ---------------------------------------------------------------------------
// computeFindingsHash — deterministic, order-insensitive
// ---------------------------------------------------------------------------

/**
 * Produce a stable SHA-256 fingerprint of a set of finding strings.
 *
 * Sorted before hashing so cosmetic reordering of identical findings produces
 * the SAME hash — the rework progress guard (WI-299) relies on this to detect
 * "the critic said the same things, just in a different order" as no-progress.
 */
export function computeFindingsHash(findings: string[]): string {
  const sorted = [...findings].sort();
  return createHash('sha256').update(JSON.stringify(sorted)).digest('hex');
}

// ---------------------------------------------------------------------------
// Output schema — validates the critic JSON into GateCriticVerdict
// ---------------------------------------------------------------------------

/**
 * A reject verdict with missing/empty findings is unactionable by definition —
 * there is nothing for the rework loop to act on and nothing for a human to
 * read (a pre-public engine review, @queso finding 2a). Checked BEFORE the generic
 * `findings must be an array` check so it also catches the reported wrong-key
 * case (`{"verdict":"reject","reasons":[...]}` — the real `findings` key is
 * simply absent), not just an explicit `findings: []`. A pass verdict with no
 * findings stays legal (unchanged).
 */
export const REJECT_WITHOUT_FINDINGS_ERROR =
  'reject verdict requires non-empty findings — expected contract: {verdict: "pass"|"reject", findings: [...], return_to?}';

function isRejectWithoutFindings(v: Record<string, unknown>): boolean {
  return v.verdict === 'reject' && (!Array.isArray(v.findings) || v.findings.length === 0);
}

const gateCriticSchema: OutputSchema<GateCriticVerdict> = {
  validate(value) {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      return { ok: false, error: 'expected object' };
    }
    const v = value as Record<string, unknown>;
    if (v.verdict !== 'pass' && v.verdict !== 'reject') {
      return { ok: false, error: 'verdict must be "pass" or "reject"' };
    }
    if (isRejectWithoutFindings(v)) {
      return { ok: false, error: REJECT_WITHOUT_FINDINGS_ERROR };
    }
    if (!Array.isArray(v.findings)) {
      return { ok: false, error: 'findings must be an array' };
    }
    return {
      ok: true,
      value: {
        verdict: v.verdict as 'pass' | 'reject',
        findings: v.findings as string[],
        return_to: typeof v.return_to === 'string' ? v.return_to : undefined,
      },
    };
  },
};

// ---------------------------------------------------------------------------
// runGateCheck
// ---------------------------------------------------------------------------

/**
 * Run one gate QC check through the injected kernel adapter (pure station).
 *
 * Routing logic:
 *   1. The transform runtime handles retries up to maxExecutionAttempts.
 *   2. On scrapped: propagate model-incompatible.
 *   3. On pass: rebuild StationOutput with findings_hash([]) and return_to: null.
 *   4. On reject:
 *      a. Resolve returnTo = verdict.return_to ?? config.onReject.
 *      b. Validate against validBackEdges; invalid → invalid_verdict.
 *      c. Valid → rebuild StationOutput with findings_hash(verdict.findings)
 *         and return_to = returnTo.
 */
export async function runGateCheck(config: GateConfig): Promise<GateDecision> {
  const result = await runTransformStation<GateCriticVerdict>({
    cardId: config.cardId,
    station: config.station,
    runId: config.runId,
    attempt: config.attempt,
    maxExecutionAttempts: config.maxExecutionAttempts,
    model: config.model,
    prompt: config.prompt,
    params: config.params,
    schema: gateCriticSchema,
    adapter: config.adapter,
    db: config.db,
  });

  if (result.status === 'scrapped') {
    return { action: 'scrapped', reason: 'model-incompatible' };
  }

  const { payload, usage } = result.output;

  if (payload.verdict === 'pass') {
    const output: StationOutput<GateCriticVerdict> = {
      payload,
      findings_hash: computeFindingsHash([]),
      return_to: null,
      usage,
    };
    return { action: 'pass', output };
  }

  // Reject path — resolve and validate the back-edge target.
  const returnTo: Lane = payload.return_to ?? config.onReject;
  const isValidEdge = config.validBackEdges.some(
    (e) => e.from === config.station && e.to === returnTo,
  );
  if (!isValidEdge) {
    return { action: 'invalid_verdict', reason: 'invalid_return_to' };
  }

  const output: StationOutput<GateCriticVerdict> = {
    payload,
    findings_hash: computeFindingsHash(payload.findings),
    return_to: returnTo,
    usage,
  };
  return { action: 'reject', returnTo, output };
}

// ---------------------------------------------------------------------------
// runHarnessGateCheck — the AGENTIC critic role (WI-570, Phase 2)
// ---------------------------------------------------------------------------

/**
 * The declared verdict artifact a harness critic writes — mirrors how a
 * harness MAKER writes its declared output file (WI-565/566): the agentic
 * critic emits its pass/reject verdict as structured output on disk rather
 * than as a returned model payload, since a harness invocation is bounded and
 * opaque, not a typed call/response.
 */
const HARNESS_CRITIC_VERDICT_FILE = 'verdict.json';

/** Everything a harness-driven (agentic) gate check needs at runtime. */
export interface HarnessGateConfig {
  cardId: string;
  station: string;
  attempt: number;
  harnessAdapter: HarnessAdapter;
  /** Rendered critic prompt (already resolved against criticInputScope). */
  prompt: string;
  /** Artifacts in scope for the critic — mounted the same way a harness maker mounts its inputs. */
  criticInputScope: string[];
  projectRoot: string;
  timeoutMs: number;
  /**
   * Resolved EFFECTIVE model for this invocation (station override or
   * adapter default, FR-10) — resolved by the CALLER, not here, mirroring
   * `effectiveModel` on the harness MAKER path (executor.ts). Omitted
   * entirely means "let the adapter fall through to its own configured
   * default," never coerced to a placeholder.
   */
  model?: string;
  onReject: Lane;
  validBackEdges: ReadonlyArray<{ from: string; to: string }>;
  /**
   * Tools allowlist for the critic's invocation (WI-595). Load-time validation
   * (flow/load.ts HARNESS_CRITIC_TOOLS_REQUIRED, RetroLearning row 16)
   * GUARANTEES this is non-empty for any harness critic — an omitted/empty
   * allowlist is rejected at flow load rather than running the critic
   * unrestricted. A non-empty list restricts the invocation to exactly those
   * tools; HARNESS_TOOLS_UNEXPRESSIBLE additionally rejects it when the adapter
   * cannot express it. The runtime `config.tools ?? []` fallback below is kept
   * only as defense-in-depth — load validation means it is never actually
   * exercised for a harness critic.
   */
  tools?: string[];
}

/**
 * Run one gate check via an AGENTIC (harness) critic instead of a model call —
 * the adversarial-research-gate driving use case. Produces the SAME
 * `GateDecision` shape `runGateCheck` does, so `runGateRework`'s downstream
 * routing (rework guards, gate_verdict journaling) never needs to know which
 * kind of critic ran.
 *
 * Unlike `runGateCheck` (which delegates retry to `runTransformStation`), this
 * is a SINGLE bounded invocation — the harness adapter's own invoke() already
 * enforces a wall-clock timeout; a malformed/missing verdict scraps rather
 * than silently passing, the same fail-closed shape a transform's
 * 'model-incompatible' scrap has, but (a pre-public engine review, @queso finding 1) under
 * its own distinct `harness-critic-*` reason per failure class — see the
 * `scrapped` branches below — rather than one shared label.
 */
export async function runHarnessGateCheck(config: HarnessGateConfig): Promise<GateDecision> {
  // Mount declared inputs the same way a harness maker does — the reserved
  // synthetic 'feedback'/'seed.json' names have no on-disk artifact of their
  // own (WI-565).
  const mountedInputs: MountedInput[] = config.criticInputScope
    .filter((name) => name !== 'feedback' && name !== 'seed.json')
    .map((name) => ({ name, path: join(config.projectRoot, name) }));

  // WI-570 rework: clear any STALE verdict.json BEFORE this attempt's invoke()
  // — mirrors WI-568's fresh-per-attempt-baseline precedent in executor.ts
  // (integrityBaseline = snapshotTree(projectRoot), taken fresh before each
  // invoke). Without this, a critic that resolves successfully but fails to
  // (re)write its verdict this attempt (a bug, wrong cwd, a forgotten Write
  // call — all plausible for a real agentic critic) would silently read a
  // leftover verdict from an earlier gate check as if it were this attempt's
  // genuine judgment. force:true so a first-ever check (no prior file) is a
  // silent no-op, not an error.
  rmSync(join(config.projectRoot, HARNESS_CRITIC_VERDICT_FILE), { force: true });

  const verdictPath = join(config.projectRoot, HARNESS_CRITIC_VERDICT_FILE);

  let result: HarnessResult;
  const invokeStartedAt = Date.now();
  try {
    result = await config.harnessAdapter.invoke({
      prompt: config.prompt,
      inputs: mountedInputs,
      tools: config.tools ?? [],
      timeoutMs: config.timeoutMs,
      ...(config.model !== undefined ? { model: config.model } : {}),
    });
  } catch (err) {
    // A thrown invocation (timeout/nonzero-exit/untagged) never yields a
    // usable verdict — scrap rather than silently pass or hang the gate.
    // a pre-public engine review (@queso finding 1): named distinctly from every other
    // failure below — the invocation itself never ran to completion.
    //
    // issue #26 AC5: a thrown invocation was still billed for whatever it did
    // before it died. usageFromThrow is the ONE reader for usage attached to a
    // harness throw — never cast and reach for `.usage` directly here.
    return {
      action: 'scrapped',
      reason: `harness-critic-invoke-failed: ${(err as Error).message ?? String(err)}`,
      criticUsage: {
        adapterName: config.harnessAdapter.name,
        ...(config.model !== undefined ? { model: config.model } : {}),
        durationMs: Date.now() - invokeStartedAt,
        usage: usageFromThrow(err) ?? { unknown: true },
      },
    };
  }

  // issue #26 AC1/AC5: the invoke() resolved (successfully or not, verdict-
  // wise) — the adapter's usage report is real and billed either way, so
  // build criticUsage ONCE here and carry it onto every return path below,
  // including every harness-critic-* scrap.
  const criticUsage: CriticUsage = {
    adapterName: config.harnessAdapter.name,
    ...(config.model !== undefined ? { model: config.model } : {}),
    durationMs: Date.now() - invokeStartedAt,
    usage: result.usage,
  };

  // a pre-public engine review (@queso finding 1): a MISSING verdict file (the critic
  // never wrote one this attempt — including the WI-570 stale-verdict clear
  // above going unfulfilled) is a distinct class from a PRESENT-but-garbled
  // one below; each gets its own named reason instead of sharing one label.
  let verdictText: string;
  try {
    verdictText = readFileSync(verdictPath, 'utf-8');
  } catch {
    return { action: 'scrapped', reason: `harness-critic-verdict-missing: ${verdictPath}`, criticUsage };
  }

  const payload = coerciveParse(verdictText);
  if (payload === null) {
    return { action: 'scrapped', reason: `harness-critic-verdict-unparseable: ${verdictPath}`, criticUsage };
  }

  const validated = gateCriticSchema.validate(payload);
  if (!validated.ok) {
    // Finding 2a: a reject verdict with missing/empty findings gets its own
    // dedicated reason (rather than the generic verdict-invalid one below) —
    // it's the specific, previously-silent bug @queso hit, and naming it
    // distinctly makes it greppable apart from other malformed verdicts.
    const rawObj = payload as Record<string, unknown>;
    const reason = isRejectWithoutFindings(rawObj)
      ? `harness-critic-reject-without-findings: ${verdictPath} — ${REJECT_WITHOUT_FINDINGS_ERROR}`
      : `harness-critic-verdict-invalid: ${verdictPath}: ${validated.error}`;
    return { action: 'scrapped', reason, criticUsage };
  }
  const verdict = validated.value;

  if (verdict.verdict === 'pass') {
    const output: StationOutput<GateCriticVerdict> = {
      payload: verdict,
      findings_hash: computeFindingsHash([]),
      return_to: null,
      usage: result.usage,
    };
    return { action: 'pass', output, criticUsage };
  }

  const returnTo: Lane = verdict.return_to ?? config.onReject;
  const isValidEdge = config.validBackEdges.some(
    (e) => e.from === config.station && e.to === returnTo,
  );
  if (!isValidEdge) {
    return { action: 'invalid_verdict', reason: 'invalid_return_to', criticUsage };
  }

  const output: StationOutput<GateCriticVerdict> = {
    payload: verdict,
    findings_hash: computeFindingsHash(verdict.findings),
    return_to: returnTo,
    usage: result.usage,
  };
  return { action: 'reject', returnTo, output, criticUsage };
}
