/**
 * Gate check + rework-loop helper (WI-356, Sosa W6).
 *
 * Extracted from the executor so it can be independently reasoned about and
 * later individually tested. Runs one gate-critic check (via `runGateCheck`)
 * and applies the durable rework guards to produce a concrete next action
 * for the caller: advance forward, rework to the back-edge, or scrap.
 *
 * The caller supplies the per-gate rework count (see `gateReworkCount` below)
 * and applies the returned decision to the DB; the only read this module makes
 * for itself is the card_log lookup guard #3 needs.
 */

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { StationGateConfig } from '../types/kernel';
import type { ModelAdapter } from '../worker/adapter';
import type { HarnessRegistry } from '../worker/harness-adapter';
import { DEFAULT_RUN_ID, type ConduitDB, type StoredCardLogEntry } from '../persistence/db';
import { runGateCheck, runHarnessGateCheck, computeFindingsHash, type CriticUsage } from '../quality/gate';
import { renderPrompt } from '../flow/render';

/** Default wall-clock bound for an agentic (harness) critic invocation (WI-570). */
const DEFAULT_HARNESS_CRITIC_TIMEOUT_MS = 5 * 60 * 1000;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface GateReworkInput {
  db: ConduitDB;
  /**
   * Run-scoped identifier for cross-run isolation in Guard #3, and threaded to
   * the critic's usage span. REQUIRED (the original per-run usage-attribution work): kept uniform with
   * GateConfig/RankConfig so an omitted run id is a compile error, never a
   * silent DEFAULT_RUN_ID mis-attribution.
   */
  runId: string;
  cardId: string;
  /** The station that produced the work being reviewed (not the gate station). */
  workerStationId: string;
  attempt: number;
  maxExecutionAttempts: number;
  /**
   * Rework cycles ALREADY SPENT AT THIS STATION'S GATE — NOT the card's lifetime
   * `cards.rework_count` (issue #1). `gateConfig.reworkCap` is declared per-gate,
   * so the counter compared against it must be scoped per-gate too; passing the
   * lifetime scalar let upstream reworks consume this gate's budget. Callers
   * derive this with `countGateReworks(db.getCardLogForRun(runId, cardId),
   * workerStationId)`.
   *
   * Named distinctly from `rework_count` on purpose: the rename turns the old
   * (buggy) call into a compile error rather than a silent mis-scoping.
   */
  gateReworkCount: number;
  gateConfig: StationGateConfig;
  adapter: ModelAdapter;
  /**
   * Engine-config harness adapter registry (WI-560), needed only when
   * gateConfig.criticHarness names an AGENTIC critic (WI-570) — resolves the
   * critic adapter by name, same as a `kind: harness` maker.
   */
  harnessRegistry?: HarnessRegistry;
  /** Absolute project root for resolving artifact inputs of the critic prompt. */
  projectRoot: string;
  /** Validated back-edges from the flow config. */
  validBackEdges: ReadonlyArray<{ from: string; to: string }>;
  /**
   * Flow-level cap policy governing what happens when the rework cap is exhausted.
   *
   * Defaults to 'scrap' when absent (fail-closed, backward-compatible).
   *
   * - 'scrap': at cap, return { action: 'scrap', reason: 'rework_cap' } directly.
   *   The executor scraps the card without consulting the FSM.
   * - 'proceed_with_findings': at cap, return { action: 'rework', ... } so the
   *   executor can fire QC_REJECT at the FSM, which recognises the cap+policy
   *   combination and advances the card forward instead of scrapping.
   *
   * NOTE: no_progress is a hard stop that does NOT honour proceed_with_findings —
   * a reject with unchanged findings is scrapped immediately regardless of policy
   * (see guard #3 below). This is intentional: no progress = stop.
   */
  capPolicy?: 'scrap' | 'proceed_with_findings';
}

/**
 * Widened decision returned by runGateRework (WI-382).
 *
 * ALL three branches now carry the gate critic's verdict, findings, returnTo,
 * and attempt so the executor can:
 *   1. Append a gate_verdict card_log entry on every gate check (FR-3).
 *   2. Thread findings into the feedback parameter of the next rework renderPrompt
 *      call (FR-5 / WI-379).
 *
 * Findings come from the critic payload and are no longer discarded inside this
 * module. Model-incompatible / invalid_verdict scraps (no critic output) carry
 * findings: [] — the executor still appends the gate_verdict entry with an empty
 * findings array; those branches are fail-closed, not missing-data.
 */
export type GateReworkDecision =
  | { action: 'pass'; verdict: 'pass'; findings: string[]; returnTo: null; attempt: number; criticUsage?: CriticUsage }
  | { action: 'rework'; verdict: 'reject'; findings: string[]; returnTo: string; attempt: number; criticUsage?: CriticUsage }
  | {
      action: 'scrap';
      /**
       * 'rework_cap' | 'no_progress' | 'invalid_verdict' are fixed guard names
       * from THIS module. A gate-check 'scrapped' decision instead PROPAGATES
       * whatever named reason the critic call produced (a pre-public engine review, @queso
       * finding 1) — the transform convention 'model-incompatible', or one of
       * gate.ts's distinct `harness-critic-*` reasons for an agentic critic —
       * rather than collapsing every critic-call failure to one shared label.
       */
      reason: 'rework_cap' | 'no_progress' | 'invalid_verdict' | string;
      verdict: 'reject';
      findings: string[];
      returnTo: null;
      attempt: number;
      /**
       * issue #26 AC3/AC5: carried even on the guard scraps (`rework_cap`,
       * `no_progress`) that this module names itself, NOT just on a gate-check
       * 'scrapped' verdict — a critic that rejected and was then scrapped by a
       * guard here was still billed for that rejection, and the caller must
       * still fold/journal it.
       */
      criticUsage?: CriticUsage;
    };

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Return the gate_verdict entry for `station` with the highest attempt number
 * strictly below `currentAttempt`, or null if no such entry exists.
 *
 * This identifies the immediately-prior critic verdict so the no-progress guard
 * can compare its findings hash against the current one (SPEC §6 guard #3).
 */
function findImmediatelyPriorVerdict(
  log: StoredCardLogEntry[],
  station: string,
  currentAttempt: number,
): Extract<StoredCardLogEntry, { kind: 'gate_verdict' }> | null {
  let best: Extract<StoredCardLogEntry, { kind: 'gate_verdict' }> | null = null;

  for (const entry of log) {
    if (entry.kind !== 'gate_verdict') continue;
    if (entry.station !== station) continue;
    if (entry.attempt >= currentAttempt) continue;

    if (best === null || entry.attempt > best.attempt) {
      best = entry as Extract<StoredCardLogEntry, { kind: 'gate_verdict' }>;
    }
  }

  return best;
}

// ---------------------------------------------------------------------------
// runGateRework
// ---------------------------------------------------------------------------

/**
 * Run one gate-critic check and apply the bounded rework guards.
 *
 * Returns a GateReworkDecision the caller applies to the DB:
 *   - pass:   advance the card along the happy path
 *   - rework: route the card to `returnTo` (back-edge) and increment rework_count
 *             (which also records this gate's spent cycle in the card_log)
 *   - scrap:  route the card to the scrap terminal
 *
 * The caller is responsible for:
 *   1. Writing the station's output artifacts to disk BEFORE calling this
 *      function, so renderPrompt can read them for the critic prompt.
 *   2. Persisting the decision (card lane/status/rework_count update + slot release).
 */
export async function runGateRework(input: GateReworkInput): Promise<GateReworkDecision> {
  const { db, runId, cardId, workerStationId, attempt, maxExecutionAttempts, gateReworkCount, gateConfig, adapter, harnessRegistry, projectRoot, validBackEdges, capPolicy = 'scrap' } = input;

  // ── Render critic prompt ──────────────────────────────────────────────────
  const criticTemplate = readFileSync(gateConfig.criticPromptFile, 'utf-8');
  const criticPrompt = renderPrompt(criticTemplate, gateConfig.criticInputScope, projectRoot);

  // ── Run gate check ────────────────────────────────────────────────────────
  // Use the worker station id (not a ':gate' suffix) so the back-edge check
  // `e.from === config.station` matches the flow's declared back-edges, which
  // record the WORKER station as the origin (e.g. {from:'ideate', to:'ideate'}).
  //
  // WI-570 (Phase 2): an AGENTIC critic (gateConfig.criticHarness set) resolves
  // a harness adapter from the registry and runs runHarnessGateCheck instead of
  // the model-based runGateCheck — both produce the identical GateDecision
  // shape, so every branch below is unaware of which kind of critic ran.
  const gateDecision = gateConfig.criticHarness !== undefined
    ? await (async () => {
        const resolved = harnessRegistry?.resolve(gateConfig.criticHarness!);
        if (resolved === undefined || !resolved.ok) {
          throw new Error(
            `harness critic unresolved for station '${workerStationId}': ` +
              (resolved === undefined
                ? `no harness adapter registry configured`
                : resolved.error),
          );
        }
        // issue #26 AC4 (FR-10): station-over-adapter precedence for the
        // critic's model, mirroring the maker path's `effectiveModel =
        // stationConfig.model ?? harnessAdapter.model` (executor.ts ~3495).
        //
        // The flow loader (flow/load.ts) sets an ABSENT critic model to the
        // EMPTY STRING (`criticModel: chk.critic.model ?? ''`), never
        // `undefined` — so `gateConfig.criticModel ?? resolved.adapter.model`
        // would never fall through to the adapter default (`''` is not
        // nullish). Resolve '' as absent explicitly instead.
        const criticModel = gateConfig.criticModel !== '' ? gateConfig.criticModel : resolved.adapter.model;
        return runHarnessGateCheck({
          cardId,
          station: workerStationId,
          attempt,
          harnessAdapter: resolved.adapter,
          prompt: criticPrompt,
          criticInputScope: gateConfig.criticInputScope,
          projectRoot,
          timeoutMs: gateConfig.criticTimeoutMs ?? DEFAULT_HARNESS_CRITIC_TIMEOUT_MS,
          model: criticModel,
          onReject: gateConfig.onReject,
          validBackEdges,
          tools: gateConfig.criticTools,
        });
      })()
    : await runGateCheck({
        cardId,
        station: workerStationId,
        // Attribute the gate critic's usage span to this run (the original per-run usage-attribution work).
        runId,
        attempt,
        maxExecutionAttempts,
        model: gateConfig.criticModel,
        prompt: criticPrompt,
        params: {},
        adapter,
        db,
        onReject: gateConfig.onReject,
        validBackEdges,
      });

  // ── Map gate decision to rework decision ──────────────────────────────────
  // All branches now carry verdict, findings, returnTo, and attempt so the
  // executor can append a gate_verdict card_log entry and thread findings
  // back into the next rework's renderPrompt feedback parameter (WI-382/FR-3).
  switch (gateDecision.action) {
    case 'pass':
      // Propagate whatever findings the pass verdict carried (typically []).
      return {
        action: 'pass',
        verdict: 'pass',
        findings: gateDecision.output.payload.findings,
        returnTo: null,
        attempt,
        criticUsage: gateDecision.criticUsage,
      };

    case 'scrapped':
      // No critic output, so findings are empty. a pre-public engine review (@queso finding
      // 1): propagate the gate check's OWN named reason instead of collapsing
      // it to a hardcoded 'model-incompatible' — that literal previously threw
      // away gate.ts's harness-critic-* classification (invoke-failed vs
      // verdict-missing vs verdict-unparseable vs verdict-invalid all read
      // identically downstream), which is exactly what named the wrong suspect
      // during a model bisect.
      return { action: 'scrap', reason: gateDecision.reason, verdict: 'reject', findings: [], returnTo: null, attempt, criticUsage: gateDecision.criticUsage };

    case 'invalid_verdict':
      // Critic returned an unresolvable return_to — no usable findings.
      return { action: 'scrap', reason: 'invalid_verdict', verdict: 'reject', findings: [], returnTo: null, attempt, criticUsage: gateDecision.criticUsage };

    case 'reject': {
      const findings = gateDecision.output.payload.findings;

      // ── Guard #3: progress-monotonicity on findings hash (SPEC §6) ──────────
      // If the critic returned the same findings as the immediately-prior attempt,
      // no progress was made — scrap immediately regardless of gateReworkCount.
      // "Immediately-prior" = the gate_verdict entry for this station with the
      // highest attempt number strictly less than the current attempt.
      const currentHash = computeFindingsHash(findings);
      const priorVerdict = findImmediatelyPriorVerdict(
        db.getCardLogForRun(runId, cardId),
        workerStationId,
        attempt,
      );
      if (priorVerdict !== null && computeFindingsHash(priorVerdict.findings) === currentHash) {
        // issue #26 AC3/AC5: the critic that rejected (and was billed for it)
        // is the SAME check that just tripped guard #3 — carry its usage even
        // though this branch names its own 'no_progress' reason rather than
        // propagating the gate check's.
        return { action: 'scrap', reason: 'no_progress', verdict: 'reject', findings, returnTo: null, attempt, criticUsage: gateDecision.criticUsage };
      }

      // ── Guard #1: durable per-gate rework cap (SPEC §6 four-guard system) ───
      // gateReworkCount counts reworks spent AT THIS GATE only (issue #1); a
      // sibling gate's exhausted budget must never scrap a card here.
      if (gateReworkCount >= gateConfig.reworkCap) {
        if (capPolicy === 'proceed_with_findings') {
          // proceed_with_findings: return 'rework' so the executor can fire
          // QC_REJECT at the FSM, which advances the card forward instead of
          // scrapping.  The gate_verdict card_log entry (appended by the
          // executor BEFORE this switch) is the durable "findings attached"
          // record — no payload mutation is needed here.
          return { action: 'rework', verdict: 'reject', findings, returnTo: gateDecision.returnTo, attempt, criticUsage: gateDecision.criticUsage };
        }
        // 'scrap' (default): the rejection that tripped the cap carries findings;
        // preserve them so triage can read the final rejecting reason. Same
        // reasoning as the no_progress branch above: this rejection was billed
        // and guard #1 (not the gate check) is what named the scrap reason.
        return { action: 'scrap', reason: 'rework_cap', verdict: 'reject', findings, returnTo: null, attempt, criticUsage: gateDecision.criticUsage };
      }

      return { action: 'rework', verdict: 'reject', findings, returnTo: gateDecision.returnTo, attempt, criticUsage: gateDecision.criticUsage };
    }
  }
}
