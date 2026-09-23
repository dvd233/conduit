/**
 * codex-exec harness adapter (WI-574).
 *
 * The SECOND shipping real HarnessAdapter, proving the no-provider-lock-in
 * seam (Goal "No provider lock-in", Phase 2): a flow swaps providers by
 * changing only the configured adapter NAME — no other flow.yaml edits.
 *
 * Mirrors claude-headless (WI-564) — builds its invocation from engine
 * config, spawns through the bounded process runner (WI-561) with the env
 * allowlist (WI-562), and reads STRUCTURED usage from the harness's own
 * output, never scraped from free text. Differs from claude-headless in three
 * capability-divergent ways (see the contract test's header for the full
 * rationale):
 *   1. Codex emits a JSONL EVENT STREAM (one JSON object per line); token
 *      counts live in the `turn.completed` events' `usage` objects. That usage
 *      is PER-TURN (not cumulative), so a multi-turn session is SUMMED across
 *      every turn.completed event, and reasoning_output_tokens (a billed output
 *      counter) is folded into the output total.
 *   2. Codex has no per-tool allowlist — it gates via OS-enforced sandbox
 *      envelopes. Since the original per-list tool-expression work a declared `call.tools` list is translated
 *      into the sandbox envelope it maps onto (planCodexSandbox: read-only /
 *      workspace-write / +network); lists no envelope matches are rejected at
 *      load (HARNESS_TOOLS_UNEXPRESSIBLE) and fail closed here as
 *      defense-in-depth. `canRestrictTools` stays FALSE for boolean-only
 *      callers; `canExpressTools` is the authoritative per-list judgment.
 *      An EMPTY list remains the waived-unrestricted path (WI-563).
 *   3. A SUCCESSFUL run whose stream carries no parseable usage event AT ALL
 *      reports usage explicitly UNKNOWN (`{ unknown: true }`), never zero
 *      (PRD NFR-2) — unlike claude, which throws on a missing usage object.
 *      A usage event that IS present but lacks a cost figure (codex's real
 *      turn.completed schema never includes total_cost_usd — verified against
 *      the official Codex manual) still reports its genuinely-known token
 *      counts, with cost approximated as 0 rather than discarding the tokens.
 *
 * `outputs` is always `[]` — the executor collects declared outputs from disk
 * (findMissingDeclaredOutputs in executor.ts); the codex stream carries no
 * file manifest, so this adapter never fabricates output references.
 *
 * Do NOT touch ./harness.ts (unrelated worker-pool subprocess harness).
 */

import type { HarnessAdapter, HarnessInvocation, HarnessResult, BinaryProbe, KnownUsage } from './harness-adapter';
import { runHarnessProcess } from './harness-runner';
import type { HarnessCommand, HarnessRunnerConfig, HarnessSpawnResult } from './harness-runner';

export interface CodexHarnessAdapterConfig {
  /** Absolute project root threaded through to the process runner's cwd confinement. */
  projectRoot: string;
  /** Env var names visible to the child (NFR-Security-2), sourced from engine config. */
  envAllowlist: string[];
  /** Binary to invoke. Defaults to 'codex' (resolved via PATH). */
  command?: string;
  /** Optional --model override. */
  model?: string;
  /** Injected process-runner seam, for testability. Defaults to runHarnessProcess. */
  run?: (cmd: HarnessCommand, config: HarnessRunnerConfig) => Promise<HarnessSpawnResult>;
  /** Injected binary-presence probe, for testability. Defaults to a real PATH check. */
  probe?: () => Promise<BinaryProbe>;
}

interface CodexUsageEvent {
  type: 'turn.completed';
  usage?: {
    input_tokens?: number;
    cached_input_tokens?: number;
    output_tokens?: number;
    reasoning_output_tokens?: number;
    total_tokens?: number;
    total_cost_usd?: number;
  };
}

/** Per-turn usage summed across every turn.completed event in the stream. */
interface AccumulatedUsage {
  inputTokens: number;
  /**
   * The CACHED SUBSET of inputTokens (issue #5). Codex reports
   * `cached_input_tokens` alongside `input_tokens`, where input_tokens is the
   * INCLUSIVE total — so uncached input is the difference, and the two must
   * never be added or the input would be double-counted.
   */
  cachedInputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  cost: number;
}

// ---------------------------------------------------------------------------
// Capability-envelope planning (the original per-list tool-expression work).
//
// Codex has no per-tool-name allowlist, but its sandbox modes are OS-enforced
// capability envelopes (seatbelt/landlock) — stronger containment than a
// harness-honor-system tool list, just a different shape. A declared allowlist
// is expressible iff a sandbox envelope exists whose capabilities match the
// list's capability CLASSES exactly:
//
//   read    (Read, Glob, Grep)     any envelope
//   write   (Write, Edit)          workspace-write only
//   exec    (Bash)                 read-only confines exec to reads;
//                                  workspace-write exec can write
//   network (WebFetch, WebSearch)  workspace-write + network_access=true
//
// Mapping rules (strict — the envelope may never GRANT a class the list
// omits):
//   - unknown tool name → unexpressible (fail closed; new tool classes must
//     be classified here deliberately, never assumed harmless)
//   - no write, no network → `-s read-only`. Bash may be present or absent:
//     read-only exec is OS-confined to reading, so sandboxed shell ⊆ the
//     already-granted read class (envelope equivalence, not a widening).
//   - write present → REQUIRES Bash in the list: workspace-write's shell can
//     write, so granting write-capable exec a flow never asked for would
//     exceed the envelope. With Bash: `-s workspace-write`.
//   - network present → REQUIRES write+Bash (network is a workspace-write
//     sub-flag; codex has no read-only+network envelope):
//     `-c sandbox_workspace_write.network_access=true`.
// ---------------------------------------------------------------------------

const READ_TOOLS = new Set(['Read', 'Glob', 'Grep']);
const WRITE_TOOLS = new Set(['Write', 'Edit']);
const NETWORK_TOOLS = new Set(['WebFetch', 'WebSearch']);
const EXEC_TOOL = 'Bash';

interface SandboxPlan {
  sandbox: 'read-only' | 'workspace-write';
  network: boolean;
}

/** Map a tools allowlist onto a codex sandbox envelope, or null when no envelope matches. */
export function planCodexSandbox(tools: readonly string[]): SandboxPlan | null {
  let write = false;
  let network = false;
  let exec = false;
  for (const tool of tools) {
    if (READ_TOOLS.has(tool)) continue;
    else if (WRITE_TOOLS.has(tool)) write = true;
    else if (NETWORK_TOOLS.has(tool)) network = true;
    else if (tool === EXEC_TOOL) exec = true;
    else return null; // unknown tool class — fail closed
  }
  if (network && !write) return null; // no read-only+network envelope exists
  if (write && !exec) return null; // workspace-write shell can write; Bash must be granted
  if (!write) return { sandbox: 'read-only', network: false };
  return { sandbox: 'workspace-write', network };
}

/**
 * Throws a NAMED error — never resolve to a silent zero-usage success.
 * An optional `code` tags the two spawn-failure classes (timeout, non-zero
 * exit) so the executor can classify the throw (WI-566), mirroring the
 * claude-headless `fail()` precedent. `detail` mirrors claude's mechanism for
 * attaching extra fields to the thrown error (issue #26 AC5: a recovered
 * `usage` figure rides through here).
 */
function fail(reason: string, code?: string, detail?: Record<string, unknown>): never {
  throw Object.assign(
    new Error(`codex-exec: ${reason}`),
    code !== undefined ? { code } : {},
    detail ?? {},
  );
}

async function defaultProbe(command: string): Promise<BinaryProbe> {
  const resolved = Bun.which(command);
  return resolved !== null
    ? { present: true, detail: resolved }
    : { present: false, detail: `'${command}' not found on PATH` };
}

/**
 * Parse a codex `exec --json` JSONL stream and SUM the usage across EVERY
 * `turn.completed` event, or return null if the stream carries no parseable
 * usage event at all. Codex `turn.completed` usage is PER-TURN, not cumulative,
 * so a multi-turn session must be summed — keeping only the last event would
 * undercount. Non-usage / malformed lines are ignored (only turn.completed
 * carries token counts); a missing counter on any event contributes 0.
 */
function accumulateUsage(stdout: string): AccumulatedUsage | null {
  let seen = false;
  const total: AccumulatedUsage = {
    inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, reasoningTokens: 0, cost: 0,
  };
  for (const line of stdout.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    let event: unknown;
    try {
      event = JSON.parse(trimmed);
    } catch {
      continue; // malformed line — never crash the parse over one bad line
    }
    if (
      typeof event === 'object' &&
      event !== null &&
      (event as { type?: unknown }).type === 'turn.completed'
    ) {
      const usage = (event as CodexUsageEvent).usage;
      if (usage !== undefined) {
        seen = true;
        total.inputTokens += usage.input_tokens ?? 0;
        total.cachedInputTokens += usage.cached_input_tokens ?? 0;
        total.outputTokens += usage.output_tokens ?? 0;
        total.reasoningTokens += usage.reasoning_output_tokens ?? 0;
        // Real codex usage carries no cost; sum it only for a hypothetical
        // future build that emits total_cost_usd (see invoke()).
        if (typeof usage.total_cost_usd === 'number') {
          total.cost += usage.total_cost_usd;
        }
      }
    }
  }
  return seen ? total : null;
}

/**
 * Build the structured `KnownUsage` object from accumulated per-turn usage.
 * ONE construction, shared by the success path and by the two throw sites
 * that can recover a genuine figure (`harness-timeout`, `harness-nonzero-exit`)
 * — issue #26 AC5 — so the shape can never drift between "this call succeeded"
 * and "this call failed but was billed".
 */
function knownUsageFrom(usage: AccumulatedUsage): KnownUsage {
  // Token counts ARE genuinely available and useful for budget/andon
  // accounting even without a cost figure — extract them unconditionally
  // rather than discarding known tokens alongside an absent cost. Codex
  // charges for reasoning_output_tokens, so they are part of the output
  // total; counts are the sum across every per-turn turn.completed event.
  const tokens = usage.inputTokens + usage.outputTokens + usage.reasoningTokens;
  // Codex's real usage schema carries no cost field — cost stays a
  // best-effort 0 (documented as "unavailable", not a real signal); any
  // total_cost_usd a future build emits is summed by accumulateUsage.
  const cost = usage.cost;

  return {
    tokens,
    cost,
    // Issue #5. input_tokens is INCLUSIVE of cached_input_tokens here, so
    // uncached input is the difference — clamped at 0 because the two
    // counters are summed independently across turns and a malformed
    // event could otherwise drive it negative. Reasoning tokens are billed
    // as output, which is where the total already counts them.
    breakdown: {
      inputTokens: Math.max(0, usage.inputTokens - usage.cachedInputTokens),
      outputTokens: usage.outputTokens + usage.reasoningTokens,
      cacheReadInputTokens: usage.cachedInputTokens,
      // Codex reports no cache-CREATION counter, only reads. Zero here is
      // "the provider does not report it", not "no cache was written".
      cacheCreationInputTokens: 0,
    },
  };
}

export function createCodexHarnessAdapter(config: CodexHarnessAdapterConfig): HarnessAdapter {
  const command = config.command ?? 'codex';
  const run = config.run ?? runHarnessProcess;
  const probe = config.probe ?? (() => defaultProbe(command));

  return {
    name: 'codex-exec',
    // True: the adapter genuinely reports token counts from the terminal
    // turn.completed event. Cost is a best-effort 0 (codex's real schema
    // carries no cost field at all) — see the cost derivation in invoke().
    reportsUsage: true,
    // Codex gates via sandbox/approval modes, not a per-tool allowlist — the
    // legacy all-or-nothing flag stays FALSE so boolean-only callers remain
    // fail-closed (PRD NFR-Security-3). Per-list expressibility below is the
    // authoritative judgment (the original per-list tool-expression work): lists that map onto an OS-enforced
    // sandbox envelope ARE restrictable — more provably than a tool-name flag.
    canRestrictTools: false,
    canExpressTools(tools: readonly string[]): boolean {
      return tools.length === 0 || planCodexSandbox(tools) !== null;
    },
    model: config.model,

    async probeBinary(): Promise<BinaryProbe> {
      return probe();
    },

    async invoke(call: HarnessInvocation): Promise<HarnessResult> {
      const args: string[] = ['exec', '--json'];
      // Station wins over the adapter's configured default (FR-10, WI-589).
      const model = call.model ?? config.model;
      if (model !== undefined) {
        args.push('--model', model);
      }
      // The original per-list tool-expression work: a declared allowlist is enforced via the OS sandbox envelope
      // it maps onto (planCodexSandbox — the same judgment canExpressTools made
      // at load). An unmappable list here means load validation was bypassed;
      // fail closed rather than silently run unrestricted. An empty list is the
      // waived-unrestricted path (WI-563) and adds no sandbox flag, exactly as
      // before.
      if (call.tools.length > 0) {
        const plan = planCodexSandbox(call.tools);
        if (plan === null) {
          fail(
            `tools allowlist [${call.tools.join(', ')}] maps onto no codex sandbox envelope — ` +
              `load validation should have rejected this (HARNESS_TOOLS_UNEXPRESSIBLE)`,
            'harness-tools-unexpressible',
          );
        }
        args.push('--sandbox', plan.sandbox);
        // Hermetic envelope (the pre-public Codex sandbox review): ALWAYS pin the network posture in
        // workspace-write mode, both directions. A user/machine-level
        // ~/.codex/config.toml `[sandbox_workspace_write] network_access = true`
        // applies to every codex run; relying on the default-off would let state
        // outside the repo grant an egress class the flow never declared
        // (verified live: the config file demonstrably reaches conduit-spawned
        // runs; CLI -c overrides it back — codex-cli 0.142.4, Linux/bwrap).
        if (plan.sandbox === 'workspace-write') {
          args.push('-c', `sandbox_workspace_write.network_access=${plan.network}`);
        }
      }
      // `--` terminates option parsing (codex exec is a clap-based CLI) so a
      // prompt beginning with `-` cannot be misread as a flag.
      args.push('--', call.prompt);

      const spawnResult = await run(
        { command, args },
        {
          projectRoot: config.projectRoot,
          timeoutMs: call.timeoutMs,
          envAllowlist: config.envAllowlist,
        },
      );

      // PARSE BEFORE THE TIMEOUT AND EXIT-CODE BAILS (issue #26 AC5) — this
      // ordering is load-bearing, mirroring the precedent in the claude
      // adapter where parseClaudeStream deliberately runs before the
      // exit-code bail (issue #3). Codex's usage lives in PER-TURN
      // `turn.completed` events streamed as the run proceeds, not in a single
      // terminal event, so a run killed at the wall-clock bound or a crash
      // after several completed turns still has real per-turn usage sitting
      // in the captured stdout. Reading it here, before either bail, means a
      // timeout or a non-zero exit can still carry the tokens the provider
      // already billed — unlike claude, codex CAN recover usage from a kill.
      const usage = accumulateUsage(spawnResult.stdout);
      const billedUsage = usage !== null ? knownUsageFrom(usage) : undefined;

      if (spawnResult.timedOut) {
        fail(
          'invocation exceeded its timeout and was killed',
          'harness-timeout',
          billedUsage !== undefined ? { usage: billedUsage } : undefined,
        );
      }
      if (spawnResult.exitCode !== 0) {
        fail(
          `exited with code ${spawnResult.exitCode}: ${spawnResult.stderr.slice(0, 500)}`,
          'harness-nonzero-exit',
          billedUsage !== undefined ? { usage: billedUsage } : undefined,
        );
      }

      // PRD NFR-2: a successful run whose stream carries NO parseable usage
      // event AT ALL is journaled EXPLICITLY unknown — never a fabricated
      // { tokens: 0, cost: 0 }. This is distinct from a usage event that IS
      // present but lacks a cost figure (handled inside knownUsageFrom):
      // codex's real turn.completed schema never includes total_cost_usd
      // (verified against the official Codex manual — input_tokens/
      // cached_input_tokens/output_tokens/reasoning_output_tokens only), so
      // gating this branch on cost's presence would fire on every real
      // invocation and defeat AC3 (parsing real usage) entirely.
      if (billedUsage === undefined) {
        return { outputs: [], usage: { unknown: true } };
      }

      return {
        outputs: [],
        usage: billedUsage,
      };
    },
  };
}
