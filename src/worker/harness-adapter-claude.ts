/**
 * claude-headless harness adapter (WI-564).
 *
 * The first shipping real HarnessAdapter (WI-560). Builds a `claude -p`
 * invocation from engine config, spawns it through the bounded process runner
 * (WI-561) with the env allowlist (WI-562), parses the harness's STRUCTURED
 * JSON usage/cost output (never scraped from free text), and translates the
 * station's declared `tools` allowlist into claude's `--allowed-tools` flag.
 *
 * `outputs` is always `[]` — the executor collects declared outputs from disk
 * (findMissingDeclaredOutputs in executor.ts); the claude JSON payload carries
 * no file manifest, so this adapter never fabricates output references.
 *
 * Do NOT touch ./harness.ts (unrelated worker-pool subprocess harness).
 */

import type {
  HarnessAdapter, HarnessInvocation, HarnessResult, BinaryProbe,
  RateLimitSnapshot, RateLimitWindow, KnownUsage,
} from './harness-adapter';
import { runHarnessProcess } from './harness-runner';
import type { HarnessCommand, HarnessRunnerConfig, HarnessSpawnResult } from './harness-runner';

export interface ClaudeHarnessAdapterConfig {
  /** Absolute project root threaded through to the process runner's cwd confinement. */
  projectRoot: string;
  /** Env var names visible to the child (NFR-Security-2), sourced from engine config. */
  envAllowlist: string[];
  /** Binary to invoke. Defaults to 'claude' (resolved via PATH). */
  command?: string;
  /** Optional --model override. */
  model?: string;
  /** Injected process-runner seam, for testability. Defaults to runHarnessProcess. */
  run?: (cmd: HarnessCommand, config: HarnessRunnerConfig) => Promise<HarnessSpawnResult>;
  /** Injected binary-presence probe, for testability. Defaults to a real PATH check. */
  probe?: () => Promise<BinaryProbe>;
}

interface ClaudeUsagePayload {
  input_tokens?: number;
  output_tokens?: number;
  cache_creation_input_tokens?: number;
  cache_read_input_tokens?: number;
}

interface ClaudeModelUsageEntry {
  canonicalModel?: string;
  inputTokens?: number;
  outputTokens?: number;
  cacheReadInputTokens?: number;
  cacheCreationInputTokens?: number;
}

interface ClaudeResultPayload {
  type?: string;
  subtype?: string;
  is_error?: boolean;
  total_cost_usd?: unknown;
  usage?: ClaudeUsagePayload;
  /** Per-model breakdown; `canonicalModel` fills the journal's `model` column. */
  modelUsage?: Record<string, ClaudeModelUsageEntry>;
  /**
   * Structured HTTP status when the CLI failed against the API. 429 is a
   * provider rate limit — the signal issue #3 was throwing away by bailing on
   * the exit code before this payload was ever parsed.
   */
  api_error_status?: number;
  terminal_reason?: string;
  /** Human-readable outcome; on a cap it names the reset time. */
  result?: string;
}

interface ClaudeRateLimitWindow {
  utilization?: number;
  /** Epoch SECONDS. */
  resetsAt?: number;
}

/**
 * `rate_limit_info` as the CLI actually emits it: ONE window per event, with
 * its fields FLAT at the top level and named by `rateLimitType`.
 *
 * `unifiedWindows` is kept as a fallback because some builds have been observed
 * carrying it, but the flat form is the one that must work — writing this
 * against the nested shape alone meant `windows` came back empty on every real
 * call, so the park silently fell back to its default interval and the
 * provider's own reset was never used.
 */
interface ClaudeRateLimitEvent {
  type?: string;
  rate_limit_info?: {
    status?: string;
    isUsingOverage?: boolean;
    /** Names the single flat window, e.g. 'five_hour' | 'seven_day'. */
    rateLimitType?: string;
    utilization?: number;
    /** Epoch SECONDS. */
    resetsAt?: number;
    unifiedWindows?: Record<string, ClaudeRateLimitWindow>;
  };
}

/**
 * The only two event types this adapter reads, matched on the RAW line so the
 * stream can be filtered without parsing (or retaining) the rest.
 *
 * A false positive — an assistant message quoting this text — is harmless:
 * parseClaudeStream still checks the real `type` field. A false NEGATIVE would
 * lose the result, so the pattern is deliberately loose about whitespace.
 */
const CLAUDE_KEPT_EVENT = /"type"\s*:\s*"(result|rate_limit_event)"/;

/** What one invocation's NDJSON stream yielded. */
interface ParsedClaudeStream {
  /** The terminal `result` event, if the stream produced one. */
  result: ClaudeResultPayload | null;
  /** Capacity snapshot from the last rate_limit_event, if any. */
  rateLimit: RateLimitSnapshot | undefined;
}

/**
 * Parse the `stream-json` NDJSON output into the two things we care about.
 *
 * Tolerant by construction: a malformed line is skipped rather than failing the
 * whole invocation, because this parse now runs on the FAILURE path too, where
 * a partially-written stream is likely. The LAST event of each kind wins — the
 * result event is terminal, and only the most recent capacity reading is
 * meaningful.
 */
export function parseClaudeStream(stdout: string): ParsedClaudeStream {
  let result: ClaudeResultPayload | null = null;
  // ACCUMULATED ACROSS EVENTS, keyed by window name. The flat payload carries
  // ONE window per event, so a stream reporting five_hour and seven_day
  // separately would otherwise keep only whichever arrived last — and since
  // bindingResetAtMs parks until the MOST-CONSUMED window resets, dropping one
  // can silently pick the wrong reset. Later readings override earlier ones for
  // the same name.
  const windowsByName = new Map<string, RateLimitWindow>();
  let rateLimitStatus: string | undefined;
  let rateLimitOverage: boolean | undefined;
  let sawRateLimitEvent = false;

  for (const line of stdout.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    let event: unknown;
    try {
      event = JSON.parse(trimmed);
    } catch {
      continue;
    }
    if (typeof event !== 'object' || event === null) continue;
    const type = (event as { type?: unknown }).type;

    if (type === 'result') {
      result = event as ClaudeResultPayload;
      continue;
    }
    if (type === 'rate_limit_event') {
      const info = (event as ClaudeRateLimitEvent).rate_limit_info;
      if (info === undefined) continue;

      sawRateLimitEvent = true;
      if (info.status !== undefined) rateLimitStatus = info.status;
      if (info.isUsingOverage !== undefined) rateLimitOverage = info.isUsingOverage;

      // FLAT FIRST — this is the shape the CLI emits. resetsAt is epoch SECONDS
      // on the wire; everything downstream works in milliseconds.
      const fromThisEvent = new Set<string>();
      if (typeof info.utilization === 'number' && typeof info.resetsAt === 'number') {
        const name = info.rateLimitType ?? 'window';
        fromThisEvent.add(name);
        windowsByName.set(name, { name, utilization: info.utilization, resetsAtMs: info.resetsAt * 1000 });
      }
      // Nested fallback, for a build that reports every window at once. The
      // flat entry wins WITHIN one event (it is this event's own subject);
      // across events, the later reading wins.
      for (const [name, w] of Object.entries(info.unifiedWindows ?? {})) {
        if (typeof w.utilization !== 'number' || typeof w.resetsAt !== 'number') continue;
        if (fromThisEvent.has(name)) continue;
        windowsByName.set(name, { name, utilization: w.utilization, resetsAtMs: w.resetsAt * 1000 });
      }
    }
  }
  const rateLimit: RateLimitSnapshot | undefined = sawRateLimitEvent
    ? {
        ...(rateLimitStatus !== undefined ? { status: rateLimitStatus } : {}),
        ...(rateLimitOverage !== undefined ? { usingOverage: rateLimitOverage } : {}),
        windows: [...windowsByName.values()],
      }
    : undefined;

  return { result, rateLimit };
}

/** Total tokens a modelUsage entry accounts for, across every class. */
function entryTokens(entry: ClaudeModelUsageEntry): number {
  return (
    (entry.inputTokens ?? 0) +
    (entry.outputTokens ?? 0) +
    (entry.cacheReadInputTokens ?? 0) +
    (entry.cacheCreationInputTokens ?? 0)
  );
}

/**
 * The model that did the bulk of the work, or undefined if none is reported.
 *
 * Ties break toward the first entry, which keeps the label stable rather than
 * dependent on key order.
 */
export function dominantModel(
  modelUsage: Record<string, ClaudeModelUsageEntry> | undefined,
): string | undefined {
  let best: { model: string; tokens: number } | undefined;
  for (const [key, entry] of Object.entries(modelUsage ?? {})) {
    const model = entry.canonicalModel ?? key;
    const tokens = entryTokens(entry);
    if (best === undefined || tokens > best.tokens) best = { model, tokens };
  }
  return best?.model;
}

/**
 * Build the structured `KnownUsage` object from a parsed result payload, or
 * undefined when the payload cannot support one (issue #26 AC5).
 *
 * ONE construction, shared by the success path and by the two throw sites
 * that can still recover a genuine figure (`harness-rate-limited`,
 * `harness-nonzero-exit`) — so the shape can never drift between "this call
 * succeeded" and "this call failed but was billed". Returning undefined here
 * must NOT be read as "usage is unknown, but the call still resolves" on the
 * success path: the caller there still `fail()`s on a missing/malformed usage
 * object, exactly as before this helper existed.
 */
function buildKnownUsage(
  payload: ClaudeResultPayload | null,
  rateLimit: RateLimitSnapshot | undefined,
): KnownUsage | undefined {
  if (payload === null) return undefined;
  if (typeof payload.usage !== 'object' || payload.usage === null || Array.isArray(payload.usage)) {
    return undefined;
  }
  if (typeof payload.total_cost_usd !== 'number') return undefined;

  const usage = payload.usage;
  // Still the TRUE TOTAL across all four classes: run and wave budgets fold
  // this number, so it must not shrink to input+output when the breakdown
  // below splits it out. (These four are disjoint in claude's schema —
  // input_tokens is uncached input, not an inclusive total — so summing
  // them double-counts nothing.)
  const tokens =
    (usage.input_tokens ?? 0) +
    (usage.output_tokens ?? 0) +
    (usage.cache_creation_input_tokens ?? 0) +
    (usage.cache_read_input_tokens ?? 0);

  // modelUsage names the models the provider actually billed, filling the
  // journal's `model` column (empty on harness rows until now).
  //
  // TWO OR MORE entries is the NORMAL case, not the exception: Claude Code
  // bills a haiku model for side tasks alongside the main model, so even a
  // trivial call returns two. Requiring exactly one meant the column fell
  // back to the station's requested model on essentially every row —
  // delivering nothing #5 asked for. Attribute to the entry that consumed
  // the most tokens instead: that is the model that did the work and drove
  // the cost.
  const billedModel = dominantModel(payload.modelUsage);

  return {
    tokens,
    cost: payload.total_cost_usd,
    breakdown: {
      inputTokens: usage.input_tokens ?? 0,
      outputTokens: usage.output_tokens ?? 0,
      cacheReadInputTokens: usage.cache_read_input_tokens ?? 0,
      cacheCreationInputTokens: usage.cache_creation_input_tokens ?? 0,
    },
    ...(billedModel !== undefined ? { model: billedModel } : {}),
    ...(rateLimit !== undefined ? { rateLimit } : {}),
  };
}

/**
 * Blocking rate-limit states. `allowed_warning` is NOT one of them — it means
 * approaching a ceiling, not stopped at it, and treating it as a cap would park
 * cards that could still run.
 */
const BLOCKED_RATE_LIMIT_STATUSES = new Set(['blocked', 'rejected', 'exhausted', 'rate_limited']);

/**
 * Phrasing that means a cap AND could not plausibly be saying anything else, so
 * it is trusted on stderr on its own.
 *
 * A bare `429` is deliberately absent. It used to be here, and it matched any
 * stack trace whose first 500 bytes reached line 429 — `.../index.js:429:15`
 * supplies the word boundaries on both sides — classifying an ordinary crash as
 * a provider cap. A status code only means a status code next to something that
 * says it is one.
 */
const RATE_LIMIT_TEXT_STRONG =
  /session limit|usage limit|too many requests|rate[ _]limit(?:ed|s)?[ _](?:exceeded|reached|hit)|rate_limit_error|\b(?:status|code|http)\s*:?\s*429\b/i;

/**
 * Phrasing that USUALLY means a cap but reads the same when something merely
 * mentions one: an agent's own failed command echoed into stderr
 * (`grep -n "rate limit" README.md`) is the shape that matters.
 *
 * Used against the result payload's `result` field only, never against stderr:
 * the result field is the CLI's own statement of why it stopped, so ambiguous
 * phrasing there is still the CLI talking about itself rather than text the
 * process happened to emit. See isRateLimited for why stderr gets no such
 * benefit of the doubt.
 */
const RATE_LIMIT_TEXT_WEAK = /rate limit|rate_limit/i;

/**
 * Did this failed invocation fail because of a provider cap?
 *
 * Ordered most to least authoritative. The structured status is the only one
 * confirmed against a genuine cap; the others exist so that a CLI which
 * reports the same condition differently still parks rather than scraps.
 *
 * Deliberately NOT given stdout. It is the FILTERED stream — only `result` and
 * `rate_limit_event` lines survive the spawn's line filter — so there is
 * nothing in it to text-match beyond what `payload` and `rateLimit` already
 * carry, and its content says nothing about whether stderr should be read.
 * Issue #7 was exactly that mistake: a kept `allowed_warning` event made
 * stdout non-empty, the CLI then died on the cap without a result event, and
 * the stderr line naming the cap was skipped because stdout "had something".
 *
 * Stderr is trusted only on RATE_LIMIT_TEXT_STRONG, and `rateLimit` does not
 * gate it (see the comment at that branch). Misclassifying a crash as a cap is
 * not the cheap mistake it was when a park merely cost one short wait: a park
 * spends no execution attempt, so nothing scraps the card, and the ingress
 * listener resumes the run unattended.
 */
export function isRateLimited(
  payload: ClaudeResultPayload | null,
  rateLimit: RateLimitSnapshot | undefined,
  stderr: string,
): boolean {
  if (payload?.api_error_status === 429) return true;
  if (rateLimit?.status !== undefined && BLOCKED_RATE_LIMIT_STATUSES.has(rateLimit.status)) return true;
  // A window the last capacity reading shows fully consumed IS the cap, whatever
  // status label was attached — but only when the CLI died without a result
  // event. A result payload that exists and names another cause keeps its say.
  if (payload === null && rateLimit !== undefined && rateLimit.windows.some((w) => w.utilization >= 1)) {
    return true;
  }
  // Text is the LAST resort, and only over the result field or a short stderr —
  // never a transcript, which could contain the phrase incidentally in a tool
  // output or a file the agent happened to read.
  //
  // The result field is the CLI's own statement of why it stopped, so any cap
  // phrasing in it counts.
  if (payload !== null) {
    const result = payload.result ?? '';
    return result.length > 0 && (RATE_LIMIT_TEXT_STRONG.test(result) || RATE_LIMIT_TEXT_WEAK.test(result));
  }
  // stderr is noisier: it now reaches this point on EVERY crash that produced
  // no result event, which is most of them (the old gate skipped it whenever
  // filtered stdout had anything at all — the issue #7 bug). Only unambiguous
  // phrasing is trusted here, so a CLI that reports the cap only on stderr
  // still parks rather than scraps.
  //
  // RATE_LIMIT_TEXT_WEAK is deliberately NOT tried against stderr, not even
  // behind a capacity snapshot. That gate was tried and it gated almost
  // nothing: the CLI emits a `rate_limit_event` on essentially every call as
  // ordinary capacity reporting, so `rateLimit !== undefined` was true in the
  // common case whether or not the process died on a cap. A harness crash that
  // merely echoes the phrase (an agent's own failed
  // `grep -n "rate limit" README.md`) then read the same as a real cap and
  // parked instead of scrapping. Ambiguous text about something the CLI never
  // claimed is still ambiguous, however little capacity was left.
  const text = stderr.slice(0, 500);
  return text.length > 0 && RATE_LIMIT_TEXT_STRONG.test(text);
}

/**
 * When a rate-limited call may be retried, in epoch milliseconds.
 *
 * The BINDING window is the most-consumed one — with a five-hour and a seven-day
 * window in play, the one that actually capped you is the one nearest its
 * ceiling, and parking until the other resets would either return too early or
 * sleep for days. Returns undefined when no window was reported, leaving the
 * caller to apply its own default rather than inventing a reset time here.
 */
export function bindingResetAtMs(snapshot: RateLimitSnapshot | undefined): number | undefined {
  if (snapshot === undefined || snapshot.windows.length === 0) return undefined;
  let binding = snapshot.windows[0]!;
  for (const w of snapshot.windows) {
    if (w.utilization > binding.utilization) binding = w;
  }
  return binding.resetsAtMs;
}

/**
 * Throws a NAMED error — never resolve to a silent zero-usage success (AC4).
 * An optional `code` tags the two spawn-failure classes (timeout, non-zero
 * exit) so the executor can classify the throw (WI-566), following the
 * openai-adapter 'vision-unsupported' precedent (transform.ts reads
 * `(err as {code?}).code`). Untagged failures (parse/schema misses) still
 * throw a named claude-headless error — just without a `code` to key on.
 */
function fail(reason: string, code?: string, detail?: Record<string, unknown>): never {
  throw Object.assign(
    new Error(`claude-headless: ${reason}`),
    code !== undefined ? { code } : {},
    detail ?? {},
  );
}

/** Is the harness CLI on PATH? The detail is the resolved path, or why not. */
async function defaultProbe(command: string): Promise<BinaryProbe> {
  const resolved = Bun.which(command);
  return resolved !== null
    ? { present: true, detail: resolved }
    : { present: false, detail: `'${command}' not found on PATH` };
}

/**
 * Build the `claude-headless` adapter: the Claude Code CLI wrapped as a harness
 * station worker. Every collaborator (`command`, `run`, `probe`) is injectable
 * so the tests can drive the adapter without spawning a real binary.
 */
export function createClaudeHarnessAdapter(config: ClaudeHarnessAdapterConfig): HarnessAdapter {
  const command = config.command ?? 'claude';
  const run = config.run ?? runHarnessProcess;
  const probe = config.probe ?? (() => defaultProbe(command));

  return {
    name: 'claude-headless',
    reportsUsage: true,
    canRestrictTools: true,
    model: config.model,

    async probeBinary(): Promise<BinaryProbe> {
      return probe();
    },

    async invoke(call: HarnessInvocation): Promise<HarnessResult> {
      // stream-json + --verbose rather than plain json: the NDJSON stream is the
      // only form that carries rate_limit_event, which reports capacity
      // utilization and reset times per window (issue #5). The terminal `result`
      // event carries the same fields the plain json output did.
      const args: string[] = ['-p', '--output-format', 'stream-json', '--verbose'];
      // Station wins over the adapter's configured default (FR-10, WI-589).
      const model = call.model ?? config.model;
      if (model !== undefined) {
        args.push('--model', model);
      }
      // Comma-joined single value — claude accepts comma- or space-separated.
      // Empty tools (the executor's encoding of a waived unrestricted_tools:
      // true station) passes through with NO narrowing flag.
      if (call.tools.length > 0) {
        args.push('--allowed-tools', call.tools.join(','));
      }
      // `--` terminates option parsing so the variadic --allowed-tools cannot
      // swallow the prompt positional.
      args.push('--', call.prompt);

      const spawnResult = await run(
        { command, args },
        {
          projectRoot: config.projectRoot,
          timeoutMs: call.timeoutMs,
          envAllowlist: config.envAllowlist,
          // stream-json carries the whole agent transcript; we need two events
          // from it. Filtering as it arrives keeps a long station's memory
          // proportional to what we actually read, not to how much it did.
          stdoutLineFilter: (line) => CLAUDE_KEPT_EVENT.test(line),
        },
      );

      if (spawnResult.timedOut) {
        // No usage to recover here (issue #26 AC5): claude-headless reports
        // usage only in a terminal `result` event, and a call killed at the
        // wall-clock bound never emits one — there is nothing in stdout for
        // buildKnownUsage to read. The absent figure stays honestly unknown;
        // it must never be fabricated as a zero.
        fail('invocation exceeded its timeout and was killed', 'harness-timeout');
      }

      // PARSE BEFORE THE EXIT-CODE BAIL (issue #3). A rate-limited call exits
      // NONZERO with an EMPTY stderr and puts every diagnostic — the 429, the
      // terminal reason, the reset time — on stdout. Bailing on the exit code
      // first, and then formatting the error from stderr, produced a journal row
      // reading "exited with code 1:" with nothing after the colon, and made a
      // provider cap indistinguishable from a segfault.
      const { result: payload, rateLimit } = parseClaudeStream(spawnResult.stdout);

      if (spawnResult.exitCode !== 0) {
        // A provider cap is not a crash: retrying it immediately cannot work,
        // and spending the card's remaining attempts on it in a few seconds is
        // what destroyed whole runs. Tag it distinctly so the executor can park
        // the card instead of burning the budget.
        //
        // THREE signals, deliberately, because only the first is confirmed
        // against a real cap. If the CLI ever exits WITHOUT a terminal result
        // event, keying solely on api_error_status would throw untagged and
        // scrap — leaving issue #3 open under the exact condition it was filed
        // for. Degrading into a park is the safe direction: the worst case is
        // one short wait before the card runs again.
        if (isRateLimited(payload, rateLimit, spawnResult.stderr)) {
          const resetAtMs = bindingResetAtMs(rateLimit);
          // A rate limit does not refund tokens already spent (issue #26
          // AC5) — when the payload carries a complete usage/cost figure,
          // fold it into the throw so the executor bills it rather than
          // recording a failed-and-therefore-free attempt.
          const usage = buildKnownUsage(payload, rateLimit);
          fail(
            `provider rate limit: ${payload?.result ?? rateLimit?.status ?? 'no detail reported'}`,
            'harness-rate-limited',
            {
              ...(resetAtMs !== undefined ? { resetAtMs } : {}),
              ...(rateLimit !== undefined ? { rateLimit } : {}),
              ...(usage !== undefined ? { usage } : {}),
            },
          );
        }
        // Still nonzero, but the payload (when present) says far more than an
        // empty stderr ever did.
        const detail =
          payload !== null
            ? `${payload.terminal_reason ?? 'unknown reason'}: ${payload.result ?? ''}`.trim()
            : spawnResult.stderr.slice(0, 500);
        // Same recovery as the rate-limit branch above: a crash after the
        // provider already billed the call is not a free attempt (issue #26
        // AC5).
        const crashUsage = buildKnownUsage(payload, rateLimit);
        fail(
          `exited with code ${spawnResult.exitCode}: ${detail}`,
          'harness-nonzero-exit',
          crashUsage !== undefined ? { usage: crashUsage } : undefined,
        );
      }

      if (payload === null) {
        fail('stdout carried no result event');
      }

      if (payload.is_error === true || (payload.subtype !== undefined && payload.subtype !== 'success')) {
        fail(`reported a non-success result (subtype=${String(payload.subtype)})`);
      }
      if (typeof payload.usage !== 'object' || payload.usage === null || Array.isArray(payload.usage)) {
        fail('response payload had a missing or malformed usage object');
      }
      if (typeof payload.total_cost_usd !== 'number') {
        fail('response payload had a missing or non-numeric total_cost_usd');
      }

      // A successful call with a missing/malformed usage object already
      // `fail()`ed above — claude throws rather than reporting unknown, and
      // that is deliberate (AC4). buildKnownUsage returning undefined here
      // would therefore be unreachable, not a silent success; the two guard
      // clauses above are what keep it that way.
      const knownUsage = buildKnownUsage(payload, rateLimit);
      if (knownUsage === undefined) {
        fail('response payload had a missing or malformed usage object');
      }

      return {
        outputs: [],
        usage: knownUsage,
      };
    },
  };
}
