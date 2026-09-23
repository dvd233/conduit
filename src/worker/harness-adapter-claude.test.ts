/**
 * claude-headless harness adapter contract (WI-564).
 *
 * The first shipping real HarnessAdapter (WI-560). It builds a `claude -p`
 * invocation from its engine-config definition, spawns it through the bounded
 * process runner (WI-561) with the env allowlist (WI-562), parses the harness's
 * STRUCTURED JSON usage/cost (never scraped from free text), and translates the
 * station's declared tools allowlist into claude's `--allowed-tools` flag.
 *
 * Contract-test discipline (PRD Technical Considerations §9 / risk row "output
 * drift"): the adapter is driven against a RECORDED `claude -p --output-format
 * json` payload via an INJECTED runner seam — no live network, no real process.
 * The runner seam (default: runHarnessProcess) lets the test both feed recorded
 * stdout AND assert the exact command/args/runner-config the adapter built.
 *
 * Contract decisions this file pins:
 *   - createClaudeHarnessAdapter(config) returns a HarnessAdapter with
 *     name='claude-headless', reportsUsage=true, canRestrictTools=true, and an
 *     injected `run` seam + `probe` seam.
 *   - invoke() builds:  ['-p','--output-format','stream-json','--verbose', (--model M)?,
 *       (--allowed-tools <csv>)?, '--', <prompt>]  — the `--` terminates option
 *     parsing so the variadic --allowed-tools cannot swallow the prompt.
 *   - --allowed-tools is added IFF call.tools is non-empty; an empty tools list
 *     (the executor's encoding of a waived `unrestricted_tools: true` station)
 *     passes through with NO narrowing flag.
 *   - usage.tokens = sum of the four disjoint usage counters (input/output/
 *     cache_creation/cache_read, absent → 0); usage.cost = total_cost_usd.
 *   - a malformed/unexpected/errored/timed-out payload REJECTS with a named
 *     ('claude-headless') error — never a silent zero-usage success.
 *   - outputs is []: the executor collects declared outputs from DISK
 *     (executor.ts findMissingDeclaredOutputs), and the claude JSON carries no
 *     file manifest, so the adapter never fabricates output references.
 */
import { describe, it, expect } from 'bun:test';
import {
  createClaudeHarnessAdapter,
  parseClaudeStream,
  bindingResetAtMs,
  isRateLimited,
  dominantModel,
  type ClaudeHarnessAdapterConfig,
} from './harness-adapter-claude';
import type {
  HarnessCommand,
  HarnessRunnerConfig,
  HarnessSpawnResult,
} from './harness-runner';
import { usageFromThrow } from './harness-adapter';
import type { HarnessAdapter, HarnessInvocation, BinaryProbe } from './harness-adapter';

// ---------------------------------------------------------------------------
// Recorded `claude -p --output-format stream-json --verbose` payloads (no live
// network). Each is ONE NDJSON line; the adapter takes the last `result` event.
// ---------------------------------------------------------------------------

/** A realistic success envelope, cache counters zero (tokens = input+output). */
const RECORDED_SUCCESS = JSON.stringify({
  type: 'result',
  subtype: 'success',
  is_error: false,
  duration_ms: 5321,
  num_turns: 3,
  result: 'Implemented the task and wrote result.md.',
  session_id: '2f7c8b1e-uuid',
  total_cost_usd: 0.0123,
  usage: {
    input_tokens: 1000,
    output_tokens: 500,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
  },
});

/** A success envelope exercising the cache counters (tokens sums all four). */
const RECORDED_SUCCESS_CACHED = JSON.stringify({
  type: 'result',
  subtype: 'success',
  is_error: false,
  result: 'done',
  total_cost_usd: 0.05,
  usage: {
    input_tokens: 1000,
    output_tokens: 500,
    cache_creation_input_tokens: 300,
    cache_read_input_tokens: 200,
  },
});

// ---------------------------------------------------------------------------
// Injected runner seam — records the (cmd, config) the adapter built and
// returns a scripted spawn result. Mirrors makeStubAdapter (transform.test.ts).
// ---------------------------------------------------------------------------

interface RunnerCall {
  cmd: HarnessCommand;
  config: HarnessRunnerConfig;
}

function makeRun(
  spawn: Partial<HarnessSpawnResult> & { stdout: string },
): { run: ClaudeHarnessAdapterConfig['run']; calls: RunnerCall[] } {
  const calls: RunnerCall[] = [];
  const run = async (cmd: HarnessCommand, config: HarnessRunnerConfig): Promise<HarnessSpawnResult> => {
    calls.push({ cmd, config });
    return {
      exitCode: 0,
      stderr: '',
      durationMs: 5321,
      timedOut: false,
      ...spawn,
    };
  };
  return { run, calls };
}

const PROJECT_ROOT = '/work/project';
const ENV_ALLOWLIST = ['ANTHROPIC_API_KEY', 'PATH'];

function makeAdapter(
  over: Partial<ClaudeHarnessAdapterConfig> & { run: ClaudeHarnessAdapterConfig['run'] },
): HarnessAdapter {
  return createClaudeHarnessAdapter({
    projectRoot: PROJECT_ROOT,
    envAllowlist: ENV_ALLOWLIST,
    ...over,
  });
}

/** A complete harness invocation; `over` names only the field under test. */
function invocation(over: Partial<HarnessInvocation> = {}): HarnessInvocation {
  return {
    prompt: 'Implement the task described in task.md',
    inputs: [{ name: 'task.md', path: '/work/project/task.md' }],
    tools: ['Read', 'Write', 'Bash'],
    timeoutMs: 120_000,
    ...over,
  };
}

// ---------------------------------------------------------------------------
// Static capabilities.
// ---------------------------------------------------------------------------

describe('claude-headless adapter: identity + static capabilities', () => {
  it('is named claude-headless and reports usage + tool-restriction capability', () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS }).run });
    expect(adapter.name).toBe('claude-headless');
    expect(adapter.reportsUsage).toBe(true);
    expect(adapter.canRestrictTools).toBe(true);
  });

  it('probes binary presence via the injected probe seam without invoking', async () => {
    const present = makeAdapter({
      run: makeRun({ stdout: RECORDED_SUCCESS }).run,
      probe: async (): Promise<BinaryProbe> => ({ present: true, detail: '/usr/local/bin/claude' }),
    });
    expect((await present.probeBinary()).present).toBe(true);

    const missing = makeAdapter({
      run: makeRun({ stdout: RECORDED_SUCCESS }).run,
      probe: async (): Promise<BinaryProbe> => ({ present: false, detail: 'claude not found' }),
    });
    expect((await missing.probeBinary()).present).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// AC1 — builds the claude -p invocation and runs it through the runner with the
// env allowlist applied.
// ---------------------------------------------------------------------------

describe('claude-headless adapter: invocation building (AC1)', () => {
  it('spawns `claude -p --output-format stream-json --verbose` with the prompt as a terminated positional', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ prompt: 'do the work' }));

    expect(calls).toHaveLength(1);
    const { cmd } = calls[0]!;
    expect(cmd.command).toBe('claude');
    // Structured print mode.
    // stream-json + --verbose (issue #5): the NDJSON stream is the only form
    // carrying rate_limit_event, and its terminal `result` event has the same
    // fields the plain json output did.
    expect(cmd.args.slice(0, 4)).toEqual(['-p', '--output-format', 'stream-json', '--verbose']);
    // The prompt is the final positional, guarded by a `--` option terminator so
    // the variadic --allowed-tools cannot consume it.
    expect(cmd.args).toContain('--');
    expect(cmd.args[cmd.args.length - 1]).toBe('do the work');
  });

  it('runs through the process runner with the project root, timeout, and env allowlist applied', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ timeoutMs: 90_000 }));

    const { config } = calls[0]!;
    expect(config.projectRoot).toBe(PROJECT_ROOT);
    expect(config.timeoutMs).toBe(90_000);
    // NFR-Security-2: only engine-config-allowlisted names reach the child.
    expect(config.envAllowlist).toEqual(ENV_ALLOWLIST);
  });

  it('passes the configured binary command through to the runner', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run, command: '/opt/claude/bin/claude' });

    await adapter.invoke(invocation());

    expect(calls[0]!.cmd.command).toBe('/opt/claude/bin/claude');
  });
});

// ---------------------------------------------------------------------------
// AC2 — usage parsed from the STRUCTURED payload; outputs collected elsewhere.
// ---------------------------------------------------------------------------

describe('claude-headless adapter: structured usage parsing (AC2)', () => {
  it('reads tokens (input+output) and cost from total_cost_usd', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS }).run });

    const result = await adapter.invoke(invocation());

    // Not scraped from free text — parsed from the JSON usage object.
    // tokens stays the TRUE TOTAL (the budget authority); the breakdown splits
    // the same 1500 into its four classes (issue #5).
    expect(result.usage).toEqual({
      tokens: 1500,
      cost: 0.0123,
      breakdown: {
        inputTokens: 1000, outputTokens: 500,
        cacheReadInputTokens: 0, cacheCreationInputTokens: 0,
      },
    });
  });

  it('sums all four disjoint usage counters (input+output+cache_creation+cache_read)', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS_CACHED }).run });

    const result = await adapter.invoke(invocation());

    // The four classes are DISJOINT in claude's schema, so the total is their
    // sum and the breakdown reports each separately (issue #5) — before this,
    // all 2000 landed in the journal's input_tokens with output_tokens 0.
    expect(result.usage).toEqual({
      tokens: 2000,
      cost: 0.05,
      breakdown: {
        inputTokens: 1000, outputTokens: 500,
        cacheReadInputTokens: 200, cacheCreationInputTokens: 300,
      },
    });
  });

  it('treats absent usage subfields as zero (older payloads with only input/output)', async () => {
    const stdout = JSON.stringify({
      type: 'result',
      subtype: 'success',
      is_error: false,
      result: 'ok',
      total_cost_usd: 0.002,
      usage: { input_tokens: 40, output_tokens: 10 },
    });
    const adapter = makeAdapter({ run: makeRun({ stdout }).run });

    const result = await adapter.invoke(invocation());

    expect(result.usage).toEqual({
      tokens: 50,
      cost: 0.002,
      breakdown: {
        inputTokens: 40, outputTokens: 10,
        cacheReadInputTokens: 0, cacheCreationInputTokens: 0,
      },
    });
  });

  it('returns no fabricated output references (declared outputs are collected from disk by the executor)', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS }).run });

    const result = await adapter.invoke(invocation());

    expect(result.outputs).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// AC3 — tools allowlist → claude --allowed-tools; waiver passes through.
// ---------------------------------------------------------------------------

describe('claude-headless adapter: tools narrowing (AC3)', () => {
  it('translates a declared tools allowlist into a single --allowed-tools flag', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ tools: ['Read', 'Write', 'Bash'] }));

    const { args } = calls[0]!.cmd;
    const idx = args.indexOf('--allowed-tools');
    expect(idx).toBeGreaterThanOrEqual(0);
    // Comma-joined single value (claude accepts comma- or space-separated).
    expect(args[idx + 1]).toBe('Read,Write,Bash');
  });

  it('adds NO narrowing flag when the tools list is empty (unrestricted_tools waiver passthrough)', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ tools: [] }));

    // The executor encodes a waived `unrestricted_tools: true` station as an empty
    // tools list; the adapter must then pass through with full tool access.
    expect(calls[0]!.cmd.args).not.toContain('--allowed-tools');
  });
});

// ---------------------------------------------------------------------------
// AC4 — malformed / unexpected / errored / timed-out payloads REJECT with a
// named error, never a silent zero-usage success.
// ---------------------------------------------------------------------------

describe('claude-headless adapter: named parse failures, never silent zero-usage (AC4)', () => {
  it.each([
    ['stdout is not JSON', { stdout: 'claude: fatal: not json {' }],
    ['the usage object is absent', { stdout: JSON.stringify({ type: 'result', subtype: 'success', is_error: false, result: 'x', total_cost_usd: 0.1 }) }],
    // A PRESENT-but-malformed usage must not crash with an unguarded TypeError
    // (`typeof null === 'object'` / array indexing) — it must reject like every
    // other AC4 row, never a silent tokens=0 "success" (Lynch/Amy regression).
    ['usage is null', { stdout: JSON.stringify({ type: 'result', subtype: 'success', is_error: false, result: 'x', total_cost_usd: 0.1, usage: null }) }],
    ['usage is a string', { stdout: JSON.stringify({ type: 'result', subtype: 'success', is_error: false, result: 'x', total_cost_usd: 0.1, usage: 'a string' }) }],
    ['usage is an array', { stdout: JSON.stringify({ type: 'result', subtype: 'success', is_error: false, result: 'x', total_cost_usd: 0.1, usage: [] }) }],
    ['total_cost_usd is absent', { stdout: JSON.stringify({ type: 'result', subtype: 'success', is_error: false, result: 'x', usage: { input_tokens: 1, output_tokens: 1 } }) }],
    ['total_cost_usd is non-numeric', { stdout: JSON.stringify({ type: 'result', subtype: 'success', is_error: false, result: 'x', total_cost_usd: 'free', usage: { input_tokens: 1, output_tokens: 1 } }) }],
    ['the harness reports is_error', { stdout: JSON.stringify({ type: 'result', subtype: 'error_during_execution', is_error: true, result: 'boom', total_cost_usd: 0.1, usage: { input_tokens: 1, output_tokens: 1 } }) }],
  ])('rejects when %s — a named claude-headless error', async (_label, spawn) => {
    const adapter = makeAdapter({ run: makeRun(spawn).run });

    // Never resolves to a zero-usage success — it rejects, and the executor's
    // invoke() try/catch escalates the card to hold.
    await expect(adapter.invoke(invocation())).rejects.toThrow(/claude-headless/i);
  });

  it('rejects when the process timed out (a killed attempt is not a zero-usage success)', async () => {
    const adapter = makeAdapter({
      run: makeRun({ stdout: '', timedOut: true, exitCode: 137 }).run,
    });

    await expect(adapter.invoke(invocation())).rejects.toThrow(/claude-headless/i);
  });

  it('rejects on a non-zero exit code even when stdout is empty', async () => {
    const adapter = makeAdapter({
      run: makeRun({ stdout: '', exitCode: 1, stderr: 'auth error' }).run,
    });

    await expect(adapter.invoke(invocation())).rejects.toThrow(/claude-headless/i);
  });
});

// ---------------------------------------------------------------------------
// Issue #3 — a provider rate limit is not a crash
// ---------------------------------------------------------------------------

/**
 * A genuinely rate-limited `claude -p` run, as probed against a capped account:
 * NONZERO exit, EMPTY stderr, and every diagnostic on stdout. Note
 * `subtype: 'success'` sitting alongside `is_error: true` — anything keying on
 * subtype to decide success gets this exactly backwards.
 */
const RECORDED_RATE_LIMITED = JSON.stringify({
  type: 'result',
  subtype: 'success',
  is_error: true,
  api_error_status: 429,
  terminal_reason: 'api_error',
  result: "You've hit your session limit · resets 3pm (UTC)",
  total_cost_usd: 0,
});

/**
 * A rate_limit_event CAPTURED FROM THE REAL CLI (2.1.226). The shape is FLAT —
 * one window per event, fields at the top level, named by `rateLimitType`.
 *
 * The earlier version of this fixture was hand-built in a nested
 * `unifiedWindows` shape the CLI does not emit, so the parser and the test
 * agreed with each other and neither agreed with reality: `windows` came back
 * empty on every real call, the reported reset was never used, and the park
 * silently fell back to its default interval.
 */
const RECORDED_RATE_LIMIT_EVENT = JSON.stringify({
  type: 'rate_limit_event',
  rate_limit_info: {
    status: 'allowed_warning',
    resetsAt: 1788328800,
    rateLimitType: 'seven_day',
    utilization: 0.77,
    isUsingOverage: false,
    surpassedThreshold: 0.75,
  },
  session_id: '2f7c8b1e-uuid',
});

/** The nested form, kept working as a fallback for builds that emit it. */
const RECORDED_RATE_LIMIT_EVENT_NESTED = JSON.stringify({
  type: 'rate_limit_event',
  rate_limit_info: {
    status: 'allowed_warning',
    isUsingOverage: false,
    unifiedWindows: {
      five_hour: { utilization: 0.06, resetsAt: 1788125400 },
      seven_day: { utilization: 0.75, resetsAt: 1788328800 },
    },
  },
});

describe('claude-headless adapter: rate limits (issue #3)', () => {
  it('classifies a 429 distinctly from a crash, so the executor can park it', async () => {
    // The WHOLE point: before this, a cap and a segfault were both
    // 'harness-nonzero-exit', so nothing downstream could tell "retrying is
    // pointless for hours" from "retrying might work".
    const adapter = makeAdapter({
      run: makeRun({ stdout: RECORDED_RATE_LIMITED, exitCode: 1, stderr: '' }).run,
    });

    await expect(adapter.invoke(invocation())).rejects.toMatchObject({
      code: 'harness-rate-limited',
    });
  });

  it('reads the 429 from stdout even though the exit code fires first', async () => {
    // stderr is EMPTY on this failure. Bailing on the exit code before parsing
    // stdout produced a journal row reading "exited with code 1:" with nothing
    // after the colon — every diagnostic discarded.
    const adapter = makeAdapter({
      run: makeRun({ stdout: RECORDED_RATE_LIMITED, exitCode: 1, stderr: '' }).run,
    });

    await expect(adapter.invoke(invocation())).rejects.toThrow(/session limit/);
  });

  it('carries the provider-reported reset time so the park is not a guess', async () => {
    const stdout = [RECORDED_RATE_LIMIT_EVENT, RECORDED_RATE_LIMITED].join('\n');
    const adapter = makeAdapter({ run: makeRun({ stdout, exitCode: 1 }).run });

    // The seven_day window is the most-consumed, so it is the one that capped
    // this call — resetsAt is epoch SECONDS on the wire, milliseconds here.
    await expect(adapter.invoke(invocation())).rejects.toMatchObject({
      code: 'harness-rate-limited',
      resetAtMs: 1788328800 * 1000,
    });
  });

  it('still reports a NON-429 nonzero exit as a crash, with the payload detail', async () => {
    const stdout = JSON.stringify({
      type: 'result', is_error: true, terminal_reason: 'refusal', result: 'nope',
    });
    const adapter = makeAdapter({ run: makeRun({ stdout, exitCode: 2, stderr: '' }).run });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);
    expect((err as { code?: string }).code).toBe('harness-nonzero-exit');
    // Strictly more informative than the empty stderr it used to print.
    expect((err as Error).message).toMatch(/refusal/);
  });

  it('falls back to stderr when the stream carried no result event at all', async () => {
    const adapter = makeAdapter({
      run: makeRun({ stdout: '', exitCode: 3, stderr: 'segfault' }).run,
    });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);
    expect((err as { code?: string }).code).toBe('harness-nonzero-exit');
    expect((err as Error).message).toMatch(/segfault/);
  });
});

// ---------------------------------------------------------------------------
// Issue #7 — the cap arrives AFTER a warning event, with no result event
// ---------------------------------------------------------------------------

/**
 * The production shape from issue #7: the five-hour window at 0.99, the CLI
 * emitted its `allowed_warning` rate_limit_event, then died on the cap WITHOUT
 * a terminal result event. The stream filter keeps the warning line, so stdout
 * is NON-empty — and the classifier's stderr fallback was gated on stdout being
 * empty, so the only place the cap was named was never read.
 */
const RECORDED_WARNING_AT_0_99 = JSON.stringify({
  type: 'rate_limit_event',
  rate_limit_info: {
    status: 'allowed_warning',
    resetsAt: 1788328800,
    rateLimitType: 'five_hour',
    utilization: 0.99,
    isUsingOverage: false,
    surpassedThreshold: 0.75,
  },
});

/** What the CLI prints on stderr when the cap stops it before any result event. */
const CAP_STDERR = "You've hit your usage limit · resets 8pm (UTC)";

describe('claude-headless adapter: a cap after a warning event, no result event (issue #7)', () => {
  it('classifies the incident shape as rate-limited, not as a crash', async () => {
    // Before this, the failure was 'harness-nonzero-exit': the executor burned
    // max_execution_attempts on it in seconds and scrapped the card with
    // attempt=0 — exactly the reported end state.
    const adapter = makeAdapter({
      run: makeRun({ stdout: RECORDED_WARNING_AT_0_99, exitCode: 1, stderr: CAP_STDERR }).run,
    });

    await expect(adapter.invoke(invocation())).rejects.toMatchObject({
      code: 'harness-rate-limited',
    });
  });

  it('parks on the reset the warning event reported, not the default interval', async () => {
    const adapter = makeAdapter({
      run: makeRun({ stdout: RECORDED_WARNING_AT_0_99, exitCode: 1, stderr: CAP_STDERR }).run,
    });

    await expect(adapter.invoke(invocation())).rejects.toMatchObject({
      code: 'harness-rate-limited',
      resetAtMs: 1788328800 * 1000,
    });
  });

  it('still reports a crash when the warning event is followed by an unrelated stderr', async () => {
    // The warning alone must not park: 0.99 is approaching the cap, not at it.
    const adapter = makeAdapter({
      run: makeRun({ stdout: RECORDED_WARNING_AT_0_99, exitCode: 1, stderr: 'segfault' }).run,
    });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);
    expect((err as { code?: string }).code).toBe('harness-nonzero-exit');
    expect((err as Error).message).toMatch(/segfault/);
  });
});

describe('parseClaudeStream / bindingResetAtMs', () => {
  it('takes the LAST result event and the LAST capacity reading', () => {
    const stdout = [
      JSON.stringify({ type: 'system', subtype: 'init' }),
      RECORDED_RATE_LIMIT_EVENT,
      RECORDED_SUCCESS,
    ].join('\n');

    const parsed = parseClaudeStream(stdout);
    expect(parsed.result?.total_cost_usd).toBe(0.0123);
    expect(parsed.rateLimit?.status).toBe('allowed_warning');
    // The real event carries exactly ONE window.
    expect(parsed.rateLimit?.windows).toEqual([
      { name: 'seven_day', utilization: 0.77, resetsAtMs: 1788328800 * 1000 },
    ]);
  });

  it('skips a malformed line rather than failing the whole invocation', () => {
    // This parse now runs on the FAILURE path too, where a half-written stream
    // is likely — one bad line must not discard the diagnostics after it.
    const parsed = parseClaudeStream(['{not json', RECORDED_SUCCESS].join('\n'));
    expect(parsed.result?.total_cost_usd).toBe(0.0123);
  });

  it('is empty for a stream with no result event', () => {
    expect(parseClaudeStream('').result).toBeNull();
    expect(parseClaudeStream(JSON.stringify({ type: 'system' })).result).toBeNull();
  });

  it('reads the FLAT window the real CLI emits', () => {
    // The regression: against the nested-only parser this returned undefined on
    // every real call, so the park never used the provider's reported reset.
    const { rateLimit } = parseClaudeStream(RECORDED_RATE_LIMIT_EVENT);
    expect(bindingResetAtMs(rateLimit)).toBe(1788328800 * 1000);
    expect(rateLimit?.windows[0]?.utilization).toBe(0.77);
  });

  it('ACCUMULATES windows reported across separate events', () => {
    // The flat payload carries ONE window per event, so a run reporting
    // five_hour and seven_day separately would keep only whichever arrived
    // last. Since the park targets the MOST-CONSUMED window, dropping one
    // silently picks the wrong reset — here it would park until the five_hour
    // reset while the seven_day cap is the one actually blocking.
    const fiveHour = JSON.stringify({
      type: 'rate_limit_event',
      rate_limit_info: { status: 'allowed', rateLimitType: 'five_hour', utilization: 0.06, resetsAt: 1788125400 },
    });
    const { rateLimit } = parseClaudeStream([fiveHour, RECORDED_RATE_LIMIT_EVENT].join('\n'));

    expect(rateLimit?.windows).toHaveLength(2);
    expect(bindingResetAtMs(rateLimit)).toBe(1788328800 * 1000);
  });

  it('keeps the LATEST reading for a window reported twice', () => {
    const early = JSON.stringify({
      type: 'rate_limit_event',
      rate_limit_info: { rateLimitType: 'seven_day', utilization: 0.10, resetsAt: 1788000000 },
    });
    const { rateLimit } = parseClaudeStream([early, RECORDED_RATE_LIMIT_EVENT].join('\n'));

    expect(rateLimit?.windows).toHaveLength(1);
    expect(rateLimit?.windows[0]?.utilization).toBe(0.77);
  });

  it('carries the latest status forward across events', () => {
    const early = JSON.stringify({
      type: 'rate_limit_event',
      rate_limit_info: { status: 'allowed', rateLimitType: 'five_hour', utilization: 0.01, resetsAt: 1788125400 },
    });
    const { rateLimit } = parseClaudeStream([early, RECORDED_RATE_LIMIT_EVENT].join('\n'));

    expect(rateLimit?.status).toBe('allowed_warning');
  });

  it('still reads the NESTED form, picking the most-consumed window', () => {
    // Parking until the five_hour reset would return while seven_day is still
    // capped, so the binding window is the one nearest its ceiling.
    const { rateLimit } = parseClaudeStream(RECORDED_RATE_LIMIT_EVENT_NESTED);
    expect(rateLimit?.windows).toHaveLength(2);
    expect(bindingResetAtMs(rateLimit)).toBe(1788328800 * 1000);
  });

  it('returns undefined with no windows, leaving the caller to default', () => {
    expect(bindingResetAtMs(undefined)).toBeUndefined();
    expect(bindingResetAtMs({ windows: [] })).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Review follow-ups: classification that degrades safely, and model attribution
// that survives the normal multi-model case.
// ---------------------------------------------------------------------------

describe('isRateLimited', () => {
  it('trusts the structured 429 first', () => {
    expect(isRateLimited({ api_error_status: 429 }, undefined, '')).toBe(true);
  });

  it('parks when the CLI exits with NO result event but a blocked status', () => {
    // The scenario that would otherwise leave #3 open under the exact condition
    // it was filed for: no result event means api_error_status is unreadable,
    // and an untagged throw scraps the card.
    expect(isRateLimited(null, { status: 'blocked', windows: [] }, '')).toBe(true);
  });

  it('does NOT read a bare 429 out of a stack trace line number', () => {
    // ':429:' supplies word boundaries on both sides, so a bare \\b429\\b used to
    // classify any crash whose first 500 bytes reached line 429 as a cap — and
    // a cap spends no execution attempt, so nothing would ever scrap the card.
    expect(isRateLimited(null, undefined, 'TypeError: x is not a function\n    at f (/app/lib/index.js:429:15)')).toBe(
      false,
    );
  });

  it('does NOT take a capacity snapshot as evidence of a cap on ambiguous stderr text', () => {
    // The CLI emits rate_limit_event lines routinely, for ordinary capacity
    // reporting, so a snapshot merely existing says nothing about whether the
    // process actually died on a cap. Only unambiguous stderr phrasing is
    // trusted now; a snapshot changes nothing about that.
    const echoed = 'Error: command failed: grep -n "rate limit" README.md';
    expect(isRateLimited(null, undefined, echoed)).toBe(false);
    expect(isRateLimited(null, { windows: [{ utilization: 0.4 }] } as never, echoed)).toBe(false);
  });

  it('does NOT park on a near-full window plus ambiguous stderr text', () => {
    // 0.99 is the exact shape of the allowed_warning reading from the original
    // incident. Even a window that close to its ceiling is not licence to
    // believe ambiguous phrasing: the stderr text still only mentions a rate
    // limit incidentally (an echoed grep command), and the CLI never says so
    // itself.
    const echoed = 'Error: command failed: grep -n "rate limit" README.md';
    const snapshot = { windows: [{ name: 'five_hour', utilization: 0.99, resetsAtMs: 1788328800_000 }] };
    expect(isRateLimited(null, snapshot as never, echoed)).toBe(false);
  });

  it('does NOT classify an ordinary crash with no cap phrasing at all', () => {
    expect(isRateLimited(null, undefined, 'Segmentation fault (core dumped)')).toBe(false);
  });

  it('falls back to the result text when nothing structured says so', () => {
    expect(isRateLimited({ result: "You've hit your session limit" }, undefined, '')).toBe(true);
  });

  it('reads UNAMBIGUOUS stderr text with no result payload — a warning event is no reason not to', () => {
    expect(isRateLimited(null, undefined, 'Error: 429 Too Many Requests')).toBe(true);
    expect(isRateLimited(null, undefined, "You've hit your session limit")).toBe(true);
    // Issue #7: the CLI had emitted an allowed_warning event (stdout non-empty
    // once filtered) and then died on the cap with no result event. The old
    // classifier consulted stderr only when stdout was empty, so the one line
    // naming the cap was never read.
    const snapshot = { status: 'allowed_warning', windows: [{ name: 'five_hour', utilization: 0.99, resetsAtMs: 1788328800_000 }] };
    expect(isRateLimited(null, snapshot, "You've hit your usage limit · resets 8pm (UTC)")).toBe(true);
  });

  it('does NOT park on a warning event plus an unrelated stderr and no result', () => {
    expect(isRateLimited(null, { status: 'allowed_warning', windows: [] }, 'segfault')).toBe(false);
  });

  it('parks when a captured window is fully consumed and no result event arrived', () => {
    // utilization is the fraction of the window spent (RateLimitWindow); at
    // 1.0 the cap IS reached, whatever `status` the CLI managed to attach
    // before dying. Only without a result payload — a payload that exists and
    // says something else keeps its say.
    const full = { status: 'allowed_warning', windows: [{ name: 'five_hour', utilization: 1, resetsAtMs: 1788328800_000 }] };
    expect(isRateLimited(null, full, '')).toBe(true);
    expect(isRateLimited({ terminal_reason: 'refusal', result: 'nope' }, full, '')).toBe(false);
  });

  it('a result payload that names another cause is NOT overridden by stderr', () => {
    expect(isRateLimited({ terminal_reason: 'refusal', result: 'nope' }, undefined, 'rate limit')).toBe(false);
  });

  it('does NOT park on allowed_warning — approaching a cap is not being stopped', () => {
    expect(isRateLimited(null, { status: 'allowed_warning', windows: [] }, '')).toBe(false);
  });

  it('does not park an ordinary crash', () => {
    expect(isRateLimited({ terminal_reason: 'refusal', result: 'nope' }, undefined, 'segfault')).toBe(false);
  });
});

describe('dominantModel', () => {
  it('attributes to the model that consumed the most tokens', () => {
    // TWO OR MORE entries is the NORMAL case: Claude Code bills a haiku model
    // for side tasks alongside the main model, so even a trivial call returns
    // two. Requiring exactly one left the column empty on essentially every row.
    expect(
      dominantModel({
        'claude-haiku-4-5-20251001': { canonicalModel: 'claude-haiku-4-5-20251001', inputTokens: 10, outputTokens: 5 },
        'claude-opus-5': { canonicalModel: 'claude-opus-5', inputTokens: 2, outputTokens: 4, cacheReadInputTokens: 26_696 },
      }),
    ).toBe('claude-opus-5');
  });

  it('counts cache tokens toward the share, not just fresh input', () => {
    expect(
      dominantModel({
        a: { canonicalModel: 'a', inputTokens: 500 },
        b: { canonicalModel: 'b', inputTokens: 1, cacheReadInputTokens: 900 },
      }),
    ).toBe('b');
  });

  it('falls back to the key when canonicalModel is absent', () => {
    expect(dominantModel({ 'some-model': { inputTokens: 1 } })).toBe('some-model');
  });

  it('is undefined when nothing was reported', () => {
    expect(dominantModel(undefined)).toBeUndefined();
    expect(dominantModel({})).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Issue #26 AC5 — a throw that was still BILLED carries the real usage figure,
// when the adapter genuinely has one in hand at the moment it fails.
// ---------------------------------------------------------------------------

describe('claude-headless adapter: usage carried through a billed throw (issue #26 AC5)', () => {
  it('a non-zero exit whose stdout carried a complete result event carries the real usage', async () => {
    // A non-429 crash (terminal_reason: refusal) that still reported a full
    // result payload with structured usage before dying nonzero.
    const stdout = JSON.stringify({
      type: 'result',
      subtype: 'error_during_execution',
      is_error: true,
      terminal_reason: 'refusal',
      result: 'the agent refused the task',
      total_cost_usd: 0.0087,
      usage: {
        input_tokens: 500,
        output_tokens: 200,
        cache_creation_input_tokens: 50,
        cache_read_input_tokens: 25,
      },
    });
    const adapter = makeAdapter({ run: makeRun({ stdout, exitCode: 2, stderr: '' }).run });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);

    expect((err as { code?: string }).code).toBe('harness-nonzero-exit');
    // The provider charged for this call before it died — the executor must
    // fold the real total, not record a failed-and-therefore-free attempt.
    expect(usageFromThrow(err)).toEqual({
      tokens: 775,
      cost: 0.0087,
      breakdown: {
        inputTokens: 500, outputTokens: 200,
        cacheReadInputTokens: 25, cacheCreationInputTokens: 50,
      },
    });
  });

  it('a rate-limited exit keeps its resetAtMs/rateLimit detail AND carries usage', async () => {
    const rateLimited = JSON.stringify({
      type: 'result',
      subtype: 'success',
      is_error: true,
      api_error_status: 429,
      terminal_reason: 'api_error',
      result: "You've hit your session limit · resets 3pm (UTC)",
      total_cost_usd: 0.0041,
      usage: {
        input_tokens: 300,
        output_tokens: 100,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
      },
    });
    const stdout = [RECORDED_RATE_LIMIT_EVENT, rateLimited].join('\n');
    const adapter = makeAdapter({ run: makeRun({ stdout, exitCode: 1 }).run });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);

    expect((err as { code?: string }).code).toBe('harness-rate-limited');
    // Existing rate-limit detail must stay intact alongside the new usage field.
    expect((err as { resetAtMs?: number }).resetAtMs).toBe(1788328800 * 1000);
    expect((err as { rateLimit?: unknown }).rateLimit).toBeDefined();
    // KnownUsage carries its own optional rateLimit field (same as the success
    // path's usage object), alongside the throw's separate top-level rateLimit
    // detail used for the park calculation.
    expect(usageFromThrow(err)).toEqual({
      tokens: 400,
      cost: 0.0041,
      breakdown: {
        inputTokens: 300, outputTokens: 100,
        cacheReadInputTokens: 0, cacheCreationInputTokens: 0,
      },
      rateLimit: {
        status: 'allowed_warning',
        usingOverage: false,
        windows: [{ name: 'seven_day', utilization: 0.77, resetsAtMs: 1788328800 * 1000 }],
      },
    });
  });

  it('a non-zero exit whose stdout carried NO result event has no usage to recover — absent, not zero', async () => {
    const adapter = makeAdapter({
      run: makeRun({ stdout: '', exitCode: 3, stderr: 'segfault' }).run,
    });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);

    expect((err as { code?: string }).code).toBe('harness-nonzero-exit');
    // AC2's "unknown is not zero" rule on the THROW path: nothing to recover
    // means the field is absent, never a fabricated { tokens: 0, cost: 0 }.
    expect(usageFromThrow(err)).toBeUndefined();
  });

  it('a wall-clock timeout carries no usage (claude reports usage only in a terminal result event a kill never emits)', async () => {
    const adapter = makeAdapter({
      run: makeRun({ stdout: '', timedOut: true, exitCode: 137 }).run,
    });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);

    expect((err as { code?: string }).code).toBe('harness-timeout');
    expect(usageFromThrow(err)).toBeUndefined();
  });
});
