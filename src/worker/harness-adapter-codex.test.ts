/**
 * codex-exec harness adapter contract (WI-574) — the SECOND shipping real
 * HarnessAdapter, proving the no-provider-lock-in seam (Goal "No provider
 * lock-in", Phase 2): a flow swaps providers by changing ONLY the adapter name.
 *
 * Like claude-headless (WI-564) it builds its invocation from engine config,
 * spawns through the bounded process runner (WI-561) with the env allowlist
 * (WI-562), and reads STRUCTURED usage from the harness's own output — never
 * scraped from free text. It is driven against a RECORDED `codex exec --json`
 * event stream via an INJECTED runner seam (no live network, no real process).
 *
 * ── REAL codex schema (verified against the official Codex manual — Amy/Lynch
 *    WI-574 re-review) ─────────────────────────────────────────────────────────
 *
 *   codex `exec --json` emits a JSONL EVENT STREAM (one JSON object per line).
 *   Token counts live in the terminal `turn.completed` event's `usage` object,
 *   which carries ONLY token counters — input_tokens / cached_input_tokens /
 *   output_tokens / reasoning_output_tokens — and NEVER a cost field. Fixtures
 *   here therefore do NOT invent a total_cost_usd; the earlier fabricated-cost
 *   fixtures were rejected because they never exercised the real path (usage
 *   present, no cost).
 *
 * ── How codex-exec DIFFERS from claude-headless (the point of a second adapter:
 *    the seam must tolerate capability-divergent providers) ─────────────────────
 *
 *   1. USAGE SHAPE: codex emits a JSONL event stream vs claude's single JSON
 *      result object. usage.tokens = input_tokens + output_tokens from the
 *      terminal turn.completed event.
 *   2. NO COST SIGNAL: codex's real usage carries no cost, so usage.cost is a
 *      best-effort 0 — the genuinely-known token counts are still reported
 *      (never discarded), never a fabricated cost.
 *   3. TOOL RESTRICTION: codex exec has no per-tool allowlist (it gates via
 *      sandbox / approval modes), so canRestrictTools is FALSE and call.tools is
 *      never translated into a flag. A tools-declaring flow then needs the
 *      `unrestricted_tools` waiver (WI-563).
 *   4. USAGE-BLIND GRACE: a SUCCESSFUL run whose stream carries NO parseable
 *      usage event AT ALL reports usage EXPLICITLY UNKNOWN ({ unknown: true }) —
 *      never zero (PRD §NFR-2). claude instead throws on a missing usage object.
 *      Both still THROW on a real spawn failure (timeout / non-zero exit).
 *
 * Contract decisions this file pins:
 *   - createCodexHarnessAdapter(config) returns a HarnessAdapter with
 *     name='codex-exec', reportsUsage=true, canRestrictTools=FALSE, and injected
 *     `run` + `probe` seams (config mirrors ClaudeHarnessAdapterConfig).
 *   - invoke() builds:  ['exec','--json', (--model M)?, <prompt>]  spawned as the
 *     configured command (default 'codex'), run through the runner with
 *     projectRoot + timeout + envAllowlist applied.
 *   - NO tools flag is ever added: call.tools is ignored.
 *   - usage.tokens = input_tokens + output_tokens from the terminal usage event;
 *     usage.cost = 0 (real schema has no cost). A success stream with NO usage
 *     event → { unknown: true } (never { tokens:0, cost:0 }).
 *   - a timed-out / non-zero-exit spawn REJECTS with a named ('codex-exec')
 *     error — never a silent success.
 *   - outputs is []: the executor collects declared outputs from DISK.
 *   - the config-only-swap seam: createHarnessRegistry([claude, codex]) resolves
 *     BOTH by name.
 */
import { describe, it, expect } from 'bun:test';
import {
  createCodexHarnessAdapter,
  type CodexHarnessAdapterConfig,
} from './harness-adapter-codex';
import { createClaudeHarnessAdapter } from './harness-adapter-claude';
import type {
  HarnessCommand,
  HarnessRunnerConfig,
  HarnessSpawnResult,
} from './harness-runner';
import { createHarnessRegistry, usageFromThrow } from './harness-adapter';
import type { HarnessAdapter, HarnessInvocation, BinaryProbe } from './harness-adapter';

// ---------------------------------------------------------------------------
// Recorded `codex exec --json` event streams (JSONL, no live network).
// The terminal `turn.completed` event carries the structured token usage in
// codex's REAL schema — token counters only, never a cost field.
// ---------------------------------------------------------------------------

const RECORDED_SUCCESS = [
  JSON.stringify({ type: 'thread.started', thread_id: 't_abc' }),
  JSON.stringify({ type: 'item.completed', item: { type: 'agent_message', text: 'Implemented the task and wrote result.json.' } }),
  JSON.stringify({
    type: 'turn.completed',
    // Real codex usage shape: token counters only, NO total_cost_usd.
    usage: { input_tokens: 1200, cached_input_tokens: 256, output_tokens: 340, reasoning_output_tokens: 120 },
  }),
].join('\n');

/** A successful stream that carries NO usage event → usage explicitly unknown. */
const RECORDED_SUCCESS_NO_USAGE = [
  JSON.stringify({ type: 'thread.started', thread_id: 't_def' }),
  JSON.stringify({ type: 'item.completed', item: { type: 'agent_message', text: 'done' } }),
].join('\n');

// ---------------------------------------------------------------------------
// Injected runner seam (records the cmd/config built; returns scripted result).
// ---------------------------------------------------------------------------

interface RunnerCall {
  cmd: HarnessCommand;
  config: HarnessRunnerConfig;
}

function makeRun(
  spawn: Partial<HarnessSpawnResult> & { stdout: string },
): { run: CodexHarnessAdapterConfig['run']; calls: RunnerCall[] } {
  const calls: RunnerCall[] = [];
  const run = async (cmd: HarnessCommand, config: HarnessRunnerConfig): Promise<HarnessSpawnResult> => {
    calls.push({ cmd, config });
    return { exitCode: 0, stderr: '', durationMs: 4200, timedOut: false, ...spawn };
  };
  return { run, calls };
}

const PROJECT_ROOT = '/work/project';
const ENV_ALLOWLIST = ['OPENAI_API_KEY', 'PATH'];

function makeAdapter(
  over: Partial<CodexHarnessAdapterConfig> & { run: CodexHarnessAdapterConfig['run'] },
): HarnessAdapter {
  return createCodexHarnessAdapter({ projectRoot: PROJECT_ROOT, envAllowlist: ENV_ALLOWLIST, ...over });
}

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
// Identity + static capabilities — a DIFFERENT capability profile.
// ---------------------------------------------------------------------------

describe('codex-exec adapter: identity + static capabilities', () => {
  it('is named codex-exec, reports usage, and CANNOT restrict tools', () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS }).run });
    expect(adapter.name).toBe('codex-exec');
    expect(adapter.reportsUsage).toBe(true);
    // Codex gates via sandbox/approval, not a per-tool allowlist — so a
    // tools-declaring flow needs the unrestricted_tools waiver (WI-563).
    expect(adapter.canRestrictTools).toBe(false);
  });

  it('probes binary presence via the injected probe seam without invoking', async () => {
    const present = makeAdapter({
      run: makeRun({ stdout: RECORDED_SUCCESS }).run,
      probe: async (): Promise<BinaryProbe> => ({ present: true, detail: '/usr/local/bin/codex' }),
    });
    expect((await present.probeBinary()).present).toBe(true);

    const missing = makeAdapter({
      run: makeRun({ stdout: RECORDED_SUCCESS }).run,
      probe: async (): Promise<BinaryProbe> => ({ present: false, detail: 'codex not found' }),
    });
    expect((await missing.probeBinary()).present).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Invocation building.
// ---------------------------------------------------------------------------

describe('codex-exec adapter: invocation building', () => {
  it('spawns `codex exec --json` with the prompt as the final positional', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ prompt: 'do the work' }));

    expect(calls).toHaveLength(1);
    const { cmd } = calls[0]!;
    expect(cmd.command).toBe('codex');
    expect(cmd.args.slice(0, 2)).toEqual(['exec', '--json']);
    expect(cmd.args[cmd.args.length - 1]).toBe('do the work');
  });

  it('places a literal `--` terminator immediately before the prompt so a dash-leading prompt is not parsed as a flag', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ prompt: '--help me refactor' }));

    const { args } = calls[0]!.cmd;
    // The prompt is the final positional; the token right before it is `--`.
    expect(args[args.length - 1]).toBe('--help me refactor');
    expect(args[args.length - 2]).toBe('--');
  });

  it('adds a --model flag only when a model is configured', async () => {
    const withModel = makeRun({ stdout: RECORDED_SUCCESS });
    await makeAdapter({ run: withModel.run, model: 'o3' }).invoke(invocation());
    const idx = withModel.calls[0]!.cmd.args.indexOf('--model');
    expect(idx).toBeGreaterThanOrEqual(0);
    expect(withModel.calls[0]!.cmd.args[idx + 1]).toBe('o3');

    const noModel = makeRun({ stdout: RECORDED_SUCCESS });
    await makeAdapter({ run: noModel.run }).invoke(invocation());
    expect(noModel.calls[0]!.cmd.args).not.toContain('--model');
  });

  it('runs through the process runner with project root, timeout, and env allowlist applied', async () => {
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
    const adapter = makeAdapter({ run, command: '/opt/codex/bin/codex' });

    await adapter.invoke(invocation());

    expect(calls[0]!.cmd.command).toBe('/opt/codex/bin/codex');
  });

  it('never adds a per-tool-name flag — codex expresses tools as a sandbox envelope (the original per-list tool-expression work)', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ tools: ['Read', 'Write', 'Bash'] }));

    const { args } = calls[0]!.cmd;
    expect(args).not.toContain('--allowed-tools');
    expect(args).not.toContain('--tools');
    expect(args).toContain('--sandbox');
    expect(args).toContain('workspace-write');
  });
});

// ---------------------------------------------------------------------------
// The original per-list tool-expression work — capability-envelope planning: a declared allowlist maps onto the
// OS-enforced sandbox envelope whose capability classes match it EXACTLY, or
// is rejected. The plan is judged identically at load (canExpressTools) and at
// invoke (defense-in-depth).
// ---------------------------------------------------------------------------

describe('codex-exec adapter: capability-envelope tool expression (the original per-list tool-expression work)', () => {
  it('maps a read-only list onto `--sandbox read-only` (Bash allowed: read-only exec is confined to reads)', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ tools: ['Read', 'Glob', 'Grep', 'Bash'] }));

    const { args } = calls[0]!.cmd;
    const sandboxAt = args.indexOf('--sandbox');
    expect(args[sandboxAt + 1]).toBe('read-only');
    expect(args.join(' ')).not.toContain('network_access');
  });

  it('maps a write list (with Bash) onto workspace-write, and network tools onto network_access=true', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(
      invocation({ tools: ['Read', 'Write', 'Bash', 'WebFetch', 'WebSearch'] }),
    );

    const { args } = calls[0]!.cmd;
    const sandboxAt = args.indexOf('--sandbox');
    expect(args[sandboxAt + 1]).toBe('workspace-write');
    expect(args).toContain('sandbox_workspace_write.network_access=true');
  });

  it('pins network_access=false explicitly for a write list WITHOUT network tools (hermetic against user config.toml)', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ tools: ['Read', 'Write', 'Bash'] }));

    // A user-level ~/.codex/config.toml can set network_access=true for every
    // run; the envelope stays hermetic only if the CLI pins the posture BOTH
    // ways (the pre-public Codex sandbox review). Verified live that -c overrides the config file.
    expect(calls[0]!.cmd.args).toContain('sandbox_workspace_write.network_access=false');
  });

  it('tags an unexpressible-list invoke failure with a stable code for the executor to classify', async () => {
    const { run } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    try {
      await adapter.invoke(invocation({ tools: ['Read', 'Write'] }));
      throw new Error('expected invoke to reject');
    } catch (err) {
      expect((err as { code?: string }).code).toBe('harness-tools-unexpressible');
    }
  });

  it('adds NO sandbox flag for an empty list — the waived-unrestricted path is unchanged (WI-563)', async () => {
    const { run, calls } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    await adapter.invoke(invocation({ tools: [] }));

    expect(calls[0]!.cmd.args).not.toContain('--sandbox');
  });

  it('fails closed at invoke on a list no envelope matches (defense-in-depth behind load validation)', async () => {
    const { run } = makeRun({ stdout: RECORDED_SUCCESS });
    const adapter = makeAdapter({ run });

    // Write without Bash: workspace-write's shell could write more than the
    // flow granted — no matching envelope.
    await expect(adapter.invoke(invocation({ tools: ['Read', 'Write'] }))).rejects.toThrow(
      /no codex sandbox envelope/,
    );
  });

  it('canExpressTools mirrors the plan: expressible lattice points true, everything else false', () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS }).run });
    const can = (tools: string[]): boolean => adapter.canExpressTools!(tools);

    expect(can(['Read', 'Glob', 'Grep'])).toBe(true); // read-only
    expect(can(['Read', 'Bash'])).toBe(true); // read-only, sandboxed exec ⊆ read
    expect(can(['Read', 'Write', 'Edit', 'Bash'])).toBe(true); // workspace-write
    expect(can(['Read', 'Write', 'Bash', 'WebFetch'])).toBe(true); // + network
    expect(can(['Read', 'Write'])).toBe(false); // write-capable exec not granted
    expect(can(['Read', 'WebFetch'])).toBe(false); // no read-only+network envelope
    expect(can(['Read', 'Task'])).toBe(false); // unknown tool class fails closed
    expect(can([])).toBe(true); // nothing to enforce
    // The legacy boolean stays conservative for boolean-only callers.
    expect(adapter.canRestrictTools).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Structured usage parsing — codex's REAL schema (token counts, no cost).
// ---------------------------------------------------------------------------

describe('codex-exec adapter: structured usage parsing (real schema)', () => {
  it('reads token counts from the terminal turn.completed usage event; cost is a best-effort 0', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS }).run });

    const result = await adapter.invoke(invocation());

    // input_tokens (1200) + output_tokens (340) + reasoning_output_tokens (120)
    // = 1660. reasoning_output_tokens is a BILLED output counter, so it IS
    // summed; cached_input_tokens is not. Real codex usage carries no cost, so
    // cost is a documented best-effort 0 — never fabricated, never dropped.
    expect(result.usage).toEqual({
      tokens: 1660,
      cost: 0,
      breakdown: {
        inputTokens: 944, outputTokens: 460,
        cacheReadInputTokens: 256, cacheCreationInputTokens: 0,
      },
    });
  });

  it('REGRESSION (Lynch/Amy re-review): a usage event WITHOUT a cost field → tokens extracted, cost 0, NOT unknown', async () => {
    // This is the real-world path the fabricated-cost fixtures failed to cover:
    // codex's turn.completed never carries total_cost_usd, so the genuinely-known
    // token counts must still be reported — never discarded as {unknown:true},
    // never a fabricated cost.
    const stdout = [
      JSON.stringify({ type: 'thread.started' }),
      JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 800, cached_input_tokens: 64, output_tokens: 200, reasoning_output_tokens: 50 } }),
    ].join('\n');

    const result = await makeAdapter({ run: makeRun({ stdout }).run }).invoke(invocation());

    expect(result.usage).not.toEqual({ unknown: true });
    // 800 input + 200 output + 50 reasoning = 1050.
    expect(result.usage).toEqual({
      tokens: 1050,
      cost: 0,
      breakdown: {
        inputTokens: 736, outputTokens: 250,
        cacheReadInputTokens: 64, cacheCreationInputTokens: 0,
      },
    });
  });

  it('SUMS per-turn usage across every turn.completed event (multi-turn is not cumulative)', async () => {
    // Codex turn.completed usage is PER-TURN, not cumulative — keeping only the
    // final event would undercount a multi-turn session. Two turns must sum.
    const stdout = [
      JSON.stringify({ type: 'thread.started' }),
      JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 100, cached_input_tokens: 10, output_tokens: 40, reasoning_output_tokens: 20 } }),
      JSON.stringify({ type: 'item.completed', item: { type: 'agent_message', text: 'more work' } }),
      JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 300, cached_input_tokens: 30, output_tokens: 60, reasoning_output_tokens: 15 } }),
    ].join('\n');

    const result = await makeAdapter({ run: makeRun({ stdout }).run }).invoke(invocation());

    // input (100+300) + output (40+60) + reasoning (20+15) = 400 + 100 + 35 = 535.
    expect(result.usage).toEqual({
      tokens: 535,
      cost: 0,
      breakdown: {
        inputTokens: 360, outputTokens: 135,
        cacheReadInputTokens: 40, cacheCreationInputTokens: 0,
      },
    });
  });

  it('folds reasoning_output_tokens into the output total (a billed output counter)', async () => {
    const withReasoning = JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 100, output_tokens: 40, reasoning_output_tokens: 25 } });
    const withoutReasoning = JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 100, output_tokens: 40 } });

    const a = await makeAdapter({ run: makeRun({ stdout: withReasoning }).run }).invoke(invocation());
    const b = await makeAdapter({ run: makeRun({ stdout: withoutReasoning }).run }).invoke(invocation());

    expect(a.usage).toEqual({
      tokens: 165,
      cost: 0,
      breakdown: {
        inputTokens: 100, outputTokens: 65,
        cacheReadInputTokens: 0, cacheCreationInputTokens: 0,
      },
    });
    // Missing reasoning_output_tokens contributes 0 — never NaN, never dropped tokens.
    expect(b.usage).toEqual({
      tokens: 140,
      cost: 0,
      breakdown: {
        inputTokens: 100, outputTokens: 40,
        cacheReadInputTokens: 0, cacheCreationInputTokens: 0,
      },
    });
  });

  it('ignores non-usage stream events and reads usage from turn.completed only', async () => {
    const stdout = [
      JSON.stringify({ type: 'thread.started' }),
      JSON.stringify({ type: 'item.started', item: { type: 'reasoning' } }),
      JSON.stringify({ type: 'item.completed', item: { type: 'agent_message', text: 'x' } }),
      JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 10, cached_input_tokens: 0, output_tokens: 5, reasoning_output_tokens: 0 } }),
    ].join('\n');
    const result = await makeAdapter({ run: makeRun({ stdout }).run }).invoke(invocation());
    expect(result.usage).toEqual({
      tokens: 15,
      cost: 0,
      breakdown: {
        inputTokens: 10, outputTokens: 5,
        cacheReadInputTokens: 0, cacheCreationInputTokens: 0,
      },
    });
  });

  it('reports usage explicitly UNKNOWN (never zero) when a success stream carries no usage event', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS_NO_USAGE }).run });

    const result = await adapter.invoke(invocation());

    // PRD §NFR-2: a usage-less success is journaled unknown, never { cost: 0 }.
    expect(result.usage).toEqual({ unknown: true });
  });

  it('forward-compat: IF a future codex build emitted total_cost_usd, the adapter would use it (defensive branch)', async () => {
    // NOT the current real schema — this only documents the impl's defensive
    // `typeof total_cost_usd === "number"` branch for a hypothetical future
    // build. Today's codex never emits this field.
    const stdout = JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 10, output_tokens: 5, total_cost_usd: 0.0002 } });
    const result = await makeAdapter({ run: makeRun({ stdout }).run }).invoke(invocation());
    expect(result.usage).toEqual({
      tokens: 15,
      cost: 0.0002,
      breakdown: {
        inputTokens: 10, outputTokens: 5,
        cacheReadInputTokens: 0, cacheCreationInputTokens: 0,
      },
    });
  });

  it('returns no fabricated output references (declared outputs are collected from disk)', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS }).run });

    const result = await adapter.invoke(invocation());

    expect(result.outputs).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Named spawn failures.
// ---------------------------------------------------------------------------

describe('codex-exec adapter: named spawn failures', () => {
  it('rejects when the process timed out — a killed attempt is not a success', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: '', timedOut: true, exitCode: 137 }).run });
    await expect(adapter.invoke(invocation())).rejects.toThrow(/codex-exec/i);
  });

  it('rejects on a non-zero exit code even when stdout is empty', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: '', exitCode: 1, stderr: 'auth error' }).run });
    await expect(adapter.invoke(invocation())).rejects.toThrow(/codex-exec/i);
  });
});

// ---------------------------------------------------------------------------
// Issue #26 AC5 — a throw that was still BILLED carries the real usage figure.
// Unlike claude (a single terminal result event), codex's usage lives in
// PER-TURN turn.completed events streamed as the run proceeds, so a kill or a
// crash after several completed turns still has real per-turn usage sitting in
// the captured stdout.
// ---------------------------------------------------------------------------

describe('codex-exec adapter: usage carried through a billed throw (issue #26 AC5)', () => {
  it('a timed-out run whose stdout carried two turn.completed events carries the SUMMED usage', async () => {
    const stdout = [
      JSON.stringify({ type: 'thread.started' }),
      JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 100, cached_input_tokens: 10, output_tokens: 40, reasoning_output_tokens: 20 } }),
      JSON.stringify({ type: 'item.completed', item: { type: 'agent_message', text: 'more work' } }),
      JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 300, cached_input_tokens: 30, output_tokens: 60, reasoning_output_tokens: 15 } }),
    ].join('\n');
    const adapter = makeAdapter({ run: makeRun({ stdout, timedOut: true, exitCode: 137 }).run });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);

    expect((err as { code?: string }).code).toBe('harness-timeout');
    // A kill does not refund the tokens the two completed turns already spent.
    // input (100+300) + output (40+60) + reasoning (20+15) = 535.
    expect(usageFromThrow(err)).toEqual({
      tokens: 535,
      cost: 0,
      breakdown: {
        inputTokens: 360, outputTokens: 135,
        cacheReadInputTokens: 40, cacheCreationInputTokens: 0,
      },
    });
  });

  it('a non-zero exit with partial turn usage carries it', async () => {
    const stdout = [
      JSON.stringify({ type: 'thread.started' }),
      JSON.stringify({
        type: 'turn.completed',
        usage: { input_tokens: 800, cached_input_tokens: 64, output_tokens: 200, reasoning_output_tokens: 50 },
      }),
    ].join('\n');
    const adapter = makeAdapter({ run: makeRun({ stdout, exitCode: 1, stderr: 'boom' }).run });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);

    expect((err as { code?: string }).code).toBe('harness-nonzero-exit');
    expect(usageFromThrow(err)).toEqual({
      tokens: 1050,
      cost: 0,
      breakdown: {
        inputTokens: 736, outputTokens: 250,
        cacheReadInputTokens: 64, cacheCreationInputTokens: 0,
      },
    });
  });

  it('a timed-out run with no parseable usage event carries no usage — absent, not zero', async () => {
    const adapter = makeAdapter({
      run: makeRun({ stdout: RECORDED_SUCCESS_NO_USAGE, timedOut: true, exitCode: 137 }).run,
    });

    const err = await adapter.invoke(invocation()).catch((e: unknown) => e);

    expect((err as { code?: string }).code).toBe('harness-timeout');
    expect(usageFromThrow(err)).toBeUndefined();
  });

  it('regression: a successful run with no usage event still returns { unknown: true }', async () => {
    const adapter = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS_NO_USAGE }).run });

    const result = await adapter.invoke(invocation());

    expect(result.usage).toEqual({ unknown: true });
  });
});

// ---------------------------------------------------------------------------
// The config-only-swap seam: two adapters, resolved by NAME alone.
// ---------------------------------------------------------------------------

describe('codex-exec adapter: config-only provider swap seam', () => {
  it('is registered and resolvable by name alongside claude-headless', () => {
    const codex = makeAdapter({ run: makeRun({ stdout: RECORDED_SUCCESS }).run });
    const claude = createClaudeHarnessAdapter({
      projectRoot: PROJECT_ROOT,
      envAllowlist: ['ANTHROPIC_API_KEY', 'PATH'],
      run: async () => ({ exitCode: 0, stdout: '{}', stderr: '', durationMs: 1, timedOut: false }),
    });
    const registry = createHarnessRegistry([claude, codex]);

    const rc = registry.resolve('codex-exec');
    const ra = registry.resolve('claude-headless');
    expect(rc.ok).toBe(true);
    expect(ra.ok).toBe(true);
    if (rc.ok) expect(rc.adapter.name).toBe('codex-exec');
    if (ra.ok) expect(ra.adapter.name).toBe('claude-headless');
    expect([...registry.list()].sort()).toEqual(['claude-headless', 'codex-exec']);
  });
});
