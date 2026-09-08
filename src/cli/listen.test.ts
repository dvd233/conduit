/**
 * Tests for wiring `conduit listen` into the CLI dispatcher (WI-412, FR-9).
 *
 * This is the FINAL ingress item: it exposes the assembled WI-410 listener as a
 * `conduit listen` subcommand. main() must dispatch 'listen' to a cmdListen
 * handler that parses the flow allowlist and constructs+starts the listener;
 * missing arguments print usage and exit non-zero WITHOUT starting; a fail-loud
 * boot error from the listener propagates as a non-zero exit (not swallowed).
 *
 * These tests drive the REAL main()/cmdListen dispatcher (src/cli/main.ts) with
 * injected CliDeps — the listener itself is replaced by an injected startListener
 * seam so no real flow is loaded and no port is bound. This mirrors the
 * injected-seam style of cli.test.ts (runEngine seam).
 *
 * Contract this file pins for src/cli/main.ts:
 *
 *   // CliDeps gains an OPTIONAL startListener seam. Optional so existing CliDeps
 *   // literals (e.g. cli.test.ts makeDeps) keep compiling; buildProductionDeps
 *   // populates it with the real WI-410 startListener (ListenerDeps bound in).
 *   import type { ListenerConfig, StartListenerResult } from '../ingress/listener';
 *   export interface CliDeps {
 *     // ...existing fields...
 *     startListener?: (config: ListenerConfig) => Promise<StartListenerResult>;
 *   }
 *
 *   // main()'s switch gains: case 'listen': return cmdListen(argv, deps);
 *   // and the default/unknown-command usage string lists 'listen' alongside
 *   // run/resume/doctor/journal/reply.
 *
 *   // cmdListen(argv, deps):
 *   //   - parses `--flows <name=path>[,<name=path>...]` into config.allowlist
 *   //   - missing/empty --flows → io.err('usage: conduit listen ...'), return 1,
 *   //     and does NOT call startListener
 *   //   - on startListener {ok:true}  → return 0 (does not block)
 *   //   - on startListener {ok:false} → io.err(each boot error), return 1 (no swallow)
 *
 * RED state before WI-412: 'listen' is not in main()'s switch, so it falls to the
 * default case ("unknown command") — the happy path never starts the listener,
 * the usage string omits 'listen', and missing-arg/boot-error paths don't apply.
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { openConduitDB, type ConduitDB } from '../persistence/db';
import type { ModelAdapter } from '../worker/adapter';
import { formatIngressAlert, main, type CliDeps, type CliIO } from './main';
import type { ListenerConfig, StartListenerResult, Listener } from '../ingress/listener';

function makeIO(): CliIO & { lines: string[]; errors: string[] } {
  const lines: string[] = [];
  const errors: string[] = [];
  return {
    lines,
    errors,
    out: (l: string) => lines.push(l),
    err: (l: string) => errors.push(l),
  };
}

const stubAdapter: ModelAdapter = {
  async call() {
    return { text: '{}', inputTokens: 1, outputTokens: 1, costUsd: 0 };
  },
};

const okListener = { alertChannels: {} } as unknown as Listener;

let db: ConduitDB;
let io: ReturnType<typeof makeIO>;
let listenerCalls: ListenerConfig[];

beforeEach(() => {
  db = openConduitDB({ stateDbPath: ':memory:', journalDbPath: ':memory:' });
  io = makeIO();
  listenerCalls = [];
});

afterEach(() => {
  db.close();
});

interface DepsOverride {
  startResult?: StartListenerResult;
}

function makeDeps(over: DepsOverride = {}): CliDeps {
  return {
    io,
    now: () => 1_000,
    db,
    adapter: stubAdapter,
    runEngine: async () => {},
    prereqs: [{ name: 'noop', check: () => ({ ok: true }) }],
    // Injected listener seam — records the config it was started with so the
    // parsed allowlist is assertable; defaults to a successful start.
    startListener: async (config: ListenerConfig): Promise<StartListenerResult> => {
      listenerCalls.push(config);
      return over.startResult ?? { ok: true, listener: okListener };
    },
  };
}

// ---------------------------------------------------------------------------
// Issue #19 — downstream failure relays parse this stderr line until #18 ships
// a delivering alert channel. Keep the fields and their order fixed so a format
// change fails CI instead of silently stopping alert delivery.
// ---------------------------------------------------------------------------
describe('conduit listen — stderr alert integration contract (#19)', () => {
  it('pins the flow, event, channel, and reason fields in that exact order', () => {
    expect(
      formatIngressAlert({
        flowId: 'order-processing',
        eventId: 'evt-abc123',
        channel: '#conduit-alerts',
        reason: 'conduit run exited with code 1',
      }),
    ).toBe(
      '[ingress alert] flow=order-processing event=evt-abc123 channel=#conduit-alerts: conduit run exited with code 1',
    );
  });
});

// ---------------------------------------------------------------------------
// AC1 — main() dispatches 'listen' to cmdListen, which starts the WI-410
//       listener with the parsed allowlist; 'listen' is also in the usage list.
// ---------------------------------------------------------------------------
describe('conduit listen — dispatch + allowlist parsing (AC1)', () => {
  it('starts the listener with the parsed flow allowlist and exits 0', async () => {
    const code = await main(['listen', '--flows', 'myflow=/flows/my.yaml'], makeDeps());

    expect(code).toBe(0);
    expect(listenerCalls).toHaveLength(1);
    expect(listenerCalls[0].allowlist).toEqual({ myflow: '/flows/my.yaml' });
  });

  it('parses multiple comma-separated --flows entries into the allowlist', async () => {
    const code = await main(
      ['listen', '--flows', 'alpha=/flows/a.yaml,beta=/flows/b.yaml'],
      makeDeps(),
    );

    expect(code).toBe(0);
    expect(listenerCalls[0].allowlist).toEqual({
      alpha: '/flows/a.yaml',
      beta: '/flows/b.yaml',
    });
  });

  it('lists "listen" alongside the other subcommands in the unknown-command usage string', async () => {
    const code = await main(['definitely-not-a-command'], makeDeps());

    expect(code).toBe(1);
    const message = io.errors.join('\n');
    expect(message).toContain('listen');
    // Sanity: it is the full dispatcher list, not just the word "listen".
    expect(message).toContain('run');
    expect(message).toContain('resume');
    // The unknown command did not start a listener.
    expect(listenerCalls).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// AC2 — missing required arguments → usage to the error stream, non-zero exit,
//       and the listener is NOT started.
// ---------------------------------------------------------------------------
describe('conduit listen — missing arguments (AC2)', () => {
  it.each([
    ['no --flows at all', ['listen']],
    ['--flows with no value', ['listen', '--flows']],
  ])('prints usage to stderr and exits non-zero without starting the listener (%s)', async (_label, argv) => {
    const code = await main(argv, makeDeps());

    expect(code).not.toBe(0);
    expect(io.errors.join('\n')).toMatch(/usage: conduit listen/);
    // Fail-before-start: the listener seam was never invoked.
    expect(listenerCalls).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// The original listener-backpressure work — --max-concurrent-runs run backpressure flag
// ---------------------------------------------------------------------------
describe('conduit listen --max-concurrent-runs (the original listener-backpressure work)', () => {
  it('passes a valid cap through to the listener config', async () => {
    const code = await main(
      ['listen', '--flows', 'myflow=/flows/my.yaml', '--max-concurrent-runs', '3'],
      makeDeps(),
    );

    expect(code).toBe(0);
    expect(listenerCalls[0]!.maxConcurrentRuns).toBe(3);
  });

  it('omits the cap from the config when the flag is absent (unlimited)', async () => {
    const code = await main(['listen', '--flows', 'myflow=/flows/my.yaml'], makeDeps());

    expect(code).toBe(0);
    expect(listenerCalls[0]!.maxConcurrentRuns).toBeUndefined();
  });

  it.each([['zero', '0'], ['negative', '-1'], ['fractional', '1.5'], ['non-numeric', 'many']])(
    'rejects a malformed value (%s) before starting the listener',
    async (_label, value) => {
      const code = await main(
        ['listen', '--flows', 'myflow=/flows/my.yaml', '--max-concurrent-runs', value],
        makeDeps(),
      );

      expect(code).toBe(1);
      expect(listenerCalls).toHaveLength(0);
      expect(io.errors.join('\n')).toContain('invalid --max-concurrent-runs value');
    },
  );
});

// ---------------------------------------------------------------------------
// AC3 — a fail-loud boot validation error from the listener propagates as a
//       non-zero exit with the reason; cmdListen does not swallow it (FR-9).
// ---------------------------------------------------------------------------
describe('conduit listen — boot failure propagation (AC3)', () => {
  it('exits non-zero and surfaces the boot error reason instead of swallowing it', async () => {
    const deps = makeDeps({
      startResult: {
        ok: false,
        errors: [
          { code: 'ROUTE_COLLISION', message: 'two flows collide on route /hooks/x', flow: 'beta' },
        ],
      },
    });

    const code = await main(['listen', '--flows', 'alpha=/flows/a.yaml,beta=/flows/b.yaml'], deps);

    expect(code).not.toBe(0);
    // The listener WAS started (the failure came from boot, not from arg parsing).
    expect(listenerCalls).toHaveLength(1);
    // The fail-loud reason reached the error stream.
    expect(io.errors.join('\n')).toContain('two flows collide on route /hooks/x');
  });
});

// ===========================================================================
// The original multi-flow engine work — engine manifest (--manifest) + strict validation (--validate)
// ===========================================================================

import { mkdtempSync, rmSync as rmTmp, writeFileSync as writeTmp } from 'node:fs';
import { tmpdir } from 'node:os';
import { join as joinPath } from 'node:path';

describe('conduit listen --manifest (the original multi-flow engine work)', () => {
  let manifestDir: string;

  beforeEach(() => {
    manifestDir = mkdtempSync(joinPath(tmpdir(), 'conduit-manifest-'));
  });

  afterEach(() => {
    rmTmp(manifestDir, { recursive: true, force: true });
  });

  function writeManifest(content: string): string {
    const path = joinPath(manifestDir, 'engine.yaml');
    writeTmp(path, content, 'utf-8');
    return path;
  }

  it('boots from a manifest: allowlist from the file, quarantine mode on', async () => {
    const path = writeManifest(`
flows:
  pic-edit: ./flows/pic-edit.yaml
  studio: ./flows/studio.yaml
global_alert_channel: "#ops"
`);
    const code = await main(['listen', '--manifest', path], makeDeps());

    expect(code).toBe(0);
    expect(listenerCalls).toHaveLength(1);
    expect(listenerCalls[0]!.allowlist).toEqual({
      'pic-edit': joinPath(manifestDir, 'flows/pic-edit.yaml'),
      studio: joinPath(manifestDir, 'flows/studio.yaml'),
    });
    expect(listenerCalls[0]!.quarantine).toBe(true);
    expect(listenerCalls[0]!.globalAlertChannel).toBe('#ops');
  });

  it('rejects --manifest combined with --flows', async () => {
    const path = writeManifest('flows:\n  a: ./a.yaml\n');
    const code = await main(
      ['listen', '--manifest', path, '--flows', 'b=/b.yaml'],
      makeDeps(),
    );

    expect(code).toBe(1);
    expect(listenerCalls).toHaveLength(0);
    expect(io.errors.join('\n')).toContain('mutually exclusive');
  });

  it('fails loud on a manifest that does not parse, naming each error', async () => {
    const path = writeManifest('flows: {}\ntypo_key: 1\n');
    const code = await main(['listen', '--manifest', path], makeDeps());

    expect(code).toBe(1);
    expect(listenerCalls).toHaveLength(0);
    const err = io.errors.join('\n');
    expect(err).toContain('MANIFEST_NO_FLOWS');
    expect(err).toContain('MANIFEST_UNKNOWN_KEY');
  });

  it('fails loud on an unreadable manifest path', async () => {
    const code = await main(
      ['listen', '--manifest', joinPath(manifestDir, 'missing.yaml')],
      makeDeps(),
    );

    expect(code).toBe(1);
    expect(io.errors.join('\n')).toContain('cannot read engine manifest');
  });

  it('--validate boots STRICTLY (no quarantine) and exits without serving', async () => {
    const path = writeManifest('flows:\n  a: ./a.yaml\n');
    let served = false;
    const deps = makeDeps();
    deps.serve = async () => {
      served = true;
    };

    const code = await main(['listen', '--manifest', path, '--validate'], deps);

    expect(code).toBe(0);
    expect(listenerCalls[0]!.quarantine).toBeUndefined();
    expect(served).toBe(false);
    expect(io.lines.join('\n')).toContain('1 flow(s) valid');
  });

  it('--validate propagates strict boot errors as a non-zero exit', async () => {
    const path = writeManifest('flows:\n  a: ./a.yaml\n');
    const code = await main(
      ['listen', '--manifest', path, '--validate'],
      makeDeps({
        startResult: {
          ok: false,
          errors: [{ flow: 'a', code: 'FLOW_LOAD_FAILED', message: "Flow 'a': not found" }],
        },
      }),
    );

    expect(code).toBe(1);
    expect(io.errors.join('\n')).toContain("Flow 'a'");
  });

  it('passes the manifest max_concurrent_runs to the listener config (the original listener-backpressure work)', async () => {
    const path = writeManifest('flows:\n  a: ./a.yaml\nmax_concurrent_runs: 2\n');
    const code = await main(['listen', '--manifest', path], makeDeps());

    expect(code).toBe(0);
    expect(listenerCalls[0]!.maxConcurrentRuns).toBe(2);
  });

  it('--max-concurrent-runs wins over the manifest value (the original listener-backpressure work)', async () => {
    const path = writeManifest('flows:\n  a: ./a.yaml\nmax_concurrent_runs: 4\n');
    const code = await main(
      ['listen', '--manifest', path, '--max-concurrent-runs', '1'],
      makeDeps(),
    );

    expect(code).toBe(0);
    expect(listenerCalls[0]!.maxConcurrentRuns).toBe(1);
  });

  it('fails loud on a malformed manifest max_concurrent_runs', async () => {
    const path = writeManifest('flows:\n  a: ./a.yaml\nmax_concurrent_runs: 0\n');
    const code = await main(['listen', '--manifest', path], makeDeps());

    expect(code).toBe(1);
    expect(listenerCalls).toHaveLength(0);
    expect(io.errors.join('\n')).toContain('MANIFEST_INVALID_MAX_CONCURRENT_RUNS');
  });

  it('reports quarantined flows loudly before serving', async () => {
    const path = writeManifest('flows:\n  a: ./a.yaml\n  b: ./b.yaml\n');
    const quarantinedListener = {
      alertChannels: { a: '#a' },
      quarantined: {
        b: [{ flow: 'b', code: 'FLOW_LOAD_FAILED', message: "Flow 'b': flow.yaml not found" }],
      },
    } as unknown as Listener;

    const code = await main(
      ['listen', '--manifest', path],
      makeDeps({ startResult: { ok: true, listener: quarantinedListener } }),
    );

    expect(code).toBe(0);
    const err = io.errors.join('\n');
    expect(err).toContain("flow 'b' QUARANTINED");
    expect(err).toContain('1 flow(s) quarantined; 1 serving');
  });
});
