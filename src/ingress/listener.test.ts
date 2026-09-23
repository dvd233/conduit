/**
 * Integration tests for the listener process assembly (WI-410, FR-1/FR-7/FR-9/NFR-1/NFR-6/D4).
 *
 * This is the integration-LAST item: a single thin long-lived process that loads
 * an explicit flow allowlist, boot-validates each flow's channels.ingress binding
 * (via the REAL WI-402 validateIngressBindings/parseIngressBinding), resolves each
 * flow's spawn-failure alert channel, runs the WI-407 boot re-drive, and wires the
 * REAL WI-408 webhook + WI-409 slack adapters — spawning per-run kernels without
 * hosting any tick.
 *
 * Per the assembly guidance, the real collaborator modules are imported and run
 * for real (validateIngressBindings, redriveOnBoot, handleWebhookRequest,
 * handleSlackEvent); only the genuine outermost I/O is faked: a temp on-disk
 * SQLite db, the Bun.spawn seam, the alert seam, the auth seams, and loadFlow
 * (disk read). The route-collision case is detected by the REAL WI-402 validator.
 *
 * Contract this file pins for src/ingress/listener.ts:
 *
 *   export interface ListenerConfig {
 *     allowlist: Record<string, string>;   // flowName -> flow.yaml path (EXPLICIT opt-in; D4/FR-9)
 *     globalAlertChannel?: string;         // listener-global fallback alert channel (Q1)
 *     slackSigningSecret?: string;         // app-global slack signing secret
 *     secrets?: Record<string, string>;    // secret_env name -> value (webhook shared secrets)
 *   }
 *
 *   export interface ListenerBootError { flow?: string; code: string; message: string }
 *
 *   export interface Listener {
 *     handleWebhook(req: WebhookRequest): Promise<WebhookResponse>;
 *     handleSlack(req: SlackRequest): SlackResponse;
 *     alertChannels: Record<string, string>;   // resolved per-flow alert channel (AC3)
 *   }
 *
 *   export type StartListenerResult =
 *     | { ok: true; listener: Listener }
 *     | { ok: false; errors: ListenerBootError[] };   // fail-loud; NO listener on boot failure
 *
 *   export interface ListenerDeps {
 *     db: ConduitDB;
 *     spawn: SpawnSeam;                                            // WI-406 (Bun.spawn) — faked
 *     alert: AlertSeam;                                            // WI-406 alert seam — faked
 *     loadFlow(path: string): LoadFlowResult;                      // default = WI-292 loadFlow
 *     verifyWebhookAuth(secret: string, req: WebhookRequest): boolean;
 *     verifySlackAuth(signingSecret: string, req: SlackRequest): boolean;
 *     respawn: RespawnSeam;                                        // wired into redriveOnBoot
 *     redriveCap: number;
 *     now(): number;
 *   }
 *
 *   export function startListener(deps: ListenerDeps, config: ListenerConfig): Promise<StartListenerResult>;
 *
 * Pinned decisions: boot is fail-closed (config-is-validated) — any absent flow.yaml,
 * malformed channels.ingress, route collision, or unresolvable alert channel returns
 * { ok:false, errors } and NO listener. On success the re-drive runs to completion
 * BEFORE the returned listener serves any request.
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openConduitDB, type ConduitDB } from '../persistence/db';
import type { SpawnSeam, SpawnInvocation, SpawnFailedAlert } from './spawn';
import type { RespawnSeam } from './recovery';
import type { LoadFlowResult } from '../flow/load';
import type { FlowConfig, FlowChannels, FlowEgressChannel } from '../types/kernel';
import type { WebhookRequest } from './adapters/webhook';
import type { SlackRequest } from './adapters/slack-events';
import type {
  SocketConnection,
  SocketHandlers,
  SocketOpenResult,
  SocketSeam,
} from './adapters/slack-socket';
import {
  startListener,
  runGatedHitlResume,
  type ListenerDeps,
  type ListenerConfig,
} from './listener';
import { createRunSlots } from './run-slots';
import { deriveIngressRunId } from './run-id';
import type { HitlResumeSpawn } from './adapters/slack-events';

const NOW = 1_700_000_000_000;

let dir: string;
let db: ConduitDB;
let spawnCalls: SpawnInvocation[];

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'conduit-listener-'));
  db = openConduitDB({ stateDbPath: join(dir, 'state.sqlite'), journalDbPath: join(dir, 'journal.sqlite') });
  spawnCalls = [];
});

afterEach(() => {
  try {
    db.close();
  } catch {
    /* already closed */
  }
  rmSync(dir, { recursive: true, force: true });
});

const spawnSeam: SpawnSeam = async (invocation) => {
  spawnCalls.push(invocation);
  return { ok: true };
};

// ── Flow fixtures. FlowChannels.ingress is typed { type?: string } but is
// runtime-rich (loadFlow preserves the full binding); the listener reads it as
// unknown via parseIngressBinding, so we cast the rich object here. ──
function webhookIngress(route: string | undefined): Record<string, unknown> {
  return {
    type: 'webhook',
    ...(route !== undefined ? { route } : {}),
    auth: { type: 'hmac', secret_env: 'WH_SECRET' },
    event_id: { from: 'json_path', path: '$.id' },
  };
}

function slackIngress(channel: string): Record<string, unknown> {
  return {
    type: 'slack',
    channel,
    auth: { type: 'signing', secret_env: 'SLACK_SECRET' },
    event_id: { from: 'json_path', path: '$.event_id' },
  };
}

/**
 * cli ingress (local `conduit run` triggering — see examples/research-flow.yaml):
 * no route, no auth, no event_id — the listener cannot route to it at all
 * (the original listener-servability work review: this must fail with a named "not listener-servable" error,
 * not the generic MISSING_EVENT_ID_SOURCE that WI-402 would otherwise raise).
 */
function cliIngress(): Record<string, unknown> {
  return { type: 'cli' };
}

function makeFlow(ingress: Record<string, unknown>, egressTarget?: string): FlowConfig {
  const channels: FlowChannels = {
    ingress: ingress as unknown as FlowChannels['ingress'],
  };
  if (egressTarget !== undefined) {
    channels.egress = [{ type: 'slack', target: egressTarget } as FlowEgressChannel];
  }
  return { version: 1, stations: {}, channels };
}

/**
 * A flow that declares NO ingress binding at all (channels.ingress absent).
 * makeFlow always sets channels.ingress, so this is a separate helper —
 * exercises the "no ingress declared" branch of INGRESS_NOT_LISTENER_SERVABLE
 * (the original listener-servability work review), distinct from the malformed/cli cases.
 */
function makeFlowNoIngress(egressTarget?: string): FlowConfig {
  const channels: FlowChannels = {};
  if (egressTarget !== undefined) {
    channels.egress = [{ type: 'slack', target: egressTarget } as FlowEgressChannel];
  }
  return { version: 1, stations: {}, channels };
}

/** Build a loadFlow seam from a path → flow map; unknown paths report not-found. */
function loadFlowFrom(flows: Record<string, FlowConfig>): (path: string) => LoadFlowResult {
  return (path: string): LoadFlowResult => {
    const flow = flows[path];
    if (flow === undefined) {
      return { ok: false, errors: [{ code: 'FILE_NOT_FOUND', message: `flow.yaml not found: ${path}` }] };
    }
    return { ok: true, flow };
  };
}

function makeDeps(over: Partial<ListenerDeps> = {}): ListenerDeps {
  return {
    db,
    spawn: spawnSeam,
    alert: async () => {},
    loadFlow: () => ({ ok: false, errors: [{ code: 'FILE_NOT_FOUND', message: 'no flow' }] }),
    verifyWebhookAuth: () => true,
    verifySlackAuth: () => true,
    respawn: async () => 'spawned',
    redriveCap: 3,
    now: () => NOW,
    ...over,
  };
}

function baseConfig(over: Partial<ListenerConfig> = {}): ListenerConfig {
  return {
    allowlist: {},
    globalAlertChannel: 'slack:ops',
    slackSigningSecret: 'sig-secret',
    secrets: { WH_SECRET: 'wh-shared', SLACK_SECRET: 'slack-shared' },
    ...over,
  };
}

function webhookReq(over: Partial<WebhookRequest> = {}): WebhookRequest {
  return {
    route: '/hooks/a',
    headers: {},
    rawBody: JSON.stringify({ id: 'evt-1', text: 'hi' }),
    ...over,
  };
}

/** Narrow a boot result to a started listener (fails loud otherwise). */
function expectStarted(result: Awaited<ReturnType<typeof startListener>>) {
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error(`listener refused to start: ${JSON.stringify(result.errors)}`);
  return result.listener;
}

// ===========================================================================
// AC1 — explicit allowlist (no auto-discovery)
// ===========================================================================

describe('explicit allowlist (AC1, D4, FR-9)', () => {
  it('does not trigger a flow whose flow.yaml exists on disk but is not in the allowlist', async () => {
    const flows = {
      '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a'),
      '/flows/b.yaml': makeFlow(webhookIngress('/hooks/b'), '#b'), // on disk, NOT allowlisted
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const result = await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } }));
    const listener = expectStarted(result);

    // The allowlisted route triggers a run...
    const ok = await listener.handleWebhook(webhookReq({ route: '/hooks/a' }));
    expect(ok.status).toBeGreaterThanOrEqual(200);
    expect(ok.status).toBeLessThan(300);
    expect(spawnCalls).toHaveLength(1);

    // ...but the un-allowlisted route is unknown — rejected, never spawned.
    const rejected = await listener.handleWebhook(webhookReq({ route: '/hooks/b' }));
    expect(rejected.status).toBeGreaterThanOrEqual(400);
    expect(spawnCalls).toHaveLength(1); // still just the one
    expect(db.getIngressLog({ outcome: 'rejected_unknown_flow' })).toHaveLength(1);
  });
});

// ===========================================================================
// AC2 — boot validation is fail-loud
// ===========================================================================

describe('boot validation fail-loud (AC2, FR-9)', () => {
  it('refuses to start when an allowlisted flow.yaml is absent', async () => {
    const deps = makeDeps({ loadFlow: loadFlowFrom({}) }); // path resolves to nothing
    const result = await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/missing.yaml' } }));

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    expect(JSON.stringify(result.errors)).toContain('flowA');
  });

  it('refuses to start when a flow channels.ingress is malformed (missing webhook route)', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress(undefined), '#a') }; // no route
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const result = await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } }));

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    expect(JSON.stringify(result.errors)).toContain('flowA');
    expect(spawnCalls).toHaveLength(0);
  });

  it('refuses to start on a webhook route collision between two active flows (REAL WI-402 validation)', async () => {
    const flows = {
      '/flows/a.yaml': makeFlow(webhookIngress('/hooks/dup'), '#a'),
      '/flows/b.yaml': makeFlow(webhookIngress('/hooks/dup'), '#b'),
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const result = await startListener(
      deps,
      baseConfig({ allowlist: { flowA: '/flows/a.yaml', flowB: '/flows/b.yaml' } }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    // The collision error must identify BOTH colliding flows (WI-402 FR-9).
    const text = JSON.stringify(result.errors);
    expect(text).toContain('flowA');
    expect(text).toContain('flowB');
  });
});

// ===========================================================================
// AC3 — per-flow spawn-failure alert channel resolution
// ===========================================================================

describe('alert channel resolution (AC3, Q1)', () => {
  it("uses the flow's own egress channel as its alert target when present", async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#flow-a-alerts') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })),
    );
    expect(listener.alertChannels.flowA).toBe('#flow-a-alerts');
  });

  it('falls back to the listener-global alert channel when a flow declares no egress', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a')) }; // no egress
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({ allowlist: { flowA: '/flows/a.yaml' }, globalAlertChannel: '#listener-global' }),
      ),
    );
    expect(listener.alertChannels.flowA).toBe('#listener-global');
  });

  it('reports at boot when a flow has neither egress nor a global fallback', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a')) }; // no egress
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const result = await startListener(
      deps,
      baseConfig({ allowlist: { flowA: '/flows/a.yaml' }, globalAlertChannel: undefined }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    expect(JSON.stringify(result.errors)).toContain('flowA');
  });
});

// ===========================================================================
// AC4 — boot runs WI-407 redrive, THEN serves the adapters
// ===========================================================================

describe('boot sequence: redrive then adapters (AC4)', () => {
  it('runs the boot re-drive for recoverable events before serving adapter requests', async () => {
    // A recorded-but-never-spawned event (crash between record and spawn).
    db.acceptIngressEvent('recover-me', 500); // spawn_state 'accepted', attempts 0 (< cap)

    const respawned: string[] = [];
    const respawn: RespawnSeam = async (event) => {
      respawned.push(event.event_id);
      return 'spawned';
    };

    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows), respawn });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })),
    );

    // The re-drive already ran during boot (before the listener was returned).
    expect(respawned).toEqual(['recover-me']);
    expect(db.getIngressLog({ outcome: 'redriven' })).toHaveLength(1);

    // And the adapters serve normally afterward.
    await listener.handleWebhook(webhookReq({ route: '/hooks/a' }));
    expect(spawnCalls).toHaveLength(1);
  });
});

// ===========================================================================
// The original ingress-attribution work FR-3 — periodic re-drive is part of the listener lifecycle
// ===========================================================================

describe('periodic re-drive lifecycle (the original ingress-attribution work, FR-3)', () => {
  it('re-drives an event that fails AFTER boot, once started, without a restart', async () => {
    const respawned: string[] = [];
    const respawn: RespawnSeam = async (event) => {
      respawned.push(event.event_id);
      return 'spawned';
    };
    let tick: (() => void) | null = null;
    const schedule = (fn: () => void, _ms: number) => {
      tick = fn;
      return { cancel: () => {} };
    };

    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows), respawn, redriveSchedule: schedule });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })),
    );

    // Boot re-drive found nothing; the sweep is not yet scheduled.
    expect(respawned).toEqual([]);
    expect(tick).toBeNull();

    listener.redrive.start();
    expect(tick).not.toBeNull();

    // An event fails after boot (the steady-listener case ingress event-isolation work describes).
    db.acceptIngressEvent('post-boot-fail', 900);
    db.incrementSpawnAttempts('post-boot-fail');
    db.markIngressFailed('post-boot-fail');

    tick!();
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(respawned).toEqual(['post-boot-fail']);
    expect(db.getIngressEvent('post-boot-fail')!.spawn_state).toBe('spawned');
    listener.redrive.stop();
  });

  it('start() is idempotent and stop() cancels the schedule', async () => {
    let scheduled = 0;
    let cancelled = 0;
    const schedule = (_fn: () => void, _ms: number) => {
      scheduled++;
      return { cancel: () => { cancelled++; } };
    };

    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows), redriveSchedule: schedule });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })),
    );

    listener.redrive.start();
    listener.redrive.start(); // second start must not double-schedule
    expect(scheduled).toBe(1);

    listener.redrive.stop();
    expect(cancelled).toBe(1);

    listener.redrive.start(); // restart after stop is allowed
    expect(scheduled).toBe(2);
    listener.redrive.stop();
  });
});

// ===========================================================================
// AC5 — separate from the per-run kernel: idle does nothing; each event = one run
// ===========================================================================

describe('separation from the per-run kernel (AC5, FR-1, NFR-1)', () => {
  it('spawns nothing while idle (no events, nothing recoverable)', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    expectStarted(await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })));

    // No request, nothing recoverable → no run was spawned at rest.
    expect(spawnCalls).toHaveLength(0);
    expect(db.getIngressLog()).toHaveLength(0);
  });

  it('spawns one conduit run per accepted event', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })),
    );

    await listener.handleWebhook(webhookReq({ rawBody: JSON.stringify({ id: 'evt-1' }) }));
    await listener.handleWebhook(webhookReq({ rawBody: JSON.stringify({ id: 'evt-2' }) }));

    expect(spawnCalls).toHaveLength(2); // distinct events → distinct runs
  });
});

// ===========================================================================
// Slack adapter wiring (AC4 — both adapters started)
// ===========================================================================

describe('slack adapter wiring', () => {
  it('wires the slack adapter so a signed event in an allowlisted channel spawns a run', async () => {
    const flows = { '/flows/s.yaml': makeFlow(slackIngress('C-DEPLOY'), '#s-alerts') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowS: '/flows/s.yaml' } })),
    );

    const req: SlackRequest = {
      headers: { 'X-Slack-Signature': 'v0=stub', 'X-Slack-Request-Timestamp': '1700000000' },
      rawBody: JSON.stringify({ event_id: 'Ev1', event: { type: 'message', channel: 'C-DEPLOY', text: 'go' } }),
    };
    const res = listener.handleSlack(req);
    expect(res.status).toBeGreaterThanOrEqual(200);
    expect(res.status).toBeLessThan(300);
    await res.processed;

    expect(spawnCalls).toHaveLength(1);
    expect(db.getIngressLog({ outcome: 'accepted' })).toHaveLength(1);
  });
});

// ===========================================================================
// Socket Mode wiring (the original Slack Socket Mode work — transport: 'socket' slack bindings)
// ===========================================================================

class FakeSocketConnection implements SocketConnection {
  sent: string[] = [];
  closed = false;
  constructor(readonly handlers: SocketHandlers) {}
  send(data: string): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
  }
  deliver(envelope: unknown): void {
    this.handlers.onMessage(JSON.stringify(envelope));
  }
}

function makeFakeSocketSeam() {
  const connections: FakeSocketConnection[] = [];
  const openCalls: string[] = [];
  const seam: SocketSeam = {
    openConnection: async (appToken): Promise<SocketOpenResult> => {
      openCalls.push(appToken);
      return { ok: true, url: `wss://fake/${openCalls.length}` };
    },
    connect: (_url, handlers) => {
      const conn = new FakeSocketConnection(handlers);
      connections.push(conn);
      return conn;
    },
  };
  return { seam, connections, openCalls };
}

/** A Slack ingress binding on the socket transport, as it appears in flow.yaml. */
function socketIngress(channel: string, appTokenEnv = 'SLACK_APP_TOKEN'): Record<string, unknown> {
  return {
    type: 'slack',
    transport: 'socket',
    channel,
    app_token_env: appTokenEnv,
    event_id: { from: 'json_path', path: '$.event_id' },
  };
}

describe('socket-transport slack wiring (the original Slack Socket Mode work)', () => {
  it('boots a socket flow, and a delivered events_api envelope is acked and spawns a run', async () => {
    const { seam, connections, openCalls } = makeFakeSocketSeam();
    const flows = { '/flows/studio.yaml': makeFlow(socketIngress('C-STUDIO'), '#studio-alerts') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows), socket: seam });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({
          allowlist: { studio: '/flows/studio.yaml' },
          secrets: { SLACK_APP_TOKEN: 'xapp-fake-token' },
        }),
      ),
    );

    // Clients are created at boot but NOT started until the caller serves.
    expect(openCalls).toHaveLength(0);
    await listener.sockets.start();
    expect(openCalls).toEqual(['xapp-fake-token']);
    expect(connections).toHaveLength(1);

    connections[0]!.deliver({
      type: 'events_api',
      envelope_id: 'env-1',
      payload: { event_id: 'Ev-sock-1', event: { type: 'message', channel: 'C-STUDIO', text: 'photo drop' } },
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(connections[0]!.sent).toEqual([JSON.stringify({ envelope_id: 'env-1' })]);
    expect(spawnCalls).toHaveLength(1);
    expect(spawnCalls[0]!.flowPath).toBe('/flows/studio.yaml');
    expect(db.getIngressLog({ outcome: 'accepted' })).toHaveLength(1);

    listener.sockets.stop();
    expect(connections[0]!.closed).toBe(true);
  });

  it('fails boot with MISSING_APP_TOKEN_SECRET when the app token env var is unset', async () => {
    const { seam } = makeFakeSocketSeam();
    const flows = { '/flows/studio.yaml': makeFlow(socketIngress('C-STUDIO'), '#studio-alerts') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows), socket: seam });
    const result = await startListener(
      deps,
      baseConfig({
        allowlist: { studio: '/flows/studio.yaml' },
        secrets: {}, // SLACK_APP_TOKEN not resolved
      }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    const err = result.errors.find((e) => e.code === 'MISSING_APP_TOKEN_SECRET');
    expect(err).toBeDefined();
    expect(err!.message).toContain('studio');
    expect(err!.message).toContain('SLACK_APP_TOKEN');
  });

  it('fails boot with SOCKET_SEAM_UNAVAILABLE when a socket flow is configured but no seam exists', async () => {
    const flows = { '/flows/studio.yaml': makeFlow(socketIngress('C-STUDIO'), '#studio-alerts') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) }); // no socket seam
    const result = await startListener(
      deps,
      baseConfig({
        allowlist: { studio: '/flows/studio.yaml' },
        secrets: { SLACK_APP_TOKEN: 'xapp-fake-token' },
      }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    expect(result.errors.some((e) => e.code === 'SOCKET_SEAM_UNAVAILABLE')).toBe(true);
  });

  it('shares ONE socket client across flows that resolve the same app token', async () => {
    const { seam, openCalls } = makeFakeSocketSeam();
    const flows = {
      '/flows/studio.yaml': makeFlow(socketIngress('C-STUDIO'), '#a'),
      '/flows/naming.yaml': makeFlow(socketIngress('C-NAMING'), '#b'),
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows), socket: seam });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({
          allowlist: { studio: '/flows/studio.yaml', naming: '/flows/naming.yaml' },
          secrets: { SLACK_APP_TOKEN: 'xapp-shared-token' },
        }),
      ),
    );

    await listener.sockets.start();
    expect(openCalls).toEqual(['xapp-shared-token']); // one client, one connection
    listener.sockets.stop();
  });

  it('a listener with no socket flows exposes a no-op sockets handle', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })),
    );

    await listener.sockets.start(); // resolves immediately, opens nothing
    listener.sockets.stop(); // no-op
  });
});

// ===========================================================================
// The original multi-flow engine work — per-flow fail-closed loading (quarantine mode)
// ===========================================================================

describe('quarantine mode (the original multi-flow engine work)', () => {
  it('quarantines a flow that fails to load while the remaining flows serve', async () => {
    const flows = {
      '/flows/good.yaml': makeFlow(webhookIngress('/hooks/good'), '#good'),
      // '/flows/broken.yaml' deliberately absent → FLOW_LOAD_FAILED
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({
          allowlist: { good: '/flows/good.yaml', broken: '/flows/broken.yaml' },
          quarantine: true,
        }),
      ),
    );

    // The broken flow is quarantined with its load error, not fatal to boot.
    expect(Object.keys(listener.quarantined)).toEqual(['broken']);
    expect(listener.quarantined['broken']![0]!.code).toBe('FLOW_LOAD_FAILED');

    // The good flow serves normally (202: accepted and launched, the original acknowledgement-on-accept work).
    const resp = await listener.handleWebhook(webhookReq({ route: '/hooks/good' }));
    expect(resp.status).toBe(202);
    expect(spawnCalls).toHaveLength(1);
  });

  it('never routes to a quarantined flow (its route 404s)', async () => {
    const flows = {
      '/flows/good.yaml': makeFlow(webhookIngress('/hooks/good'), '#good'),
      // a flow with a malformed binding: webhook with no route AND no auth
      '/flows/bad.yaml': makeFlow({ type: 'webhook' }, '#bad'),
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({
          allowlist: { good: '/flows/good.yaml', bad: '/flows/bad.yaml' },
          quarantine: true,
        }),
      ),
    );

    expect(Object.keys(listener.quarantined)).toEqual(['bad']);
    const resp = await listener.handleWebhook(webhookReq({ route: '/hooks/bad' }));
    expect(resp.status).toBe(404);
    expect(spawnCalls).toHaveLength(0);
  });

  it('a slack channel collision quarantines EVERY claimant, not a silent winner', async () => {
    const flows = {
      '/flows/a.yaml': makeFlow(slackIngress('C0AAA'), '#a'),
      '/flows/b.yaml': makeFlow(slackIngress('C0AAA'), '#b'), // same channel!
      '/flows/c.yaml': makeFlow(slackIngress('C0CCC'), '#c'),
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({
          allowlist: { a: '/flows/a.yaml', b: '/flows/b.yaml', c: '/flows/c.yaml' },
          quarantine: true,
        }),
      ),
    );

    expect(Object.keys(listener.quarantined).sort()).toEqual(['a', 'b']);
    expect(listener.quarantined['a']![0]!.code).toBe('CHANNEL_COLLISION');
    expect(listener.quarantined['b']![0]!.code).toBe('CHANNEL_COLLISION');
    // The non-colliding flow still serves.
    expect(listener.alertChannels['c']).toBe('#c');
  });

  it('refuses to boot when EVERY flow is quarantined (NO_SERVABLE_FLOWS)', async () => {
    const deps = makeDeps({ loadFlow: loadFlowFrom({}) }); // nothing loads
    const result = await startListener(
      deps,
      baseConfig({
        allowlist: { a: '/flows/a.yaml', b: '/flows/b.yaml' },
        quarantine: true,
      }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    expect(result.errors.map((e) => e.code)).toContain('NO_SERVABLE_FLOWS');
    // Every flow's own error is surfaced alongside the backstop.
    expect(result.errors.filter((e) => e.code === 'FLOW_LOAD_FAILED')).toHaveLength(2);
  });

  it('strict mode (default) still withholds the whole listener on one bad flow', async () => {
    const flows = { '/flows/good.yaml': makeFlow(webhookIngress('/hooks/good'), '#good') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const result = await startListener(
      deps,
      baseConfig({ allowlist: { good: '/flows/good.yaml', broken: '/flows/broken.yaml' } }),
    );

    expect(result.ok).toBe(false);
  });

  it('a clean boot reports an empty quarantine map in both modes', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    for (const quarantine of [true, false]) {
      const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
      const listener = expectStarted(
        await startListener(
          deps,
          baseConfig({ allowlist: { a: '/flows/a.yaml' }, quarantine }),
        ),
      );
      expect(listener.quarantined).toEqual({});
    }
  });

  it('strict mode rejects a slack channel collision at boot (CHANNEL_COLLISION)', async () => {
    const flows = {
      '/flows/a.yaml': makeFlow(slackIngress('C0AAA'), '#a'),
      '/flows/b.yaml': makeFlow(slackIngress('C0AAA'), '#b'),
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const result = await startListener(
      deps,
      baseConfig({ allowlist: { a: '/flows/a.yaml', b: '/flows/b.yaml' } }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    expect(result.errors.map((e) => e.code)).toContain('CHANNEL_COLLISION');
  });
});

// ===========================================================================
// The original multi-flow engine work — the engine IS the router: one shared socket, N flows, correct
// per-channel attribution (this is the guarantee that dissolves Slack's
// round-robin-across-connections problem structurally)
// ===========================================================================

describe('multi-flow envelope routing over one shared socket (the original multi-flow engine work)', () => {
  it('routes each envelope to its channel-owning flow with its own run id', async () => {
    const { seam, connections } = makeFakeSocketSeam();
    const flows = {
      '/flows/studio.yaml': makeFlow(socketIngress('C-STUDIO'), '#a'),
      '/flows/naming.yaml': makeFlow(socketIngress('C-NAMING'), '#b'),
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows), socket: seam });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({
          allowlist: { studio: '/flows/studio.yaml', naming: '/flows/naming.yaml' },
          secrets: { SLACK_APP_TOKEN: 'xapp-shared-token' },
          quarantine: true, // engine mode
        }),
      ),
    );
    await listener.sockets.start();
    expect(connections).toHaveLength(1); // ONE connection serves both flows

    connections[0]!.deliver({
      type: 'events_api',
      envelope_id: 'env-studio',
      payload: { event_id: 'Ev-A', event: { type: 'message', channel: 'C-STUDIO', text: 'photo' } },
    });
    connections[0]!.deliver({
      type: 'events_api',
      envelope_id: 'env-naming',
      payload: { event_id: 'Ev-B', event: { type: 'message', channel: 'C-NAMING', text: 'name it' } },
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(spawnCalls).toHaveLength(2);
    const byFlow = Object.fromEntries(spawnCalls.map((c) => [c.flowPath, c]));
    expect(byFlow['/flows/studio.yaml']).toBeDefined();
    expect(byFlow['/flows/naming.yaml']).toBeDefined();
    // Distinct events → distinct derived run ids (ingress event-isolation work + multi-flow engine work working together).
    expect(spawnCalls[0]!.runId).not.toBe(spawnCalls[1]!.runId);

    listener.sockets.stop();
  });
});

// ===========================================================================
// The original listener-servability work review (@queso) — cli-ingress / no-ingress flows are not
// listener-servable. Previously these fell through to WI-402's generic
// MISSING_EVENT_ID_SOURCE (cli) or would have booted "valid" and served
// silently unreachable (a cli flow WITH an event_id lands in neither the
// webhook route map nor the slack channel map). Both cases now fail with the
// named INGRESS_NOT_LISTENER_SERVABLE code, checked BEFORE Phase 2 validation.
// ===========================================================================

describe('cli-ingress / no-ingress flows are not listener-servable (the original listener-servability work review)', () => {
  it('strict mode: a cli-ingress flow fails boot with INGRESS_NOT_LISTENER_SERVABLE, not MISSING_EVENT_ID_SOURCE', async () => {
    const flows = { '/flows/cli.yaml': makeFlow(cliIngress(), '#cli') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const result = await startListener(deps, baseConfig({ allowlist: { cliFlow: '/flows/cli.yaml' } }));

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    const codes = result.errors.map((e) => e.code);
    expect(codes).toContain('INGRESS_NOT_LISTENER_SERVABLE');
    expect(codes).not.toContain('MISSING_EVENT_ID_SOURCE');
    const text = JSON.stringify(result.errors);
    expect(text).toContain('cliFlow');
    expect(text).toContain('cli');
  });

  it('strict mode: a flow declaring no ingress binding fails boot with INGRESS_NOT_LISTENER_SERVABLE', async () => {
    const flows = { '/flows/noingress.yaml': makeFlowNoIngress('#none') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const result = await startListener(
      deps,
      baseConfig({ allowlist: { bare: '/flows/noingress.yaml' } }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected boot failure');
    const codes = result.errors.map((e) => e.code);
    expect(codes).toContain('INGRESS_NOT_LISTENER_SERVABLE');
    expect(JSON.stringify(result.errors)).toContain('bare');
  });

  it('quarantine mode: a cli-ingress flow is quarantined with INGRESS_NOT_LISTENER_SERVABLE while a good webhook flow still serves', async () => {
    const flows = {
      '/flows/cli.yaml': makeFlow(cliIngress(), '#cli'),
      '/flows/good.yaml': makeFlow(webhookIngress('/hooks/good'), '#good'),
    };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({
          allowlist: { cliFlow: '/flows/cli.yaml', good: '/flows/good.yaml' },
          quarantine: true,
        }),
      ),
    );

    expect(Object.keys(listener.quarantined)).toEqual(['cliFlow']);
    expect(listener.quarantined['cliFlow']![0]!.code).toBe('INGRESS_NOT_LISTENER_SERVABLE');

    const resp = await listener.handleWebhook(webhookReq({ route: '/hooks/good' }));
    expect(resp.status).toBe(202);
    expect(spawnCalls).toHaveLength(1);
  });
});

// ===========================================================================
// The original listener-backpressure work — listener-wide run backpressure (max_concurrent_runs)
// ===========================================================================

describe('run backpressure (the original listener-backpressure work)', () => {
  it('refuses to boot on a malformed maxConcurrentRuns instead of silently uncapping', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    for (const bad of [0, -2, 1.5, NaN]) {
      const result = await startListener(
        makeDeps({ loadFlow: loadFlowFrom(flows) }),
        baseConfig({ allowlist: { flowA: '/flows/a.yaml' }, maxConcurrentRuns: bad }),
      );
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error('expected boot failure');
      expect(result.errors[0]!.code).toBe('INVALID_MAX_CONCURRENT_RUNS');
    }
  });

  it('the listener-backpressure work incident shape: a burst beyond the cap queues, then drains through the kicked sweep', async () => {
    // max_concurrent_runs: 1, two "photos" dropped at once. The first LAUNCHES
    // and holds the only slot until its child exits (the original acknowledgement-on-accept work — the webhook
    // acks immediately, the slot outlives the response); the second is accepted
    // + queued, NOT spawned. When the first run finishes, the freed slot kicks
    // the re-drive sweep, which launches the queued event — no interval wait,
    // no listener restart.
    const flows = { '/flows/photo.yaml': makeFlow(webhookIngress('/hooks/photo'), '#photo') };

    let finishFirstRun: () => void = () => {};
    const firstRunExited = new Promise<{ code: number }>((resolve) => {
      finishFirstRun = () => resolve({ code: 0 });
    });
    const launchingSpawn: SpawnSeam = async (invocation) => {
      spawnCalls.push(invocation);
      // Launched, still running: the response no longer waits on the run.
      return { ok: true, exited: firstRunExited };
    };

    const respawned: string[] = [];
    const respawn: RespawnSeam = async (event) => {
      respawned.push(event.event_id);
      return 'spawned';
    };

    // Manual schedule: the periodic interval NEVER fires in this test — any
    // sweep that runs was triggered by the release kick alone.
    const deps = makeDeps({
      loadFlow: loadFlowFrom(flows),
      spawn: launchingSpawn,
      respawn,
      redriveSchedule: () => ({ cancel: () => {} }),
    });
    const listener = expectStarted(
      await startListener(
        deps,
        baseConfig({ allowlist: { photo: '/flows/photo.yaml' }, maxConcurrentRuns: 1 }),
      ),
    );
    listener.redrive.start();

    // Photo 1 arrives — claims the only slot and is answered while its run is
    // still executing (the original acknowledgement-on-accept work: the ack no longer waits for the exit).
    const firstDelivery = await listener.handleWebhook(
      webhookReq({ route: '/hooks/photo', rawBody: JSON.stringify({ id: 'photo-1' }) }),
    );
    expect(firstDelivery.status).toBe(202);
    expect(JSON.parse(firstDelivery.body!)).toMatchObject({ outcome: 'accepted' });
    expect(spawnCalls).toHaveLength(1);

    // Photo 2 arrives — accepted and QUEUED, not spawned: photo-1's slot is
    // held by its live child, not by a pending HTTP response.
    const second = await listener.handleWebhook(
      webhookReq({ route: '/hooks/photo', rawBody: JSON.stringify({ id: 'photo-2' }) }),
    );
    expect(second.status).toBe(202); // the provider gets its ack — nothing dropped
    expect(JSON.parse(second.body!)).toMatchObject({ outcome: 'queued' });
    expect(spawnCalls).toHaveLength(1); // hot path did NOT spawn photo-2
    expect(db.getIngressEvent('photo-2')).toMatchObject({ spawn_state: 'accepted', spawn_attempts: 0 });
    expect(db.getIngressLog({ outcome: 'queued' }).map((e) => e.eventId)).toEqual(['photo-2']);

    // Photo 1's run completes → slot frees → kick → sweep launches photo-2.
    finishFirstRun();
    await new Promise<void>((resolve) => setTimeout(resolve, 0)); // let the kicked sweep settle

    expect(respawned).toEqual(['photo-2']);
    expect(db.getIngressEvent('photo-1')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    expect(db.getIngressEvent('photo-2')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
    listener.redrive.stop();
  });

  it('without maxConcurrentRuns a burst spawns concurrently (before listener backpressure behavior preserved)', async () => {
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })),
    );

    await Promise.all(
      ['e1', 'e2', 'e3'].map((id) =>
        listener.handleWebhook(webhookReq({ route: '/hooks/a', rawBody: JSON.stringify({ id }) })),
      ),
    );

    expect(spawnCalls).toHaveLength(3);
    expect(db.getIngressLog({ outcome: 'queued' })).toHaveLength(0);
  });
});

// ===========================================================================
// The original listener-backpressure work × HITL work — HITL resumes and the run-slot gate (the pre-public run-slot and HITL review, @queso)
// ===========================================================================

describe('runGatedHitlResume (the original listener-backpressure work × HITL work)', () => {
  it('claims a slot for the resume when one is free, and releases it afterwards', async () => {
    const slots = createRunSlots({ capacity: 1 });
    let inFlightDuringResume = false;

    await runGatedHitlResume(slots, db, 'run-1', async () => {
      inFlightDuringResume = slots.inFlight('hitl-resume:run-1');
    });

    // Honest accounting in the common case: the resume held the slot...
    expect(inFlightDuringResume).toBe(true);
    // ...and freed it when the resume settled.
    expect(slots.inFlightCount()).toBe(0);
  });

  it('BYPASSES the gate under saturation — a human pick never queues behind a backlog', async () => {
    const slots = createRunSlots({ capacity: 1 });
    slots.tryAcquire('photo-backlog-run'); // the only slot is busy
    let resumed = false;

    await runGatedHitlResume(slots, db, 'run-1', async () => {
      resumed = true;
    });

    expect(resumed).toBe(true); // ran anyway — deliberate cap exceedance
    // The bypassed resume never touched the gate's accounting.
    expect(slots.inFlightCount()).toBe(1);
    expect(slots.inFlight('hitl-resume:run-1')).toBe(false);
  });

  it('a bypassed resume must not release the slot it never claimed', async () => {
    const slots = createRunSlots({ capacity: 1 });
    slots.tryAcquire('photo-backlog-run');

    await runGatedHitlResume(slots, db, 'run-1', async () => {});

    // The backlog run's slot survived the bypassed resume's finally block.
    expect(slots.inFlight('photo-backlog-run')).toBe(true);
  });

  it('suppresses a concurrent second resume for the SAME run (double-driver guard)', async () => {
    const slots = createRunSlots({ capacity: 5 });
    let resolveFirst: () => void = () => {};
    const firstBlocked = new Promise<void>((resolve) => {
      resolveFirst = resolve;
    });
    let resumeCalls = 0;

    const first = runGatedHitlResume(slots, db, 'run-1', async () => {
      resumeCalls++;
      await firstBlocked;
    });
    await Promise.resolve(); // let the first claim its slot and block
    await runGatedHitlResume(slots, db, 'run-1', async () => {
      resumeCalls++;
    });

    expect(resumeCalls).toBe(1); // the duplicate never spawned a second driver
    const duplicates = db.getIngressLog({ outcome: 'duplicate' });
    expect(duplicates).toHaveLength(1);
    expect(duplicates[0]!.reason).toContain('run-1');

    resolveFirst();
    await first;
    expect(slots.inFlightCount()).toBe(0);
  });

  it('a resume for a DIFFERENT run is not suppressed', async () => {
    const slots = createRunSlots({ capacity: 5 });
    const resumed: string[] = [];

    await runGatedHitlResume(slots, db, 'run-1', async () => {
      resumed.push('run-1');
    });
    await runGatedHitlResume(slots, db, 'run-2', async () => {
      resumed.push('run-2');
    });

    expect(resumed).toEqual(['run-1', 'run-2']);
  });

  it('releases the slot when the resume throws', async () => {
    const slots = createRunSlots({ capacity: 1 });

    await expect(
      runGatedHitlResume(slots, db, 'run-1', async () => {
        throw new Error('resume exploded');
      }),
    ).rejects.toThrow('resume exploded');

    expect(slots.inFlightCount()).toBe(0);
  });
});

// ===========================================================================
// Issue #8 — the listener must hand its alert seam (and the per-flow channel
// resolution) to BOTH re-drive paths. Before this, only the hot spawn path
// alerted; a re-driven run that died was silent on every channel.
// ===========================================================================

describe('re-drive failure alerting is wired into both sweeps (#8)', () => {
  const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

  /** A respawn seam that launches, then lets the test kill the child. */
  function launchingRespawn() {
    const ends = new Map<string, (exit: { code: number }) => void>();
    const respawn: RespawnSeam = async (event) => ({
      result: 'spawned' as const,
      exited: new Promise<{ code: number }>((resolve) => ends.set(event.event_id, resolve)),
    });
    return { respawn, exit: (eventId: string, code: number) => ends.get(eventId)!({ code }) };
  }

  function seedFailedFor(eventId: string, flowId: string, flowPath: string): void {
    db.acceptIngressEvent(eventId, 500, {
      flowId,
      flowPath,
      runId: `run-${eventId}`,
      substrateJson: '{}',
    });
    db.incrementSpawnAttempts(eventId);
    db.markIngressFailed(eventId);
  }

  it("alerts on the flow's own egress channel when a boot-re-driven child dies", async () => {
    seedFailedFor('boot-dies', 'flowA', '/flows/a.yaml');
    const alerts: SpawnFailedAlert[] = [];
    const { respawn, exit } = launchingRespawn();

    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#flow-a-alerts') };
    const deps = makeDeps({
      loadFlow: loadFlowFrom(flows),
      respawn,
      alert: async (a) => { alerts.push(a); },
    });
    expectStarted(await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })));

    exit('boot-dies', 7);
    await settle();

    expect(alerts).toEqual([
      {
        flowId: 'flowA',
        channel: '#flow-a-alerts',
        eventId: 'boot-dies',
        reason: 're-driven run exited with code 7',
      },
    ]);
  });

  it('falls back to the global alert channel for a flow with no egress', async () => {
    seedFailedFor('boot-perm', 'flowA', '/flows/a.yaml');
    const alerts: SpawnFailedAlert[] = [];

    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a')) }; // no egress
    const deps = makeDeps({
      loadFlow: loadFlowFrom(flows),
      respawn: async () => 'permanent_failure',
      alert: async (a) => { alerts.push(a); },
    });
    expectStarted(
      await startListener(
        deps,
        baseConfig({ allowlist: { flowA: '/flows/a.yaml' }, globalAlertChannel: 'slack:ops' }),
      ),
    );

    expect(alerts).toHaveLength(1);
    expect(alerts[0]).toMatchObject({ flowId: 'flowA', channel: 'slack:ops', eventId: 'boot-perm' });
  });

  it('alerts from the periodic sweep too, not only from boot recovery', async () => {
    const alerts: SpawnFailedAlert[] = [];
    let tick: (() => void) | null = null;
    const { respawn, exit } = launchingRespawn();

    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#flow-a-alerts') };
    const deps = makeDeps({
      loadFlow: loadFlowFrom(flows),
      respawn,
      alert: async (a) => { alerts.push(a); },
      redriveSchedule: (fn) => { tick = fn; return { cancel: () => {} }; },
    });
    const listener = expectStarted(
      await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })),
    );

    listener.redrive.start();
    seedFailedFor('sweep-dies', 'flowA', '/flows/a.yaml'); // fails AFTER boot
    tick!();
    await settle();
    exit('sweep-dies', 2);
    await settle();
    listener.redrive.stop();

    expect(alerts).toEqual([
      {
        flowId: 'flowA',
        channel: '#flow-a-alerts',
        eventId: 'sweep-dies',
        reason: 're-driven run exited with code 2',
      },
    ]);
  });
});

// ===========================================================================
// Issue #7 — the listener resumes a parked run; it does not bury it
// ===========================================================================

describe('parked-run resume (issue #7)', () => {
  const NOW_S = NOW / 1000;

  /** An ingress-launched run of flowA, parked the way cmdRun leaves it. */
  function seedParkedIngressRun(eventId: string, releaseAt: number): string {
    const runId = deriveIngressRunId(eventId);
    db.acceptIngressEvent(eventId, NOW - 60_000, { flowId: 'flowA', flowPath: '/flows/a.yaml', runId, substrateJson: '{}' });
    db.incrementSpawnAttempts(eventId);
    db.markIngressSpawned(eventId);
    db.insertRun({ run_id: runId, flow: '/flows/a.yaml', input_fingerprint: 'fp', status: 'halted', outcome: 'parked' });
    db.insertCard({ run_id: runId, id: 'c1', parent_id: null, lane: 'narrate', status: 'ready', attempt: 0, wave: 0, owned_paths: [], rework_count: 0 });
    db.getStateDb().prepare('UPDATE cards SET release_at = $at WHERE run_id = $r').run({ $at: releaseAt, $r: runId });
    // A real park also records WHY the card is gated: `release_at` alone cannot
    // distinguish a provider cap from the fan-out cache-warming stagger, which
    // stamps the same column.
    db.appendCardLog({
      runId,
      kind: 'entered_lane',
      cardId: 'c1',
      station: 'narrate',
      attempt: 0,
      sourceLane: 'narrate',
      destLane: 'narrate',
      reasonClass: 'rate_limited',
    });
    return runId;
  }

  function recordingResume(): { seam: HitlResumeSpawn; calls: Array<{ flowPath: string; runId: string }> } {
    const calls: Array<{ flowPath: string; runId: string }> = [];
    // Never resolves: the resumed run is "still executing" for the whole test.
    return { calls, seam: (req) => { calls.push(req); return new Promise(() => {}); } };
  }

  it('resumes a run that was parked at boot, once its gate has passed, on the boot sweep', async () => {
    const runId = seedParkedIngressRun('evt-parked', NOW_S - 30);
    const resume = recordingResume();
    const respawned: string[] = [];
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({
      loadFlow: loadFlowFrom(flows),
      resumeSpawn: resume.seam,
      respawn: async (event) => { respawned.push(event.event_id); return 'spawned'; },
    });

    expectStarted(await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })));

    // `conduit resume`, not a re-driven `conduit run --run-id` (which only
    // prints the run state and exits 0 for an existing run).
    expect(resume.calls).toEqual([{ flowPath: '/flows/a.yaml', runId }]);
    expect(respawned).toEqual([]);
    expect(spawnCalls).toEqual([]);
    expect(db.getIngressEvent('evt-parked')).toMatchObject({ spawn_state: 'spawned', spawn_attempts: 1 });
  });

  it('resumes a run that parked AFTER boot from the periodic tick, once its gate has passed', async () => {
    let clock = NOW;
    let tick: (() => void) | null = null;
    const resume = recordingResume();
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({
      loadFlow: loadFlowFrom(flows),
      resumeSpawn: resume.seam,
      now: () => clock,
      redriveSchedule: (fn) => { tick = fn; return { cancel: () => {} }; },
    });
    const listener = expectStarted(await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })));
    listener.redrive.start();

    // Parks after boot, gate 10 minutes out.
    const runId = seedParkedIngressRun('evt-later', NOW_S + 600);
    tick!();
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(resume.calls).toEqual([]); // not due yet

    clock = NOW + 601_000;
    tick!();
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(resume.calls).toEqual([{ flowPath: '/flows/a.yaml', runId }]);
    listener.redrive.stop();
  });

  it('resumes nothing at boot when no resume seam is wired (replies journal a manual resume)', async () => {
    seedParkedIngressRun('evt-parked', NOW_S - 30);
    const flows = { '/flows/a.yaml': makeFlow(webhookIngress('/hooks/a'), '#a') };
    const deps = makeDeps({ loadFlow: loadFlowFrom(flows) });
    expectStarted(await startListener(deps, baseConfig({ allowlist: { flowA: '/flows/a.yaml' } })));
    expect(spawnCalls).toEqual([]);
    expect(db.getIngressEvent('evt-parked')!.spawn_state).toBe('spawned');
  });
});
