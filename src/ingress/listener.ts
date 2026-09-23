/**
 * WI-410 — Listener process assembly (integration-last).
 *
 * startListener wires every ingress collaborator into a single long-lived
 * process. It is intentionally THIN: every validation rule, event-id derivation,
 * secret hygiene, and spawn-path decision lives in the module it belongs to.
 * This file only orchestrates the boot sequence and routes incoming requests.
 *
 * Boot sequence (fail-closed):
 *   1. Load each allowlisted flow.yaml via deps.loadFlow — absent path → boot error.
 *   2. Validate all ingress bindings via validateIngressBindings (WI-402) —
 *      malformed config or route collision → boot error.
 *   3. Resolve per-flow alert channels — no egress + no globalAlertChannel → boot error.
 *   4. Parse each binding and build the webhook-route and slack-channel maps;
 *      resolve app tokens for socket-transport slack flows (the original Slack Socket Mode work) — an
 *      unset app token or a missing socket seam is a boot error.
 *   5. Run redriveOnBoot (WI-407) — completes BEFORE the listener is returned —
 *      then resume any parked ingress run whose gate has passed (issue #7,
 *      ./parked.ts); the periodic sweep repeats both.
 *      Both re-drive paths (boot + periodic sweep) get the alert seam and the
 *      per-flow channels resolved in step 3, so a re-driven failure alerts the
 *      way a hot-path failure does (#8).
 *   6. Return the wired Listener (socket clients created but not started —
 *      the caller starts them when it begins serving).
 *
 * Any boot error returns { ok: false, errors } with no listener.
 */
import {
  validateIngressBindings,
  parseIngressBinding,
  findIngressCollisions,
  type FlowIngressDeclaration,
} from './binding';
import {
  redriveOnBoot,
  startPeriodicRedrive,
  type PeriodicRedrive,
  type RedriveAlerting,
  type RedriveReport,
  type RespawnSeam,
} from './recovery';
import {
  handleWebhookRequest,
  type WebhookRequest,
  type WebhookResponse,
  type WebhookAdapterDeps,
  type WebhookRouteResolution,
} from './adapters/webhook';
import {
  handleSlackEvent,
  type SlackRequest,
  type SlackResponse,
  type SlackAdapterDeps,
  type SlackChannelResolution,
  type HitlResumeSpawn,
  resumeAfterHitlReply,
} from './adapters/slack-events';
import {
  createSocketModeClient,
  defaultSocketBackoffMs,
  type SocketSeam,
  type SocketLifecycleEvent,
  type SocketModeClient,
} from './adapters/slack-socket';
import type { SpawnPathDeps, SpawnSeam, AlertSeam } from './spawn';
import { createRunSlots } from './run-slots';
import { runGatedHitlResume } from './gated-resume';
import { resumeDueParkedRuns } from './parked';
import type { ConduitDB } from '../persistence/db';
import type { LoadFlowResult } from '../flow/load';
import type { FlowConfig } from '../types/kernel';

// Re-exported for the listener's tests: the gate moved to its own module when
// the parked-run resume (issue #7) needed the same gate WITHOUT the HITL bypass.
export { runGatedHitlResume } from './gated-resume';

// ---------------------------------------------------------------------------
// Public types (pinned by listener.test.ts)
// ---------------------------------------------------------------------------

export interface ListenerConfig {
  /** Explicit per-flow allowlist: flowName → flow.yaml path (D4/FR-9, no auto-discovery). */
  allowlist: Record<string, string>;
  /** Listener-global fallback alert channel; required when any flow has no egress (Q1). */
  globalAlertChannel?: string;
  /** App-global Slack signing secret. */
  slackSigningSecret?: string;
  /** Resolved secret values keyed by secret_env name. */
  secrets?: Record<string, string>;
  /**
   * Per-flow fail-closed loading (the original multi-flow engine work). When true, a flow that fails
   * load/validation/secret resolution is QUARANTINED — excluded from routing
   * and reported on listener.quarantined — while the remaining flows serve.
   * Cross-flow collisions quarantine every implicated flow (a collision is a
   * property of the set). Boot still fails when ZERO flows survive: an engine
   * serving nothing is a misconfiguration, not a service.
   * Default false: any error withholds the whole listener (single-flow CLI
   * behavior, and the strict mode used for deploy-time validation).
   */
  quarantine?: boolean;
  /**
   * Listener-wide run backpressure (the original listener-backpressure work): at most this many spawned
   * `conduit run` processes in flight at once, across all flows and both
   * launch paths (hot accept-spawn + re-drive sweep). Events beyond the cap
   * stay in ingress_events as 'accepted' and spawn as slots free. Must be a
   * positive integer. Omitted → unlimited (before listener backpressure behavior), though in-flight
   * events are still never concurrently re-driven.
   */
  maxConcurrentRuns?: number;
}

export interface ListenerBootError {
  /** Flow name that caused the error, when applicable. */
  flow?: string;
  code: string;
  message: string;
}

export interface Listener {
  handleWebhook(req: WebhookRequest): Promise<WebhookResponse>;
  handleSlack(req: SlackRequest): SlackResponse;
  /** Resolved per-flow alert channel (used for monitoring / Q1 alerting). */
  alertChannels: Record<string, string>;
  /**
   * Socket Mode clients for socket-transport slack flows (the original Slack Socket Mode work) — one per
   * distinct app token. No-op when no flow declares transport: 'socket'.
   * start() opens the outbound wss connections; stop() closes them and
   * suppresses reconnection (call on shutdown).
   */
  sockets: {
    start(): Promise<void>;
    stop(): void;
  };
  /**
   * Periodic re-drive sweep (the original ingress-attribution work, FR-3): re-runs the bounded boot
   * re-drive on an interval so transiently-failed events recover while the
   * listener is up — not only at the next restart. Created but not started;
   * the caller starts it when it begins serving (mirrors sockets).
   */
  redrive: {
    start(): void;
    stop(): void;
  };
  /**
   * Flows excluded by per-flow fail-closed loading (the original multi-flow engine work), with the
   * errors that quarantined each. Always empty when config.quarantine is off.
   */
  quarantined: Record<string, ListenerBootError[]>;
}

export type StartListenerResult =
  | { ok: true; listener: Listener }
  | { ok: false; errors: ListenerBootError[] };

export interface ListenerDeps {
  db: ConduitDB;
  spawn: SpawnSeam;
  alert: AlertSeam;
  /**
   * The original HITL reply-and-resume work: spawns `conduit resume <flowPath> --run <runId>` after a HITL
   * reply un-holds a card (the parked run's kernel process exited when it
   * held). Optional: absent means replies record the selection and journal
   * the manual-resume instruction instead of relaunching.
   */
  resumeSpawn?: HitlResumeSpawn;
  /** Flow config loader — injected so tests avoid real disk I/O. */
  loadFlow(path: string): LoadFlowResult;
  /** Webhook HMAC/shared-secret auth seam. */
  verifyWebhookAuth(secret: string, req: WebhookRequest): boolean;
  /** Slack request-signing auth seam. */
  verifySlackAuth(signingSecret: string, req: SlackRequest): boolean;
  /** WI-407 respawn seam (launch-only, does NOT touch ingress_events/log). */
  respawn: RespawnSeam;
  /** Total spawn-attempt cap for boot re-drive (rows at cap are excluded). */
  redriveCap: number;
  /**
   * Periodic re-drive sweep interval in milliseconds (the original ingress-attribution work, FR-3).
   * Default 60_000. The same attempt cap applies as at boot.
   */
  redriveIntervalMs?: number;
  /** Scheduling seam for the periodic sweep (default: real setInterval). */
  redriveSchedule?: (tick: () => void, ms: number) => { cancel(): void };
  /** Per-sweep observability sink for the periodic sweep (default: silent). */
  onRedriveSweep?(report: RedriveReport): void;
  /** Deterministic clock — returns received_at in unix milliseconds. */
  now(): number;
  /**
   * Socket Mode I/O seam (apps.connections.open + websocket) — required only
   * when a flow declares a slack binding with transport: 'socket' (the original Slack Socket Mode work).
   * Boot fails closed (SOCKET_SEAM_UNAVAILABLE) if a socket flow is configured
   * and this seam is absent.
   */
  socket?: SocketSeam;
  /** Socket lifecycle observability sink (default: silent). */
  onSocketLifecycle?(event: SocketLifecycleEvent): void;
  /** Socket reconnect backoff schedule (default: 1s doubling, 30s cap). */
  socketBackoffMs?(attempt: number): number;
  /** Injected sleep for socket reconnection (default: real setTimeout). */
  socketSleep?(ms: number): Promise<void>;
}

// ---------------------------------------------------------------------------
// Boot + wiring
// ---------------------------------------------------------------------------

/**
 * Assemble and boot the ingress listener.
 *
 * Validates every allowlisted flow, runs the boot re-drive, then returns a
 * bound Listener. Any boot error causes the entire listener to be withheld —
 * the caller must fix the configuration before retrying.
 */
export async function startListener(
  deps: ListenerDeps,
  config: ListenerConfig,
): Promise<StartListenerResult> {
  // ── Run-slot gate (the original listener-backpressure work) — validated before anything boots ──────────
  // Config is validated, not trusted: a malformed cap is a boot error, never
  // silently coerced into "unlimited".
  if (
    config.maxConcurrentRuns !== undefined &&
    (!Number.isInteger(config.maxConcurrentRuns) || config.maxConcurrentRuns < 1)
  ) {
    return {
      ok: false,
      errors: [
        {
          code: 'INVALID_MAX_CONCURRENT_RUNS',
          message:
            `maxConcurrentRuns must be a positive integer, got ` +
            `${JSON.stringify(config.maxConcurrentRuns)}`,
        },
      ],
    };
  }
  // One gate for the whole listener: the hot spawn path and the re-drive
  // sweeps all draw from the same slot pool. When a slot frees, kick the
  // periodic sweep so queued events launch immediately instead of waiting out
  // the interval. (periodicRedrive is declared before the boot re-drive below
  // so releases fired during boot never hit a TDZ.)
  let periodicRedrive: PeriodicRedrive | null = null;
  const runSlots = createRunSlots({
    ...(config.maxConcurrentRuns !== undefined && { capacity: config.maxConcurrentRuns }),
    onRelease: () => periodicRedrive?.kick(),
  });

  // Per-flow fail-closed loading (the original multi-flow engine work): in quarantine mode a failing
  // flow is excluded and recorded instead of withholding the whole listener.
  const quarantineMode = config.quarantine === true;
  const quarantined: Record<string, ListenerBootError[]> = {};
  const quarantineFlow = (flow: string, error: ListenerBootError): void => {
    (quarantined[flow] ??= []).push(error);
  };

  // ── Phase 1: Load all allowlisted flows ───────────────────────────────────
  type LoadedFlow = {
    flowName: string;
    flowPath: string;
    flow: FlowConfig;
    rawIngress: unknown;
  };

  const loadErrors: ListenerBootError[] = [];
  let loadedFlows: LoadedFlow[] = [];

  for (const [flowName, flowPath] of Object.entries(config.allowlist)) {
    const loadResult = deps.loadFlow(flowPath);
    if (!loadResult.ok) {
      const detail = loadResult.errors.map((e) => e.message).join('; ');
      const error: ListenerBootError = {
        flow: flowName,
        code: 'FLOW_LOAD_FAILED',
        message: `Flow '${flowName}': ${detail}`,
      };
      if (quarantineMode) quarantineFlow(flowName, error);
      else loadErrors.push(error);
      continue;
    }
    loadedFlows.push({
      flowName,
      flowPath,
      flow: loadResult.flow,
      // channels.ingress is typed as { type?: string } but is runtime-rich.
      // Cast to unknown so parseIngressBinding/validateIngressBindings can
      // read the full raw binding (route, auth, event_id, substrate, channel).
      rawIngress: loadResult.flow.channels?.ingress as unknown,
    });
  }

  if (loadErrors.length > 0) return { ok: false, errors: loadErrors };

  // ── Phase 1b: Reject flows that are not listener-servable at all ─────────
  // WI-402 validation (Phase 2 below) treats `cli` as a legal-but-incomplete
  // ingress type and a missing binding as MISSING_EVENT_ID_SOURCE — both
  // technically true but the wrong diagnosis (the original listener-servability work review, @queso): a
  // `cli`-ingress flow (local `conduit run` triggering, see
  // examples/research-flow.yaml) is not something the LISTENER can ever route
  // to, and neither is a flow with no ingress binding declared at all. Worse,
  // a `cli` flow that happens to carry a well-formed event_id would PASS
  // Phase 2 and boot silently unreachable — it lands in neither the webhook
  // route map nor the slack channel map (Phase 4b only populates those for
  // `webhook`/`slack` bindings). Catching both shapes here, before Phase 2,
  // gives `--validate` runs the real diagnosis instead of a missing-field
  // complaint. Every other shape (webhook/slack/malformed/unknown) is left to
  // the existing Phase 2 validator untouched.
  const servabilityErrors: ListenerBootError[] = [];

  loadedFlows = loadedFlows.filter(({ flowName, rawIngress }) => {
    const rawType =
      rawIngress !== null && typeof rawIngress === 'object' && !Array.isArray(rawIngress)
        ? (rawIngress as Record<string, unknown>)['type']
        : undefined;

    let error: ListenerBootError | null = null;
    if (rawType === 'cli') {
      error = {
        flow: flowName,
        code: 'INGRESS_NOT_LISTENER_SERVABLE',
        message:
          `Flow '${flowName}' declares 'cli' ingress, which is not listener-servable — ` +
          `cli-ingress flows are triggered via 'conduit run', not the listener`,
      };
    } else if (rawIngress === null || rawIngress === undefined) {
      error = {
        flow: flowName,
        code: 'INGRESS_NOT_LISTENER_SERVABLE',
        message: `Flow '${flowName}' declares no ingress binding — the listener cannot route events to it`,
      };
    }

    if (error === null) return true;
    if (quarantineMode) {
      quarantineFlow(flowName, error);
      return false;
    }
    servabilityErrors.push(error);
    return true; // strict mode fails collectively below
  });

  if (servabilityErrors.length > 0) return { ok: false, errors: servabilityErrors };

  // ── Phase 2: Validate ingress bindings (WI-402) ───────────────────────────
  if (!quarantineMode) {
    const declarations: FlowIngressDeclaration[] = loadedFlows.map(({ flowName, rawIngress }) => ({
      flow: flowName,
      ingress: rawIngress,
    }));

    const validationResult = validateIngressBindings(declarations);
    if (!validationResult.ok) {
      return {
        ok: false,
        errors: validationResult.errors.map((e) => ({ code: e.code, message: e.message })),
      };
    }
  } else {
    // Per-flow validation first, so one flow's malformed binding never hides
    // another's; then cross-flow collisions, which quarantine EVERY implicated
    // flow — a collision is a property of the set, and picking a winner would
    // silently route one team's events to another team's flow.
    loadedFlows = loadedFlows.filter((lf) => {
      const result = validateIngressBindings([{ flow: lf.flowName, ingress: lf.rawIngress }]);
      if (result.ok) return true;
      for (const e of result.errors) {
        quarantineFlow(lf.flowName, { flow: lf.flowName, code: e.code, message: e.message });
      }
      return false;
    });

    const collisions = findIngressCollisions(
      loadedFlows.map(({ flowName, rawIngress }) => ({ flow: flowName, ingress: rawIngress })),
    );
    if (collisions.length > 0) {
      const collided = new Set<string>();
      for (const collision of collisions) {
        const label = collision.kind === 'route' ? 'Webhook route' : 'Slack channel';
        for (const flow of collision.flows) {
          collided.add(flow);
          quarantineFlow(flow, {
            flow,
            code: collision.kind === 'route' ? 'ROUTE_COLLISION' : 'CHANNEL_COLLISION',
            message:
              `${label} '${collision.key}' is claimed by multiple flows: ` +
              `${collision.flows.join(', ')} — all claimants quarantined`,
          });
        }
      }
      loadedFlows = loadedFlows.filter((lf) => !collided.has(lf.flowName));
    }
  }

  // ── Phase 3: Resolve per-flow alert channels ─────────────────────────────
  const alertChannels: Record<string, string> = {};
  const alertErrors: ListenerBootError[] = [];

  loadedFlows = loadedFlows.filter(({ flowName, flow }) => {
    const channel = flow.channels?.egress?.[0]?.target ?? config.globalAlertChannel;
    if (channel === undefined) {
      const error: ListenerBootError = {
        flow: flowName,
        code: 'NO_ALERT_CHANNEL',
        message:
          `Flow '${flowName}' has no egress channel configured and no globalAlertChannel is set`,
      };
      if (quarantineMode) {
        quarantineFlow(flowName, error);
        return false;
      }
      alertErrors.push(error);
      return true; // strict mode fails collectively below
    }
    alertChannels[flowName] = channel;
    return true;
  });

  if (alertErrors.length > 0) return { ok: false, errors: alertErrors };

  // ── Phase 4: Parse bindings and build route/channel maps ─────────────────
  // One shared SpawnPathDeps for all adapters. runSpawnPath uses the flow's
  // own egress channel first; globalAlertChannel is the cross-flow fallback
  // (guaranteed to exist if we reached this point — every no-egress flow passed
  // the alert-channel check above because globalAlertChannel is set).
  const sharedSpawnDeps: SpawnPathDeps = {
    db: deps.db,
    spawn: deps.spawn,
    alert: deps.alert,
    globalAlertChannel: config.globalAlertChannel ?? '',
    redriveCap: deps.redriveCap,
    slots: runSlots,
    now: deps.now,
  };

  // ── Phase 4a: Resolve socket-transport requirements per flow ─────────────
  // Resolved BEFORE the routing maps are built so a quarantined socket flow
  // never lands in slackChannelMap. An unset app token cannot fail-close per
  // request like webhook auth does (there is no request), it would just
  // retry-loop invalid_auth against Slack forever — fail loud at boot
  // (WI-402 validation guarantees app_token_env is present on socket bindings).
  const socketErrors: ListenerBootError[] = [];
  const socketFlowTokens = new Map<string, string>(); // flowName → resolved app token

  loadedFlows = loadedFlows.filter(({ flowName, rawIngress }) => {
    const parseResult = parseIngressBinding(rawIngress);
    if (!parseResult.ok) return true; // phase 2 ensures this is unreachable
    const { binding } = parseResult;
    if (binding.type !== 'slack' || binding.transport !== 'socket') return true;

    const appTokenEnv = binding.app_token_env ?? '';
    const appToken = config.secrets?.[appTokenEnv] ?? '';
    if (appToken === '') {
      const error: ListenerBootError = {
        flow: flowName,
        code: 'MISSING_APP_TOKEN_SECRET',
        message:
          `Flow '${flowName}' declares a socket-transport slack binding but the app ` +
          `token env var '${appTokenEnv}' is not set — Socket Mode cannot open its ` +
          `connection without the app-level token (xapp-…, scope connections:write)`,
      };
      if (quarantineMode) {
        quarantineFlow(flowName, error);
        return false;
      }
      socketErrors.push(error);
      return true; // strict mode fails collectively below
    }
    socketFlowTokens.set(flowName, appToken);
    return true;
  });

  if (socketFlowTokens.size > 0 && deps.socket === undefined) {
    const seamError: ListenerBootError = {
      code: 'SOCKET_SEAM_UNAVAILABLE',
      message:
        'a socket-transport slack binding is configured but this environment provides ' +
        'no Socket Mode I/O seam — the listener cannot open outbound wss connections',
    };
    if (quarantineMode) {
      // The seam is an environment property, but only socket flows need it:
      // quarantine those and let HTTP-transport flows serve.
      for (const flowName of socketFlowTokens.keys()) {
        quarantineFlow(flowName, { ...seamError, flow: flowName });
      }
      const socketFlows = new Set(socketFlowTokens.keys());
      loadedFlows = loadedFlows.filter((lf) => !socketFlows.has(lf.flowName));
      socketFlowTokens.clear();
    } else {
      socketErrors.push(seamError);
    }
  }

  if (socketErrors.length > 0) return { ok: false, errors: socketErrors };

  // ── Quarantine backstop: an engine with ZERO servable flows must not boot ──
  if (quarantineMode && loadedFlows.length === 0) {
    return {
      ok: false,
      errors: [
        ...Object.values(quarantined).flat(),
        {
          code: 'NO_SERVABLE_FLOWS',
          message:
            'every configured flow failed validation — an engine serving nothing is a ' +
            'misconfiguration, not a service',
        },
      ],
    };
  }

  // ── Phase 4b: Build route/channel maps from the surviving flows ──────────
  const webhookRouteMap = new Map<string, WebhookRouteResolution>();
  const slackChannelMap = new Map<string, SlackChannelResolution>();
  // Socket Mode flows (the original Slack Socket Mode work): one client per DISTINCT resolved app token —
  // flows sharing a Slack app share a connection; routing stays per-channel via
  // slackChannelMap, exactly like the webhook transport.
  const socketAppTokens = new Set<string>(socketFlowTokens.values());

  for (const { flowName, flowPath, flow, rawIngress } of loadedFlows) {
    const parseResult = parseIngressBinding(rawIngress);
    if (!parseResult.ok) continue; // validation in phase 2 ensures this is unreachable

    const { binding } = parseResult;

    if (binding.type === 'webhook' && binding.route !== undefined) {
      const secretEnv =
        typeof (binding.auth as Record<string, unknown> | undefined)?.['secret_env'] === 'string'
          ? (binding.auth as Record<string, unknown>)['secret_env'] as string
          : undefined;
      const secret = secretEnv !== undefined ? (config.secrets?.[secretEnv] ?? '') : '';

      webhookRouteMap.set(binding.route, { flowId: flowName, flowPath, flow, binding, secret });
    } else if (binding.type === 'slack') {
      // `channel` is not part of the typed IngressBinding — extract from the raw value.
      const rawRecord =
        rawIngress !== null && typeof rawIngress === 'object' && !Array.isArray(rawIngress)
          ? (rawIngress as Record<string, unknown>)
          : {};
      const channel = typeof rawRecord['channel'] === 'string' ? rawRecord['channel'] : '';

      slackChannelMap.set(channel, { flowId: flowName, flowPath, flow, binding });
    }
  }

  // Failure alerting for BOTH re-drive paths (#8), built from the per-flow
  // channels resolved in phase 3 — so a re-driven run that dies is as loud as
  // one that failed on the hot spawn path. Passing this is not optional in
  // production: without it a halted re-driven run reaches ingress_log and
  // nothing else (listener.test.ts pins the wiring).
  const redriveAlerts: RedriveAlerting = {
    alert: deps.alert,
    channels: alertChannels,
    globalAlertChannel: config.globalAlertChannel ?? '',
  };

  // ── Phase 5: Boot re-drive (WI-407) — must complete before serving ────────
  // Slot-gated (the original listener-backpressure work): a backlog of recoverable events re-drives at most
  // maxConcurrentRuns at a time instead of stampeding the model endpoint.
  await redriveOnBoot({
    db: deps.db,
    respawn: deps.respawn,
    cap: deps.redriveCap,
    slots: runSlots,
    alerts: redriveAlerts,
    now: deps.now,
  });

  // Parked-run resume (issue #7): a run halted behind a provider rate limit is
  // `conduit resume`d — never re-driven — once its gate passes. Runs here after
  // the boot re-drive (a listener restarted mid-wait still resumes) and again
  // after every periodic sweep. Launch-only, like the sweep: each resume is
  // supervised detached, holding its run slot until the child exits.
  const resumeSpawn = deps.resumeSpawn;
  const resumeParked =
    resumeSpawn !== undefined
      ? () =>
          resumeDueParkedRuns({
            db: deps.db,
            slots: runSlots,
            resumeSpawn,
            alerts: redriveAlerts,
            now: deps.now,
          })
      : undefined;
  await resumeParked?.();

  // ── Phase 6: Build and return the wired listener ─────────────────────────
  const webhookAdapterDeps: WebhookAdapterDeps = {
    spawnDeps: sharedSpawnDeps,
    resolveRoute: (route) => webhookRouteMap.get(route) ?? null,
    verifyAuth: deps.verifyWebhookAuth,
    now: deps.now,
  };

  // The original HITL reply-and-resume work: after a HITL reply lands, relaunch the parked run. Built once,
  // shared by the webhook-events path and every socket client. Slot-gated
  // opportunistically (the original listener-backpressure work × HITL work, see runGatedHitlResume) — a human's
  // pick participates in the run-slot accounting when a slot is free but is
  // NEVER queued behind an ingress backlog.
  const onHitlResumed =
    deps.resumeSpawn !== undefined
      ? (runId: string) =>
          runGatedHitlResume(runSlots, deps.db, runId, () =>
            resumeAfterHitlReply(deps.db, runId, deps.resumeSpawn!),
          )
      : undefined;

  const slackAdapterDeps: SlackAdapterDeps = {
    spawnDeps: sharedSpawnDeps,
    signingSecret: config.slackSigningSecret ?? '',
    resolveChannel: (channel) => slackChannelMap.get(channel) ?? null,
    verifyAuth: deps.verifySlackAuth,
    now: deps.now,
    ...(onHitlResumed !== undefined ? { onHitlResumed } : {}),
  };

  // Socket Mode clients (the original Slack Socket Mode work) — created but NOT started; the caller
  // starts them when it begins serving (mirrors how the HTTP handlers are
  // bound here but only served by the caller).
  const socketClients: SocketModeClient[] = [...socketAppTokens].map((appToken) =>
    createSocketModeClient(
      {
        spawnDeps: sharedSpawnDeps,
        resolveChannel: (channel) => slackChannelMap.get(channel) ?? null,
        now: deps.now,
        ...(onHitlResumed !== undefined ? { onHitlResumed } : {}),
        socket: deps.socket!, // guaranteed above when socketAppTokens is non-empty
        onLifecycle: deps.onSocketLifecycle ?? (() => {}),
        backoffMs: deps.socketBackoffMs ?? defaultSocketBackoffMs,
        sleep: deps.socketSleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms))),
      },
      appToken,
    ),
  );

  // Periodic re-drive (the original ingress-attribution work, FR-3) — created lazily on start() so a
  // listener that is booted but never served (unit tests, dry boot) schedules
  // nothing. Shares the boot re-drive's respawn seam, attempt cap, and run-slot
  // gate. (periodicRedrive itself is declared at the top of startListener so
  // the gate's onRelease kick can reference it safely during boot.)
  const listener: Listener = {
    handleWebhook: (req) => handleWebhookRequest(webhookAdapterDeps, req),
    handleSlack: (req) => handleSlackEvent(slackAdapterDeps, req),
    alertChannels,
    sockets: {
      start: async () => {
        await Promise.all(socketClients.map((client) => client.start()));
      },
      stop: () => {
        for (const client of socketClients) client.stop();
      },
    },
    quarantined,
    redrive: {
      start: () => {
        if (periodicRedrive !== null) return; // idempotent — one schedule per listener
        periodicRedrive = startPeriodicRedrive({
          db: deps.db,
          respawn: deps.respawn,
          cap: deps.redriveCap,
          slots: runSlots,
          alerts: redriveAlerts,
          now: deps.now,
          intervalMs: deps.redriveIntervalMs ?? 60_000,
          ...(deps.redriveSchedule !== undefined && { schedule: deps.redriveSchedule }),
          ...(deps.onRedriveSweep !== undefined && { onSweep: deps.onRedriveSweep }),
          ...(resumeParked !== undefined && { afterSweep: resumeParked }),
        });
      },
      stop: () => {
        periodicRedrive?.stop();
        periodicRedrive = null;
      },
    },
  };

  return { ok: true, listener };
}
