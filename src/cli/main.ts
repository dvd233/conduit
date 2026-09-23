/**
 * conduit CLI lifecycle: run / resume / doctor / journal inspect-tail (WI-306).
 *
 * SPEC §10A/§15, FR-1a/15/17. This is the integration entrypoint — it wires the
 * real kernel modules into a runnable binary:
 *
 *   conduit run <flow.yaml>           — fail-closed validation; drive to terminal; exit 0
 *   conduit resume [--rebind] <flow.yaml> — reconcile dead leases; never blind-retry outbox
 *   conduit doctor                    — run prereq probes; report + exit non-zero on any fail
 *   conduit journal inspect <cardId>  — read-only journal print
 *   conduit journal tail   <cardId>   — read-only journal print
 *
 * The binary wires the real planTick loop as `runEngine`; tests inject a fake so
 * the lifecycle is unit-testable without a full e2e (WI-307 owns the terminal drive).
 */

import { readFileSync, writeFileSync, rmSync, existsSync, statSync, mkdirSync } from 'node:fs';
import { join, dirname, resolve, isAbsolute } from 'node:path';
import { createHmac, timingSafeEqual } from 'node:crypto';
import { Database } from 'bun:sqlite';
import type { FlowConfig } from '../types/kernel';
import type { ConduitDB } from '../persistence/db';
import type { WorkerMessage } from '../worker/ipc-protocol';
import { parseWorkerMessage, serializeWorkerMessage } from '../worker/ipc-protocol';
import { startWorkerMain } from '../worker/worker-entry';
import { openConduitDB, DEFAULT_RUN_ID } from '../persistence/db';
import { validateRunId } from '../run/run-id';
import { registerRun, computeFingerprint } from '../run/run-registry';
import { acquireRunLease, releaseRunLease, peekRunLeaseHolder, defaultIsPidAlive } from '../run/run-lock';
import { getRunState, getRunParkedRelease, formatParkedRun, type RunStateResult } from '../run/run-state';
import type { ModelAdapter, ModelCall, ModelResponse } from '../worker/adapter';
import {
  buildHarnessDefinitionRegistry,
  bindHarnessDefinitions,
  bindHarnessDefinitionsForIntrospection,
} from '../worker/harness-adapter';
import type {
  HarnessRegistry,
  HarnessDefinitionRegistry,
  HarnessAdapterDefinition,
} from '../worker/harness-adapter';
import { parseHarnessConfig } from '../worker/harness-config';
import { loadFlow, probeHarnessBinaries } from '../flow/load';
import { reclaimOrphanedWorkers } from '../dispatch/claim';
import { ensureCheckpointSchema, reconcileOnResume } from '../checkpoint/checkpoint';
import { runExecutor, resolveSlackFetchTimeoutMs, resolveSocketConnectionsOpenUrl, type SubflowSeam, type SubflowInvocation } from '../controller/executor';
import { createOpenAiAdapter } from '../worker/openai-adapter';
import { cmdReply } from './reply';
import { renderFlow } from './explain-renderer';
import {
  startListener as startListenerImpl,
  type ListenerConfig,
  type StartListenerResult,
  type Listener,
} from '../ingress/listener';
import { parseEngineManifest } from '../ingress/manifest';
import type { RedriveLaunch } from '../ingress/recovery';
import type { SpawnFailedAlert } from '../ingress/spawn';
import { socketDialOptions } from '../ingress/socket-proxy';
import { verifySlackSignature } from '../ingress/adapters/slack-events';
import type { SocketSeam } from '../ingress/adapters/slack-socket';
import { parseIngressBinding } from '../ingress/binding';

// ---------------------------------------------------------------------------
// Public interfaces
// ---------------------------------------------------------------------------

export interface CliIO {
  out(line: string): void;
  err(line: string): void;
}

/**
 * Render the human-readable ingress failure line consumed by existing local
 * failure relays. Keep the field names and order stable until a delivering
 * alert channel replaces this stderr integration (issue #19 / #18).
 */
export function formatIngressAlert(alert: SpawnFailedAlert): string {
  return `[ingress alert] flow=${alert.flowId} event=${alert.eventId} channel=${alert.channel}: ${alert.reason}`;
}

export interface PrereqProbe {
  name: string;
  check(): { ok: boolean; detail?: string } | Promise<{ ok: boolean; detail?: string }>;
}

export interface SpawnedWorker {
  pid: number;
  send: (msg: WorkerMessage) => void;
  /** Hard-terminate a past-lease worker's subprocess on a planDrain hard_kill action. */
  kill?: () => void;
}

export interface SpawnWorkerArgs {
  cardId: string;
  station: string;
}

export type SpawnWorker = (args: SpawnWorkerArgs) => SpawnedWorker;

/** A wired worker pool: the spawn + single inbound-IPC seam runExecutor consumes, plus teardown. */
export interface WorkerPool {
  spawn: SpawnWorker;
  /** Register the ONE inbound handler (the kernel is the sole writer; NFR-3). */
  onMessage: (handler: (msg: WorkerMessage) => void) => void;
  /** Kill any workers still alive when the run ends (best-effort cleanup). */
  dispose: () => void;
}

export interface WorkerPoolArgs {
  /** Absolute flow path the worker subprocess re-loads to run its station. */
  flowPath: string;
  /** Project root the worker uses as cwd for deterministic commands. */
  projectRoot: string;
}

/**
 * Build a real worker pool: one Bun subprocess per dispatched card, running the
 * `__worker <flowPath> <projectRoot>` entry. Each child's IPC is routed into the
 * single handler runExecutor registers via onMessage (the kernel is the sole DB
 * writer; NFR-3). Messages cross the wire as codec-validated strings. Exported so
 * the multi-process integration test drives the exact production spawn path.
 */
export function buildWorkerPool(args: WorkerPoolArgs, io: CliIO): WorkerPool {
  const { flowPath, projectRoot } = args;
  let handler: ((msg: WorkerMessage) => void) | null = null;
  const live = new Set<ReturnType<typeof Bun.spawn>>();
  return {
    onMessage: (h) => {
      handler = h;
    },
    spawn: ({ cardId, station }) => {
      const child = Bun.spawn(
        [process.execPath, import.meta.path, '__worker', flowPath, projectRoot],
        {
          stdout: 'inherit',
          stderr: 'inherit',
          ipc(raw: unknown) {
            if (typeof raw !== 'string' || handler === null) return;
            const parsed = parseWorkerMessage(raw);
            if (parsed.ok) {
              handler(parsed.message);
            } else {
              io.err(
                `[pool] rejected IPC from worker (card ${cardId}, station ${station}): ${parsed.error}`,
              );
            }
          },
        },
      );
      live.add(child);
      void child.exited.then(() => live.delete(child));
      return {
        pid: child.pid,
        send: (msg: WorkerMessage) => child.send(serializeWorkerMessage(msg)),
        kill: () => {
          try {
            child.kill();
          } catch {
            /* already exited */
          }
        },
      };
    },
    dispose: () => {
      for (const c of live) {
        try {
          c.kill();
        } catch {
          /* already exited */
        }
      }
      live.clear();
    },
  };
}

export interface RunEngineArgs {
  db: ConduitDB;
  flow: FlowConfig;
  projectRoot?: string;
  now: () => number;
  adapter: ModelAdapter;
  io: CliIO;
  concurrency?: number;
  spawn?: SpawnWorker;
  onMessage?: (handler: (msg: WorkerMessage) => void) => void;
  runId?: string;
  /**
   * Registry of engine-config-defined harness adapters (WI-560), resolved by
   * name at `kind: harness` station dispatch. Optional so pre-harness
   * RunEngineArgs literals keep compiling; buildProductionDeps populates it.
   */
  harnessRegistry?: HarnessRegistry;
  /**
   * Seam that runs a `kind: subflow` station's child flow to terminal state
   * (the original multi-flow engine work). Production spawns a `conduit run` subprocess and reads the
   * child's terminal state + journaled spend back off the shared DB; tests
   * inject fakes. Absent → subflow stations escalate to hold (config failure).
   */
  runSubflow?: SubflowSeam;
  /**
   * Run-budget ceilings imposed by a CALLER (the original multi-flow engine work): when this run is a
   * subflow child, the parent passes its remaining budget here and the
   * executor takes min(flow-declared, caller-imposed) — a child can never
   * out-spend either ceiling. Absent for top-level runs.
   */
  budgetMaxTokens?: number;
  budgetWallClockSeconds?: number;
  /**
   * Real-time sleep between ticks when the only reason a card can't dispatch is a
   * future release_at gate (the fan-out cache-warming stagger). Defaults to
   * setTimeout; tests inject a fast/immediate sleep so a fake advancing clock can
   * re-tick without burning wall-clock. Never used unless a card is release-gated.
   */
  sleep?: (ms: number) => Promise<void>;
}

export interface CliDeps {
  io: CliIO;
  now: () => number;
  db: ConduitDB;
  adapter: ModelAdapter;
  runEngine: (args: RunEngineArgs) => Promise<void>;
  prereqs: PrereqProbe[];
  /**
   * Registry of engine-config-defined harness adapters (WI-560). Optional so
   * existing CliDeps literals keep compiling; buildProductionDeps populates
   * it. Threaded into RunEngineArgs at cmdRun/cmdResume so the harness
   * execution path (a later item) reads it from the run context.
   */
  harnessRegistry?: HarnessRegistry;
  /**
   * Optional per-run binding seam (WI-588): given the run's resolved
   * projectRoot, returns a HarnessRegistry whose adapters are confined to
   * that root. When present, cmdRun/cmdResume call this instead of reusing
   * `harnessRegistry` (which is bound to a load-time placeholder root and
   * must never be invoked) to build the registry threaded into RunEngineArgs.
   * Optional so existing CliDeps literals keep compiling; buildProductionDeps
   * populates it with the real binder.
   */
  bindHarnessRegistry?: (projectRoot: string) => HarnessRegistry;
  /**
   * Optional config-time definition registry (WI-592), exposing envAllowlist +
   * command per adapter — fields `harnessRegistry` deliberately strips (it's
   * bound via bindHarnessDefinitionsForIntrospection for load/probe use only).
   * cmdDoctor's flow-independent adapter listing reads from this so it can
   * warn on env-allowlist/command misconfigurations without weakening the
   * introspection registry's binding. Optional so existing CliDeps literals
   * keep compiling; buildProductionDeps populates it with the same
   * definition registry it already builds.
   */
  harnessDefinitions?: HarnessDefinitionRegistry;
  /**
   * Optional subflow child runner (the original multi-flow engine work): runs a `kind: subflow`
   * station's child flow to terminal state. Production spawns a `conduit run`
   * subprocess and reads the child's terminal state + journaled spend back
   * off the shared DB. Optional so existing CliDeps literals keep compiling.
   */
  runSubflow?: SubflowSeam;
  /**
   * Optional flow-aware probe factory, invoked by cmdDoctor only when a flow arg is
   * provided. Optional so existing CliDeps literals keep compiling; buildProductionDeps
   * populates it with the real factory.
   */
  flowProbes?: (flow: FlowConfig) => PrereqProbe[];
  /** Optional ingress listener seam — present in production; omitted in unit tests. */
  startListener?: (config: ListenerConfig) => Promise<StartListenerResult>;
  /**
   * Optional HTTP-serving seam — present in production, omitted in unit tests.
   *
   * When present, cmdListen hands the booted listener to this seam, which binds
   * an HTTP server and BLOCKS until a shutdown signal (SIGTERM/SIGINT). When
   * absent (the injected-seam test path), cmdListen returns immediately after a
   * successful boot so the lifecycle stays unit-testable without binding a port.
   */
  serve?: (listener: Listener, opts: { port: number }) => Promise<void>;
  /**
   * Optional worker-pool factory — present in production (real Bun.spawn workers),
   * omitted in unit tests that drive the synchronous path. cmdRun/cmdResume call
   * it only when concurrency > 1, then thread the resulting spawn + onMessage
   * seams into runEngine so deterministic stations run as out-of-process workers.
   */
  makeWorkerPool?: (args: WorkerPoolArgs) => WorkerPool;
}

// ---------------------------------------------------------------------------
// Probe builders — individually testable, injectable seams for doctor/pre-flight
// ---------------------------------------------------------------------------

interface StateDirProbeOptions {
  /**
   * Paths under this root must be backed by an actual mount. Defaults to /data,
   * the container state-volume mount point. Custom local DB paths only need the
   * writable probe so non-container development remains ergonomic.
   */
  requireMountRoot?: string;
  /** Test seam for synthetic mount tables. Defaults to /proc/self/mountinfo. */
  mountInfoPath?: string;
}

function decodeMountInfoPath(path: string): string {
  return path.replace(/\\([0-7]{3})/g, (_m, octal: string) =>
    String.fromCharCode(parseInt(octal, 8)),
  );
}

function pathIsUnder(root: string, candidate: string): boolean {
  const r = resolve(root);
  const c = resolve(candidate);
  return c === r || c.startsWith(`${r}/`);
}

function stateDirHasDedicatedMount(dir: string, mountInfoPath: string): boolean {
  let mountInfo: string;
  try {
    mountInfo = readFileSync(mountInfoPath, 'utf-8');
  } catch {
    return false;
  }

  const target = resolve(dir);
  for (const line of mountInfo.split('\n')) {
    if (!line.trim()) continue;
    const fields = line.split(' ');
    const mountPoint = fields[4] ? resolve(decodeMountInfoPath(fields[4])) : '';
    if (!mountPoint || mountPoint === '/') continue;
    if (target === mountPoint || target.startsWith(`${mountPoint}/`)) return true;
  }
  return false;
}

/**
 * State-dir write probe: create and immediately delete a temp file in the directory
 * that houses the state DB. A real write test (not a SELECT-1 read) so a read-only
 * bind mount is detected as a FAILURE, satisfying the FR-7 footgun guard.
 *
 * Replaces the legacy SELECT-1 check in buildProductionDeps.
 */
export function buildStateDirProbe(
  stateDbPath: string,
  options: StateDirProbeOptions = {},
): PrereqProbe {
  return {
    name: 'state_db_volume',
    check() {
      const dir = dirname(stateDbPath);
      const requireMountRoot = resolve(options.requireMountRoot ?? '/data');
      const mountInfoPath = options.mountInfoPath ?? '/proc/self/mountinfo';
      const probeFile = join(dir, `.conduit-write-probe-${process.pid}`);
      let created = false;
      try {
        writeFileSync(probeFile, 'probe', 'utf-8');
        created = true;
      } catch {
        return {
          ok: false,
          detail: `state directory is not writable — mount a writable volume at ${dir}`,
        };
      } finally {
        if (created) {
          try {
            rmSync(probeFile);
          } catch {
            /* ignore cleanup errors */
          }
        }
      }

      if (pathIsUnder(requireMountRoot, dir) && !stateDirHasDedicatedMount(dir, mountInfoPath)) {
        return {
          ok: false,
          detail: `state directory is writable but is not a mounted volume — mount a Docker volume at ${requireMountRoot}`,
        };
      }

      return { ok: true };
    },
  };
}

/**
 * Project-root presence probe.
 *
 * Three cases:
 *  - undefined      — env var was never set (missing from environment entirely) → FAIL
 *  - "" (empty)     — env var is set but empty (engine image with no flow baked in) → ok no-op
 *  - non-empty path — CONDUIT_PROJECT_ROOT is configured; FAIL only when that path is absent
 */
export function buildProjectRootProbe(projectRoot: string | undefined): PrereqProbe {
  return {
    name: 'project-root-present',
    check() {
      if (projectRoot === undefined) {
        return { ok: false, detail: 'project root is not configured' };
      }
      if (projectRoot === '') {
        return { ok: true, detail: 'no project root configured' };
      }
      if (!existsSync(projectRoot)) {
        return {
          ok: false,
          detail: `project root directory not found: ${projectRoot} — set CONDUIT_PROJECT_ROOT to an existing path`,
        };
      }
      return { ok: true };
    },
  };
}

/**
 * Flow-prerequisites probe: for each name in the prerequisites list (from
 * FlowConfig.prerequisites, WI-434), verify it is present on PATH via Bun.which.
 * Reports FAIL naming every missing package, not just the first.
 */
export function buildFlowPrereqsProbe(prerequisites: readonly string[]): PrereqProbe {
  return {
    name: 'flow-prereqs-present',
    check() {
      const missing = prerequisites.filter((pkg) => Bun.which(pkg) === null);
      if (missing.length > 0) {
        return { ok: false, detail: `missing system packages: ${missing.join(', ')}` };
      }
      return { ok: true };
    },
  };
}

/**
 * Default HTTP connect implementation for buildModelEndpointProbe.
 * Tries HEAD first (lightweight); falls back to GET if HEAD fails.
 */
async function defaultModelEndpointConnect(baseUrl: string): Promise<boolean> {
  const probeUrl = `${baseUrl.replace(/\/+$/, '')}/models`;
  // Bound both probes on the shared network-fetch budget so an unreachable
  // endpoint fails `conduit doctor` fast instead of hanging it (same stall class
  // as the Slack transport fix; SLACK_FETCH_TIMEOUT_MS is the operator knob).
  try {
    const headResp = await fetch(probeUrl, {
      method: 'HEAD',
      signal: AbortSignal.timeout(resolveSlackFetchTimeoutMs()),
    });
    if (headResp.ok) return true;
    const getResp = await fetch(probeUrl, {
      method: 'GET',
      signal: AbortSignal.timeout(resolveSlackFetchTimeoutMs()),
    });
    return getResp.ok;
  } catch {
    return false;
  }
}

/**
 * Model-endpoint reachability probe: attempts an HTTP HEAD/GET to the configured
 * base URL's OpenAI-compatible /models endpoint. The `connect` seam is
 * injectable for unit tests (no real network I/O).
 * When baseUrl is undefined, reports FAIL naming CONDUIT_BASE_URL so the operator
 * knows which env var to set. When connect throws, the error is swallowed and
 * reported as FAIL (never crashes the doctor run).
 */
export function buildModelEndpointProbe(
  baseUrl: string | undefined,
  connect: (url: string) => Promise<boolean> = defaultModelEndpointConnect,
): PrereqProbe {
  return {
    name: 'model-endpoint-reachable',
    async check() {
      if (!baseUrl) {
        return {
          ok: false,
          detail: 'CONDUIT_BASE_URL is not set — configure the model endpoint URL',
        };
      }
      try {
        const reachable = await connect(baseUrl);
        if (!reachable) {
          return { ok: false, detail: `model endpoint unreachable: ${baseUrl}` };
        }
        return { ok: true };
      } catch {
        return { ok: false, detail: `model endpoint unreachable: ${baseUrl}` };
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Pre-flight gate — shared by cmdRun and cmdListen (FR-2)
// ---------------------------------------------------------------------------

/**
 * Run all base prereq probes and print each result. Returns true if every probe
 * passes, false if any fails. Used as a pre-flight gate before dispatch/boot so
 * environmental issues are caught before any irreversible work starts.
 */
async function runPreflightGate(deps: CliDeps): Promise<boolean> {
  let anyFail = false;
  for (const probe of deps.prereqs) {
    const result = await probe.check();
    const status = result.ok ? 'ok' : 'FAIL';
    const detail = result.detail ? ` — ${result.detail}` : '';
    deps.io.out(`  ${probe.name}: ${status}${detail}`);
    if (!result.ok) anyFail = true;
  }
  return !anyFail;
}

// ---------------------------------------------------------------------------
// run_meta — CLI-managed table for pinned flow version
// ---------------------------------------------------------------------------

function ensureRunMetaTable(stateDb: Database): void {
  stateDb.exec(
    `CREATE TABLE IF NOT EXISTS run_meta (
       key   TEXT PRIMARY KEY,
       value TEXT NOT NULL
     )`,
  );
}

/**
 * Upsert the pinned flow version in the CLI-managed run_meta table.
 * The binary pins the version on each `run` so `resume` can detect schema drift.
 */
export function setPinnedFlowVersion(db: ConduitDB, version: number): void {
  const stateDb = db.getStateDb();
  ensureRunMetaTable(stateDb);
  stateDb
    .prepare(
      `INSERT INTO run_meta (key, value) VALUES ('pinned_flow_version', $value)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    )
    .run({ $value: String(version) });
}

/**
 * Persist a terminal status + outcome onto the `runs` row (#2).
 *
 * Runs are inserted as status='running' by registerRun and were never advanced,
 * so (a) bare `conduit resume` re-selected completed runs forever and (b)
 * getRunState reported outcome='unknown'. We write a terminal status that the
 * resume-selection query (`status NOT IN ('done', ...)`) excludes — `'done'` for
 * a clean finish, `'halted'` for a stalled/held finish. `'halted'` is left
 * RESUMABLE on purpose (not in the exclude list) so an operator can re-drive it.
 *
 * Done inline (not on ConduitDB) because db.ts is owned by another agent; uses
 * the same prepared-statement seam other run-meta mutations in this file use.
 */
export function updateRunStatus(
  db: ConduitDB,
  runId: string,
  status: string,
  outcome: string | null,
): void {
  db.getStateDb()
    .prepare(`UPDATE runs SET status = $status, outcome = $outcome WHERE run_id = $run_id`)
    .run({ $status: status, $outcome: outcome, $run_id: runId });
}

/**
 * Record how the engine left a run, and — when it parked — tell the operator
 * when and how to continue (issue #7).
 *
 * A run whose every unfinished card is waiting on a provider reset stays
 * `'halted'` (so the resume sweep still selects it) but is recorded with
 * outcome `'parked'`: nothing failed, and the andon that stopped it only
 * bounded how long the process would wait. Recording it as a plain halt
 * printed a "did not reach done" failure summary for a run that was merely
 * told to come back later. The predicate lives in one place
 * (getRunParkedRelease) so run and resume cannot drift apart.
 *
 * Returns whether the run parked, so cmdRun can keep its failure summary for
 * the halts that ARE failures.
 */
function recordRunExit(deps: CliDeps, runId: string, completed: boolean, flowPath: string): { parked: boolean } {
  if (completed) {
    updateRunStatus(deps.db, runId, 'done', 'complete');
    return { parked: false };
  }
  const parked = getRunParkedRelease(deps.db, runId, deps.now());
  updateRunStatus(deps.db, runId, 'halted', parked !== null ? 'parked' : 'halted');
  if (parked !== null) {
    deps.io.err(`run ${JSON.stringify(runId)} ${formatParkedRun(runId, flowPath, parked.releaseAt)}`);
  }
  return { parked: parked !== null };
}

/**
 * Argument vector (after `run`) for a subflow child invocation (the original multi-flow engine work).
 * Pure and exported so the production seam's CLI contract is unit-testable:
 * the child gets its derived run id, the PARENT's project root (v1 contract —
 * the parent reads the child's declared outputs directly), the calling
 * station's input artifact as seed, and the parent's remaining budget as its
 * ceiling (min()ed with the child's own declaration by runExecutor).
 */
export function buildSubflowRunArgv(invocation: SubflowInvocation): string[] {
  return [
    invocation.flowPath,
    '--run-id',
    invocation.runId,
    '--project-root',
    invocation.projectRoot,
    ...(invocation.seedPath !== undefined ? ['--input', invocation.seedPath] : []),
    ...(invocation.budgetMaxTokens !== undefined
      ? ['--budget-tokens', String(invocation.budgetMaxTokens)]
      : []),
    ...(invocation.budgetWallClockSeconds !== undefined
      ? ['--budget-wall-clock-seconds', String(invocation.budgetWallClockSeconds)]
      : []),
  ];
}

/**
 * Format the operator-facing message for a run-lease conflict (the original run-lock and busy-retry work):
 * names the run id, the live holder's pid, and the remediation (wait, or let
 * the automatic dead-holder reclaim on the next attempt take over).
 */
function formatRunLeaseConflict(runId: string, holderPid: number): string {
  return (
    `error: another conduit process (pid ${holderPid}) is driving run ${JSON.stringify(runId)} ` +
    `on this DB; wait for it to finish, or if it crashed the lease will be reclaimed ` +
    `automatically on the next attempt.`
  );
}

/**
 * Format the operator-facing message when a bare (unscoped) `conduit resume`
 * sweep skips one run because another live process holds its lease. Unlike
 * formatRunLeaseConflict, this is NOT fatal to the sweep — the sweep continues
 * on to the remaining runs, so the message says "skipping" rather than "error".
 */
function formatRunLeaseSkip(runId: string, holderPid: number): string {
  return (
    `warning: skipping run ${JSON.stringify(runId)} — another conduit process ` +
    `(pid ${holderPid}) is driving it on this DB.`
  );
}

/**
 * Render a RunStateResult as a single human-readable line (#5/#11).
 *
 * Replaces the raw `JSON.stringify(state)` dump on the existing-run and
 * `run status` paths with a stable, script-readable summary: run id, status,
 * and either the outcome (terminal) or the held-card count (held).
 */
export function formatRunState(runId: string, state: RunStateResult): string {
  switch (state.status) {
    case 'not_found':
      return `run ${runId}: not_found`;
    case 'running':
      return `run ${runId}: running`;
    case 'held':
      return `run ${runId}: held (${state.heldCards.length} held card${state.heldCards.length === 1 ? '' : 's'})`;
    case 'parked':
      return `run ${runId}: ${formatParkedRun(runId, state.flow, state.releaseAt)}`;
    case 'terminal':
      return `run ${runId}: terminal (outcome=${state.outcome})`;
  }
}

/**
 * Return the pinned flow version from a prior `run`, or null if none was pinned.
 */
export function getPinnedFlowVersion(db: ConduitDB): number | null {
  const stateDb = db.getStateDb();
  ensureRunMetaTable(stateDb);
  const row = stateDb
    .prepare(`SELECT value FROM run_meta WHERE key = 'pinned_flow_version'`)
    .get() as { value: string } | undefined;
  if (!row) return null;
  return Number(row.value);
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/**
 * Parse and validate the --concurrency flag value from an argv array.
 * Returns the parsed integer, or undefined if the flag is absent.
 * Writes an error to io.err and returns null on invalid input.
 */
function parseConcurrencyFlag(
  argv: string[],
  io: { err: (msg: string) => void },
): number | undefined | null {
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--concurrency') {
      if (i + 1 < argv.length) {
        const raw = argv[i + 1]!;
        const parsed = Number(raw);
        if (!Number.isInteger(parsed) || parsed < 1) {
          io.err(`invalid --concurrency value ${JSON.stringify(raw)}: must be an integer ≥ 1`);
          return null;
        }
        return parsed;
      } else {
        io.err('invalid --concurrency flag: a value is required (e.g. --concurrency 4)');
        return null;
      }
    }
  }
  return undefined; // flag not present
}

function parseProjectRootFlag(
  argv: string[],
  io: { err: (msg: string) => void },
): string | undefined | null {
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--project-root') {
      const raw = argv[i + 1];
      if (raw === undefined || raw.startsWith('--')) {
        io.err('invalid --project-root flag: a directory is required (e.g. --project-root /path/to/project)');
        return null;
      }
      const projectRoot = resolve(raw);
      if (!existsSync(projectRoot)) {
        io.err(`invalid --project-root ${JSON.stringify(raw)}: directory does not exist`);
        return null;
      }
      try {
        if (!statSync(projectRoot).isDirectory()) {
          io.err(`invalid --project-root ${JSON.stringify(raw)}: path is not a directory`);
          return null;
        }
      } catch (err) {
        const detail = err instanceof Error ? err.message : String(err);
        io.err(`invalid --project-root ${JSON.stringify(raw)}: ${detail}`);
        return null;
      }
      return projectRoot;
    }
  }
  return undefined;
}

/**
 * Build an operator-facing summary of every card that did NOT reach the
 * 'done' terminal on a halted run (the original silent deterministic-failure work). Exit code 1 alone tells the
 * operator nothing actionable — they were left to go spelunking through
 * `journal inspect` card-by-card. This lists, per non-done card: its id, the
 * lane it ended in (scrap/hold, or the station it's stuck at), the station it
 * was actually working when it stopped, its attempt count, and — when a
 * card_log 'terminal' entry exists — the last recorded failure reason, so the
 * common scrap/hold triage doesn't require a separate round-trip.
 *
 * READ-ONLY: reads `cards` + card_log only, scoped to runId. Mirrors the
 * access pattern `journal inspect` already uses (getCardLogForRun).
 */
export function buildHaltedRunSummary(db: ConduitDB, runId: string): string[] {
  const rows = db
    .getStateDb()
    .prepare("SELECT id, lane, attempt FROM cards WHERE run_id = $run_id AND lane != 'done' ORDER BY id")
    .all({ $run_id: runId }) as { id: string; lane: string; attempt: number }[];

  const lines: string[] = [];
  for (const row of rows) {
    // The card's post-transition lane for scrap/hold is just 'scrap'/'hold' —
    // the station it was actually working comes from the last 'terminal'
    // card_log entry (appended in the same transaction that moves the card
    // there), which also carries the human-readable reason. A card still
    // parked at a work-station lane (stuck, non-terminal) has no such entry;
    // its lane IS the station in that case.
    let station = row.lane;
    let reason: string | undefined;
    const cardLog = db.getCardLogForRun(runId, row.id);
    for (let i = cardLog.length - 1; i >= 0; i--) {
      const entry = cardLog[i]!;
      if (entry.kind === 'terminal') {
        station = entry.station;
        reason = entry.reason;
        break;
      }
    }
    const reasonSuffix = reason !== undefined ? ` — ${reason}` : '';
    lines.push(`  ${row.id}: lane=${row.lane} station=${station} attempt=${row.attempt}${reasonSuffix}`);
  }
  return lines;
}

/**
 * Print the halted-run summary (the original silent deterministic-failure work) to stderr, scoped to the
 * `return 1` path only — a completed run must print nothing new. No-ops
 * when there are no non-done cards (defensive; callers only invoke this when
 * `completed` is already false).
 */
function printHaltedRunSummary(deps: CliDeps, runId: string): void {
  const lines = buildHaltedRunSummary(deps.db, runId);
  if (lines.length === 0) return;
  deps.io.err(`run ${JSON.stringify(runId)} halted — ${lines.length} card(s) did not reach done:`);
  for (const line of lines) deps.io.err(line);
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

/**
 * `conduit run <flow.yaml>` — seed a run (or pick up an existing one by
 * `--run-id`) and drive the engine to a terminal state.
 *
 * Exit code is the run's verdict (FR-12): 0 when the work reached `done`, 1
 * otherwise. "Otherwise" is not always a failure — a run every unfinished card
 * of which is parked behind a provider reset exits 1 too, and is recorded
 * `outcome='parked'` so it stays resumable. Only a genuinely halted run gets
 * the stuck-card summary; a parked one has no failed card to list.
 */
async function cmdRun(argv: string[], deps: CliDeps): Promise<number> {
  // ── Parse flags: run <flow.yaml> [--input <file>] [--input-inline <text>] [--concurrency <n>] [--run-id <id>] [--project-root <dir>] ──
  let flowPath: string | undefined;
  let inputFilePath: string | undefined;
  let inputInlineText: string | undefined;
  let runIdFlag: string | undefined;
  let budgetTokensFlag: number | undefined;
  let budgetWallClockFlag: number | undefined;
  for (let i = 1; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === '--input' && i + 1 < argv.length) {
      inputFilePath = argv[++i];
    } else if (arg === '--input-inline' && i + 1 < argv.length) {
      inputInlineText = argv[++i];
    } else if (arg === '--run-id' && i + 1 < argv.length) {
      runIdFlag = argv[++i];
    } else if (arg === '--budget-tokens' && i + 1 < argv.length) {
      // Caller-imposed run-budget ceiling (the original multi-flow engine work, subflow composition):
      // min()ed with the flow's own declaration inside runExecutor.
      const parsed = parseInt(argv[++i]!, 10);
      if (Number.isNaN(parsed) || parsed < 0) {
        deps.io.err(`invalid --budget-tokens value: ${argv[i]} (expected an integer >= 0)`);
        return 1;
      }
      budgetTokensFlag = parsed;
    } else if (arg === '--budget-wall-clock-seconds' && i + 1 < argv.length) {
      const parsed = parseInt(argv[++i]!, 10);
      if (Number.isNaN(parsed) || parsed < 0) {
        deps.io.err(`invalid --budget-wall-clock-seconds value: ${argv[i]} (expected an integer >= 0)`);
        return 1;
      }
      budgetWallClockFlag = parsed;
    } else if (arg === '--concurrency') {
      i++; // consumed by parseConcurrencyFlag below
    } else if (arg === '--project-root') {
      i++; // consumed by parseProjectRootFlag below
    } else if (!arg.startsWith('--')) {
      flowPath = arg;
    }
  }

  if (!flowPath) {
    deps.io.err('usage: conduit run <flow.yaml> [--input <file>] [--input-inline <text>] [--project-root <dir>]');
    return 1;
  }

  const concurrencyFromFlag = parseConcurrencyFlag(argv.slice(1), deps.io);
  if (concurrencyFromFlag === null) return 1;

  const projectRootOverride = parseProjectRootFlag(argv.slice(1), deps.io);
  if (projectRootOverride === null) return 1;

  // ── Doctor pre-flight gate (FR-2): abort before dispatch on any probe failure ──
  const preflightOk = await runPreflightGate(deps);
  if (!preflightOk) return 1;

  // ── Fail-closed: validate flow before any dispatch (FR-1) ─────────────────
  const resolvedFlowPath = resolve(flowPath);
  const result = loadFlow(resolvedFlowPath, { harnessRegistry: deps.harnessRegistry });
  if (!result.ok) {
    for (const err of result.errors) {
      deps.io.err(`validation error [${err.code}]: ${err.message}`);
    }
    return 1;
  }

  const flow = result.flow;

  // ── Fail-closed: probe declared harness binaries at startup, never at first
  // dispatch (FR-10). Shares the one probeHarnessBinaries source of truth with
  // doctor so "present vs missing/non-executable" cannot drift between them.
  if (deps.harnessRegistry !== undefined) {
    const harnessErrors = await probeHarnessBinaries(flow, deps.harnessRegistry);
    if (harnessErrors.length > 0) {
      for (const err of harnessErrors) {
        deps.io.err(`validation error [${err.code}]: ${err.message}`);
      }
      return 1;
    }
  }

  const flowDir = dirname(resolvedFlowPath);
  const resolvedProjectRoot =
    projectRootOverride ?? (flow.project_root ? resolve(flowDir, flow.project_root) : flowDir);

  // ── Boot-time egress secret validation (fail-loud before engine starts) ────
  // If the flow declares a Slack egress channel, SLACK_BOT_TOKEN must be set.
  // An empty/absent token means every HITL or delivery post will fail with
  // invalid_auth at runtime — fail closed here so the operator is told exactly
  // what to fix before any work is dispatched.
  const hasSlackEgress = (flow.channels?.egress ?? []).some((ch) => ch.type === 'slack');
  if (hasSlackEgress && !process.env.SLACK_BOT_TOKEN) {
    deps.io.err(
      'boot error: this flow declares a Slack egress channel but SLACK_BOT_TOKEN ' +
        'is not set — HITL short-list posts and delivery notifications require a ' +
        'valid bot token. Set SLACK_BOT_TOKEN before running this flow.',
    );
    return 1;
  }

  // Pin the flow version so resume can detect schema drift.
  setPinnedFlowVersion(deps.db, flow.version);

  // Ensure checkpoint schema exists before the engine loop uses it.
  ensureCheckpointSchema(deps.db.getStateDb());

  // ── Run-id validation ──────────────────────────────────────────────────────
  // Validate --run-id if supplied; fall back to DEFAULT_RUN_ID otherwise.
  let effectiveRunId: string = DEFAULT_RUN_ID;
  if (runIdFlag !== undefined) {
    try {
      effectiveRunId = validateRunId(runIdFlag);
    } catch {
      deps.io.err(`error: invalid run-id ${JSON.stringify(runIdFlag)} — run-id must match [A-Za-z0-9_-]{1..128}`);
      return 1;
    }
  }

  // ── Per-run workspace binding (the original ingress-attribution work step 2) ───────────────────────────
  // defaults.workspace: per_run → this run's EFFECTIVE project root is a
  // run-scoped directory under the resolved root. Every per-run path consumer
  // (seed staging, station inputs/outputs, owned_paths resolution, the
  // integrity gate, harness cwd confinement, the worker pool) anchors at
  // `projectRoot` below, so N concurrent runs of this flow are
  // filesystem-disjoint by construction. registerRun records the workspace
  // path, so resume re-anchors here automatically (WI-593 recorded-root).
  //
  // An explicit --project-root override is CALLER-IMPOSED and wins verbatim —
  // no workspace nesting under it. This is what lets a subflow parent (issue
  // multi-flow engine work) call a child flow that declares per_run for its own standalone runs:
  // the parent passes --project-root so the child's outputs land exactly
  // where the parent's station contract reads them, not inside a nested
  // .conduit/runs/ the parent can't see. Ingress-spawned runs never pass
  // --project-root, so their workspace isolation is unaffected.
  let projectRoot = resolvedProjectRoot;
  if (flow.defaults?.workspace === 'per_run' && projectRootOverride === undefined) {
    projectRoot = join(resolvedProjectRoot, '.conduit', 'runs', effectiveRunId);
    try {
      mkdirSync(projectRoot, { recursive: true });
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      deps.io.err(`error: cannot create per-run workspace '${projectRoot}': ${detail}`);
      return 1;
    }
  }

  // ── Find the entry station (no predecessor in happyPathNext) ──────────────
  // The entry station is the station whose id is NOT a value (successor) in the
  // happyPathNext map — it is the first station in the declared topology.
  // Hoisted above registerRun: the pre-staged-overwrite guard below needs the
  // entry station resolved and the seed bytes read BEFORE the run is
  // registered (see next comment).
  const allSuccessors = new Set(
    Object.values(flow.happyPathNext ?? {}).filter((v): v is string => v !== null),
  );
  const entryStationId = Object.keys(flow.stations).find((id) => !allSuccessors.has(id));
  const entryStation = entryStationId ? flow.stations[entryStationId] : undefined;

  // ── Pre-registration seeding guard (FR-9, safe input-seeding work) ─────────────────────────────
  // This validation MUST run before registerRun below. registerRun commits a
  // status='running' row as soon as it creates a fresh run; if the overwrite
  // refusal fired only after that row existed, the run would be permanently
  // stuck: fixing the file and re-running the identical command recomputes an
  // identical fingerprint, so registerRun would report 'existing' (print
  // state, exit 0) without ever seeding a card or invoking the engine — a
  // silent no-op the operator would read as success. Validating first means a
  // refusal leaves no run-registry row at all, so a corrected retry of the
  // same command lands on registerRun's 'created' path and actually runs.
  let seedBuffer: Buffer | undefined;
  let seedTargetPath: string | undefined;
  let seedTargetPreStaged = false;
  const artifactName = entryStation?.inputs?.[0];
  if (inputFilePath !== undefined || inputInlineText !== undefined) {
    // Compare raw bytes, never UTF-8-decoded strings: two different invalid-
    // UTF-8 byte sequences can both decode to U+FFFD replacement-character
    // runs and compare equal as strings, which would let a differing
    // pre-staged binary file silently pass the guard.
    seedBuffer = inputFilePath !== undefined ? readFileSync(inputFilePath) : Buffer.from(inputInlineText!, 'utf-8');

    if (artifactName) {
      seedTargetPath = join(projectRoot, artifactName);
      // Fail-closed (safe input-seeding work): a pre-staged, non-empty file at the declared entry
      // input path may be real operator-provided data (e.g. an already-staged
      // image) rather than a leftover seed from a prior run. Silently
      // overwriting it destroys that data and the resulting corruption
      // surfaces far downstream (as a confusing worker/parse failure or a
      // watchdog stall) with no trace back to the seeding step. Only proceed
      // when there is nothing to lose: no file, an empty file, or a file whose
      // bytes are already identical to what we're about to write (an
      // idempotent re-run).
      // Any filesystem error while inspecting the target — a directory, an
      // unreadable file (permissions), a broken symlink, or the path vanishing
      // in the race window between existsSync and stat/read — must fail closed
      // as a clean refusal, never surface as an unhandled crash: "escalate
      // ambiguity, never guess".
      let existingBuffer: Buffer | null = null;
      try {
        if (existsSync(seedTargetPath)) {
          const stat = statSync(seedTargetPath);
          if (!stat.isFile()) {
            deps.io.err(
              `error: refusing to seed entry input at ${seedTargetPath} — this path exists ` +
                `but is not a regular file (e.g. a directory). Remove it or point the entry ` +
                `input at a readable regular file, then re-run.`,
            );
            return 1;
          }
          if (stat.size > 0) {
            existingBuffer = readFileSync(seedTargetPath);
          }
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        deps.io.err(
          `error: refusing to seed entry input at ${seedTargetPath} — could not inspect the ` +
            `existing file (${message}). Make the entry input path a readable regular file, ` +
            `or remove it, then re-run.`,
        );
        return 1;
      }
      if (existingBuffer !== null && !existingBuffer.equals(seedBuffer)) {
        deps.io.err(
          `error: refusing to overwrite pre-staged entry input at ${seedTargetPath} — ` +
            `this file already exists, is non-empty, and its content differs from the ` +
            `--input/--input-inline data being seeded. Seeding would destroy the ` +
            `pre-staged file. Remove or move the existing file if it is stale, or stage ` +
            `the intended content there yourself and re-run without --input/--input-inline.`,
        );
        return 1;
      }
      seedTargetPreStaged = existingBuffer !== null;
      // else: existing content is already byte-identical — proceed without rewriting.
    }
  }

  // Compute fingerprint from the input source for idempotent re-submit detection.
  const inputPayload: Record<string, unknown> =
    inputInlineText !== undefined ? { inline: inputInlineText } :
    inputFilePath !== undefined ? { file: inputFilePath } :
    {};
  const fingerprint = computeFingerprint(resolvedFlowPath, inputPayload, projectRoot);

  // Register the run. On existing → report state and exit. On conflict → error and exit.
  const registration = registerRun(deps.db, effectiveRunId, resolvedFlowPath, fingerprint, projectRoot);
  if (registration.kind === 'existing') {
    // This path never drives the engine (read-only status report), so it must
    // NOT acquire the run lease — but it's still worth refusing loudly if a
    // different live process currently holds it, rather than printing
    // possibly-stale run state out from under an in-progress run (the original run-lock and busy-retry work).
    const holder = peekRunLeaseHolder(deps.db, effectiveRunId);
    if (holder !== null && holder.holderPid !== process.pid && defaultIsPidAlive(holder.holderPid)) {
      deps.io.err(formatRunLeaseConflict(effectiveRunId, holder.holderPid));
      return 1;
    }
    const state = getRunState(deps.db, effectiveRunId, deps.now());
    deps.io.out(formatRunState(effectiveRunId, state));
    return 0;
  }
  if (registration.kind === 'conflict') {
    deps.io.err(
      `error: run-id conflict — run ${JSON.stringify(effectiveRunId)} already exists with a different flow or input. ` +
        `Use a different --run-id to start a new run.`,
    );
    return 1;
  }
  // kind === 'created' — proceed to seed entry card and run engine.

  // ── Card seeding (FR-9) ───────────────────────────────────────────────────
  let seededCardId: string | null = null;

  if (seedBuffer !== undefined) {
    // Write the entry artifact to the project root, unless the pre-registration
    // guard above already found a byte-identical pre-staged file there (an
    // idempotent re-run) — validation (existence/content) happened above,
    // before registerRun, so there is nothing left to check here.
    // The parent directory is created first: a per-run workspace (the original ingress-attribution work
    // step 2) starts empty, so a nested entry input (`work/in.json`) has no
    // pre-existing directory to land in. No-op for shared roots that already
    // have the directory.
    if (artifactName && seedTargetPath && !seedTargetPreStaged) {
      mkdirSync(dirname(seedTargetPath), { recursive: true });
      writeFileSync(seedTargetPath, seedBuffer);
    }

    // Seed the entry card at the entry station so the engine can dispatch it.
    // owned_paths must cover ALL stations the root card visits (not just the entry
    // station): follow the next/resume_at chain and collect every input + output
    // along the parent's path.  Child-lane stations (child_entry) are skipped —
    // those get their own owned_paths from the fan-out expansion.
    const rootOwnedPaths = new Set<string>();
    const childEntryIds = new Set(
      Object.values(flow.stations)
        .map((s) => s.child_entry)
        .filter((id): id is string => id !== undefined),
    );
    const terminalSet = new Set(flow.terminal_lanes ?? []);
    // Visited-set guard: a malformed flow with a cycle in resume_at/happyPathNext
    // would otherwise spin forever here at seed time (this runs at the config
    // trust boundary). Stop the first time we revisit a station.
    const visited = new Set<string>();
    let cursor: string | undefined = entryStationId;
    while (cursor !== undefined && !terminalSet.has(cursor) && !visited.has(cursor)) {
      visited.add(cursor);
      const st = flow.stations[cursor];
      if (!st || childEntryIds.has(cursor)) break;
      for (const p of st.inputs ?? []) rootOwnedPaths.add(p);
      for (const p of st.outputs ?? []) rootOwnedPaths.add(p);
      // If this station fans out, the parent card resumes at resume_at after children
      // complete — jump there rather than following child_entry. happyPathNext maps
      // to `string | null`; coalesce null → undefined to exit the walk cleanly.
      cursor = st.resume_at ?? flow.happyPathNext?.[cursor] ?? undefined;
    }
    seededCardId = effectiveRunId === DEFAULT_RUN_ID ? 'conduit-run-entry' : `entry-${effectiveRunId}`;
    deps.db.insertCard({
      run_id: effectiveRunId,
      id: seededCardId,
      parent_id: null,
      lane: entryStationId ?? 'intake',
      status: 'ready',
      attempt: 0,
      wave: 0,
      owned_paths: rootOwnedPaths.size > 0 ? [...rootOwnedPaths] : [...(entryStation?.inputs ?? []), ...(entryStation?.outputs ?? [])],
      rework_count: 0,
    });
  } else if (flow.happyPathNext !== undefined && entryStationId !== undefined) {
    // FR-9 fail-closed: applies only to flows with declared entry stations (WI-351+
    // flows that use `next` declarations). If no --input and no runnable card exists,
    // there is nothing to seed — fail loudly rather than silently no-op.
    const stateDb = deps.db.getStateDb();
    // Lane names come from flow.yaml (`terminal_lanes`) with no charset
    // restriction, so they MUST be bound as parameters — never interpolated —
    // to keep an attacker-authored or typo'd flow from injecting SQL.
    const terminalLanes = [...new Set(flow.terminal_lanes ?? ['done', 'scrap', 'hold'])];
    const lanePlaceholders = terminalLanes.map(() => '?').join(', ');
    const { n } = stateDb
      .prepare(
        `SELECT COUNT(*) AS n FROM cards WHERE lane NOT IN (${lanePlaceholders}) AND status NOT IN ('complete', 'scrapped', 'held')`,
      )
      .get(...terminalLanes) as { n: number };

    if (n === 0) {
      deps.io.err(
        'no runnable card in DB and no --input/--input-inline provided — ' +
          'nothing to seed for this run (use --input <file> to provide the entry data)',
      );
      return 1;
    }
  }

  // ── Resolve effective concurrency cap: flag > flow default > 1 ───────────
  const concurrency = concurrencyFromFlag ?? flow.defaults?.concurrency ?? 1;

  // ── Build the worker pool for real parallelism (concurrency > 1) ───────────
  // Only built when a pool factory is wired (production); the synchronous path
  // (concurrency === 1) and unit-test deps without makeWorkerPool run unchanged.
  const runArgs: RunEngineArgs = {
    db: deps.db,
    flow,
    projectRoot,
    now: deps.now,
    adapter: deps.adapter,
    io: deps.io,
    concurrency,
    runId: effectiveRunId,
    harnessRegistry: deps.bindHarnessRegistry ? deps.bindHarnessRegistry(projectRoot) : deps.harnessRegistry,
    // The original multi-flow engine work: subflow child runner + any caller-imposed budget ceilings
    // (min()ed with the flow's own declarations inside runExecutor).
    ...(deps.runSubflow !== undefined && { runSubflow: deps.runSubflow }),
    ...(budgetTokensFlag !== undefined && { budgetMaxTokens: budgetTokensFlag }),
    ...(budgetWallClockFlag !== undefined && { budgetWallClockSeconds: budgetWallClockFlag }),
  };
  let pool: WorkerPool | undefined;
  if (concurrency > 1 && deps.makeWorkerPool) {
    pool = deps.makeWorkerPool({ flowPath: resolve(flowPath), projectRoot });
    runArgs.spawn = pool.spawn;
    runArgs.onMessage = pool.onMessage;
  }

  // ── Acquire the per-run advisory lease (the original run-lock and busy-retry work) ──────────────────────────
  // After registration succeeds, before the engine dispatches any work: fail
  // fast (no wait/poll) if another live process is already driving this run.
  const leaseResult = acquireRunLease(deps.db, effectiveRunId, process.pid, deps.now());
  if (!leaseResult.acquired) {
    deps.io.err(formatRunLeaseConflict(effectiveRunId, leaseResult.holderPid));
    return 1;
  }

  // ── Reclaim orphaned in-flight workers for this run before dispatch ─────────
  // Symmetric with cmdResume (see the reclaim below acquireRunLease there).
  // A plain `conduit run` against a REUSED state DB — the same run_id whose
  // prior process crashed mid-work, or a persistent DB that market-flow drives
  // repeatedly — can inherit `active_workers` rows left behind by that dead
  // process, with `claimed`/`working` cards still holding their slots. Those
  // slots count against the station WIP cap (attemptClaim measures WIP from
  // active_workers, run-scoped), so without a reclaim the very first tick finds
  // the cap already full and the run silently stalls at that station with no
  // diagnostic. Holding the run lease we just acquired guarantees no OTHER live
  // process owns this run, so every in-flight row for effectiveRunId is orphaned
  // by definition and safe to reclaim unconditionally (no isPidAlive predicate),
  // exactly as resume does. Scoped to effectiveRunId so a shared DB's other live
  // runs are untouched. A genuinely fresh run has no such rows → no-op.
  const { reclaimed } = reclaimOrphanedWorkers(deps.db, deps.now(), undefined, effectiveRunId);
  if (reclaimed.length > 0) {
    deps.io.err(
      `run: reclaimed ${reclaimed.length} orphaned in-flight worker(s) for run ` +
        `${JSON.stringify(effectiveRunId)} left by a prior crashed process — the ` +
        `affected card(s) were re-hydrated so this run does not stall at the WIP cap.`,
    );
  }

  // ── Run the engine ─────────────────────────────────────────────────────────
  try {
    await deps.runEngine(runArgs);
  } finally {
    pool?.dispose();
    releaseRunLease(deps.db, effectiveRunId, process.pid);
  }

  // ── Exit code (FR-12): 0 = completed (card at done), 1 = halted ──────────
  // #2: also persist a TERMINAL status + outcome on the runs row so (a) a bare
  // `conduit resume` (status NOT IN terminal) no longer re-resumes a finished
  // run forever and (b) getRunState reports a real outcome (not 'unknown').
  if (seededCardId !== null) {
    // We seeded exactly one card — check whether it reached the 'done' terminal.
    const card = deps.db.getCard(effectiveRunId, seededCardId);
    const completed = card?.lane === 'done';
    const { parked } = recordRunExit(deps, effectiveRunId, completed, resolvedFlowPath);
    // The original silent deterministic-failure work: exit 1 alone is a silent failure — print which card(s) are
    // stuck in scrap/hold (or a non-terminal lane) before returning. A parked
    // run has no failed card to list — its notice was printed above.
    if (!completed && !parked) printHaltedRunSummary(deps, effectiveRunId);
    return completed ? 0 : 1;
  }

  // No seeded card (existing runnable card path) — exit 0 if all cards at 'done'.
  // Scope the completion check to THIS run so a sibling run's in-flight cards
  // don't mark this run halted.
  const stateDb2 = deps.db.getStateDb();
  const { n: notAtDone } = stateDb2
    .prepare("SELECT COUNT(*) AS n FROM cards WHERE run_id = $run_id AND lane != 'done'")
    .get({ $run_id: effectiveRunId }) as { n: number };
  const completed = notAtDone === 0;
  const { parked } = recordRunExit(deps, effectiveRunId, completed, resolvedFlowPath);
  // The original silent deterministic-failure work: exit 1 alone is a silent failure — print which card(s) are
  // stuck in scrap/hold (or a non-terminal lane) before returning. A parked
  // run has no failed card to list — its notice was printed above.
  if (!completed && !parked) printHaltedRunSummary(deps, effectiveRunId);
  return completed ? 0 : 1;
}

/**
 * `conduit resume <flow.yaml>` — re-drive a run that stopped before its work
 * was done, whether it was interrupted, halted, or parked on a rate limit.
 *
 * Resume runs in a FRESH process, so it first repairs what the dead one left
 * behind: orphaned in-flight workers are reclaimed to `interrupted`, and any
 * pending outbox intent is escalated to `hold` rather than blind-retried
 * (SPEC §5) — a human decides whether the effect landed. Then the engine runs.
 *
 * Unlike `cmdRun`, the exit code is NOT the run's verdict: resume returns 0
 * whenever it drove the engine at all, reserving non-zero for the reasons it
 * could not start (an unknown run, a lease conflict, a bad flow). Callers that
 * need the verdict read the `runs` row — which is what the ingress listener's
 * parked-resume supervisor does, precisely because a halted or re-parked run
 * still exits 0 here.
 */
async function cmdResume(argv: string[], deps: CliDeps): Promise<number> {
  // Parse: resume [--rebind] [--run <id>] [--concurrency <n>] [--project-root <dir>] <flow.yaml>
  let hasRebind = false;
  let flowPath: string | undefined;
  let runIdFlag: string | undefined;

  for (let i = 1; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === '--rebind') {
      hasRebind = true;
    } else if (arg === '--concurrency') {
      i++; // consumed by parseConcurrencyFlag below
    } else if ((arg === '--run' || arg === '--run-id') && i + 1 < argv.length) {
      runIdFlag = argv[++i];
    } else if (arg === '--project-root') {
      i++; // consumed by parseProjectRootFlag below
    } else if (!arg.startsWith('--')) {
      flowPath = arg;
    }
  }

  if (!flowPath) {
    deps.io.err('usage: conduit resume [--rebind] [--run <id>] [--project-root <dir>] <flow.yaml>');
    return 1;
  }

  // Validate --run id early, before any DB or engine work (fail-closed).
  let scopedRunId: string | undefined;
  if (runIdFlag !== undefined) {
    try {
      scopedRunId = validateRunId(runIdFlag);
    } catch {
      deps.io.err(
        `error: invalid run-id ${JSON.stringify(runIdFlag)} — run-id must match [A-Za-z0-9_-]{1..128}`,
      );
      return 1;
    }
  }

  const concurrencyFromFlagResume = parseConcurrencyFlag(argv.slice(1), deps.io);
  if (concurrencyFromFlagResume === null) return 1;

  const projectRootOverride = parseProjectRootFlag(argv.slice(1), deps.io);
  if (projectRootOverride === null) return 1;

  const result = loadFlow(flowPath, { harnessRegistry: deps.harnessRegistry });
  if (!result.ok) {
    for (const err of result.errors) {
      deps.io.err(`validation error [${err.code}]: ${err.message}`);
    }
    return 1;
  }

  // ── Fail-closed: probe declared harness binaries at startup, never at first
  // dispatch (FR-10). Same shared source of truth as cmdRun / doctor.
  if (deps.harnessRegistry !== undefined) {
    const harnessErrors = await probeHarnessBinaries(result.flow, deps.harnessRegistry);
    if (harnessErrors.length > 0) {
      for (const err of harnessErrors) {
        deps.io.err(`validation error [${err.code}]: ${err.message}`);
      }
      return 1;
    }
  }

  // Guard: a flow_version mismatch requires an explicit --rebind.
  const pinned = getPinnedFlowVersion(deps.db);
  if (pinned !== null && pinned !== result.flow.version && !hasRebind) {
    deps.io.err(
      `flow version mismatch: pinned=${pinned}, current=${result.flow.version}. ` +
        `Run with --rebind to re-validate binding stamps and update the pinned version.`,
    );
    return 1;
  }

  // On rebind, update the pinned version (binding stamps re-validated on next tick).
  if (hasRebind) {
    setPinnedFlowVersion(deps.db, result.flow.version);
  }

  ensureCheckpointSchema(deps.db.getStateDb());
  const resumeFlowDir = dirname(resolve(flowPath));
  const defaultResumeProjectRoot = result.flow.project_root
    ? resolve(resumeFlowDir, result.flow.project_root)
    : resumeFlowDir;
  const resumeConcurrency = concurrencyFromFlagResume ?? result.flow.defaults?.concurrency ?? 1;

  // Build the list of run ids to resume. With --run: just the one specified run.
  // Without --run: query the runs table for all non-terminal runs.
  const runIdsToResume: string[] = scopedRunId !== undefined
    ? [scopedRunId]
    : (() => {
        const rows = deps.db
          .getStateDb()
          .prepare(
            `SELECT run_id FROM runs WHERE status NOT IN ('done', 'complete', 'terminal', 'scrapped')`,
          )
          .all() as { run_id: string }[];
        return rows.map((r) => r.run_id);
      })();

  // If no runs registered (e.g. legacy single-run DB with no runs table rows),
  // fall back to the DEFAULT_RUN_ID sweep for back-compat.
  const effectiveRunIds = runIdsToResume.length > 0 ? runIdsToResume : [DEFAULT_RUN_ID];

  for (const runId of effectiveRunIds) {
    const recordedRun = deps.db.getRun(runId);
    const resumeProjectRoot = projectRootOverride ?? recordedRun?.project_root ?? defaultResumeProjectRoot;

    // WI-593 (FR-9): an explicit --project-root override that RE-ANCHORS
    // containment away from the run's recorded root must never be silent —
    // resume still proceeds with the override, but the operator is told.
    if (
      projectRootOverride !== undefined &&
      recordedRun?.project_root !== undefined &&
      recordedRun.project_root !== null &&
      resolve(projectRootOverride) !== resolve(recordedRun.project_root)
    ) {
      deps.io.err(
        `warning: resume --project-root '${projectRootOverride}' re-anchors run '${runId}' away ` +
          `from its recorded project root '${recordedRun.project_root}' — proceeding with the ` +
          `override; cwd confinement and owned_paths now anchor at the override root`,
      );
    }

    // ── Acquire the per-run advisory lease (the original run-lock and busy-retry work) ────────────────────────
    // Before ANY mutation for this run — including the orphan reclaim below,
    // which would otherwise yank in-flight workers out from under a genuinely
    // live driving process. Fail fast (no wait/poll) on conflict.
    const leaseResult = acquireRunLease(deps.db, runId, process.pid, deps.now());
    if (!leaseResult.acquired) {
      // An explicit --run request is entirely about this one run: the conflict
      // IS the failure, so fail fast as before. A bare (unscoped) sweep instead
      // treats a live-held run as a normal, expected member of a shared DB
      // (one live run + several crashed-but-resumable ones is the supported
      // shape) — warn and move on to the rest of the sweep rather than
      // abandoning every run it hasn't reached yet.
      if (scopedRunId === undefined) {
        deps.io.err(formatRunLeaseSkip(runId, leaseResult.holderPid));
        continue;
      }
      deps.io.err(formatRunLeaseConflict(runId, leaseResult.holderPid));
      return 1;
    }

    // Reclaim orphaned in-flight workers for this run → interrupted.
    // Resume runs in a FRESH process: any card left 'claimed'/'working' by the
    // crashed run is orphaned regardless of its (long) lease, so we reclaim
    // unconditionally — a lease-based reconcile would no-op on an immediate resume
    // and strand the card in 'working', never re-dispatched.
    reclaimOrphanedWorkers(deps.db, deps.now(), undefined, runId);

    // pending-outbox recovery work (SPEC §5): pending outbox intents are NEVER blind-retried — but they
    // must not be silently left stuck either. Enumerate pending intents for this
    // run and ESCALATE: surface an operator-visible signal AND move the affected
    // card to the `hold` lane so a human reconciles it (did the effect land?).
    const escalated = escalatePendingOutboxIntents(deps.db, deps.io, runId);
    if (escalated > 0) {
      deps.io.err(
        `resume: ${escalated} pending outbox intent(s) escalated to hold — ` +
          `reconcile each effect before re-running (never blind-retry; SPEC §5).`,
      );
    }

    // Drive the flow forward so interrupted / re-hydrated cards actually progress.
    const resumeArgs: RunEngineArgs = {
      db: deps.db,
      flow: result.flow,
      projectRoot: resumeProjectRoot,
      now: deps.now,
      adapter: deps.adapter,
      io: deps.io,
      concurrency: resumeConcurrency,
      runId,
      harnessRegistry: deps.bindHarnessRegistry
        ? deps.bindHarnessRegistry(resumeProjectRoot)
        : deps.harnessRegistry,
    };
    let resumePool: WorkerPool | undefined;
    if (resumeConcurrency > 1 && deps.makeWorkerPool) {
      resumePool = deps.makeWorkerPool({ flowPath: resolve(flowPath), projectRoot: resumeProjectRoot });
      resumeArgs.spawn = resumePool.spawn;
      resumeArgs.onMessage = resumePool.onMessage;
    }

    try {
      await deps.runEngine(resumeArgs);
    } finally {
      resumePool?.dispose();
      releaseRunLease(deps.db, runId, process.pid);
    }

    // #2: advance the runs row to a terminal status on resume completion so a
    // finished run is no longer re-selected by the bare-resume sweep (status NOT
    // IN terminal) and getRunState reports a real outcome. A run with any card
    // still off the 'done' lane (held / stalled / parked) stays 'halted' —
    // RESUMABLE — rather than 'done'.
    const { n: notAtDone } = deps.db
      .getStateDb()
      .prepare("SELECT COUNT(*) AS n FROM cards WHERE run_id = $run_id AND lane != 'done'")
      .get({ $run_id: runId }) as { n: number };
    const completed = notAtDone === 0;
    recordRunExit(deps, runId, completed, resolve(flowPath));
  }

  return 0;
}

// ---------------------------------------------------------------------------
// Pending-outbox escalation (pending-outbox recovery work, SPEC §5 — reconcile or hold, never blind-retry)
// ---------------------------------------------------------------------------

interface PendingOutboxRow {
  idempotency_key: string;
  payload_json: string;
}

/**
 * Enumerate every PENDING (delivered_at IS NULL) outbox intent and escalate it.
 *
 * Per SPEC §5 a pending effect on resume must NOT be blind-retried. We surface
 * each one to the operator and route the affected card to the `hold` lane so a
 * human can reconcile the external system. `reconcileOnResume` (with no
 * reconciler) is consulted to confirm the fail-closed `escalate_hold` verdict —
 * we never auto-commit or auto-fire here. Returns the number escalated.
 */
function escalatePendingOutboxIntents(db: ConduitDB, io: CliIO, runId?: string): number {
  const stateDb = db.getStateDb();
  ensureCheckpointSchema(stateDb); // outbox table must exist before we read it.

  // When runId is supplied, scope to outbox intents on the run_id column (#7).
  // The outbox table carries a `run_id` column, so filter on it directly rather
  // than json_extract-joining payload→cards on '$.card': simpler, faster, and it
  // never drops an intent whose payload happens to lack a `.card` field.
  const rows = runId !== undefined
    ? (stateDb
        .prepare(
          `SELECT idempotency_key, payload_json
           FROM outbox
           WHERE run_id = $run_id AND delivered_at IS NULL
           ORDER BY id ASC`,
        )
        .all({ $run_id: runId }) as PendingOutboxRow[])
    : (stateDb
        .prepare(
          `SELECT idempotency_key, payload_json
           FROM outbox
           WHERE delivered_at IS NULL
           ORDER BY id ASC`,
        )
        .all() as PendingOutboxRow[]);

  let count = 0;
  for (const row of rows) {
    // Fail-closed reconciliation — no reconciler → escalate_hold (never fire).
    const decision = reconcileOnResume(stateDb, row.idempotency_key);
    if (decision.action === 'skip') continue; // already landed/committed — nothing to do.

    let cardId: string | null = null;
    try {
      const payload = JSON.parse(row.payload_json) as { card?: string };
      cardId = typeof payload.card === 'string' ? payload.card : null;
    } catch {
      cardId = null;
    }

    io.err(
      `resume: pending outbox intent '${row.idempotency_key}'` +
        (cardId ? ` (card ${cardId})` : '') +
        ` → ${decision.action}`,
    );

    // Move the affected card to the `hold` lane so it is visible and stalls
    // safely rather than progressing on an unreconciled effect.
    if (cardId) {
      const runFilter = runId !== undefined ? 'AND run_id = $run_id' : '';
      stateDb
        .prepare(`UPDATE cards SET lane = 'hold', status = 'held' WHERE id = $id ${runFilter}`)
        .run(runId !== undefined ? { $id: cardId, $run_id: runId } : { $id: cardId });
    }
    count++;
  }
  return count;
}

/**
 * conduit explain <flow.yaml> [--color auto|always|never]
 *
 * Read-only: loads the flow and prints the renderFlow() diagram to stdout.
 * Never touches the DB, model adapter, or engine (AC5).
 *
 * Output style follows --color (default auto): on a TTY it draws the compact
 * boxed topology overview, station table, and backflow table with color;
 * piped/redirected (or --color never / NO_COLOR) falls back to clean,
 * script-friendly ASCII.
 */
async function cmdExplain(argv: string[], deps: CliDeps): Promise<number> {
  // Value-consuming parser: --color takes an argument, so the first BAREWORD
  // (never a flag or a flag's value) is the flow path.
  let flowPath: string | undefined;
  let colorMode: 'auto' | 'always' | 'never' = 'auto';
  for (let i = 1; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === '--color') {
      const val = argv[i + 1];
      if (val === 'auto' || val === 'always' || val === 'never') {
        colorMode = val;
        i++;
      } else {
        colorMode = 'always'; // bare --color
      }
      continue;
    }
    if (arg.startsWith('--color=')) {
      const val = arg.slice('--color='.length);
      if (val === 'auto' || val === 'always' || val === 'never') colorMode = val;
      continue;
    }
    if (!arg.startsWith('--') && flowPath === undefined) flowPath = arg;
  }

  if (!flowPath) {
    deps.io.err('usage: conduit explain <flow.yaml> [--color auto|always|never]');
    return 1;
  }

  // Fail-closed: validate flow before rendering (FR-1 parity with cmdRun).
  const result = loadFlow(resolve(flowPath), { harnessRegistry: deps.harnessRegistry });
  if (!result.ok) {
    for (const err of result.errors) {
      deps.io.err(`validation error [${err.code}]: ${err.message}`);
    }
    return 1;
  }

  // Decide style: rich+color on a TTY by default; clean ASCII when piped or
  // explicitly disabled. NO_COLOR (https://no-color.org) suppresses color.
  const noColorEnv = (process.env.NO_COLOR ?? '') !== '';
  const isTty = Boolean((process.stdout as { isTTY?: boolean }).isTTY);
  const rich = colorMode === 'always' ? true : colorMode === 'never' ? false : isTty;
  const color = rich && !noColorEnv && colorMode !== 'never';

  // Emit the rendered diagram line-by-line via io.out. Thread the harness
  // registry so the usage-blind indicator (NFR-Op-2) can resolve each harness
  // station's adapter capability — the frozen FlowConfig alone cannot carry it.
  for (const line of renderFlow(result.flow, { rich, color, harnessRegistry: deps.harnessRegistry }).split('\n')) {
    deps.io.out(line);
  }
  return 0;
}

/**
 * Emit the two adapter-misconfiguration warnings for one registered harness
 * definition, independent of any flow scope. Shared by both doctor modes
 * (no-flow listing AND flow-scoped probing) so an operator diagnosing a
 * specific flow still learns of a broken allowlist/command — finding #4.
 */
function emitHarnessAdapterWarnings(def: HarnessAdapterDefinition, io: CliIO): void {
  // AC2/FR-7/FR-11: HOME and PATH are the universal env floor — a missing
  // credential var (e.g. ANTHROPIC_API_KEY) is NEVER flagged here; that's
  // the operator's deployment choice, not a doctor concern.
  const missingFloor = ['HOME', 'PATH'].filter((v) => !def.envAllowlist.includes(v));
  if (missingFloor.length > 0) {
    io.out(
      `  warning: harness '${def.name}' env allowlist omits ${missingFloor.join(' and ')} — ` +
        `recommend including HOME and PATH (the universal floor; credential vars are a separate operator choice)`,
    );
  }

  // AC3/FR-7: a RELATIVE _COMMAND override resolves against whatever cwd
  // the process happens to have at probe time vs. dispatch time — those
  // can differ, so it silently doctor-greens then fails at first dispatch.
  // A bare PATH-resolved name (no '/') has no such cwd dependency.
  if (def.command !== undefined && def.command.includes('/') && !isAbsolute(def.command)) {
    io.out(
      `  warning: harness '${def.name}' _COMMAND override '${def.command}' is a relative path — ` +
        `it resolves against different working directories at probe time and at dispatch time; ` +
        `use an absolute path instead`,
    );
  }
}

async function cmdDoctor(argv: string[], deps: CliDeps): Promise<number> {
  // Parse optional positional flow.yaml (argv[1] if present and not a flag).
  const flowPath = argv.length > 1 && argv[1] !== undefined && !argv[1].startsWith('--')
    ? argv[1]
    : undefined;

  // Run base probes first — they must always be reported, even when a flow arg
  // is invalid. An early return before these would suppress diagnostics the
  // operator needs to debug the container environment (AC10).
  let anyFail = false;
  for (const probe of deps.prereqs) {
    const result = await probe.check();
    const status = result.ok ? 'ok' : 'FAIL';
    const detail = result.detail ? ` — ${result.detail}` : '';
    deps.io.out(`  ${probe.name}: ${status}${detail}`);
    if (!result.ok) anyFail = true;
  }

  // Validate and probe the flow when a flow arg is given.
  // Flow validation is unconditional once a path is supplied (invalid flow →
  // non-zero even if no flowProbes are configured). flowProbes are applied only
  // when the flow loads successfully AND the caller registered them.
  if (flowPath !== undefined) {
    const flowResult = loadFlow(resolve(flowPath), { harnessRegistry: deps.harnessRegistry });
    if (!flowResult.ok) {
      for (const err of flowResult.errors) {
        deps.io.err(`validation error [${err.code}]: ${err.message}`);
      }
      // Flow load failed → non-zero even if base probes all passed.
      anyFail = true;
    } else {
      if (deps.flowProbes !== undefined) {
        for (const probe of deps.flowProbes(flowResult.flow)) {
          const result = await probe.check();
          const status = result.ok ? 'ok' : 'FAIL';
          const detail = result.detail ? ` — ${result.detail}` : '';
          deps.io.out(`  ${probe.name}: ${status}${detail}`);
          if (!result.ok) anyFail = true;
        }
      }

      // WI-573 (FR-10): probe each declared harness station's configured binary
      // for presence/invocability — caught at doctor/load time, never at first
      // dispatch. The pass/fail verdict comes SOLELY from probeHarnessBinaries —
      // the same startup gate cmdRun/cmdResume use — so "present vs missing/
      // non-executable" has one source of truth and cannot drift. Doctor only
      // layers per-station present + usage-blind display on top; it does not
      // re-decide the verdict from its own probe.
      if (deps.harnessRegistry !== undefined) {
        const registry = deps.harnessRegistry;
        const harnessErrors = await probeHarnessBinaries(flowResult.flow, registry);
        if (harnessErrors.length > 0) anyFail = true;
        for (const [stationId, station] of Object.entries(flowResult.flow.stations)) {
          if (station.kind !== 'harness' || station.harness === undefined) continue;
          const resolved = registry.resolve(station.harness);
          if (!resolved.ok) continue; // load-time UNKNOWN_HARNESS_ADAPTER already covers this
          const probe = await resolved.adapter.probeBinary();
          const status = probe.present ? 'ok' : 'FAIL';
          const pathDetail = probe.detail !== undefined ? ` at '${probe.detail}'` : '';
          deps.io.out(
            `  harness '${resolved.adapter.name}' (station '${stationId}')${pathDetail}: ${status}`,
          );
          if (resolved.adapter.reportsUsage === false) {
            deps.io.out(
              `  harness '${resolved.adapter.name}' is usage-blind (cannot report token/cost usage)`,
            );
          }
        }
      }

      // finding #4: the adapter allowlist/_COMMAND warnings are NOT flow-scoped
      // — a broken allowlist is broken regardless of which flow is probed. Run
      // them here too (reading the config-time definitions, which carry
      // envAllowlist/command) so a `doctor <flow.yaml>` operator learns of a
      // misconfiguration instead of only seeing a green per-station probe.
      if (deps.harnessDefinitions !== undefined) {
        const definitions = deps.harnessDefinitions;
        for (const name of definitions.list()) {
          const resolved = definitions.resolve(name);
          if (!resolved.ok) continue;
          emitHarnessAdapterWarnings(resolved.adapter, deps.io);
        }
      }
    }
  } else if (deps.harnessDefinitions !== undefined) {
    // WI-592 (FR-7, FR-11): with NO flow arg, list every REGISTERED harness
    // adapter (not scoped to any one flow's stations) so a broken deployment
    // is diagnosed before the first dispatch. Reads envAllowlist/command from
    // the config-time definition registry — `deps.harnessRegistry` (the
    // introspection binding) deliberately strips those fields.
    const definitions = deps.harnessDefinitions;
    for (const name of definitions.list()) {
      const resolved = definitions.resolve(name);
      if (!resolved.ok) continue;
      const def = resolved.adapter;
      const probe = await def.probeBinary();
      const status = probe.present ? 'ok' : 'FAIL';
      const pathDetail = probe.detail !== undefined ? ` at '${probe.detail}'` : '';
      // Per-list expressibility (the original per-list tool-expression work): without this signal an operator
      // reading `canRestrictTools=false` would conclude the adapter can never
      // host a critic — false for lattice adapters like codex-exec.
      const perList = def.canExpressTools !== undefined ? ' perListTools=yes' : '';
      deps.io.out(
        `  harness '${def.name}' canRestrictTools=${def.canRestrictTools}${perList} ` +
          `reportsUsage=${def.reportsUsage}${pathDetail}: ${status}`,
      );
      if (!probe.present) anyFail = true;

      emitHarnessAdapterWarnings(def, deps.io);
    }
  }

  return anyFail ? 1 : 0;
}

/** Default number of trailing spans printed by `journal tail`. */
const JOURNAL_TAIL_DEFAULT = 20;

async function cmdJournal(argv: string[], deps: CliDeps): Promise<number> {
  // argv = ['journal', 'inspect'|'tail', '<cardId>', ['--run', '<id>']]
  const sub = argv[1];

  // journal subcommand-validation work: validate the subcommand explicitly — reject anything but inspect|tail.
  if (sub !== 'inspect' && sub !== 'tail') {
    deps.io.err(
      `unknown journal subcommand: ${sub ?? '(none)'}. ` +
        `Available: inspect, tail. usage: conduit journal <inspect|tail> <cardId> [--run <id>]`,
    );
    return 1;
  }

  // #4: parse an optional --run/--run-id flag and the positional cardId. The
  // journal getters union across all runs; scoping to a run keeps cross-run
  // cards isolated. Default to DEFAULT_RUN_ID when absent (back-compat).
  let cardId: string | undefined;
  let runIdFlag: string | undefined;
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i]!;
    if ((arg === '--run' || arg === '--run-id') && i + 1 < argv.length) {
      runIdFlag = argv[++i];
    } else if (!arg.startsWith('--') && cardId === undefined) {
      cardId = arg;
    }
  }

  if (!cardId) {
    deps.io.err('usage: conduit journal <inspect|tail> <cardId> [--run <id>]');
    return 1;
  }

  // Validate --run id when supplied (fail-closed) before any DB read.
  let runId: string = DEFAULT_RUN_ID;
  if (runIdFlag !== undefined) {
    try {
      runId = validateRunId(runIdFlag);
    } catch {
      deps.io.err(
        `error: invalid run-id ${JSON.stringify(runIdFlag)} — run-id must match [A-Za-z0-9_-]{1..128}`,
      );
      return 1;
    }
  }

  // READ-ONLY: never write the state DB. Run-scoped getters keep cross-run
  // journal spans / card-logs isolated (#4).
  const spans = deps.db.getJournalSpansForRun(runId, cardId);

  // journal subcommand-validation work: `inspect` prints the full journal; `tail` prints only the last N spans.
  const printable =
    sub === 'tail' && spans.length > JOURNAL_TAIL_DEFAULT
      ? spans.slice(spans.length - JOURNAL_TAIL_DEFAULT)
      : spans;

  for (const span of printable) {
    deps.io.out(`[${span.cardId}] ${span.station}@${span.attempt} ${span.name}`);
  }

  // FR-9: `inspect` also renders the card transition log so a human can answer
  // "why is this card here?" when triaging scrap/hold lanes.
  //
  // READ-ONLY: getCardLog never writes.  tail keeps its existing span-only
  // behaviour; card_log is inspect-only.
  if (sub === 'inspect') {
    const cardLog = deps.db.getCardLogForRun(runId, cardId);
    for (const entry of cardLog) {
      const p = `[${entry.cardId}]`;
      switch (entry.kind) {
        case 'entered_lane':
          deps.io.out(
            `${p} entered_lane: ${entry.sourceLane} → ${entry.destLane} (${entry.reasonClass})`,
          );
          break;
        case 'gate_verdict':
          deps.io.out(`${p} gate_verdict: ${entry.verdict}`);
          for (const finding of entry.findings) {
            deps.io.out(`${p}   ${finding}`);
          }
          break;
        case 'terminal':
          deps.io.out(`${p} terminal: ${entry.reason}`);
          break;
      }
    }
  }

  return 0;
}

/**
 * conduit listen --flows <name=path>[,...] | --manifest <engine.yaml>
 *
 * Boots the ingress listener assembly (WI-410) over an explicit per-flow
 * allowlist and keeps the process alive to serve webhook and Slack events.
 *
 * Two config surfaces (mutually exclusive):
 *   --flows     inline name=path list — single-flow / dev use. Boot is
 *               fail-closed: any flow error withholds the listener (FR-9).
 *   --manifest  engine manifest file (the original multi-flow engine work) — the N-flow engine. Flows
 *               that fail validation are QUARANTINED per-flow (reported
 *               loudly, excluded from routing) while the rest serve; boot
 *               still fails when zero flows survive.
 *
 * --validate boots strictly (no quarantine) and exits without serving —
 * the CI/deploy-time gate that keeps quarantine from hiding config rot.
 *
 * --max-concurrent-runs <n> (the original listener-backpressure work): listener-wide run backpressure — at
 * most n spawned `conduit run` processes in flight at once. Precedence: flag >
 * manifest max_concurrent_runs > CONDUIT_MAX_CONCURRENT_RUNS env. Omitted →
 * unlimited (before listener backpressure behavior).
 */
async function cmdListen(argv: string[], deps: CliDeps): Promise<number> {
  // ── Parse: listen [--flows <name=path>[...]] [--manifest <file>] [--validate]
  //           [--port <n>] [--global-alert-channel <c>] [--max-concurrent-runs <n>] ──
  let flowsValue: string | undefined;
  let manifestPath: string | undefined;
  let validateOnly = false;
  let port = 3000;
  let globalAlertChannel: string | undefined;
  let maxConcurrentRuns: number | undefined;

  for (let i = 1; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === '--flows' && i + 1 < argv.length) {
      flowsValue = argv[++i];
    } else if (arg === '--manifest' && i + 1 < argv.length) {
      manifestPath = argv[++i];
    } else if (arg === '--validate') {
      validateOnly = true;
    } else if (arg === '--port' && i + 1 < argv.length) {
      const parsed = parseInt(argv[++i]!, 10);
      if (Number.isNaN(parsed)) {
        deps.io.err(`invalid --port value: ${argv[i]} (expected an integer)`);
        return 1;
      }
      port = parsed;
    } else if (arg === '--global-alert-channel' && i + 1 < argv.length) {
      globalAlertChannel = argv[++i];
    } else if (arg === '--max-concurrent-runs' && i + 1 < argv.length) {
      // The original listener-backpressure work: run backpressure. Fail loud here — a malformed cap must
      // never silently mean "unlimited" against a serial model endpoint.
      const parsed = parseInt(argv[++i]!, 10);
      if (Number.isNaN(parsed) || parsed < 1 || String(parsed) !== argv[i]) {
        deps.io.err(`invalid --max-concurrent-runs value: ${argv[i]} (expected a positive integer)`);
        return 1;
      }
      maxConcurrentRuns = parsed;
    }
  }

  const usage =
    'usage: conduit listen (--flows <name=path>[,<name=path>...] | --manifest <engine.yaml>) ' +
    '[--validate] [--port <n>] [--max-concurrent-runs <n>]';

  if (flowsValue !== undefined && manifestPath !== undefined) {
    deps.io.err(`--flows and --manifest are mutually exclusive. ${usage}`);
    return 1;
  }
  if (!flowsValue?.trim() && manifestPath === undefined) {
    deps.io.err(usage);
    return 1;
  }

  // ── Build the allowlist: manifest file or comma-separated name=path list ──
  let allowlist: Record<string, string> = {};
  if (manifestPath !== undefined) {
    let manifestText: string;
    try {
      manifestText = readFileSync(resolve(manifestPath), 'utf-8');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      deps.io.err(`cannot read engine manifest '${manifestPath}': ${detail}`);
      return 1;
    }
    const parsed = parseEngineManifest(manifestText, dirname(resolve(manifestPath)));
    if (!parsed.ok) {
      for (const e of parsed.errors) {
        deps.io.err(`manifest error [${e.code}]: ${e.message}`);
      }
      return 1;
    }
    allowlist = parsed.manifest.flows;
    // CLI flag wins over the manifest's channel (mirrors the env fallback order).
    globalAlertChannel ??= parsed.manifest.globalAlertChannel;
    // The original listener-backpressure work: same precedence for the run backpressure cap.
    maxConcurrentRuns ??= parsed.manifest.maxConcurrentRuns;
  } else {
    for (const entry of flowsValue!.split(',')) {
      const eqIdx = entry.indexOf('=');
      if (eqIdx > 0) {
        const name = entry.slice(0, eqIdx).trim();
        const path = entry.slice(eqIdx + 1).trim();
        if (name && path) {
          allowlist[name] = path;
        }
      }
    }
  }

  // ── Doctor pre-flight gate (FR-2): abort before boot on any probe failure ────
  const listenPreflightOk = await runPreflightGate(deps);
  if (!listenPreflightOk) return 1;

  if (!deps.startListener) {
    deps.io.err('startListener is not available in this environment');
    return 1;
  }

  // ── Boot the listener ─────────────────────────────────────────────────────
  // The CLI passes the allowlist and (optional) CLI-supplied global alert
  // channel; buildProductionDeps' wrapper enriches this config with the Slack
  // signing secret, per-binding secrets, and the env fallback for the alert
  // channel before delegating to the real startListener.
  //
  // Manifest mode serves with per-flow quarantine (the original multi-flow engine work); --flows and
  // --validate boot strictly (fail-closed, FR-9).
  const quarantine = manifestPath !== undefined && !validateOnly;
  const result = await deps.startListener({
    allowlist,
    ...(globalAlertChannel !== undefined && { globalAlertChannel }),
    ...(quarantine && { quarantine }),
    ...(maxConcurrentRuns !== undefined && { maxConcurrentRuns }),
  });

  if (!result.ok) {
    for (const bootError of result.errors) {
      deps.io.err(bootError.message);
    }
    return 1;
  }

  // ── --validate: strict dry boot, report, exit without serving ────────────
  if (validateOnly) {
    deps.io.out(
      `conduit listen --validate: all ${Object.keys(allowlist).length} flow(s) valid`,
    );
    return 0;
  }

  // ── Quarantine report (the original multi-flow engine work): loud, per flow, before serving ────────
  // (?? {} keeps older test fakes of the Listener shape working.)
  const quarantinedFlows = Object.entries(result.listener.quarantined ?? {});
  if (quarantinedFlows.length > 0) {
    for (const [flowName, errors] of quarantinedFlows) {
      for (const e of errors) {
        deps.io.err(`[engine] flow '${flowName}' QUARANTINED [${e.code}]: ${e.message}`);
      }
    }
    deps.io.err(
      `[engine] ${quarantinedFlows.length} flow(s) quarantined; ` +
        `${Object.keys(allowlist).length - quarantinedFlows.length} serving. ` +
        `Run 'conduit listen --manifest <file> --validate' to fail on these errors at deploy time.`,
    );
  }

  // ── Serve HTTP and block until shutdown (#1) ──────────────────────────────
  // In production, deps.serve binds an HTTP server routing webhook/Slack POSTs
  // to the booted listener and blocks until SIGTERM/SIGINT. In unit tests the
  // serve seam is absent, so cmdListen returns immediately after a clean boot.
  if (deps.serve) {
    await deps.serve(result.listener, { port });
  }

  return 0;
}

// ---------------------------------------------------------------------------
// build
// ---------------------------------------------------------------------------

/**
 * Pure, exported Dockerfile generator for `conduit build`.
 *
 * Produces a Dockerfile that:
 *   1. Starts FROM the tagged conduit engine image.
 *   2. Copies the flow directory into the image so the flow is baked in.
 *   3. Installs exactly the packages listed in `flow.prerequisites` via a
 *      single `apt-get install` step — omitted entirely when the list is
 *      empty or absent (AC4).
 *
 * Pure: no I/O, no side effects. The result is deterministic for a given
 * (flow, opts) pair, which makes unit testing trivial.
 */
export function generateFlowDockerfile(
  flow: FlowConfig,
  opts: { flowDir: string },
): string {
  const engineImage = 'conduit-engine:latest';
  // Use || instead of ?? so an empty string after split (trailing slash) also
  // falls back to 'flow' rather than producing an empty source token.
  const flowDirName = opts.flowDir.replace(/\\/g, '/').split('/').pop() || 'flow';

  const lines: string[] = [
    `FROM ${engineImage}`,
    '',
    `# Temporarily regain root for image-build steps; the final runtime user is conduit.`,
    `USER root`,
    '',
    `# Bake the flow directory into the image so conduit run can locate it.`,
    // JSON-exec form keeps the source as a single token even when the directory
    // name contains spaces — shell form would tokenize "my flow/" as two args.
    `COPY --chown=conduit:conduit ["${flowDirName}/", "/flow/"]`,
    // Point CONDUIT_PROJECT_ROOT at the baked-in flow directory so the
    // project-root-present doctor probe finds it and reports ok.
    `ENV CONDUIT_PROJECT_ROOT=/flow`,
  ];

  const prereqs = flow.prerequisites ?? [];
  if (prereqs.length > 0) {
    lines.push('');
    lines.push('# Install flow-declared prerequisites (from flow.yaml prerequisites field).');
    lines.push(
      `RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \\`,
    );
    for (const pkg of prereqs) {
      // Every package line ends with ' \' so the next line (another package or
      // the cleanup '&& rm -rf') is a valid continuation of the RUN instruction.
      lines.push(`    ${pkg} \\`);
    }
    lines.push('    && rm -rf /var/lib/apt/lists/*');
  }

  lines.push('');
  lines.push('# Drop back to least-privilege runtime after build-time package install.');
  lines.push('USER conduit');
  lines.push('');
  return lines.join('\n');
}

async function cmdBuild(argv: string[], deps: CliDeps): Promise<number> {
  // argv[0] = 'build', argv[1] = optional flow path
  const flowArg = argv[1] !== undefined && !argv[1].startsWith('--') ? argv[1] : undefined;

  if (flowArg === undefined) {
    deps.io.err('usage: conduit build <flow.yaml>');
    return 1;
  }

  const flowResult = loadFlow(resolve(flowArg), { harnessRegistry: deps.harnessRegistry });
  if (!flowResult.ok) {
    for (const err of flowResult.errors) {
      deps.io.err(`validation error [${err.code}]: ${err.message}`);
    }
    return 1;
  }

  const dockerfile = generateFlowDockerfile(flowResult.flow, { flowDir: dirname(resolve(flowArg)) });
  deps.io.out(dockerfile);
  return 0;
}

/**
 * conduit run delete --run <id>   — delete every row owned by a run (#5).
 * conduit run status --run <id>   — print a run's state summary (#5).
 *
 * Both REQUIRE an explicit --run id (delete refuses to wipe a run without one;
 * status needs to know which run to report). The run id is validated fail-closed
 * before any DB access.
 */
async function cmdRunManage(argv: string[], deps: CliDeps): Promise<number> {
  const sub = argv[1]; // 'delete' | 'status' (gated by the dispatcher)

  // Parse the required --run/--run-id flag.
  let runIdFlag: string | undefined;
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i]!;
    if ((arg === '--run' || arg === '--run-id') && i + 1 < argv.length) {
      runIdFlag = argv[++i];
    }
  }

  if (runIdFlag === undefined) {
    deps.io.err(`usage: conduit run ${sub} --run <id>`);
    return 1;
  }

  let runId: string;
  try {
    runId = validateRunId(runIdFlag);
  } catch {
    deps.io.err(
      `error: invalid run-id ${JSON.stringify(runIdFlag)} — run-id must match [A-Za-z0-9_-]{1..128}`,
    );
    return 1;
  }

  if (sub === 'status') {
    const state = getRunState(deps.db, runId, deps.now());
    deps.io.out(formatRunState(runId, state));
    // A not_found run is a non-zero (lookup miss) outcome; everything else is 0.
    return state.status === 'not_found' ? 1 : 0;
  }

  // sub === 'delete'
  deps.db.deleteRun(runId);
  deps.io.out(`run ${runId}: deleted`);
  return 0;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

/**
 * CLI entry point.
 *
 * @param argv  - Command-line arguments (starting with the sub-command).
 * @param deps  - Injected seams for io, clock, db, adapter, engine, prereqs.
 * @returns     - Exit code (0 = success, non-zero = failure).
 */
export async function main(argv: string[], deps: CliDeps): Promise<number> {
  const cmd = argv[0];

  switch (cmd) {
    case 'run':
      // #5: `run delete` / `run status` are management subcommands; everything
      // else (`run <flow.yaml> ...`) drives a flow. The subcommand keywords are
      // reserved — a flow path is always a *.yaml file, never a bare keyword.
      if (argv[1] === 'delete' || argv[1] === 'status') {
        return cmdRunManage(argv, deps);
      }
      return cmdRun(argv, deps);

    case 'resume':
      return cmdResume(argv, deps);

    case 'doctor':
      return cmdDoctor(argv, deps);

    case 'journal':
      return cmdJournal(argv, deps);

    case 'reply':
      return cmdReply(argv, deps);

    case 'listen':
      return cmdListen(argv, deps);

    case 'build':
      return cmdBuild(argv, deps);

    case 'explain':
      return cmdExplain(argv, deps);

    default:
      deps.io.err(
        `unknown command: ${cmd ?? '(none)'}. Available: run, run delete, run status, ` +
          `resume, doctor, journal, reply, listen, build, explain`,
      );
      return 1;
  }
}

// ---------------------------------------------------------------------------
// Production wiring (#3) — the real binary entrypoint.
//
// `package.json` points `bin` at this file, but the module only EXPORTED main()
// — so `conduit run flow.yaml` used to execute the module and exit 0 doing
// NOTHING. The `import.meta.main` block below builds PRODUCTION CliDeps (on-disk
// DB from a mounted path/env var, a lazily-constructed real ModelAdapter, the
// real planTick-driven engine loop, and default prereq probes) and runs main().
//
// The injected-seam design is preserved: tests still import and call main(argv,
// deps) with stubs. Only the binary path uses buildProductionDeps().
// ---------------------------------------------------------------------------

const DEFAULT_STATE_DB = '/data/conduit.sqlite';
const DEFAULT_JOURNAL_DB = '/data/conduit.journal.sqlite';

/**
 * Construct a real ModelAdapter LAZILY. Conduit drives any provider per station;
 * the API key is read from the environment only when a call is actually made, so
 * `--help`, `doctor`, and fail-closed validation paths work WITHOUT a key (#3).
 */
function buildLazyModelAdapter(): ModelAdapter {
  return {
    async call(_req: ModelCall): Promise<ModelResponse> {
      const apiKey = process.env.CONDUIT_API_KEY ?? process.env.OPENAI_API_KEY;
      if (!apiKey) {
        throw new Error(
          'no model API key available (set CONDUIT_API_KEY / OPENAI_API_KEY). ' +
            'The adapter is constructed lazily so validation and doctor run without one.',
        );
      }
      // The concrete provider transport is wired by the worker layer (WI-296);
      // the binary deliberately fails loudly rather than silently no-op a billed
      // call. A real provider HTTP client is dropped in here when configured.
      throw new Error('production ModelAdapter transport is not configured in this build');
    },
  };
}

/** Build the PRODUCTION CliDeps for the real binary (mounted DB, lazy adapter). */
export function buildProductionDeps(): CliDeps {
  const stateDbPath = process.env.CONDUIT_STATE_DB ?? process.env.CONDUIT_DB ?? DEFAULT_STATE_DB;
  const journalDbPath = process.env.CONDUIT_JOURNAL_DB ?? DEFAULT_JOURNAL_DB;

  // (a pre-public engine review finding 4a): both DB defaults are Docker paths (/data/...), so a
  // bare-metal boot with no env vars set hits ENOENT here with no clue which
  // var to set. openConduitDB's error already names the resolved path; add
  // the controlling env var and a one-line bare-metal hint on top of it — this
  // is the only caller that knows which env var supplied which path.
  let db: ConduitDB;
  try {
    db = openConduitDB({ stateDbPath, journalDbPath });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes(`'${stateDbPath}'`)) {
      throw new Error(
        `${msg} (set CONDUIT_STATE_DB to a writable path when running bare-metal — ` +
          `the default ${DEFAULT_STATE_DB} only exists inside the Docker image)`,
      );
    }
    if (msg.includes(`'${journalDbPath}'`)) {
      throw new Error(
        `${msg} (set CONDUIT_JOURNAL_DB to a writable path when running bare-metal — ` +
          `the default ${DEFAULT_JOURNAL_DB} only exists inside the Docker image)`,
      );
    }
    throw err;
  }
  ensureCheckpointSchema(db.getStateDb());

  const io: CliIO = {
    out: (line: string) => process.stdout.write(line + '\n'),
    err: (line: string) => process.stderr.write(line + '\n'),
  };

  // buildStateDirProbe replaces the legacy SELECT-1 read check with a real write
  // test so a read-only bind mount (FR-7 footgun) is detected at doctor time.
  const prereqs: PrereqProbe[] = [
    buildStateDirProbe(stateDbPath),
    buildProjectRootProbe(process.env.CONDUIT_PROJECT_ROOT),
    {
      name: 'model_api_key',
      check: () => {
        const key = process.env.CONDUIT_API_KEY ?? process.env.OPENAI_API_KEY;
        return key
          ? { ok: true }
          : { ok: false, detail: 'CONDUIT_API_KEY / OPENAI_API_KEY not set' };
      },
    },
    {
      name: 'gateway_base_url',
      check: () => {
        const baseUrl = process.env.CONDUIT_BASE_URL;
        return baseUrl
          ? { ok: true, detail: 'CONDUIT_BASE_URL is configured' }
          : { ok: false, detail: 'CONDUIT_BASE_URL not set (required for model gateway)' };
      },
    },
  ];

  // ── #12: NaN-guarded re-drive cap ─────────────────────────────────────────
  // parseInt returns NaN for an unparseable env value; an unguarded NaN would
  // make listRedrivable(cap) compare every spawn_attempts < NaN → false, silently
  // disabling boot re-drive. Fall back to the default and warn if it was set.
  const redriveCapRaw = process.env.CONDUIT_REDRIVE_CAP;
  let redriveCap = parseInt(redriveCapRaw ?? '3', 10);
  if (Number.isNaN(redriveCap)) {
    if (redriveCapRaw !== undefined) {
      io.err(
        `warning: CONDUIT_REDRIVE_CAP='${redriveCapRaw}' is not an integer — ` +
          `falling back to the default re-drive cap of 3`,
      );
    }
    redriveCap = 3;
  }

  // ── #3: spawn `conduit run` as a child of the same Bun binary ─────────────
  // Shared by the accept-spawn seam and the boot re-drive seam so both launch
  // through one well-tested path.
  //
  // The original acknowledgement-on-accept work: launch and completion are separate events. launchConduitRun
  // returns as soon as the child is live and hands back its exit as a promise;
  // the ingress seams ack on launch and supervise the exit asynchronously, while
  // callers that genuinely need the terminal state (subflow children) await it
  // through spawnConduitRun. Bun.spawn throws synchronously when the binary
  // cannot be exec'd — inside the async seams that surfaces as a rejected
  // promise, which runSpawnPath already treats as a spawn failure.
  const launchConduitRun = (args: string[]): { exited: Promise<{ code: number }> } => {
    const proc = Bun.spawn([process.execPath, import.meta.path, 'run', ...args], {
      stdout: null,
      stderr: null,
    });
    return { exited: proc.exited.then((code) => ({ code })) };
  };

  const spawnConduitRun = async (args: string[]): Promise<{ ok: boolean; error?: string }> => {
    const { exited } = launchConduitRun(args);
    const { code } = await exited;
    return code === 0 ? { ok: true } : { ok: false, error: `conduit run exited with code ${code}` };
  };

  // The original HITL reply-and-resume work: relaunch a parked run after a HITL reply — same child-process
  // discipline as spawnConduitRun, `resume` subcommand. The run's recorded
  // project root re-anchors automatically (WI-593), so no --project-root here.
  const spawnConduitResume = async (req: { flowPath: string; runId: string }): Promise<{ ok: boolean; error?: string }> => {
    const proc = Bun.spawn(
      [process.execPath, import.meta.path, 'resume', req.flowPath, '--run', req.runId],
      { stdout: null, stderr: null },
    );
    const exitCode = await proc.exited;
    return exitCode === 0
      ? { ok: true }
      : { ok: false, error: `conduit resume exited with code ${exitCode}` };
  };

  // Harness adapter registry (WI-560/586/587/588): engine-config-defined,
  // env/DI adapter definitions — never a flow-supplied command line. Parsed
  // from CONDUIT_HARNESS_* (WI-586); an unshipped adapter name or a malformed
  // allowlist/collision FAILS BOOT here, never deferred to first dispatch
  // (FR-8) — no CONDUIT_HARNESS_* configured yields the same empty,
  // fail-closed registry as before this item (zero behavior change).
  // `harnessRegistry` is the LOAD-TIME-ONLY registry: its only consumers
  // (loadFlow, probeHarnessBinaries, explain, doctor, build) introspect
  // caps/probeBinary/name and never call invoke(). It is NEVER bound to a
  // real filesystem root at boot (FR-4: the confinement root comes from the
  // run being executed) — invoke() on it fails closed with a named error
  // instead. The run path (cmdRun/cmdResume) instead calls
  // `bindHarnessRegistry` with the run's real resolved projectRoot to get a
  // genuinely invocable registry (WI-588).
  const parsedHarnessConfig = parseHarnessConfig(process.env);
  if (!parsedHarnessConfig.ok) {
    throw new Error(parsedHarnessConfig.error);
  }
  const harnessDefinitionRegistry = buildHarnessDefinitionRegistry(parsedHarnessConfig.defs);
  const harnessRegistry = bindHarnessDefinitionsForIntrospection(harnessDefinitionRegistry);

  return {
    io,
    now: () => Math.floor(Date.now() / 1000),
    db,
    // createOpenAiAdapter reads credentials from env at each call() invocation —
    // construction without a key never throws (doctor + validation run key-free).
    adapter: createOpenAiAdapter(),
    harnessRegistry,
    harnessDefinitions: harnessDefinitionRegistry,
    bindHarnessRegistry: (projectRoot: string) => bindHarnessDefinitions(harnessDefinitionRegistry, projectRoot),
    runEngine: runExecutor,
    prereqs,
    // Flow-aware probes: run only when a flow arg is provided (conduit doctor <flow.yaml>
    // or during run/listen). Reads CONDUIT_BASE_URL at probe time, not at construction,
    // so env changes between doctor invocations are reflected correctly.
    flowProbes: (flow: FlowConfig): PrereqProbe[] => [
      buildFlowPrereqsProbe(flow.prerequisites ?? []),
      buildModelEndpointProbe(process.env.CONDUIT_BASE_URL),
    ],
    // Real worker pool: one Bun subprocess per dispatched card (see buildWorkerPool).
    makeWorkerPool: (args) => buildWorkerPool(args, io),
    // The original multi-flow engine work: subflow child runner — spawn a `conduit run` subprocess for
    // the child flow (the same supervisor pattern the ingress spawn path
    // uses), then read the child's terminal state and journaled spend back
    // off the shared DB. The child holds its own run lease; its per-run
    // tables are namespaced by its derived run id.
    runSubflow: async (invocation: SubflowInvocation) => {
      const spawnResult = await spawnConduitRun(buildSubflowRunArgv(invocation));
      const childRun = db.getRun(invocation.runId);
      const usage = db.getRunUsageTotals(invocation.runId);
      const spend = { tokens: usage.tokens, costUsd: usage.costUsd };

      if (childRun === null) {
        return {
          outcome: 'error' as const,
          reason: `child run '${invocation.runId}' was never registered` +
            (spawnResult.ok ? '' : ` (${spawnResult.error})`),
          ...spend,
        };
      }
      if (childRun.status === 'done') {
        return { outcome: 'done' as const, ...spend };
      }
      // Distinguish scrap (a card reached the scrap terminal) from a stall/hold.
      const scrapCount = db
        .getStateDb()
        .prepare(`SELECT COUNT(*) AS n FROM cards WHERE run_id = $run_id AND lane = 'scrap'`)
        .get({ $run_id: invocation.runId }) as { n: number };
      if (scrapCount.n > 0) {
        return {
          outcome: 'scrap' as const,
          reason: `child run '${invocation.runId}' scrapped (${scrapCount.n} card(s) in scrap)`,
          ...spend,
        };
      }
      return {
        outcome: 'halted' as const,
        reason: `child run '${invocation.runId}' terminated with status '${childRun.status}'`,
        ...spend,
      };
    },
    // #1: real HTTP server — binds a port, routes webhook/Slack POSTs to the
    // booted listener, and blocks until SIGTERM/SIGINT, stopping gracefully.
    serve: (listener, { port }) => serveListener(listener, port, io),
    // Wire the real WI-410 listener assembly with production I/O seams.
    startListener: (config) => {
      // #3: resolve secrets, the Slack signing secret, and the global alert
      // channel from the environment, then merge them into the boot config.
      const slackSigningSecret = process.env.CONDUIT_SLACK_SIGNING_SECRET ?? '';
      const { secrets, hasSlackEventsBinding } = resolveIngressSecrets(config.allowlist, loadFlow);

      if (hasSlackEventsBinding && !process.env.CONDUIT_SLACK_SIGNING_SECRET) {
        io.err(
          'warning: a Slack ingress binding is configured but ' +
            'CONDUIT_SLACK_SIGNING_SECRET is not set — Slack signature verification ' +
            'will reject every request (set the signing secret to enable Slack ingress)',
        );
      }

      // CLI --global-alert-channel (in config) wins; otherwise fall back to env.
      // The listener hard-fails at boot (NO_ALERT_CHANNEL) if a no-egress flow is
      // present and neither source supplied a channel.
      const globalAlertChannel =
        config.globalAlertChannel ?? process.env.CONDUIT_GLOBAL_ALERT_CHANNEL;

      // The original listener-backpressure work: run backpressure cap — CLI flag / manifest (in config) wins;
      // otherwise fall back to env. An unparseable env value warns and is
      // ignored (mirrors CONDUIT_REDRIVE_CAP) — explicit config sources instead
      // fail loud at parse/boot.
      let maxConcurrentRuns = config.maxConcurrentRuns;
      const envMaxRaw = process.env.CONDUIT_MAX_CONCURRENT_RUNS;
      if (maxConcurrentRuns === undefined && envMaxRaw !== undefined) {
        const envMax = parseInt(envMaxRaw, 10);
        if (Number.isNaN(envMax) || envMax < 1 || String(envMax) !== envMaxRaw) {
          io.err(
            `warning: CONDUIT_MAX_CONCURRENT_RUNS='${envMaxRaw}' is not a positive ` +
              `integer — ignoring it (runs stay uncapped)`,
          );
        } else {
          maxConcurrentRuns = envMax;
        }
      }

      const enrichedConfig: ListenerConfig = {
        ...config,
        slackSigningSecret,
        secrets,
        ...(globalAlertChannel !== undefined && { globalAlertChannel }),
        ...(maxConcurrentRuns !== undefined && { maxConcurrentRuns }),
      };

      return startListenerImpl(
        {
          db,
          // Spawn seam: launch `conduit run <flowPath> --input-inline <json>
          // --run-id <derived>` (the original ingress-attribution work: the derived run id keeps concurrent
          // events from colliding on the default run). Resolves on LAUNCH so the
          // webhook acks immediately (the original acknowledgement-on-accept work); the child's exit rides back on
          // `exited` for the spawn path's async supervision.
          spawn: async (invocation) => ({
            ok: true,
            ...launchConduitRun([
              invocation.flowPath,
              '--input-inline',
              invocation.inputInline,
              '--run-id',
              invocation.runId,
            ]),
          }),
          // The original HITL reply-and-resume work: HITL replies relaunch the parked run via `conduit resume`.
          resumeSpawn: spawnConduitResume,
          // Alert seam: surface spawn failures to stderr.
          alert: async (a) => {
            io.err(formatIngressAlert(a));
          },
          // WI-576: registry-aware so a malformed harness station in an
          // ingress-triggered flow fails boot (FLOW_LOAD_FAILED) rather than
          // reaching a spawned run.
          loadFlow: (path) => loadFlow(path, { harnessRegistry }),
          // Webhook HMAC-SHA256 verification (GitHub-style X-Hub-Signature-256).
          verifyWebhookAuth: (secret, req) => {
            const sigHeader = req.headers['X-Hub-Signature-256'];
            if (!sigHeader) return false;
            const expected =
              'sha256=' + createHmac('sha256', secret).update(req.rawBody).digest('hex');
            const expectedBuf = Buffer.from(expected, 'utf8');
            const sigBuf = Buffer.from(sigHeader, 'utf8');
            if (expectedBuf.length !== sigBuf.length) return false;
            return timingSafeEqual(expectedBuf, sigBuf);
          },
          // Slack request-signing verification (v0 HMAC-SHA256, constant-time).
          verifySlackAuth: (signingSecret, req) => verifySlackSignature(signingSecret, req),
          // ingress secret-resolution and event-isolation work: Respawn seam — re-drive a recoverable event by relaunching
          // its OWNING flow with its ORIGINAL payload and derived run id, all
          // read from the v9-attributed ingress_events row. Reusing the run id
          // makes re-drive idempotent: if the original run already exists with
          // an identical fingerprint, `conduit run` reports its state and exits
          // 0 (served → 'spawned'); if another live process holds the run's
          // lease it exits non-zero ('transient_failure', retried later).
          //
          // Pre-v9 rows have no attribution, so the owning flow can only be
          // recovered when the allowlist is unambiguous:
          //   - single-flow allowlist → re-drive that flow (transient on
          //     failure so a later sweep can retry up to the cap);
          //   - multi-flow allowlist → fail PERMANENTLY so the row stops
          //     re-driving every sweep instead of looping forever.
          //
          // The original acknowledgement-on-accept work: like the hot path, this reports the LAUNCH and hands the
          // child's exit back on `exited`, so a sweep no longer runs as long as
          // the run it recovered. A synchronous Bun.spawn throw (bad binary) is a
          // transient failure — the sweep must survive it and retry within cap.
          respawn: async (event) => {
            const launch = (args: string[]): RedriveLaunch => {
              try {
                return { result: 'spawned', ...launchConduitRun(args) };
              } catch (err) {
                io.err(
                  `[ingress] re-drive launch for event '${event.event_id}' failed: ` +
                    `${err instanceof Error ? err.message : String(err)}`,
                );
                return { result: 'transient_failure' };
              }
            };

            if (event.flow_path !== null && event.substrate_json !== null && event.run_id !== null) {
              return launch([
                event.flow_path,
                '--input-inline',
                event.substrate_json,
                '--run-id',
                event.run_id,
              ]);
            }
            const flowPaths = Object.values(config.allowlist);
            if (flowPaths.length !== 1) {
              io.err(
                `[ingress] cannot re-drive event '${event.event_id}': the pre-v9 record ` +
                  `carries no flow attribution and the allowlist has ${flowPaths.length} ` +
                  `flows — marking permanently failed`,
              );
              return 'permanent_failure';
            }
            return launch([flowPaths[0]!]);
          },
          redriveCap,
          // The original ingress-attribution work FR-3: periodic re-drive sweep interval (default 60s);
          // override via CONDUIT_REDRIVE_INTERVAL_MS for tests/tuning.
          ...(Number(process.env.CONDUIT_REDRIVE_INTERVAL_MS) > 0 && {
            redriveIntervalMs: Number(process.env.CONDUIT_REDRIVE_INTERVAL_MS),
          }),
          onRedriveSweep: (report) => {
            const total =
              report.spawned.length + report.failed.length + report.permanentlyFailed.length;
            if (total > 0) {
              // The original listener-backpressure work burst visibility: deferred events are queued behind
              // busy run slots, not failed — say so instead of staying silent.
              const deferredSuffix =
                report.deferred.length > 0
                  ? ` (${report.deferred.length} queued behind busy run slots)`
                  : '';
              io.out(
                `[ingress redrive] re-drove ${total} event(s): ` +
                  `${report.spawned.length} spawned, ${report.failed.length} transient, ` +
                  `${report.permanentlyFailed.length} permanent${deferredSuffix}`,
              );
            } else if (report.deferred.length > 0) {
              io.out(
                `[ingress redrive] ${report.deferred.length} event(s) queued behind busy run slots`,
              );
            }
          },
          now: () => Date.now(),
          // Socket Mode transport (the original Slack Socket Mode work): real apps.connections.open +
          // Bun-native WebSocket. Only exercised when a flow declares a slack
          // binding with transport: 'socket'.
          socket: buildProductionSocketSeam(),
          onSocketLifecycle: (event) => {
            switch (event.kind) {
              case 'connected':
                io.out('[slack socket] connected');
                break;
              case 'refresh':
                io.out('[slack socket] Slack requested a connection refresh — opening replacement');
                break;
              case 'open_failed':
                io.err(`[slack socket] apps.connections.open failed: ${event.error}`);
                break;
              case 'reconnect_scheduled':
                io.err(
                  `[slack socket] reconnecting (attempt ${event.attempt}) in ${event.delayMs}ms`,
                );
                break;
              case 'terminal':
                io.err(
                  `[slack socket] TERMINAL disconnect (${event.reason}) — Socket Mode stopped; ` +
                    'the Slack app appears to be disabled',
                );
                break;
              case 'stopped':
                io.out('[slack socket] stopped');
                break;
            }
          },
        },
        enrichedConfig,
      );
    },
  };
}

// ---------------------------------------------------------------------------
// Slack Socket Mode production seam (the original Slack Socket Mode work)
// ---------------------------------------------------------------------------

/**
 * The real Socket Mode I/O: apps.connections.open over fetch with the
 * app-level token, and Bun's native WebSocket for the wss connection. All
 * lifecycle logic (ack, refresh handoff, backoff) lives in the seam's consumer
 * (createSocketModeClient) — this is delivery only.
 */
/** Bun's proxy-capable WebSocket constructor — see the cast in `connect`. */
type ProxyCapableWebSocket = new (url: string, options: { proxy: string }) => WebSocket;

function buildProductionSocketSeam(): SocketSeam {
  return {
    openConnection: async (appToken) => {
      try {
        const res = await fetch(resolveSocketConnectionsOpenUrl(), {
          method: 'POST',
          headers: { authorization: `Bearer ${appToken}` },
          // Bound the open handshake (same stall class as the transport fix): a
          // hung connection would otherwise wedge the Socket Mode listener. The
          // try/catch below converts the abort throw into the seam's normal
          // {ok:false} failure, which the client retries with backoff.
          signal: AbortSignal.timeout(resolveSlackFetchTimeoutMs()),
        });
        const json = (await res.json()) as { ok?: boolean; url?: string; error?: string };
        return json.ok === true && typeof json.url === 'string'
          ? { ok: true, url: json.url }
          : { ok: false, error: json.error ?? `HTTP ${res.status}` };
      } catch (err) {
        return { ok: false, error: err instanceof Error ? err.message : String(err) };
      }
    },
    connect: (url, handlers) => {
      // The original Socket Mode proxy work: `fetch` honours HTTPS_PROXY/HTTP_PROXY/NO_PROXY; Bun's
      // WebSocket accepts a `proxy` option but does NOT read those variables.
      // Without this the Socket Mode dial silently bypasses an operator's
      // egress proxy while every other outbound call respects it — which makes
      // domain-level egress allowlisting (deployment-hardening rule 9)
      // impossible: closing the last direct hole kills the listener.
      const dialOptions = socketDialOptions(url, process.env);
      // Bun's WebSocket accepts `{ proxy }` at runtime (verified on 1.3.11),
      // but the ambient DOM lib's constructor signature wins at type level and
      // types the second argument as protocols only. Narrow, local cast rather
      // than dropping DOM from the project's `lib`, which would re-type
      // everything else too.
      const ws =
        dialOptions === undefined
          ? new WebSocket(url)
          : new (WebSocket as unknown as ProxyCapableWebSocket)(url, dialOptions);
      ws.onmessage = (event) => {
        handlers.onMessage(typeof event.data === 'string' ? event.data : String(event.data));
      };
      ws.onclose = () => handlers.onClose();
      return {
        send: (data) => ws.send(data),
        close: () => ws.close(),
      };
    },
  };
}

/** Build minimal production deps for commands that must not open the state DB. */
export function buildReadOnlyProductionDeps(): CliDeps {
  const io: CliIO = {
    out: (line: string) => process.stdout.write(line + '\n'),
    err: (line: string) => process.stderr.write(line + '\n'),
  };

  const db = new Proxy(
    {},
    {
      get(_target, prop) {
        if (prop === 'close') return () => {};
        throw new Error(`read-only command unexpectedly touched deps.db (.${String(prop)})`);
      },
    },
  ) as unknown as ConduitDB;

  // WI-588 fix (Amy FLAG on WI-588 review): `explain` routes through THIS deps
  // builder, not buildProductionDeps — it must build an equivalent config-
  // driven, load-time-only harness registry from the same config source (the
  // CONDUIT_HARNESS_* env), or a malformed/unshipped harness adapter silently
  // passes explain's validation (loadFlow treats an undefined registry as
  // "skip harness checks entirely", not fail-closed). Constructs its own
  // instance the same way buildProductionDeps does; still never opens the
  // state DB.
  const parsedHarnessConfig = parseHarnessConfig(process.env);
  if (!parsedHarnessConfig.ok) {
    throw new Error(parsedHarnessConfig.error);
  }
  const harnessRegistry = bindHarnessDefinitionsForIntrospection(
    buildHarnessDefinitionRegistry(parsedHarnessConfig.defs),
  );

  return {
    io,
    now: () => Math.floor(Date.now() / 1000),
    db,
    adapter: {
      call: async () => {
        throw new Error('read-only command unexpectedly called the model adapter');
      },
    },
    runEngine: async () => {
      throw new Error('read-only command unexpectedly started the engine');
    },
    prereqs: [],
    harnessRegistry,
  };
}

// ---------------------------------------------------------------------------
// #3 — Ingress secret resolution (secret_env → process.env value)
// ---------------------------------------------------------------------------

/**
 * Walk each allowlisted flow's ingress binding, find every `auth.secret_env`
 * and `app_token_env` reference, and resolve them from the environment into a
 * `secrets` record keyed by the env-var name (the shape the listener consumes
 * via config.secrets).
 *
 * Also reports whether any flow declares an EVENTS-transport Slack binding, so
 * the caller can warn when CONDUIT_SLACK_SIGNING_SECRET is missing — a
 * socket-transport binding (the original Slack Socket Mode work) never verifies request signatures (the
 * wss connection is the auth), so it must not trigger that warning. Flow-load
 * failures are ignored here — the listener boot re-loads every flow and fails
 * closed with a precise FLOW_LOAD_FAILED error, so this resolution pass stays
 * best-effort.
 */
function resolveIngressSecrets(
  allowlist: Record<string, string>,
  load: typeof loadFlow,
): { secrets: Record<string, string>; hasSlackEventsBinding: boolean } {
  const secrets: Record<string, string> = {};
  let hasSlackEventsBinding = false;

  for (const flowPath of Object.values(allowlist)) {
    const loaded = load(flowPath);
    if (!loaded.ok) continue;

    const parsed = parseIngressBinding(loaded.flow.channels?.ingress as unknown);
    if (!parsed.ok) continue;

    const { binding } = parsed;
    if (binding.type === 'slack' && binding.transport !== 'socket') {
      hasSlackEventsBinding = true;
    }

    const auth = binding.auth as Record<string, unknown> | undefined;
    const secretEnv = typeof auth?.['secret_env'] === 'string' ? auth['secret_env'] : undefined;
    if (secretEnv !== undefined) {
      const value = process.env[secretEnv];
      if (value !== undefined) {
        secrets[secretEnv] = value;
      }
      // A referenced-but-unset secret_env is left absent: the listener resolves
      // config.secrets?.[secretEnv] ?? '' and webhook auth then fails closed.
    }

    if (binding.app_token_env !== undefined) {
      const value = process.env[binding.app_token_env];
      if (value !== undefined) {
        secrets[binding.app_token_env] = value;
      }
      // A referenced-but-unset app_token_env is left absent: the listener boot
      // fails LOUD with MISSING_APP_TOKEN_SECRET (a socket client cannot
      // fail-close per request — there is no request).
    }
  }

  return { secrets, hasSlackEventsBinding };
}

// ---------------------------------------------------------------------------
// #1 — HTTP server: bind a port, route webhook/Slack POSTs, block on signal
// ---------------------------------------------------------------------------

/** The Slack Events API route served by the listener. */
const SLACK_EVENTS_ROUTE = '/slack/events';

/**
 * Serve the booted listener over HTTP until a shutdown signal arrives.
 *
 * Routing (POST only):
 *   - POST /slack/events → listener.handleSlack (ack-fast, processes async)
 *   - POST <any other path> → listener.handleWebhook (the adapter resolves the
 *     route and itself returns 404 for an unwatched path)
 *   - everything else → 404
 *
 * Resolves (allowing cmdListen to return 0) once SIGTERM or SIGINT is received
 * and the server has been stopped gracefully.
 */
async function serveListener(listener: Listener, port: number, io: CliIO): Promise<void> {
  const server = Bun.serve({
    port,
    async fetch(req): Promise<Response> {
      const url = new URL(req.url);

      if (req.method !== 'POST') {
        return new Response('Not Found', { status: 404 });
      }

      // Slack Events API — verify-then-ack-fast, async processing.
      if (url.pathname === SLACK_EVENTS_ROUTE) {
        const rawBody = await req.text();
        const slackResp = listener.handleSlack({
          headers: headersToRecord(req.headers),
          rawBody,
        });
        // Let the async accept/spawn run in the background; do not block the ack.
        void slackResp.processed;
        // body is only set for synchronous protocol replies (e.g. url_verification).
        // Normal event acks carry no body (null).
        return new Response(slackResp.body ?? null, {
          status: slackResp.status,
          headers: slackResp.body !== undefined
            ? { 'Content-Type': 'application/json' }
            : undefined,
        });
      }

      // All other POST paths → webhook adapter (it 404s unknown routes itself).
      // The original acknowledgement-on-accept work: this resolves once the event is accepted and its run is
      // launched — not when the run finishes — so the caller gets its ack in
      // milliseconds and can read the outcome off the JSON body.
      const rawBody = await req.text();
      const webhookResp = await listener.handleWebhook({
        route: url.pathname,
        headers: headersToRecord(req.headers),
        rawBody,
      });
      return new Response(webhookResp.body ?? null, {
        status: webhookResp.status,
        headers: webhookResp.body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
      });
    },
  });

  io.out(`conduit listen: serving ingress on http://0.0.0.0:${server.port}`);

  // Socket Mode transport (the original Slack Socket Mode work): open outbound wss connections for any
  // socket-transport slack flows. Deliberately NOT awaited — an unreachable
  // Slack retries with backoff forever, and the HTTP transports must serve
  // regardless. A listener with no socket flows makes this a no-op. The catch
  // covers failures OUTSIDE the client's own retry loop (e.g. a WebSocket
  // constructor throw on a malformed URL) so they surface as a log line, not
  // an unhandled rejection.
  listener.sockets.start().catch((err) => {
    io.err(
      `[slack socket] failed to start socket clients: ${
        err instanceof Error ? err.message : String(err)
      }`,
    );
  });

  // Periodic re-drive sweep (the original ingress-attribution work, FR-3): recover transiently-failed
  // events while the listener is up, not only at the next boot.
  listener.redrive.start();

  // Block until SIGTERM/SIGINT, then stop the server gracefully.
  await new Promise<void>((resolveSignal) => {
    const onSignal = (sig: NodeJS.Signals) => {
      io.err(`conduit listen: received ${sig}, shutting down`);
      process.off('SIGTERM', onSignal);
      process.off('SIGINT', onSignal);
      resolveSignal();
    };
    process.on('SIGTERM', onSignal);
    process.on('SIGINT', onSignal);
  });

  listener.redrive.stop();
  listener.sockets.stop();
  await server.stop();
}

/**
 * Flatten a Fetch Headers object into the plain record the adapters expect.
 *
 * The Fetch Headers object lowercases every name, but the auth seams and the
 * event-id deriver look up canonically-cased names (e.g. 'X-Hub-Signature-256',
 * 'X-Slack-Signature', 'X-GitHub-Delivery'). HTTP header names are
 * case-insensitive, so we return a case-insensitive view: a Proxy that lowercases
 * every string key before reading from the underlying lowercased record.
 */
function headersToRecord(headers: Headers): Record<string, string> {
  const lower: Record<string, string> = {};
  headers.forEach((value, key) => {
    lower[key.toLowerCase()] = value;
  });
  return new Proxy(lower, {
    get(target, prop) {
      if (typeof prop === 'string') {
        return target[prop.toLowerCase()];
      }
      return Reflect.get(target, prop);
    },
    has(target, prop) {
      if (typeof prop === 'string') {
        return prop.toLowerCase() in target;
      }
      return Reflect.has(target, prop);
    },
  });
}

// The real binary entrypoint. Without this, `conduit run flow.yaml` was a no-op.
if (import.meta.main) {
  // Worker subprocess entry — intercepted BEFORE buildProductionDeps so a worker
  // never opens the state DB (NFR-3: workers do no DB I/O). startWorkerMain wires
  // the harness to real process IPC + timers and self-exits after one task.
  if (process.argv[2] === '__worker') {
    startWorkerMain(process.argv.slice(3));
  } else {
    // Amy's WI-594 FLAG: buildProductionDeps()/buildReadOnlyProductionDeps()
    // throw SYNCHRONOUSLY on a malformed CONDUIT_HARNESS_* config (WI-588
    // fail-at-boot). Constructing deps outside a try/catch let that throw
    // escape as a raw uncaught exception (a scary Bun stack trace exposing
    // internal file paths) instead of the same clean `fatal: <message>`
    // formatting main()'s async .catch() below already gives every other
    // startup failure. deps.io doesn't exist yet at this point, so this
    // branch writes to stderr directly rather than through deps.io.err.
    let deps: CliDeps;
    try {
      deps = process.argv[2] === 'explain'
        ? buildReadOnlyProductionDeps()
        : buildProductionDeps();
    } catch (err) {
      process.stderr.write(`fatal: ${err instanceof Error ? err.message : String(err)}\n`);
      process.exit(1);
    }
    main(process.argv.slice(2), deps)
      .then((code) => {
        deps.db.close();
        process.exit(code);
      })
      .catch((err) => {
        deps.io.err(`fatal: ${err instanceof Error ? err.message : String(err)}`);
        try {
          deps.db.close();
        } catch {
          /* already closed */
        }
        process.exit(1);
      });
  }
}
