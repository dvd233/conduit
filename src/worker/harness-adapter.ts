/**
 * Harness adapter seam (WI-560).
 *
 * Mirrors the kernel-mediated model-call seam (src/worker/adapter.ts) but for
 * a spawned external agent CLI (`claude -p`, `codex exec`, ...) instead of an
 * HTTP model call. A `kind: harness` station names an adapter by string only
 * — the engine resolves that name against a registry built from engine
 * config (env/DI, following the createOpenAiAdapter pattern in
 * openai-adapter.ts), never from a raw command line supplied by flow.yaml.
 * This is the seam item: downstream work (load-time validation, the spawn
 * runner, executor wiring, the real claude-headless adapter, explain/doctor)
 * builds against the HarnessAdapter interface defined here.
 *
 * NAMING COLLISION: src/worker/harness.ts is the unrelated worker-POOL
 * subprocess harness (HEARTBEAT/MARK_DONE) — do not confuse the two.
 */

import { createClaudeHarnessAdapter } from './harness-adapter-claude';
import { createCodexHarnessAdapter } from './harness-adapter-codex';
import type { HarnessAdapterConfigDef } from './harness-config';
import type { HarnessCommand, HarnessRunnerConfig, HarnessSpawnResult } from './harness-runner';

/** A declared input mounted into the harness invocation's working directory. */
export interface MountedInput {
  name: string;
  path: string;
}

/** Bounded request handed to a harness adapter's `invoke`. */
export interface HarnessInvocation {
  /** Rendered prompt string — the full task text sent to the agent CLI. */
  prompt: string;
  /** Declared inputs mounted for the invocation. */
  inputs: MountedInput[];
  /** Tools allowlist honoured by adapters that can restrict tools. */
  tools: string[];
  /** Wall-clock timeout bound in milliseconds. */
  timeoutMs: number;
  /**
   * Per-call model override (WI-589). Model is a per-STATION concern, not a
   * per-run one, so it cannot ride the per-run bound adapter config (WI-587)
   * — it flows through here instead. `invoke()` pushes `--model` from
   * `call.model ?? config.model` (station wins over the adapter's configured
   * default, FR-10).
   */
  model?: string;
}

/** A reference to one output the harness produced (name + path, not bytes). */
export interface ProducedOutput {
  name: string;
  path: string;
}

/**
 * The four token classes a call can consume, reported separately (issue #5).
 *
 * `inputTokens` is UNCACHED input only. Before this split every class was summed
 * into a single scalar that the executor then wrote to the journal's
 * `input_tokens` with `output_tokens` hard-coded to 0 — so `output_tokens` was
 * never populated on any row, and the cache-read fraction (the majority of real
 * spend) was not derivable at all.
 */
export interface UsageBreakdown {
  /** Fresh, uncached input. */
  inputTokens: number;
  outputTokens: number;
  cacheReadInputTokens: number;
  cacheCreationInputTokens: number;
}

/** One provider rate-limit window as reported alongside a call. */
export interface RateLimitWindow {
  /** Provider's name for the window, e.g. 'five_hour', 'seven_day'. */
  name: string;
  /** Fraction of the window consumed, 0..1. */
  utilization: number;
  /** When the window resets, epoch milliseconds. */
  resetsAtMs: number;
}

/**
 * Provider capacity at the moment of a call, when the harness reports it.
 *
 * `status` moves through a warning state before a cap is actually hit, which is
 * the difference between seeing a wall coming and discovering it as a 1-second
 * failed attempt.
 */
export interface RateLimitSnapshot {
  status?: string;
  usingOverage?: boolean;
  windows: RateLimitWindow[];
}

/** Structured usage for one call. */
export interface KnownUsage {
  /**
   * TOTAL tokens across every class. This is the BUDGET AUTHORITY — run and
   * wave budgets fold this number, so it must stay a true total even as
   * `breakdown` splits it. Summing only breakdown.input+output would drop cache
   * reads and silently stop the budget guards from tripping.
   */
  tokens: number;
  cost: number;
  /** The per-class split, when the adapter can report one. */
  breakdown?: UsageBreakdown;
  /** Model the provider actually billed, when reported (fills the `model` column). */
  model?: string;
  /** Provider capacity snapshot, when reported. */
  rateLimit?: RateLimitSnapshot;
}

/**
 * Per-call usage signal. Either structured usage, or an explicit
 * `{ unknown: true }` when the adapter cannot report usage for that call —
 * distinct from the static `reportsUsage` capability flag below.
 */
export type UsageReport = KnownUsage | { unknown: true };

/** What a harness adapter returns after a bounded invocation. */
export interface HarnessResult {
  outputs: ProducedOutput[];
  usage: UsageReport;
}

/**
 * A harness invocation that FAILED but was still BILLED (issue #26 AC5).
 *
 * A timeout or a non-zero exit does not refund the tokens already consumed —
 * the provider charged for whatever the call did before it died. An adapter
 * that can still recover a usage figure at that point attaches it here rather
 * than throwing it away, so the executor folds real spend into the run/wave
 * budgets instead of recording a failed-and-therefore-free call.
 *
 * `code` is the existing failure-class tag ('harness-timeout',
 * 'harness-nonzero-exit', 'harness-rate-limited') the executor already keys on.
 *
 * NOT every failure can carry usage, and that is not a defect: claude-headless
 * reports usage only in a terminal `result` event, so a call killed at the
 * wall-clock bound genuinely has no figure to recover. Absent `usage` stays
 * honestly unknown — never a fabricated zero.
 */
export interface BilledHarnessError extends Error {
  code?: string;
  usage?: UsageReport;
}

/**
 * The single reader for usage attached to a harness throw (issue #26 AC5).
 *
 * Every site that catches a harness invocation goes through HERE rather than
 * casting and reaching for `.usage` itself, so the shape can never skew between
 * the maker path and the critic path.
 *
 * Returns undefined when the throw carried nothing — which the caller must
 * treat as UNKNOWN usage, not as zero.
 */
export function usageFromThrow(err: unknown): UsageReport | undefined {
  if (typeof err !== 'object' || err === null) return undefined;
  const usage = (err as BilledHarnessError).usage;
  if (typeof usage !== 'object' || usage === null) return undefined;
  if ('unknown' in usage) return usage.unknown === true ? { unknown: true } : undefined;
  return typeof usage.tokens === 'number' && typeof usage.cost === 'number' ? usage : undefined;
}

/** Result of probing a harness adapter's underlying binary without invoking it. */
export interface BinaryProbe {
  /** True when the configured binary is present and executable. */
  present: boolean;
  /** Optional human-readable detail (e.g. the resolved path, or why it's missing). */
  detail?: string;
}

/**
 * The per-harness adapter interface. The engine supplies a concrete
 * implementation (real agent-CLI spawn) at runtime; tests inject
 * `makeFakeHarnessAdapter`.
 */
export interface HarnessAdapter {
  /** Adapter name, as resolved from engine config (e.g. 'claude-headless'). */
  readonly name: string;
  /**
   * Static capability flag: whether this adapter can report usage AT ALL.
   * Queryable without invoking — distinct from the per-call unknown-usage
   * signal in HarnessResult.usage. Consumed by explain/doctor to render the
   * usage-blind indicator.
   */
  readonly reportsUsage: boolean;
  /**
   * Static capability flag: whether this adapter can enforce/narrow ANY
   * declared `tools` allowlist. Consumed by load-time validation to
   * fail-closed or honor the `unrestricted_tools` waiver. When
   * `canExpressTools` is implemented, that per-list verdict is authoritative
   * and this flag is only the fallback for callers that never learned about
   * per-list negotiation — keep it CONSERVATIVE (false unless every list is
   * expressible) so legacy boolean-only paths stay fail-closed.
   */
  readonly canRestrictTools: boolean;
  /**
   * Per-list expressibility negotiation (the original per-list tool-expression work): can this adapter enforce
   * THIS specific allowlist? Adapters whose containment surface is a
   * capability lattice rather than a per-tool-name flag (codex-exec's OS
   * sandbox modes) cannot express every list but CAN provably express some —
   * a boolean capability flag forces them out of critic seats entirely.
   * Optional: adapters that don't implement it fall back to
   * `canRestrictTools` (all-lists-or-nothing). Judge via the
   * `adapterCanExpressTools` helper, never by calling this directly.
   */
  canExpressTools?(tools: readonly string[]): boolean;
  /**
   * The adapter's configured default model (WI-589), e.g. from
   * CONDUIT_HARNESS_<NAME>_MODEL. Read by the executor to compute the
   * effective model (`station.model ?? adapter default`) threaded into both
   * the invocation and the resume binding stamp.
   */
  readonly model?: string;
  /** Probe binary presence/executability without a full invocation. */
  probeBinary(): Promise<BinaryProbe>;
  /** Bounded invocation of the underlying agent CLI. */
  invoke(call: HarnessInvocation): Promise<HarnessResult>;
}

/**
 * The single judgment seam for tools-allowlist expressibility (the original per-list tool-expression work).
 * An empty list is trivially expressible (nothing to enforce). A non-empty
 * list is judged by the adapter's per-list `canExpressTools` when implemented,
 * else by the legacy all-or-nothing `canRestrictTools` flag. Every validation
 * and dispatch site judges through HERE so the two mechanisms can never skew.
 */
export function adapterCanExpressTools(
  adapter: Pick<HarnessAdapter, 'canRestrictTools' | 'canExpressTools'>,
  tools: readonly string[],
): boolean {
  if (tools.length === 0) return true;
  if (adapter.canExpressTools !== undefined) return adapter.canExpressTools(tools);
  return adapter.canRestrictTools;
}

// ---------------------------------------------------------------------------
// Registry — resolves an adapter by NAME only (AC5, AC7).
// ---------------------------------------------------------------------------

export type ResolveResult = { ok: true; adapter: HarnessAdapter } | { ok: false; error: string };

export interface HarnessRegistry {
  /** Resolve a registered adapter by name; not-found errors NAME the request. */
  resolve(name: string): ResolveResult;
  /** Enumerate every registered adapter name. */
  list(): readonly string[];
}

/**
 * Build a registry from engine-config adapter definitions. Pure, no I/O —
 * a flow can only ever supply a name string to `resolve`, never a command line.
 */
export function createHarnessRegistry(adapters: readonly HarnessAdapter[]): HarnessRegistry {
  const byName = new Map<string, HarnessAdapter>();
  for (const adapter of adapters) {
    byName.set(adapter.name, adapter);
  }
  return {
    resolve(name: string): ResolveResult {
      const adapter = byName.get(name);
      if (adapter === undefined) {
        return { ok: false, error: `unknown harness adapter: ${name}` };
      }
      return { ok: true, adapter };
    },
    list(): readonly string[] {
      return [...byName.keys()];
    },
  };
}

// ---------------------------------------------------------------------------
// Two-phase registry (WI-587) — config-time adapter DEFINITIONS, bound to a
// per-run projectRoot at dispatch to yield the invocable HarnessAdapter above.
// Each real entry point (the CLI commands + listener in src/cli/main.ts, and
// src/worker/worker-entry.ts's out-of-process worker) constructs one of these
// from the single config source (the CONDUIT_HARNESS_* env), per entry point.
// ---------------------------------------------------------------------------

/**
 * A config-time harness adapter definition. Identity, capability flags, the
 * configured env allowlist/command, and binary presence are all resolvable
 * with NO projectRoot in hand (load-validation, doctor). `bind` supplies the
 * run's projectRoot to produce the existing, invocable `HarnessAdapter`.
 */
export interface HarnessAdapterDefinition {
  readonly name: string;
  readonly reportsUsage: boolean;
  readonly canRestrictTools: boolean;
  /** Per-list expressibility passthrough (the original per-list tool-expression work) — see HarnessAdapter.canExpressTools. */
  canExpressTools?(tools: readonly string[]): boolean;
  readonly envAllowlist: readonly string[];
  readonly command: string | undefined;
  probeBinary(): Promise<BinaryProbe>;
  bind(projectRoot: string): HarnessAdapter;
}

export type DefinitionResolveResult =
  | { ok: true; adapter: HarnessAdapterDefinition }
  | { ok: false; error: string };

export interface HarnessDefinitionRegistry {
  resolve(name: string): DefinitionResolveResult;
  list(): readonly string[];
}

interface ShippedAdapterFactoryConfig {
  projectRoot: string;
  envAllowlist: string[];
  command?: string;
  model?: string;
  run?: (cmd: HarnessCommand, config: HarnessRunnerConfig) => Promise<HarnessSpawnResult>;
  probe?: () => Promise<BinaryProbe>;
}

/** Every adapter the engine ships, keyed by the name a config def can name. */
const SHIPPED_HARNESS_FACTORIES: Record<string, (config: ShippedAdapterFactoryConfig) => HarnessAdapter> = {
  'claude-headless': createClaudeHarnessAdapter,
  'codex-exec': createCodexHarnessAdapter,
};

/** Test/production seam forwarded into every adapter this registry builds. */
export interface HarnessRegistryDeps {
  run?: (cmd: HarnessCommand, config: HarnessRunnerConfig) => Promise<HarnessSpawnResult>;
  probe?: () => Promise<BinaryProbe>;
}

/**
 * Build the config-time harness adapter DEFINITION registry from parsed
 * engine config (WI-586's `HarnessAdapterConfigDef[]`). This is the
 * two-phase surface (WI-587): identity/caps/probe/allowlist/command resolve
 * with NO projectRoot, and `definition.bind(projectRoot)` yields the existing,
 * invocable `HarnessAdapter` at per-run dispatch time. Each real entry point
 * (CLI commands + listener in src/cli/main.ts, worker-entry.ts's out-of-process
 * worker) constructs one of these from the CONDUIT_HARNESS_* env config and
 * binds it at the dispatch site where a run's projectRoot is known (WI-588).
 *
 * A config def naming an adapter the engine does not ship fails registry
 * CONSTRUCTION (never silently skipped) — the caller decides how a startup
 * failure surfaces. An empty `configDefs` list yields a registry that fail-
 * closes every `resolve`, with a message hinting that registration is
 * configuration-driven via CONDUIT_HARNESS_*.
 */
export function buildHarnessDefinitionRegistry(
  configDefs: readonly HarnessAdapterConfigDef[] = [],
  deps: HarnessRegistryDeps = {},
): HarnessDefinitionRegistry {
  const definitions = new Map<string, HarnessAdapterDefinition>();

  for (const configDef of configDefs) {
    const factory = SHIPPED_HARNESS_FACTORIES[configDef.name];
    if (factory === undefined) {
      throw new Error(
        `harness registry: "${configDef.name}" is not an adapter this engine ships — adapter ` +
          `registration is configuration-driven (CONDUIT_HARNESS_ADAPTERS); check your CONDUIT_HARNESS_* config`,
      );
    }

    const buildAdapter = (projectRoot: string): HarnessAdapter =>
      factory({
        projectRoot,
        envAllowlist: [...configDef.envAllowlist],
        command: configDef.command,
        model: configDef.model,
        run: deps.run,
        probe: deps.probe,
      });

    // Root-independent instance used ONLY to read static identity/caps and to
    // probe the binary — never invoked, so the empty sentinel root is safe.
    const identityAdapter = buildAdapter('');

    const definition: HarnessAdapterDefinition = {
      name: configDef.name,
      reportsUsage: identityAdapter.reportsUsage,
      canRestrictTools: identityAdapter.canRestrictTools,
      envAllowlist: [...configDef.envAllowlist],
      command: configDef.command,
      probeBinary: () => identityAdapter.probeBinary(),
      bind: (projectRoot: string) => buildAdapter(projectRoot),
    };
    // Per-list expressibility (the original per-list tool-expression work) is root-independent static capability
    // knowledge — passthrough only when the adapter implements it, so the
    // definition's absent-method shape matches the adapter's.
    if (identityAdapter.canExpressTools !== undefined) {
      definition.canExpressTools = (tools) => identityAdapter.canExpressTools!(tools);
    }
    definitions.set(configDef.name, definition);
  }

  return {
    resolve(name: string): DefinitionResolveResult {
      const definition = definitions.get(name);
      if (definition === undefined) {
        return {
          ok: false,
          error:
            `unknown harness adapter: ${name} — adapter registration is configuration-driven ` +
            `(set CONDUIT_HARNESS_ADAPTERS and CONDUIT_HARNESS_<NAME>_ENV)`,
        };
      }
      return { ok: true, adapter: definition };
    },
    list(): readonly string[] {
      return [...definitions.keys()];
    },
  };
}

/**
 * Bind every definition in a config-time registry to ONE projectRoot,
 * producing the standard (legacy) `HarnessRegistry` (WI-588). For load-time-
 * only consumers (flow validation, doctor, explain, build) that introspect
 * caps/probeBinary/name but never call `invoke()` — the run path (cmdRun/
 * cmdResume/worker-entry) binds a run-specific registry immediately before
 * dispatch instead, so this is never the registry a real invocation uses.
 */
export function bindHarnessDefinitions(
  registry: HarnessDefinitionRegistry,
  projectRoot: string,
): HarnessRegistry {
  return createHarnessRegistry(
    registry.list().flatMap((name) => {
      const resolved = registry.resolve(name);
      return resolved.ok ? [resolved.adapter.bind(projectRoot)] : [];
    }),
  );
}

/**
 * Build a load-time-only `HarnessRegistry` from a config-time registry, for
 * consumers (loadFlow, probeHarnessBinaries, explain, doctor, build) that
 * only ever introspect name/caps/probeBinary and are constructed BEFORE any
 * run's projectRoot is known (e.g. buildProductionDeps at engine boot).
 *
 * Deliberately NEVER binds to a real filesystem root (e.g. process.cwd()) —
 * FR-4's confinement root comes from the run being executed, never captured
 * at boot. `invoke()` on the returned adapters always fails closed with a
 * named error instead of silently running anchored to the engine's boot cwd;
 * the run path (cmdRun/cmdResume/worker-entry) binds a genuinely per-run
 * registry via `bindHarnessDefinitions` immediately before dispatch.
 */
export function bindHarnessDefinitionsForIntrospection(
  registry: HarnessDefinitionRegistry,
): HarnessRegistry {
  const adapters: HarnessAdapter[] = registry.list().flatMap((name) => {
    const resolved = registry.resolve(name);
    if (!resolved.ok) return [];
    const def = resolved.adapter;
    return [
      {
        name: def.name,
        reportsUsage: def.reportsUsage,
        canRestrictTools: def.canRestrictTools,
        // Load-time validation judges per-list expressibility (the original per-list tool-expression work), so
        // the introspection binding must carry it — omitting it here would
        // silently demote a lattice adapter back to its conservative boolean.
        ...(def.canExpressTools !== undefined
          ? { canExpressTools: (tools: readonly string[]) => def.canExpressTools!(tools) }
          : {}),
        probeBinary: (): Promise<BinaryProbe> => def.probeBinary(),
        invoke: async (): Promise<HarnessResult> => {
          throw new Error(
            `harness adapter '${def.name}' is not bound to a run — this is the load-time-only ` +
              `registry (introspection: caps/probeBinary/name); invoke() requires a per-run ` +
              `projectRoot binding, which the run path supplies immediately before dispatch`,
          );
        },
      },
    ];
  });
  return createHarnessRegistry(adapters);
}

// ---------------------------------------------------------------------------
// Test-fake adapter (AC6) — canonical, exported so downstream item tests
// import one deterministic fake rather than each re-implementing it.
// ---------------------------------------------------------------------------

export interface FakeHarnessAdapterConfig {
  name?: string;
  reportsUsage?: boolean;
  canRestrictTools?: boolean;
  /** Per-list expressibility fake (the original per-list tool-expression work); absent = boolean-only adapter. */
  canExpressTools?: (tools: readonly string[]) => boolean;
  binaryPresent?: boolean;
  results?: HarnessResult[];
}

export function makeFakeHarnessAdapter(config: FakeHarnessAdapterConfig = {}): {
  adapter: HarnessAdapter;
  calls: HarnessInvocation[];
} {
  const {
    name = 'fake-harness',
    reportsUsage = true,
    canRestrictTools = true,
    canExpressTools,
    binaryPresent = true,
    results = [],
  } = config;
  const calls: HarnessInvocation[] = [];
  let i = 0;
  const adapter: HarnessAdapter = {
    name,
    reportsUsage,
    canRestrictTools,
    ...(canExpressTools !== undefined ? { canExpressTools } : {}),
    async probeBinary(): Promise<BinaryProbe> {
      return { present: binaryPresent };
    },
    async invoke(call: HarnessInvocation): Promise<HarnessResult> {
      calls.push(call);
      if (i >= results.length) {
        throw new Error(`fake harness adapter over-called: no scripted result for call #${i + 1}`);
      }
      return results[i++]!;
    },
  };
  return { adapter, calls };
}
