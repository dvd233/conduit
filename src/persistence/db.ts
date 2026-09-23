/**
 * Split persistence layer for the Conduit kernel (SPEC §11, WI-290).
 *
 * Two physical SQLite files:
 *   - STATE DB   — transactional, low-volume (cards, station_outputs, outbox,
 *                  active_workers, ingress_events). Schema versioned via
 *                  `PRAGMA user_version`.
 *   - JOURNAL DB — append-only, high-volume, on its own WAL connection so
 *                  journal writes never contend on the state write-lock at
 *                  fan-out scale (journal, work_summaries).
 */

import { Database } from 'bun:sqlite';
import type { Card, Status } from '../types/kernel';
import { withBusyRetry } from './busy-retry';

// ---------------------------------------------------------------------------
// Read-side trust boundary (finding #12)
//
// Rows coming back out of SQLite are NOT trusted: a corrupt or hand-edited row
// could carry a bogus `status` the FSM has no transition for, or malformed JSON
// in a *_json column. We validate on read and fail loud WITH the card id rather
// than returning a Card that lies to the kernel or throwing a context-free
// SyntaxError from deep inside JSON.parse.
// ---------------------------------------------------------------------------

/** The ten legal FSM sub-states (SPEC §3). Mirrors the `Status` union. */
const VALID_STATUSES: ReadonlySet<Status> = new Set<Status>([
  'waiting',
  'ready',
  'claimed',
  'working',
  'done_pending_ack',
  'interrupted',
  'held',
  'awaiting_children',
  'complete',
  'scrapped',
]);

function validateStatus(cardId: string, status: string): Status {
  if (!VALID_STATUSES.has(status as Status)) {
    throw new Error(
      `corrupt card row '${cardId}': invalid status '${status}' — not one of ` +
        `${[...VALID_STATUSES].join(', ')}`,
    );
  }
  return status as Status;
}

/**
 * `lane` is an open set (any flow.yaml station id OR a kernel terminal), so we
 * cannot match it to a closed list. We can still reject a structurally invalid
 * lane (empty/blank), which would make a card un-routable.
 */
function validateLane(cardId: string, lane: string): string {
  if (typeof lane !== 'string' || lane.trim() === '') {
    throw new Error(`corrupt card row '${cardId}': empty/invalid lane`);
  }
  return lane;
}

/** JSON.parse a persisted column, rethrowing SyntaxError WITH card id + column. */
function parseColumn(cardId: string, column: string, raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch (err) {
    const detail = err instanceof Error ? err.message : String(err);
    throw new Error(
      `corrupt card row '${cardId}': column '${column}' is not valid JSON — ${detail}`,
    );
  }
}

// ---------------------------------------------------------------------------
// Schema version — stored as PRAGMA user_version on the state DB.
// ---------------------------------------------------------------------------

export const SCHEMA_VERSION = 10;

export const DEFAULT_RUN_ID = 'default';

// ---------------------------------------------------------------------------
// Sensitive attribute filtering (AC7, SPEC §11 L2)
//
// SPEC §11 prefers NOT logging secrets over masking. We keep a blocklist as
// defense-in-depth, but harden it on two axes the original shallow filter
// missed (findings #11):
//
//   1. RECURSION — nested objects/arrays are screened too, so a secret hiding
//      under {headers:{Authorization}} or {config:{api_key}} cannot leak.
//   2. SUBSTRING matching — the key is sensitive if it CONTAINS any known
//      sensitive token (openai_api_key, x-api-key, aws_secret_access_key,
//      gh_token, db_password, *_secret, *_token, bearer, …), not only if it
//      equals one. A vendor-prefixed key must not slip through.
//
// The raw secret value must never appear in the journal file. Non-sensitive
// keys (e.g. 'gen_ai.request.model') pass through untouched.
// ---------------------------------------------------------------------------

/**
 * Matches known-sensitive attribute key names (case-insensitive) anywhere in
 * the key — substring, not anchored — so vendor- and header-prefixed variants
 * are caught: api_key, apikey, api-key, x-api-key, openai_api_key,
 * authorization, authentication, auth, bearer, bearer_token, password, passwd,
 * db_password, secret, secret_key, client_secret, aws_secret_access_key,
 * *_secret, token, gh_token, *_token, access_key, access_key_id, private_key,
 * credential(s).
 */
const SENSITIVE_KEY_RE =
  /(api[_-]?key|auth(?:orization|entication)?|bearer|passw(?:or)?d|secret|token|access[_-]?key|private[_-]?key|credentials?)/i;

export function isSensitiveKey(key: string): boolean {
  return SENSITIVE_KEY_RE.test(key);
}

/**
 * Recursively drop any key (at any nesting depth) whose name matches the
 * sensitive pattern. Plain objects are screened key-by-key; arrays are
 * traversed element-wise (a sensitive key nested inside an array element is
 * still dropped). Primitives are returned as-is.
 */
function filterValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(filterValue);
  }
  if (value !== null && typeof value === 'object') {
    return filterAttributes(value as Record<string, unknown>);
  }
  return value;
}

/**
 * Strip secrets out of an attribute bag before it is persisted to the journal.
 * A sensitive key drops its whole subtree, not just a scalar, so a token nested
 * under `credentials` cannot survive by being one level deeper.
 */
export function filterAttributes(attrs: Record<string, unknown>): Record<string, unknown> {
  const safe: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(attrs)) {
    if (isSensitiveKey(key)) continue; // drop the whole subtree under a sensitive key
    safe[key] = filterValue(value);
  }
  return safe;
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

const STATE_DDL = `
CREATE TABLE IF NOT EXISTS cards (
  run_id       TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
  id           TEXT NOT NULL,
  parent_id    TEXT,
  lane         TEXT NOT NULL,
  status       TEXT NOT NULL,
  attempt      INTEGER NOT NULL DEFAULT 0,
  wave         INTEGER NOT NULL DEFAULT 0,
  owned_paths  TEXT NOT NULL DEFAULT '[]',
  rework_count INTEGER NOT NULL DEFAULT 0,
  -- v10: not-before dispatch gate (epoch seconds). NULL = dispatchable now.
  -- Used by the fan-out cache-warming stagger: siblings are stamped
  -- release_at = now + child_stagger_seconds so planTick holds them until the
  -- first child has warmed the shared prompt-prefix cache.
  release_at   INTEGER,
  PRIMARY KEY (run_id, id)
);

-- v10 hot-path indexes (the (run_id, id) PK does not cover status/parent_id filters):
--   • idx_cards_dispatch — planTick's per-tick dispatch scan
--     (WHERE run_id AND status IN (...) ORDER BY id) and the release-gate MIN query
--     (release_at trailing makes it index-covered).
--   • idx_cards_parent — the fan-out MIN(id) sibling subquery + parent→children lookups.
-- (active_workers' COUNT(*) WHERE run_id is already served by its (run_id, ...) PK.)
CREATE INDEX IF NOT EXISTS idx_cards_dispatch ON cards(run_id, status, id, release_at);
CREATE INDEX IF NOT EXISTS idx_cards_parent ON cards(run_id, parent_id, id);

CREATE TABLE IF NOT EXISTS station_outputs (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id         TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
  card_id        TEXT NOT NULL,
  station        TEXT NOT NULL,
  attempt        INTEGER NOT NULL,
  findings_hash  TEXT NOT NULL,
  payload_json   TEXT NOT NULL DEFAULT '{}',
  return_to      TEXT,
  binding_stamp  TEXT,
  created_at     INTEGER NOT NULL DEFAULT (unixepoch()),
  -- One checkpointed output per (run, card, station, attempt).
  UNIQUE (run_id, card_id, station, attempt)
);

-- station_outputs is read by the checkpoint layer on (run_id, card_id, station, attempt).

CREATE TABLE IF NOT EXISTS outbox (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id           TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
  idempotency_key  TEXT NOT NULL,
  payload_json     TEXT NOT NULL,
  created_at       INTEGER NOT NULL DEFAULT (unixepoch()),
  delivered_at     INTEGER,
  UNIQUE (run_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS active_workers (
  run_id      TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
  card_id     TEXT NOT NULL,
  station     TEXT NOT NULL,
  worker_id   TEXT NOT NULL,
  started_at  INTEGER NOT NULL DEFAULT (unixepoch()),
  lease_until INTEGER NOT NULL,
  pid         INTEGER,              -- nullable: populated after spawn; NULL rows are unconditionally reclaimed on resume
  PRIMARY KEY (run_id, card_id, station)
);

CREATE TABLE IF NOT EXISTS runs (
  run_id            TEXT PRIMARY KEY,
  flow              TEXT NOT NULL,
  project_root      TEXT,
  input_fingerprint TEXT NOT NULL,
  status            TEXT NOT NULL,
  outcome           TEXT,
  created_at        INTEGER NOT NULL DEFAULT (unixepoch()),
  holder_pid        INTEGER,
  lease_acquired_at INTEGER
);

CREATE TABLE IF NOT EXISTS ingress_events (
  event_id       TEXT    PRIMARY KEY,
  received_at    INTEGER NOT NULL,
  spawn_state    TEXT    NOT NULL DEFAULT 'accepted',
  spawn_attempts INTEGER NOT NULL DEFAULT 0,
  -- v9 (the original ingress-attribution work): attribution persisted at accept time so re-drive can
  -- relaunch the owning flow with its original payload and derived run id.
  -- Nullable: pre-v9 rows have no attribution and stay re-drivable only via
  -- the legacy single-flow fallback.
  flow_id        TEXT,
  flow_path      TEXT,
  run_id         TEXT,
  substrate_json TEXT
);

-- Hot-path index (finding #10): the atomic-claim WIP guard counts active_workers
-- with WHERE station = ? on every dispatch. Without this it is a full table scan.
CREATE INDEX IF NOT EXISTS idx_active_workers_station ON active_workers(station);

-- Hot-path partial index (the pre-public ingress-deduplication review): getFlowPathForRun filters
-- WHERE run_id = ? AND flow_path IS NOT NULL, ordered by received_at DESC LIMIT 1.
-- The partial WHERE mirrors the query's own NULL filter so SQLite can use this
-- index for both the search and the ORDER BY, skipping a temp b-tree sort.
CREATE INDEX IF NOT EXISTS idx_ingress_events_run_id
  ON ingress_events(run_id, received_at DESC) WHERE flow_path IS NOT NULL;

-- Sweep-path partial index (the pre-public run-slot and HITL review): listRedrivable filters
-- WHERE spawn_state IN ('accepted','failed') ordered by received_at ASC.
-- ingress_events grows forever but almost every row settles to 'spawned';
-- the partial WHERE keeps the index to just the recoverable minority so the
-- boot re-drive and every periodic sweep skip the full-table scan + sort.
CREATE INDEX IF NOT EXISTS idx_ingress_events_redrivable
  ON ingress_events(spawn_state, received_at) WHERE spawn_state IN ('accepted', 'failed');
`;

const JOURNAL_DDL = `
CREATE TABLE IF NOT EXISTS journal (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id           TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
  card_id          TEXT NOT NULL,
  station          TEXT NOT NULL,
  attempt          INTEGER NOT NULL,
  name             TEXT NOT NULL,
  attributes_json  TEXT NOT NULL DEFAULT '{}',
  model            TEXT,
  input_tokens     INTEGER,
  output_tokens    INTEGER,
  cost_usd         REAL,
  -- WI-567: harness/adapter identity, measured wall-clock duration, and the
  -- explicit unknown-usage sentinel. Additive/nullable-or-defaulted so every
  -- pre-WI-567 span (transform/deterministic) reads back unchanged.
  adapter          TEXT,
  duration_ms      INTEGER,
  usage_unknown    INTEGER NOT NULL DEFAULT 0,
  -- Issue #5: the cache split, as its OWN columns rather than folded into
  -- input_tokens. Before this, the harness path summed all four token classes
  -- into input_tokens and wrote output_tokens = 0, so "how much of our spend is
  -- cache reads" was not derivable and output cost could not be compared across
  -- providers with different output:input ratios.
  --
  -- input_tokens is now UNCACHED input only. Anything summing "total tokens"
  -- must add all four columns — see getRunUsageTotals, which is the budget's
  -- source of truth and would otherwise silently under-count by the cache-read
  -- fraction (~55-70% of real traffic).
  cache_read_input_tokens      INTEGER,
  cache_creation_input_tokens  INTEGER,
  created_at       INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS work_summaries (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id      TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
  card_id     TEXT NOT NULL,
  station     TEXT NOT NULL,
  attempt     INTEGER NOT NULL,
  summary     TEXT NOT NULL,
  created_at  INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS card_log (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id        TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
  card_id       TEXT NOT NULL,
  station       TEXT NOT NULL,
  attempt       INTEGER NOT NULL,
  kind          TEXT NOT NULL,
  -- entered_lane fields
  source_lane   TEXT,
  dest_lane     TEXT,
  reason_class  TEXT,
  -- gate_verdict fields
  verdict       TEXT,
  findings_json TEXT,
  return_to     TEXT,
  -- terminal fields
  reason        TEXT,
  -- Idempotency key: a replayed append with the same (card,station,attempt,kind)
  -- is a no-op via INSERT OR IGNORE (NFR-3, FR-7).
  UNIQUE(run_id, card_id, station, attempt, kind)
);

-- Hot-path indexes (finding #10): getJournalSpans filters by card_id;
-- getStationUsage aggregates by (card_id, station, attempt). Both are read on
-- every cost/observability query — without these they full-scan the journal.
CREATE INDEX IF NOT EXISTS idx_journal_card ON journal(card_id);
CREATE INDEX IF NOT EXISTS idx_journal_cqa ON journal(card_id, station, attempt);

-- getCardLog filters by card_id ordered by insertion — index makes it fast at scale.
CREATE INDEX IF NOT EXISTS idx_card_log_card ON card_log(card_id);

-- Run-scoped composite indexes (finding #10): the run-scoped getters filter
-- WHERE run_id = ? AND card_id = ?. card_id is the high-selectivity column, so a
-- (run_id, card_id) index covers those queries directly instead of falling back
-- to the single-column index plus a run_id filter. Additive — the single-column
-- indexes above are kept for any plain card_id-only lookups.
CREATE INDEX IF NOT EXISTS idx_journal_run_card ON journal(run_id, card_id);
CREATE INDEX IF NOT EXISTS idx_card_log_run_card ON card_log(run_id, card_id);
CREATE INDEX IF NOT EXISTS idx_card_log_hitl_reason ON card_log(reason) WHERE kind = 'terminal';
CREATE INDEX IF NOT EXISTS idx_journal_hitl_ask ON journal(name, json_extract(attributes_json, '$.ts')) WHERE name = 'hitl.ask';
-- Same shape (the pre-public ingress-deduplication review): findHitlAskByThreadTs's answered-set lookup filters
-- name = 'hitl.selection' AND json_extract(...'$.correlation_id') IN (...) — this
-- makes that a covering index search instead of a full scan of every selection span.
CREATE INDEX IF NOT EXISTS idx_journal_hitl_selection ON journal(name, json_extract(attributes_json, '$.correlation_id')) WHERE name = 'hitl.selection';

-- ingress_log: append-only observability store for every ingress event outcome
-- (accepted, duplicate, rejected_*, spawn_failed, redriven). Lives on the journal
-- connection — never blocks the state write lock (NFR-1, D5, FR-11). Secret-filtered
-- before insert. Created via IF NOT EXISTS so JOURNAL_DDL self-heals existing journals.
CREATE TABLE IF NOT EXISTS ingress_log (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  source          TEXT    NOT NULL,
  event_id        TEXT,            -- null when rejected before id derivation
  outcome         TEXT    NOT NULL,
  reason          TEXT,
  attributes_json TEXT    NOT NULL DEFAULT '{}'
);
`;

// ---------------------------------------------------------------------------
// card_log constants
// ---------------------------------------------------------------------------

/**
 * Per-finding text cap (NFR-4): a verbose critic cannot grow a single card_log
 * entry without bound. Each finding string is sliced to this length before
 * insert — excess bytes are discarded (prefix is kept). The exact value is an
 * implementation detail; tests pin the *behaviour* (shared cap, prefix kept,
 * short text untouched).
 */
const CARD_LOG_FINDING_MAX_LENGTH = 4096;

// ---------------------------------------------------------------------------
// card_log public types
// ---------------------------------------------------------------------------

/**
 * The reason classes that describe why a card moved to a new lane.
 *
 * 'rate_limited' is deliberately its OWN class rather than folded into 'rework'
 * or 'hold' (issue #3): countGateReworks counts 'rework' entries against a
 * gate's cap, so a provider cap classed that way would consume budget it never
 * used, and 'hold' would imply a human has to release it when the release gate
 * is automatic.
 */
export type ReasonClass = 'forward' | 'rework' | 'scrap' | 'hold' | 'rate_limited';

/** The two possible gate verdicts from a QC station. */
export type GateVerdict = 'pass' | 'reject';

/** Fields common to every card_log entry. */
export interface CardLogBase {
  runId: string;
  cardId: string;
  station: string;
  attempt: number;
}

/**
 * Discriminated-union input shape for appendCardLog.
 * Caller supplies one variant; extra keys are screened by the sensitive-key
 * filter before storage (NFR-5).
 */
export type CardLogEntryInput =
  | (CardLogBase & {
      kind: 'entered_lane';
      sourceLane: string;
      destLane: string;
      reasonClass: ReasonClass;
    })
  | (CardLogBase & {
      kind: 'gate_verdict';
      verdict: GateVerdict;
      findings: string[];
      returnTo: string | null;
    })
  | (CardLogBase & { kind: 'terminal'; reason: string });

/**
 * A stored card_log row as returned by getCardLog. Shape mirrors
 * CardLogEntryInput; sensitive fields were already removed on write.
 */
export type StoredCardLogEntry =
  | (CardLogBase & {
      kind: 'entered_lane';
      sourceLane: string;
      destLane: string;
      reasonClass: ReasonClass;
    })
  | (CardLogBase & {
      kind: 'gate_verdict';
      verdict: GateVerdict;
      findings: string[];
      returnTo: string | null;
    })
  | (CardLogBase & { kind: 'terminal'; reason: string });

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** Input shape for appending a journal span. */
export interface JournalSpanInput {
  cardId: string;
  station: string;
  attempt: number;
  name: string;
  /**
   * Run that owns this span. REQUIRED (the original per-run usage-attribution work): a usage/journal span written
   * without its owning run silently collapses to DEFAULT_RUN_ID, making per-run
   * cost attribution impossible — the exact bug this field exists to prevent.
   * Making it required (rather than optional-with-default) turns a forgotten
   * run id into a compile error instead of a silent mis-attribution.
   */
  runId: string;
  /** Filtered through the sensitive-key blocklist before storage. */
  attributes?: Record<string, unknown>;
  /** Per-call token/cost for OTel-GenAI backend export. */
  usage?: {
    model: string;
    /**
     * UNCACHED input only (issue #5). Adapters that report a cache split put
     * cache reads/creations in their own fields below; anything computing a
     * TOTAL must add all four, never just input+output.
     */
    inputTokens: number;
    outputTokens: number;
    costUsd: number;
    /** Cache-read input, when the adapter reports it separately. */
    cacheReadInputTokens?: number;
    /** Cache-creation input, when the adapter reports it separately. */
    cacheCreationInputTokens?: number;
  };
  /** Harness/adapter identity (WI-567) — e.g. 'claude-headless', 'codex-exec'. */
  adapter?: string;
  /** Measured wall-clock duration of the attempt in milliseconds (WI-567). */
  durationMs?: number;
  /**
   * Explicit usage-unknown sentinel (WI-567, PRD NFR-2): true when the
   * harness reported no usage for this attempt. Never inferred from absent
   * usage — the caller must say so explicitly, so a genuinely-unknown cost
   * is never confused with a fabricated zero.
   */
  usageUnknown?: boolean;
}

/** A journal span as returned by getJournalSpans — attributes are pre-filtered. */
export interface StoredJournalSpan {
  cardId: string;
  station: string;
  attempt: number;
  name: string;
  /** Allowlist-filtered attributes — sensitive keys are absent. */
  attributes: Record<string, unknown>;
  /** Harness/adapter identity (WI-567) — null for a pre-WI-567 / non-harness span. */
  adapter?: string | null;
  /** Measured wall-clock duration in milliseconds (WI-567) — null when not recorded. */
  durationMs?: number | null;
  /** True iff this attempt's usage was explicitly reported unknown (WI-567). */
  usageUnknown?: boolean;
}

/** The typed DB handle every caller interacts with. */
// ---------------------------------------------------------------------------
// Ingress log types (WI-404)
// ---------------------------------------------------------------------------

/** The closed set of ingress event outcomes recorded in ingress_log (D5, FR-11). */
export type IngressOutcome =
  | 'accepted'
  | 'duplicate'
  | 'rejected_auth'
  | 'rejected_unknown_flow'
  | 'rejected_malformed'
  // A bare thread reply that cannot be attributed to one of several live HITL
  // asks on the same thread root (Fix 4) — refused rather than guessed.
  | 'rejected_ambiguous'
  | 'spawn_failed'
  | 'redriven'
  // Issue #7: the launched run halted PARKED behind a provider rate limit
  // (runs.outcome = 'parked'). Not a failure — the row stays 'spawned' and the
  // listener resumes the run once its gate passes. Logged on every park; the
  // alert for it fires once per event (see ingress/parked.ts recordPark).
  | 'parked'
  // The original listener-backpressure work: accepted but no run slot free — the row stays 'accepted' in
  // ingress_events and the re-drive sweep launches it as slots free.
  | 'queued';

export interface IngressLogInput {
  source: string;
  /** Absent or null when the event was rejected before id derivation. */
  eventId?: string | null;
  /** Any value outside the closed set is rejected at runtime (throws before insert). */
  outcome: IngressOutcome;
  reason?: string;
  /** Filtered through filterAttributes before storage (NFR-5). */
  attributes?: Record<string, unknown>;
}

export interface StoredIngressLogEntry {
  source: string;
  eventId: string | null;
  outcome: IngressOutcome;
  reason: string | null;
  /** Already secret-filtered. */
  attributes: Record<string, unknown>;
}

export interface IngressLogFilter {
  outcome?: IngressOutcome;
  /** Only rows for this event id (issue #7: the once-per-event park alert keys on the log). */
  eventId?: string;
  /** Only return rows with id strictly greater than this cursor (keyset pagination). */
  sinceId?: number;
  /** Cap the number of rows returned. Defaults to DEFAULT_INGRESS_LOG_LIMIT. */
  limit?: number;
}

/**
 * Default page size for getIngressLog when no explicit limit is given.
 * ingress_log is append-only and unbounded; a busy listener would otherwise
 * load its entire history into memory (The original pre-public hardening work).
 */
export const DEFAULT_INGRESS_LOG_LIMIT = 1000;

/** Runtime guard for the closed IngressOutcome set — validated before every insert. */
const VALID_INGRESS_OUTCOMES: ReadonlySet<string> = new Set<IngressOutcome>([
  'accepted',
  'duplicate',
  'rejected_auth',
  'rejected_unknown_flow',
  'rejected_malformed',
  'rejected_ambiguous',
  'spawn_failed',
  'redriven',
  'parked',
  'queued',
]);

// ---------------------------------------------------------------------------
// Ingress event types (WI-401)
// ---------------------------------------------------------------------------

export type IngressSpawnState = 'accepted' | 'spawned' | 'failed';

/** snake_case fields mirroring the Card record returned by getCard. */
export interface IngressEventRecord {
  event_id: string;
  received_at: number;
  spawn_state: IngressSpawnState;
  spawn_attempts: number;
  /** Owning flow name (v9, the original ingress-attribution work) — null on pre-v9 rows. */
  flow_id: string | null;
  /** Owning flow.yaml path (v9) — null on pre-v9 rows. */
  flow_path: string | null;
  /** Derived run id (v9, deriveIngressRunId) — null on pre-v9 rows. */
  run_id: string | null;
  /** Projected substrate JSON passed as --input-inline (v9) — null on pre-v9 rows. */
  substrate_json: string | null;
}

/**
 * Attribution persisted atomically with the accept (v9, the original ingress-attribution work): everything
 * a later re-drive needs to faithfully relaunch the event — the owning flow,
 * the exact payload, and the derived run id.
 */
export interface IngressAttribution {
  flowId: string;
  flowPath: string;
  runId: string;
  substrateJson: string;
}

export interface IngressAcceptResult {
  /**
   * true  → this call won the accept (fresh insert OR re-drive of a failed row);
   *         caller should spawn.
   * false → an 'accepted'/'spawned' row already exists; suppress the spawn.
   */
  accepted: boolean;
}

/**
 * Result of resolving a Slack thread reply to a posted HITL ask (Fix 4).
 *
 * A single thread root can address SEVERAL asks at once — in a fan-out each
 * child parks its own rank+HITL under the same triggering-message ts. A bare
 * reply ("1") carries no correlation id, so when two or more asks under that
 * root are still LIVE (unanswered) there is no sound way to tell which one the
 * human meant. That is surfaced as `{ ambiguous: true }` rather than guessing
 * the newest; the listener journals `rejected_ambiguous` and leaves every card
 * held (fail-closed, never-guess). A single addressed ask — or exactly one live
 * ask among older answered ones — resolves normally as `{ ambiguous: false }`.
 */
export type HitlAskLookup =
  | { ambiguous: true; correlationIds: string[] }
  | {
      ambiguous: false;
      runId: string;
      cardId: string;
      correlationId: string;
      shortList: string[];
    };

/** Raw ingress_events row shape as returned by bun:sqlite. */
interface RawIngressEventRow {
  event_id: string;
  received_at: number;
  spawn_state: string;
  spawn_attempts: number;
  flow_id: string | null;
  flow_path: string | null;
  run_id: string | null;
  substrate_json: string | null;
}

/**
 * Project a raw ingress_events row onto the record the listener reads, naming
 * the `spawn_state` column's string as the state union. Attribution columns
 * (`flow_id`, `flow_path`, `run_id`) are nullable because a pre-v9 row predates
 * them — callers must handle the null rather than assume an owning flow.
 */
function toIngressEventRecord(row: RawIngressEventRow): IngressEventRecord {
  return {
    event_id: row.event_id,
    received_at: row.received_at,
    spawn_state: row.spawn_state as IngressSpawnState,
    spawn_attempts: row.spawn_attempts,
    flow_id: row.flow_id,
    flow_path: row.flow_path,
    run_id: row.run_id,
    substrate_json: row.substrate_json,
  };
}

// ---------------------------------------------------------------------------

export interface RunRecord {
  run_id: string;
  flow: string;
  project_root: string | null;
  input_fingerprint: string;
  status: string;
  outcome: string | null;
  created_at: number;
  /** Advisory run-lock holder (the original run-lock and busy-retry work); null when no process holds the lease. */
  holder_pid?: number | null;
  lease_acquired_at?: number | null;
}

export interface RunInput {
  run_id: string;
  flow: string;
  project_root?: string | null;
  input_fingerprint: string;
  status: string;
  outcome?: string;
}

export interface ConduitDB {
  /** Insert a card row. Throws on duplicate (run_id, id). */
  insertCard(card: Card): void;
  /** Return the card with this run_id and id, or null if not found. */
  getCard(runId: string, id: string): Card | null;
  /** Insert a run record. Throws on duplicate run_id. */
  insertRun(run: RunInput): void;
  /** Return the run record for runId, or null if not found. */
  getRun(runId: string): RunRecord | null;
  /**
   * Record an ingress event for idempotent delivery.
   * @throws if event_id is already present (PRIMARY KEY violation).
   */
  recordIngressEvent(eventId: string, receivedAt: number): void;
  /**
   * Atomically accept an ingress event before spawning.
   *
   * Single statement — no read-then-write race (NFR-2):
   *   - Fresh event_id → INSERT with spawn_state='accepted', spawn_attempts=0 → accepted: true
   *   - event_id already 'failed' → UPDATE spawn_state='accepted' (preserve spawn_attempts) → accepted: true
   *   - event_id already 'accepted' or 'spawned' → no-op → accepted: false
   *
   * When attribution is supplied it is written in the SAME atomic statement
   * (v9, the original ingress-attribution work) — a crash after a winning accept can never leave a row
   * that re-drive cannot attribute. On the failed→accepted re-drive path,
   * supplied attribution overwrites; absent attribution preserves what exists.
   */
  acceptIngressEvent(
    eventId: string,
    receivedAt: number,
    attribution?: IngressAttribution,
  ): IngressAcceptResult;
  /** Transition an 'accepted' row to 'spawned'. */
  markIngressSpawned(eventId: string): void;
  /** Transition a row to 'failed'. Does NOT increment spawn_attempts. */
  markIngressFailed(eventId: string): void;
  /**
   * Increment spawn_attempts for eventId in-place. Caller must invoke this
   * BEFORE the spawn seam so a crash mid-spawn leaves a counted 'accepted' row
   * that boot recovery can pick up via listRedrivable.
   */
  incrementSpawnAttempts(eventId: string): void;
  /** Return the stored record for eventId, or null if not found. */
  getIngressEvent(eventId: string): IngressEventRecord | null;
  /**
   * Return the substrate_json of the EARLIEST (received_at ASC) ingress_events
   * row attributed to runId, or null when the run has no ingress event (e.g. a
   * CLI-triggered run with no triggering Slack/webhook event) or the event
   * carries no substrate (WI-599, FR-6/FR-8: thread-address resolution). A run
   * spawned from several redrive attempts of the same event may have more than
   * one row; the earliest is the originating trigger.
   */
  getIngressSubstrateForRun(runId: string): string | null;
  /**
   * Return rows in spawn_state 'accepted' or 'failed' whose spawn_attempts is
   * strictly under cap, ordered by received_at ASC so a queued burst launches
   * in arrival order (the original listener-backpressure work). Used for boot recovery (FR-3) and the
   * periodic re-drive sweep.
   */
  listRedrivable(cap: number): IngressEventRecord[];
  /**
   * Append a span to the journal. Sensitive attributes are filtered first.
   * Uses the journal connection — never blocks on the state write-lock.
   */
  appendJournalSpan(span: JournalSpanInput): void;
  /**
   * Return the OTel-GenAI-aligned token/cost row for (cardId, station, attempt),
   * summed across ALL usage spans recorded for that triple, or null if none was
   * recorded.
   */
  getStationUsage(
    cardId: string,
    station: string,
    attempt: number,
  ): Record<string, unknown> | null;
  /** Return all journal spans for a card, ordered by insertion. */
  getJournalSpans(cardId: string): StoredJournalSpan[];
  /**
   * Return journal spans scoped to a specific run and card, ordered by insertion.
   * Use this instead of getJournalSpans when cross-run isolation is required.
   */
  getJournalSpansForRun(runId: string, cardId: string): StoredJournalSpan[];
  /**
   * Total journaled token/cost spend for an entire run (the original multi-flow engine work): a parent
   * flow attributes a subflow child's spend into its own budgets from this.
   */
  getRunUsageTotals(runId: string): { tokens: number; costUsd: number };
  /**
   * Append an ingress event outcome to the observability log (D5, FR-11).
   *
   * Validates that `outcome` is a member of the closed IngressOutcome set —
   * throws BEFORE the insert on an unknown value, leaving no row. Attributes
   * are filtered through filterAttributes before storage (NFR-5).
   *
   * Uses the journal connection — never blocks on the state write-lock (NFR-1).
   */
  appendIngressLog(entry: IngressLogInput): void;
  /**
   * Return ingress_log rows in insertion order. When filter.outcome is set,
   * returns only rows with that outcome. Paginated (The original pre-public hardening work): filter.sinceId
   * applies a keyset cursor (id > sinceId) and filter.limit caps the page size
   * (default DEFAULT_INGRESS_LOG_LIMIT) so this never loads the unbounded
   * append-only table fully into memory. Never throws on an empty result.
   *
   * Uses the journal connection.
   */
  getIngressLog(filter?: IngressLogFilter): StoredIngressLogEntry[];
  /**
   * Append a transition entry to the per-card log.
   *
   * The full entry is run through the sensitive-key filter before storage so
   * credentials embedded in unexpected payload keys never reach the journal
   * file (NFR-5). The insert is IGNORE on the UNIQUE(card_id, station,
   * attempt, kind) key, making replay idempotent (NFR-3, FR-7).
   *
   * Uses the journal connection — never blocks on the state write-lock.
   */
  appendCardLog(entry: CardLogEntryInput): void;
  /**
   * Resolve which run a HITL correlation id belongs to (the original HITL reply-and-resume work).
   *
   * The await_selection path appends a card_log terminal entry whose `reason`
   * IS the correlation id (executeRankStation, WI-398); the correlation id
   * itself carries no run id, so an inbound Slack reply — an interactive
   * envelope or a thread reply — needs this lookup to call applyHitlReply
   * with the right run scope. Returns the MOST RECENT run that recorded the
   * correlation id, or null when unknown (the reply is then refused, mirroring
   * applyHitlReply's unknown-id posture).
   */
  findRunForHitlCorrelation(correlationId: string): string | null;
  /**
   * Resolve the flow.yaml path an ingress-spawned run was launched with
   * (the original HITL reply-and-resume work) — a successful HITL reply spawns `conduit resume` for the
   * parked run, and resume needs the flow path. Returns null for runs with
   * no ingress attribution (CLI-triggered): those resume manually, journaled.
   */
  getFlowPathForRun(runId: string): string | null;
  /**
   * Look up a posted HITL ask by its Slack message ts (the original HITL reply-and-resume work thread-reply
   * routing). The await_selection path journals a `hitl.ask` span carrying
   * {correlation_id, channel, ts, short_list}; an inbound channel message whose
   * thread_ts equals a recorded ask ts is a selection reply, not a trigger.
   * Returns the resolved ask, an ambiguity sentinel when two or more asks under
   * this thread root are still live (Fix 4), or null when no ask matches.
   */
  findHitlAskByThreadTs(threadTs: string): HitlAskLookup | null;
  /**
   * Return all card_log entries for a card, ordered by insertion (id ASC).
   * Returns an empty array when the card has no entries — never throws.
   */
  getCardLog(cardId: string): StoredCardLogEntry[];
  /**
   * Return card_log entries scoped to a specific run and card, ordered by insertion (id ASC).
   * Use this instead of getCardLog when cross-run isolation is required.
   */
  getCardLogForRun(runId: string, cardId: string): StoredCardLogEntry[];
  /**
   * Expose the raw state Database so the dispatch layer (claim.ts) can run
   * active_workers + cards writes in a single atomic `BEGIN IMMEDIATE`
   * transaction on one connection — the only safe linearisation point.
   *
   * Callers outside src/dispatch/ should not use this accessor.
   */
  getStateDb(): Database;
  /** Delete all rows belonging to runId across all per-run tables. Safe no-op for unknown ids. */
  deleteRun(runId: string): void;
  /** Close both database connections. */
  close(): void;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class ConduitDBImpl implements ConduitDB {
  constructor(
    private readonly stateDb: Database,
    private readonly journalDb: Database,
  ) {}

  insertCard(card: Card): void {
    this.stateDb
      .prepare(
        `INSERT INTO cards (run_id, id, parent_id, lane, status, attempt, wave, owned_paths, rework_count)
         VALUES ($run_id, $id, $parent_id, $lane, $status, $attempt, $wave, $owned_paths, $rework_count)`,
      )
      .run({
        $run_id: card.run_id,
        $id: card.id,
        $parent_id: card.parent_id,
        $lane: card.lane,
        $status: card.status,
        $attempt: card.attempt,
        $wave: card.wave,
        $owned_paths: JSON.stringify(card.owned_paths),
        $rework_count: card.rework_count ?? 0,
      });
  }

  getCard(runId: string, id: string): Card | null {
    const row = this.stateDb
      .prepare(
        `SELECT run_id, id, parent_id, lane, status, attempt, wave, owned_paths, rework_count
         FROM cards WHERE run_id = $run_id AND id = $id`,
      )
      .get({ $run_id: runId, $id: id }) as {
        run_id: string;
        id: string;
        parent_id: string | null;
        lane: string;
        status: string;
        attempt: number;
        wave: number;
        owned_paths: string;
        rework_count: number;
      } | undefined;

    if (!row) return null;

    return {
      run_id: row.run_id,
      id: row.id,
      parent_id: row.parent_id,
      lane: validateLane(row.id, row.lane),
      status: validateStatus(row.id, row.status),
      attempt: row.attempt,
      wave: row.wave,
      owned_paths: parseColumn(row.id, 'owned_paths', row.owned_paths) as string[],
      rework_count: row.rework_count,
    };
  }

  insertRun(run: RunInput): void {
    this.stateDb
      .prepare(
        `INSERT INTO runs (run_id, flow, project_root, input_fingerprint, status, outcome)
         VALUES ($run_id, $flow, $project_root, $input_fingerprint, $status, $outcome)`,
      )
      .run({
        $run_id: run.run_id,
        $flow: run.flow,
        $project_root: run.project_root ?? null,
        $input_fingerprint: run.input_fingerprint,
        $status: run.status,
        $outcome: run.outcome ?? null,
      });
  }

  getRun(runId: string): RunRecord | null {
    const row = this.stateDb
      .prepare(
        `SELECT run_id, flow, project_root, input_fingerprint, status, outcome, created_at,
                holder_pid, lease_acquired_at
         FROM runs WHERE run_id = $run_id`,
      )
      .get({ $run_id: runId }) as RunRecord | undefined;

    return row ?? null;
  }

  recordIngressEvent(eventId: string, receivedAt: number): void {
    this.stateDb
      .prepare(
        `INSERT INTO ingress_events (event_id, received_at) VALUES ($event_id, $received_at)`,
      )
      .run({ $event_id: eventId, $received_at: receivedAt });
  }

  acceptIngressEvent(
    eventId: string,
    receivedAt: number,
    attribution?: IngressAttribution,
  ): IngressAcceptResult {
    // Single atomic statement — mirrors claim.ts linearization (changes > 0 idiom).
    // ON CONFLICT DO UPDATE with a WHERE clause: the update only fires for 'failed'
    // rows, so 'accepted'/'spawned' rows keep changes === 0 (duplicate suppressed).
    // spawn_attempts is intentionally NOT reset — the cap still bites after re-drive.
    // Attribution (v9) rides the same statement: COALESCE(excluded.…, …) means a
    // caller that supplies attribution overwrites, one that doesn't preserves the
    // stored values (pre-v9 rows and legacy callers keep working unchanged).
    const result = this.stateDb
      .prepare(
        `INSERT INTO ingress_events
           (event_id, received_at, spawn_state, spawn_attempts,
            flow_id, flow_path, run_id, substrate_json)
         VALUES ($event_id, $received_at, 'accepted', 0,
                 $flow_id, $flow_path, $run_id, $substrate_json)
         ON CONFLICT(event_id) DO UPDATE SET
           spawn_state = 'accepted',
           flow_id = COALESCE(excluded.flow_id, flow_id),
           flow_path = COALESCE(excluded.flow_path, flow_path),
           run_id = COALESCE(excluded.run_id, run_id),
           substrate_json = COALESCE(excluded.substrate_json, substrate_json)
         WHERE spawn_state = 'failed'`,
      )
      .run({
        $event_id: eventId,
        $received_at: receivedAt,
        $flow_id: attribution?.flowId ?? null,
        $flow_path: attribution?.flowPath ?? null,
        $run_id: attribution?.runId ?? null,
        $substrate_json: attribution?.substrateJson ?? null,
      });

    return { accepted: result.changes > 0 };
  }

  markIngressSpawned(eventId: string): void {
    this.stateDb
      .prepare(
        `UPDATE ingress_events SET spawn_state = 'spawned' WHERE event_id = $event_id`,
      )
      .run({ $event_id: eventId });
  }

  markIngressFailed(eventId: string): void {
    this.stateDb
      .prepare(
        `UPDATE ingress_events SET spawn_state = 'failed' WHERE event_id = $event_id`,
      )
      .run({ $event_id: eventId });
  }

  incrementSpawnAttempts(eventId: string): void {
    this.stateDb
      .prepare(
        `UPDATE ingress_events SET spawn_attempts = spawn_attempts + 1 WHERE event_id = $event_id`,
      )
      .run({ $event_id: eventId });
  }

  getIngressEvent(eventId: string): IngressEventRecord | null {
    const row = this.stateDb
      .prepare(
        `SELECT event_id, received_at, spawn_state, spawn_attempts,
                flow_id, flow_path, run_id, substrate_json
         FROM ingress_events
         WHERE event_id = $event_id`,
      )
      .get({ $event_id: eventId }) as RawIngressEventRow | undefined;

    if (!row) return null;

    return toIngressEventRecord(row);
  }

  getIngressSubstrateForRun(runId: string): string | null {
    const row = this.stateDb
      .prepare(
        `SELECT substrate_json FROM ingress_events
         WHERE run_id = $run_id
         ORDER BY received_at ASC
         LIMIT 1`,
      )
      .get({ $run_id: runId }) as { substrate_json: string | null } | undefined;

    return row?.substrate_json ?? null;
  }

  listRedrivable(cap: number): IngressEventRecord[] {
    const rows = this.stateDb
      .prepare(
        `SELECT event_id, received_at, spawn_state, spawn_attempts,
                flow_id, flow_path, run_id, substrate_json
         FROM ingress_events
         WHERE spawn_state IN ('accepted', 'failed')
           AND spawn_attempts < $cap
         ORDER BY received_at ASC`,
      )
      .all({ $cap: cap }) as RawIngressEventRow[];

    return rows.map(toIngressEventRecord);
  }

  appendJournalSpan(span: JournalSpanInput): void {
    const safeAttrs = filterAttributes(span.attributes ?? {});
    this.journalDb
      .prepare(
        `INSERT INTO journal
           (run_id, card_id, station, attempt, name, attributes_json,
            model, input_tokens, output_tokens, cost_usd,
            adapter, duration_ms, usage_unknown,
            cache_read_input_tokens, cache_creation_input_tokens)
         VALUES
           ($run_id, $card_id, $station, $attempt, $name, $attributes_json,
            $model, $input_tokens, $output_tokens, $cost_usd,
            $adapter, $duration_ms, $usage_unknown,
            $cache_read_input_tokens, $cache_creation_input_tokens)`,
      )
      .run({
        $run_id: span.runId,
        $card_id: span.cardId,
        $station: span.station,
        $attempt: span.attempt,
        $name: span.name,
        $attributes_json: JSON.stringify(safeAttrs),
        $model: span.usage?.model ?? null,
        $input_tokens: span.usage?.inputTokens ?? null,
        $output_tokens: span.usage?.outputTokens ?? null,
        $cost_usd: span.usage?.costUsd ?? null,
        $adapter: span.adapter ?? null,
        $duration_ms: span.durationMs ?? null,
        $usage_unknown: span.usageUnknown === true ? 1 : 0,
        $cache_read_input_tokens: span.usage?.cacheReadInputTokens ?? null,
        $cache_creation_input_tokens: span.usage?.cacheCreationInputTokens ?? null,
      });
  }

  getStationUsage(
    cardId: string,
    station: string,
    attempt: number,
  ): Record<string, unknown> | null {
    // Cost is the SUM across every usage span for the (card, station, attempt)
    // triple (finding #9). A single attempt can make multiple billed calls
    // (e.g. a tool-loop turn); the previous LIMIT-1-with-no-ORDER-BY picked one
    // non-deterministic row and under-counted. MAX(model) yields a stable model
    // label for the triple; this assumes all spans within one attempt share the
    // same model (the common case — multi-model attempts are not yet supported).
    // n counts the rows that actually carried usage so we can return null when
    // none did (SUM over zero rows is NULL).
    const row = this.journalDb
      .prepare(
        `SELECT
           COUNT(*)             AS n,
           MAX(model)           AS model,
           SUM(input_tokens)    AS input_tokens,
           SUM(output_tokens)   AS output_tokens,
           SUM(cache_read_input_tokens)     AS cache_read_input_tokens,
           SUM(cache_creation_input_tokens) AS cache_creation_input_tokens,
           SUM(cost_usd)        AS cost_usd
         FROM journal
         WHERE card_id = $card_id
           AND station = $station
           AND attempt = $attempt
           AND input_tokens IS NOT NULL`,
      )
      .get({ $card_id: cardId, $station: station, $attempt: attempt }) as {
        n: number;
        model: string | null;
        input_tokens: number | null;
        output_tokens: number | null;
        cache_read_input_tokens: number | null;
        cache_creation_input_tokens: number | null;
        cost_usd: number | null;
      } | undefined;

    // No usage rows for this triple → no cost attribution.
    if (!row || row.n === 0) return null;

    return {
      'gen_ai.usage.input_tokens': row.input_tokens ?? 0,
      'gen_ai.usage.output_tokens': row.output_tokens ?? 0,
      // Not OTel-GenAI standard names, but the cache split is the whole point
      // of issue #5: without it "what fraction of spend is cache reads" is not
      // answerable, and neither is a cross-provider output-cost comparison.
      'gen_ai.usage.cache_read_input_tokens': row.cache_read_input_tokens ?? 0,
      'gen_ai.usage.cache_creation_input_tokens': row.cache_creation_input_tokens ?? 0,
      'gen_ai.request.model': row.model,
      cost_usd: row.cost_usd ?? 0,
    };
  }

  getJournalSpans(cardId: string): StoredJournalSpan[] {
    const rows = this.journalDb
      .prepare(
        `SELECT card_id, station, attempt, name, attributes_json,
                adapter, duration_ms, usage_unknown
         FROM journal
         WHERE card_id = $card_id
         ORDER BY id ASC`,
      )
      .all({ $card_id: cardId }) as {
        card_id: string;
        station: string;
        attempt: number;
        name: string;
        attributes_json: string;
        adapter: string | null;
        duration_ms: number | null;
        usage_unknown: number;
      }[];

    return rows.map((row) => ({
      cardId: row.card_id,
      station: row.station,
      attempt: row.attempt,
      name: row.name,
      attributes: parseColumn(
        row.card_id,
        'attributes_json',
        row.attributes_json,
      ) as Record<string, unknown>,
      adapter: row.adapter,
      durationMs: row.duration_ms,
      usageUnknown: row.usage_unknown === 1,
    }));
  }

  getRunUsageTotals(runId: string): { tokens: number; costUsd: number } {
    const row = this.journalDb
      .prepare(
        // ALL FOUR token columns. input_tokens is uncached input only (issue
        // #5), so summing just input+output would drop cache reads — the
        // majority of real traffic — and the run budget would stop tripping.
        // Pre-#5 rows carry the old summed total in input_tokens with the two
        // cache columns NULL, so COALESCE keeps them counted exactly once.
        `SELECT
           COALESCE(SUM(
             COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)
             + COALESCE(cache_read_input_tokens, 0)
             + COALESCE(cache_creation_input_tokens, 0)
           ), 0) AS tokens,
           COALESCE(SUM(COALESCE(cost_usd, 0)), 0) AS cost_usd
         FROM journal
         WHERE run_id = $run_id`,
      )
      .get({ $run_id: runId }) as { tokens: number; cost_usd: number };
    return { tokens: row.tokens, costUsd: row.cost_usd };
  }

  getJournalSpansForRun(runId: string, cardId: string): StoredJournalSpan[] {
    const rows = this.journalDb
      .prepare(
        `SELECT card_id, station, attempt, name, attributes_json,
                adapter, duration_ms, usage_unknown
         FROM journal
         WHERE run_id = $run_id AND card_id = $card_id
         ORDER BY id ASC`,
      )
      .all({ $run_id: runId, $card_id: cardId }) as {
        card_id: string;
        station: string;
        attempt: number;
        name: string;
        attributes_json: string;
        adapter: string | null;
        duration_ms: number | null;
        usage_unknown: number;
      }[];

    return rows.map((row) => ({
      cardId: row.card_id,
      station: row.station,
      attempt: row.attempt,
      name: row.name,
      attributes: parseColumn(
        row.card_id,
        'attributes_json',
        row.attributes_json,
      ) as Record<string, unknown>,
      adapter: row.adapter,
      durationMs: row.duration_ms,
      usageUnknown: row.usage_unknown === 1,
    }));
  }

  appendIngressLog(entry: IngressLogInput): void {
    // Validate outcome before touching the DB — a bad value leaves no row.
    if (!VALID_INGRESS_OUTCOMES.has(entry.outcome)) {
      throw new Error(
        `invalid IngressOutcome '${entry.outcome}' — must be one of: ${[...VALID_INGRESS_OUTCOMES].join(', ')}`,
      );
    }

    const safeAttrs = filterAttributes(entry.attributes ?? {});

    this.journalDb
      .prepare(
        `INSERT INTO ingress_log (source, event_id, outcome, reason, attributes_json)
         VALUES ($source, $event_id, $outcome, $reason, $attributes_json)`,
      )
      .run({
        $source: entry.source,
        $event_id: entry.eventId ?? null,
        $outcome: entry.outcome,
        $reason: entry.reason ?? null,
        $attributes_json: JSON.stringify(safeAttrs),
      });
  }

  getIngressLog(filter?: IngressLogFilter): StoredIngressLogEntry[] {
    // Bound the result set (The original pre-public hardening work): ingress_log is append-only and unbounded,
    // so a full SELECT would grow without limit. Apply an optional outcome
    // filter, an optional keyset cursor (id > sinceId), and always a LIMIT.
    const conditions: string[] = [];
    const params: Record<string, string | number> = {};

    if (filter?.outcome) {
      conditions.push('outcome = $outcome');
      params.$outcome = filter.outcome;
    }
    if (filter?.eventId !== undefined) {
      conditions.push('event_id = $eventId');
      params.$eventId = filter.eventId;
    }
    if (filter?.sinceId !== undefined) {
      conditions.push('id > $sinceId');
      params.$sinceId = filter.sinceId;
    }
    params.$limit = filter?.limit ?? DEFAULT_INGRESS_LOG_LIMIT;

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const rows = this.journalDb
      .prepare(
        `SELECT source, event_id, outcome, reason, attributes_json
         FROM ingress_log
         ${whereClause}
         ORDER BY id ASC
         LIMIT $limit`,
      )
      .all(params) as {
      source: string;
      event_id: string | null;
      outcome: string;
      reason: string | null;
      attributes_json: string;
    }[];

    return rows.map((row) => ({
      source: row.source,
      eventId: row.event_id,
      outcome: row.outcome as IngressOutcome,
      reason: row.reason,
      attributes: JSON.parse(row.attributes_json) as Record<string, unknown>,
    }));
  }

  getFlowPathForRun(runId: string): string | null {
    const row = this.stateDb
      .prepare(
        `SELECT flow_path FROM ingress_events
         WHERE run_id = $run_id AND flow_path IS NOT NULL
         ORDER BY received_at DESC LIMIT 1`,
      )
      .get({ $run_id: runId }) as { flow_path: string } | null;
    return row?.flow_path ?? null;
  }

  findRunForHitlCorrelation(correlationId: string): string | null {
    // card_log rows are append-only; the newest row for this correlation id is
    // the live hold (a re-ask on a later attempt appends a fresh row).
    const row = this.journalDb
      .prepare(
        `SELECT run_id FROM card_log
         WHERE kind = 'terminal' AND reason = $reason
         ORDER BY id DESC LIMIT 1`,
      )
      .get({ $reason: correlationId }) as { run_id: string } | null;
    return row?.run_id ?? null;
  }

  findHitlAskByThreadTs(threadTs: string): HitlAskLookup | null {
    // json_extract over the filtered attributes — the ask span is written by
    // the kernel itself (executeRankStation), so the shape is trusted here;
    // malformed attributes yield null fields and are skipped below. Fetch ALL
    // asks under this thread root, newest first: a fan-out parks several asks
    // on one triggering-message ts, and a bare reply must be disambiguated
    // across them (Fix 4).
    const rows = this.journalDb
      .prepare(
        `SELECT run_id, card_id, attributes_json FROM journal
         WHERE name = 'hitl.ask'
           AND json_extract(attributes_json, '$.ts') = $ts
         ORDER BY id DESC`,
      )
      .all({ $ts: threadTs }) as {
      run_id: string;
      card_id: string;
      attributes_json: string;
    }[];
    if (rows.length === 0) return null;

    type Ask = { runId: string; cardId: string; correlationId: string; shortList: string[] };
    const asks: Ask[] = [];
    for (const row of rows) {
      let attrs: Record<string, unknown>;
      try {
        attrs = JSON.parse(row.attributes_json) as Record<string, unknown>;
      } catch {
        continue;
      }
      const correlationId = attrs['correlation_id'];
      if (typeof correlationId !== 'string' || correlationId.length === 0) continue;
      const shortList = Array.isArray(attrs['short_list'])
        ? (attrs['short_list'] as unknown[]).filter((x): x is string => typeof x === 'string')
        : [];
      asks.push({ runId: row.run_id, cardId: row.card_id, correlationId, shortList });
    }
    if (asks.length === 0) return null;

    // Collapse to distinct correlation ids, newest-first (first occurrence wins
    // — a re-ask replaying the same correlation id is one logical ask).
    const distinct: Ask[] = [];
    const seen = new Set<string>();
    for (const ask of asks) {
      if (seen.has(ask.correlationId)) continue;
      seen.add(ask.correlationId);
      distinct.push(ask);
    }

    // A single addressed ask resolves normally — even once answered, so a late
    // bare reply is still journaled 'duplicate' downstream rather than dropped.
    if (distinct.length === 1) {
      return { ambiguous: false, ...distinct[0]! };
    }

    // Several asks share this thread root. A bare reply carries no correlation
    // id, so disambiguate by liveness: an ask is ANSWERED once a hitl.selection
    // span records its correlation id (the same journal signal applyHitlReply
    // writes on a successful pick). Restrict to the correlation ids already
    // fetched for THIS thread root (the pre-public ingress-deduplication review) — there are only ever a
    // handful (one fan-out's worth), so this is a bounded lookup rather than an
    // unfiltered scan of every hitl.selection span ever journaled.
    const answered = new Set<string>();
    const correlationIds = distinct.map((ask) => ask.correlationId);
    const placeholders = correlationIds.map(() => '?').join(', ');
    const selRows = this.journalDb
      .prepare(
        `SELECT DISTINCT json_extract(attributes_json, '$.correlation_id') AS cid
         FROM journal
         WHERE name = 'hitl.selection'
           AND json_extract(attributes_json, '$.correlation_id') IN (${placeholders})`,
      )
      .all(...correlationIds) as { cid: string | null }[];
    for (const sel of selRows) {
      if (typeof sel.cid === 'string' && sel.cid.length > 0) answered.add(sel.cid);
    }
    const live = distinct.filter((ask) => !answered.has(ask.correlationId));

    if (live.length >= 2) {
      // Genuinely ambiguous — refuse and hold, never guess (SPEC never-guess).
      return { ambiguous: true, correlationIds: live.map((a) => a.correlationId) };
    }
    if (live.length === 1) {
      // Exactly one ask is still open; a reply unambiguously targets it.
      return { ambiguous: false, ...live[0]! };
    }
    // Every addressed ask is answered — resolve to the newest so a late reply is
    // journaled 'duplicate' downstream rather than silently dropped.
    return { ambiguous: false, ...distinct[0]! };
  }

  appendCardLog(entry: CardLogEntryInput): void {
    // Screen the entire entry through the sensitive-key filter before binding
    // any column — drops credential keys (api_key, authorization, …) that a
    // caller might inject as forward-compat payload (NFR-5).
    const safe = filterAttributes(entry as unknown as Record<string, unknown>);

    const kind = entry.kind;
    let sourceLane: string | null = null;
    let destLane: string | null = null;
    let reasonClass: string | null = null;
    let verdict: string | null = null;
    let findingsJson: string | null = null;
    let returnTo: string | null = null;
    let reason: string | null = null;

    if (kind === 'entered_lane') {
      sourceLane = safe.sourceLane as string;
      destLane = safe.destLane as string;
      reasonClass = safe.reasonClass as string;
    } else if (kind === 'gate_verdict') {
      verdict = safe.verdict as string;
      const rawFindings = (safe.findings as unknown[]) ?? [];
      // Truncate each finding to the shared cap before serialising (NFR-4).
      // Short text is stored verbatim; only strings beyond the cap are sliced.
      findingsJson = JSON.stringify(
        rawFindings.map((f) =>
          typeof f === 'string' ? f.slice(0, CARD_LOG_FINDING_MAX_LENGTH) : String(f),
        ),
      );
      returnTo = (safe.returnTo as string | null) ?? null;
    } else if (kind === 'terminal') {
      reason = safe.reason as string;
    }

    // INSERT OR IGNORE implements idempotency: a duplicate (run_id,card,station,attempt,kind)
    // from a crash/resume replay silently leaves the existing row unchanged (NFR-3).
    this.journalDb
      .prepare(
        `INSERT OR IGNORE INTO card_log
           (run_id, card_id, station, attempt, kind,
            source_lane, dest_lane, reason_class,
            verdict, findings_json, return_to,
            reason)
         VALUES
           ($run_id, $card_id, $station, $attempt, $kind,
            $source_lane, $dest_lane, $reason_class,
            $verdict, $findings_json, $return_to,
            $reason)`,
      )
      .run({
        $run_id: entry.runId ?? DEFAULT_RUN_ID,
        $card_id: entry.cardId,
        $station: entry.station,
        $attempt: entry.attempt,
        $kind: kind,
        $source_lane: sourceLane,
        $dest_lane: destLane,
        $reason_class: reasonClass,
        $verdict: verdict,
        $findings_json: findingsJson,
        $return_to: returnTo,
        $reason: reason,
      });
  }

  getCardLog(cardId: string): StoredCardLogEntry[] {
    const rows = this.journalDb
      .prepare(
        `SELECT run_id, card_id, station, attempt, kind,
                source_lane, dest_lane, reason_class,
                verdict, findings_json, return_to,
                reason
         FROM card_log
         WHERE card_id = $card_id
         ORDER BY id ASC`,
      )
      .all({ $card_id: cardId }) as {
        run_id: string;
        card_id: string;
        station: string;
        attempt: number;
        kind: string;
        source_lane: string | null;
        dest_lane: string | null;
        reason_class: string | null;
        verdict: string | null;
        findings_json: string | null;
        return_to: string | null;
        reason: string | null;
      }[];

    return rows.map((row): StoredCardLogEntry => {
      const base: CardLogBase = {
        runId: row.run_id,
        cardId: row.card_id,
        station: row.station,
        attempt: row.attempt,
      };

      if (row.kind === 'entered_lane') {
        return {
          ...base,
          kind: 'entered_lane',
          sourceLane: row.source_lane ?? '',
          destLane: row.dest_lane ?? '',
          reasonClass: (row.reason_class ?? 'forward') as ReasonClass,
        };
      }

      if (row.kind === 'gate_verdict') {
        const findings =
          row.findings_json !== null
            ? (parseColumn(row.card_id, 'findings_json', row.findings_json) as string[])
            : [];
        return {
          ...base,
          kind: 'gate_verdict',
          verdict: (row.verdict ?? 'pass') as GateVerdict,
          findings,
          returnTo: row.return_to,
        };
      }

      if (row.kind === 'terminal') {
        return {
          ...base,
          kind: 'terminal',
          reason: row.reason ?? '',
        };
      }

      throw new Error(
        `corrupt card_log row for card '${row.card_id}': unknown kind '${row.kind}'`,
      );
    });
  }

  getCardLogForRun(runId: string, cardId: string): StoredCardLogEntry[] {
    const rows = this.journalDb
      .prepare(
        `SELECT run_id, card_id, station, attempt, kind,
                source_lane, dest_lane, reason_class,
                verdict, findings_json, return_to,
                reason
         FROM card_log
         WHERE run_id = $run_id AND card_id = $card_id
         ORDER BY id ASC`,
      )
      .all({ $run_id: runId, $card_id: cardId }) as {
        run_id: string;
        card_id: string;
        station: string;
        attempt: number;
        kind: string;
        source_lane: string | null;
        dest_lane: string | null;
        reason_class: string | null;
        verdict: string | null;
        findings_json: string | null;
        return_to: string | null;
        reason: string | null;
      }[];

    return rows.map((row): StoredCardLogEntry => {
      const base: CardLogBase = {
        runId: row.run_id,
        cardId: row.card_id,
        station: row.station,
        attempt: row.attempt,
      };

      if (row.kind === 'entered_lane') {
        return {
          ...base,
          kind: 'entered_lane',
          sourceLane: row.source_lane ?? '',
          destLane: row.dest_lane ?? '',
          reasonClass: (row.reason_class ?? 'forward') as ReasonClass,
        };
      }

      if (row.kind === 'gate_verdict') {
        const findings =
          row.findings_json !== null
            ? (parseColumn(row.card_id, 'findings_json', row.findings_json) as string[])
            : [];
        return {
          ...base,
          kind: 'gate_verdict',
          verdict: (row.verdict ?? 'pass') as GateVerdict,
          findings,
          returnTo: row.return_to,
        };
      }

      if (row.kind === 'terminal') {
        return {
          ...base,
          kind: 'terminal',
          reason: row.reason ?? '',
        };
      }

      throw new Error(
        `corrupt card_log row for card '${row.card_id}': unknown kind '${row.kind}'`,
      );
    });
  }

  getStateDb(): Database {
    return this.stateDb;
  }

  /** Whether a table exists in the state DB (sqlite_master lookup). */
  private stateTableExists(name: string): boolean {
    const row = this.stateDb
      .prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = $n`)
      .get({ $n: name });
    return row != null;
  }

  /**
   * Delete every row belonging to `runId` across both physical databases.
   *
   * Idempotent — safe to re-run. Every statement is an unconditional
   * `DELETE ... WHERE run_id = ?`, so a second call after a complete or partial
   * delete simply removes whatever rows survived and is a no-op once clean.
   *
   * Split-DB atomicity (finding #9): a transaction cannot span the two SQLite
   * files, so a crash *between* them would leave a run half-deleted with no
   * engine-level reconciliation. We therefore delete the JOURNAL db FIRST and the
   * STATE db (which holds the authoritative `runs` and `cards` rows) SECOND. An
   * interrupted delete thus leaves the authoritative state rows intact as the
   * recoverable source of truth; re-running deleteRun (idempotent, above) then
   * cleans up the remainder. Ordering it the other way would orphan journal rows
   * under a run_id whose authoritative rows are already gone.
   */
  deleteRun(runId: string): void {
    // Journal DB first (append-only, non-authoritative).
    this.journalDb.transaction(() => {
      this.journalDb.prepare('DELETE FROM journal WHERE run_id = $r').run({ $r: runId });
      this.journalDb.prepare('DELETE FROM card_log WHERE run_id = $r').run({ $r: runId });
      this.journalDb.prepare('DELETE FROM work_summaries WHERE run_id = $r').run({ $r: runId });
    })();

    // State DB second (authoritative — recoverable source of truth if interrupted).
    this.stateDb.transaction(() => {
      this.stateDb.prepare('DELETE FROM cards WHERE run_id = $r').run({ $r: runId });
      this.stateDb.prepare('DELETE FROM station_outputs WHERE run_id = $r').run({ $r: runId });
      this.stateDb.prepare('DELETE FROM outbox WHERE run_id = $r').run({ $r: runId });
      // checkpoints lives in the state DB but is owned by the checkpoint layer
      // (ensureCheckpointSchema runs against stateDb), NOT by STATE_DDL — so a
      // state DB whose executor never ran has no checkpoints table yet. Guard
      // the delete on existence so deleteRun stays a safe no-op (AC-4) on such a
      // DB. Without this delete a later run reusing the same
      // (run_id, flow, card, station, attempt) reads a stale binding stamp and
      // could skip-on-resume over deleted work (finding #3).
      if (this.stateTableExists('checkpoints')) {
        this.stateDb.prepare('DELETE FROM checkpoints WHERE run_id = $r').run({ $r: runId });
      }
      this.stateDb.prepare('DELETE FROM active_workers WHERE run_id = $r').run({ $r: runId });
      this.stateDb.prepare('DELETE FROM runs WHERE run_id = $r').run({ $r: runId });
    })();
  }

  close(): void {
    this.stateDb.close();
    this.journalDb.close();
  }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Retry a bootstrap pragma on a SQLITE_BUSY ("database is locked") thrown
 * during the COLD-START race: `docs/single-host-concurrency.md` documents N
 * concurrent `conduit run`/`resume` processes against ONE shared DB file pair
 * as a supported deployment pattern, and the very first time two such
 * processes race to create/initialize a brand-new DB file, one can hit
 * SQLITE_BUSY on the connection's OWN bootstrap pragma — before `PRAGMA
 * busy_timeout` has taken effect FOR THAT CONNECTION (busy_timeout governs how
 * a connection waits on FUTURE lock contention, not this initial establishment
 * moment). Once the pragma succeeds, SQLite's own busy_timeout handles every
 * subsequent statement automatically — this wrapper exists ONLY to get a fresh
 * connection through that one unprotected bootstrap window.
 *
 * Uses the shared `withBusyRetry` (busy-retry.ts) so the busy-detection and
 * bounded-backoff logic lives in one tested place. The cold-start throw can
 * arrive WITHOUT a populated `.code`, so `isBusyError`'s message fallback is
 * load-bearing here. A generous 20-attempt budget with a small, tight backoff
 * mirrors the original bootstrap loop that stabilised the cold-start race
 * (harness-projectroot-binding.test.ts AC1): react fast, but keep retrying long
 * enough for the racing process to finish its own bootstrap.
 */
const COLD_START_RETRY: Parameters<typeof withBusyRetry>[1] = {
  attempts: 20,
  baseDelayMs: 10,
  maxDelayMs: 150,
};

/**
 * Open (or create) the split Conduit persistence layer.
 *
 * Fresh state DB (user_version = 0): initialise schema and stamp SCHEMA_VERSION.
 * Existing state DB (user_version = SCHEMA_VERSION): connect as-is.
 * Migratable state DBs (user_version = 2..6): apply the migration ladder and
 * stamp SCHEMA_VERSION. Additive ALTER TABLE steps are safe to re-run when
 * guarded against "duplicate column" errors.
 * Incompatible state DB (user_version < 2 or > SCHEMA_VERSION): throw —
 *   version left untouched so the caller can inspect and recover manually.
 *
 * Journal DB: JOURNAL_DDL is always executed — every statement uses
 * CREATE TABLE/INDEX IF NOT EXISTS so it is fully idempotent and self-heals
 * existing journals that pre-date a new table (e.g. card_log added in v3).
 */
export function openConduitDB({
  stateDbPath,
  journalDbPath,
}: {
  stateDbPath: string;
  journalDbPath: string;
}): ConduitDB {
  // ── State DB ─────────────────────────────────────────────────────────────
  // (a pre-public engine review finding 4a): bun:sqlite's open failure ("unable to open database
  // file") names neither the path nor which of the two DBs it was. Wrap the
  // open so every caller of openConduitDB — not just the production binary —
  // gets a message that at least identifies the file it tried. The env-var
  // hint (CONDUIT_STATE_DB) is added one layer up, in buildProductionDeps,
  // which is the only caller that knows which env var supplied the path.
  let stateDb: Database;
  try {
    stateDb = new Database(stateDbPath);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    throw new Error(`unable to open state database file '${stateDbPath}': ${msg}`);
  }
  withBusyRetry(() => stateDb.exec('PRAGMA busy_timeout = 5000'), COLD_START_RETRY);
  // WAL mode before any transaction: concurrent state-DB readers (tick planner)
  // must not be blocked by BEGIN IMMEDIATE writers (atomic claim). SPEC §11.
  withBusyRetry(() => stateDb.exec('PRAGMA journal_mode = WAL'), COLD_START_RETRY);
  // NOTE (finding #30): we deliberately do NOT enable `PRAGMA foreign_keys`.
  // The schema declares no REFERENCES constraints — the natural parent for
  // journal/work_summaries rows is `cards`, which lives in a SEPARATE physical
  // database file (the journal DB). SQLite foreign keys cannot span database
  // files, so a referential constraint here would be impossible to express and
  // turning the pragma on would only be misleading (it would enforce nothing).
  // Cross-store integrity is maintained by the kernel, not the engine.

  const { user_version } = stateDb.query('PRAGMA user_version').get() as {
    user_version: number;
  };

  // Track which migrations need to run.
  let needsV3ToV4Migration = false;
  let needsV4ToV5Migration = false;
  let needsV5ToV6Migration = false;
  let needsV6ToV7Migration = false;
  let needsV7ToV8Migration = false;
  let needsV8ToV9Migration = false;
  let needsV9ToV10Migration = false;

  const tableExists = (name: string): boolean => {
    const row = stateDb
      .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")
      .get(name);
    return row !== undefined && row !== null;
  };
  const colExists = (table: string, col: string): boolean => {
    const cols = stateDb.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[];
    return cols.some((c) => c.name === col);
  };

  if (user_version === 0) {
    // Fresh DB — create schema then stamp version.
    stateDb.exec(STATE_DDL);
    stateDb.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
  } else if (user_version === 2) {
    // Additive v2→v3 migration: add rework_count column to existing cards table.
    // SQLite does not support IF NOT EXISTS on ALTER TABLE ADD COLUMN, so we catch
    // the "duplicate column name" error and treat it as a no-op (idempotent).
    try {
      stateDb.exec(
        'ALTER TABLE cards ADD COLUMN rework_count INTEGER NOT NULL DEFAULT 0',
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('duplicate column name')) throw err;
    }
    needsV3ToV4Migration = true;
    needsV4ToV5Migration = true;
    needsV5ToV6Migration = true;
    needsV6ToV7Migration = true;
    needsV7ToV8Migration = true;
    needsV8ToV9Migration = true;
    needsV9ToV10Migration = true;
  } else if (user_version === 3) {
    needsV3ToV4Migration = true;
    needsV4ToV5Migration = true;
    needsV5ToV6Migration = true;
    needsV6ToV7Migration = true;
    needsV7ToV8Migration = true;
    needsV8ToV9Migration = true;
    needsV9ToV10Migration = true;
  } else if (user_version === 4) {
    needsV4ToV5Migration = true;
    needsV5ToV6Migration = true;
    needsV6ToV7Migration = true;
    needsV7ToV8Migration = true;
    needsV8ToV9Migration = true;
    needsV9ToV10Migration = true;
  } else if (user_version === 5) {
    needsV5ToV6Migration = true;
    needsV6ToV7Migration = true;
    needsV7ToV8Migration = true;
    needsV8ToV9Migration = true;
    needsV9ToV10Migration = true;
  } else if (user_version === 6) {
    needsV6ToV7Migration = true;
    needsV7ToV8Migration = true;
    needsV8ToV9Migration = true;
    needsV9ToV10Migration = true;
  } else if (user_version === 7) {
    needsV7ToV8Migration = true;
    needsV8ToV9Migration = true;
    needsV9ToV10Migration = true;
  } else if (user_version === 8) {
    needsV8ToV9Migration = true;
    needsV9ToV10Migration = true;
  } else if (user_version === 9) {
    needsV9ToV10Migration = true;
  } else if (user_version !== SCHEMA_VERSION) {
    stateDb.close();
    throw new Error(
      `conduit state DB schema mismatch: found version ${user_version}, ` +
        `expected ${SCHEMA_VERSION}. Manual migration required.`,
    );
  }

  if (needsV3ToV4Migration) {
    // Each ALTER in its own try/catch — wrapping both in one would fail the
    // idempotency test when only the first column is already present.
    try {
      stateDb.exec(
        "ALTER TABLE ingress_events ADD COLUMN spawn_state TEXT NOT NULL DEFAULT 'accepted'",
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('duplicate column name')) throw err;
    }
    try {
      stateDb.exec(
        'ALTER TABLE ingress_events ADD COLUMN spawn_attempts INTEGER NOT NULL DEFAULT 0',
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('duplicate column name')) throw err;
    }
  }

  if (needsV4ToV5Migration) {
    // Additive v4→v5 migration: add pid column to active_workers for dead-PID detection.
    // Two non-fatal cases: column already exists (idempotency), or the table
    // doesn't exist yet (legacy DB fixtures from before active_workers was added).
    try {
      stateDb.exec('ALTER TABLE active_workers ADD COLUMN pid INTEGER');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
    }
  }

  if (needsV5ToV6Migration) {
    // v5→v6: add run_id to all per-run tables. Tables that need a composite PK
    // or composite UNIQUE change require TABLE-RECREATE (SQLite cannot ALTER
    // those in place). Each recreate is guarded: if the table doesn't exist yet
    // (legacy partial fixtures), we skip the copy and let STATE_DDL create it
    // fresh. If run_id is already present (idempotent re-run), we skip too.

    // TABLE-RECREATE for cards (PK: id → (run_id, id)):
    if (tableExists('cards') && !colExists('cards', 'run_id')) {
      stateDb.exec(`
        CREATE TABLE IF NOT EXISTS cards_v6 (
          run_id       TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
          id           TEXT NOT NULL,
          parent_id    TEXT,
          lane         TEXT NOT NULL,
          status       TEXT NOT NULL,
          attempt      INTEGER NOT NULL DEFAULT 0,
          wave         INTEGER NOT NULL DEFAULT 0,
          owned_paths  TEXT NOT NULL DEFAULT '[]',
          rework_count INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (run_id, id)
        )
      `);
      stateDb.exec(`
        INSERT OR IGNORE INTO cards_v6 (run_id, id, parent_id, lane, status, attempt, wave, owned_paths, rework_count)
        SELECT '${DEFAULT_RUN_ID}', id, parent_id, lane, status, attempt, wave, owned_paths, rework_count
        FROM cards
      `);
      stateDb.exec('DROP TABLE IF EXISTS cards');
      stateDb.exec('ALTER TABLE cards_v6 RENAME TO cards');
    }

    // TABLE-RECREATE for active_workers (PK: (card_id, station) → (run_id, card_id, station)):
    if (tableExists('active_workers') && !colExists('active_workers', 'run_id')) {
      stateDb.exec(`
        CREATE TABLE IF NOT EXISTS active_workers_v6 (
          run_id      TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
          card_id     TEXT NOT NULL,
          station     TEXT NOT NULL,
          worker_id   TEXT NOT NULL,
          started_at  INTEGER NOT NULL DEFAULT (unixepoch()),
          lease_until INTEGER NOT NULL,
          pid         INTEGER,
          PRIMARY KEY (run_id, card_id, station)
        )
      `);
      stateDb.exec(`
        INSERT OR IGNORE INTO active_workers_v6 (run_id, card_id, station, worker_id, started_at, lease_until, pid)
        SELECT '${DEFAULT_RUN_ID}', card_id, station, worker_id, started_at, lease_until, pid
        FROM active_workers
      `);
      stateDb.exec('DROP TABLE IF EXISTS active_workers');
      stateDb.exec('ALTER TABLE active_workers_v6 RENAME TO active_workers');
      stateDb.exec(`CREATE INDEX IF NOT EXISTS idx_active_workers_station ON active_workers(station)`);
    }

    // TABLE-RECREATE for outbox (UNIQUE: idempotency_key → (run_id, idempotency_key)):
    if (tableExists('outbox') && !colExists('outbox', 'run_id')) {
      stateDb.exec(`
        CREATE TABLE IF NOT EXISTS outbox_v6 (
          id               INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id           TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
          idempotency_key  TEXT NOT NULL,
          payload_json     TEXT NOT NULL,
          created_at       INTEGER NOT NULL DEFAULT (unixepoch()),
          delivered_at     INTEGER,
          UNIQUE (run_id, idempotency_key)
        )
      `);
      stateDb.exec(`
        INSERT OR IGNORE INTO outbox_v6 (run_id, idempotency_key, payload_json, created_at, delivered_at)
        SELECT '${DEFAULT_RUN_ID}', idempotency_key, payload_json, created_at, delivered_at
        FROM outbox
      `);
      stateDb.exec('DROP TABLE IF EXISTS outbox');
      stateDb.exec('ALTER TABLE outbox_v6 RENAME TO outbox');
    }

    // TABLE-RECREATE for station_outputs (UNIQUE: (card_id, station, attempt) → (run_id, card_id, station, attempt)):
    if (tableExists('station_outputs') && !colExists('station_outputs', 'run_id')) {
      stateDb.exec(`
        CREATE TABLE IF NOT EXISTS station_outputs_v6 (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id         TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
          card_id        TEXT NOT NULL,
          station        TEXT NOT NULL,
          attempt        INTEGER NOT NULL,
          findings_hash  TEXT NOT NULL,
          payload_json   TEXT NOT NULL DEFAULT '{}',
          return_to      TEXT,
          binding_stamp  TEXT,
          created_at     INTEGER NOT NULL DEFAULT (unixepoch()),
          UNIQUE (run_id, card_id, station, attempt)
        )
      `);
      stateDb.exec(`
        INSERT OR IGNORE INTO station_outputs_v6 (run_id, card_id, station, attempt, findings_hash, payload_json, return_to, binding_stamp, created_at)
        SELECT '${DEFAULT_RUN_ID}', card_id, station, attempt, findings_hash, payload_json, return_to, binding_stamp, created_at
        FROM station_outputs
      `);
      stateDb.exec('DROP TABLE IF EXISTS station_outputs');
      stateDb.exec('ALTER TABLE station_outputs_v6 RENAME TO station_outputs');
    }

    // Create runs table (additive — idempotent via IF NOT EXISTS):
    stateDb.exec(`
      CREATE TABLE IF NOT EXISTS runs (
        run_id            TEXT PRIMARY KEY,
        flow              TEXT NOT NULL,
        project_root      TEXT,
        input_fingerprint TEXT NOT NULL,
        status            TEXT NOT NULL,
        outcome           TEXT,
        created_at        INTEGER NOT NULL DEFAULT (unixepoch()),
        holder_pid        INTEGER,
        lease_acquired_at INTEGER
      )
    `);

    stateDb.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
  }

  if (needsV6ToV7Migration) {
    stateDb.exec(`
      CREATE TABLE IF NOT EXISTS runs (
        run_id            TEXT PRIMARY KEY,
        flow              TEXT NOT NULL,
        project_root      TEXT,
        input_fingerprint TEXT NOT NULL,
        status            TEXT NOT NULL,
        outcome           TEXT,
        created_at        INTEGER NOT NULL DEFAULT (unixepoch()),
        holder_pid        INTEGER,
        lease_acquired_at INTEGER
      )
    `);
    if (!colExists('runs', 'project_root')) {
      try {
        stateDb.exec('ALTER TABLE runs ADD COLUMN project_root TEXT');
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (!msg.includes('duplicate column name')) throw err;
      }
    }
    stateDb.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
  }

  if (needsV7ToV8Migration) {
    // Additive v7→v8 migration: add holder_pid/lease_acquired_at to runs for the
    // per-run advisory lease lock (the original run-lock and busy-retry work). Both nullable — a run with no
    // recorded holder is simply unlocked. Guarded the same way as every other
    // additive ALTER above: a "duplicate column name" means a fresh CREATE TABLE
    // (above) already had the column, so the ALTER is a harmless no-op.
    try {
      stateDb.exec('ALTER TABLE runs ADD COLUMN holder_pid INTEGER');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('duplicate column name')) throw err;
    }
    try {
      stateDb.exec('ALTER TABLE runs ADD COLUMN lease_acquired_at INTEGER');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('duplicate column name')) throw err;
    }
    stateDb.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
  }

  if (needsV8ToV9Migration) {
    // Additive v8→v9 migration (the original ingress-attribution work): attribution columns on
    // ingress_events so re-drive can relaunch the owning flow with its original
    // payload and derived run id. All nullable — pre-v9 rows simply have no
    // attribution. Guarded per-column like every other additive ALTER above;
    // 'no such table' covers legacy fixtures from before ingress_events existed
    // (STATE_DDL is not re-run on migrated DBs, matching active_workers in v5).
    for (const column of [
      'flow_id TEXT',
      'flow_path TEXT',
      'run_id TEXT',
      'substrate_json TEXT',
    ]) {
      try {
        stateDb.exec(`ALTER TABLE ingress_events ADD COLUMN ${column}`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
      }
    }
    stateDb.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
  }

  if (needsV9ToV10Migration) {
    // Additive v9→v10 migration: add release_at to cards for the fan-out
    // cache-warming stagger. Nullable — pre-v10 rows (and non-staggered children)
    // simply have no dispatch gate and run immediately. Guarded per-column like
    // every other additive ALTER above.
    try {
      stateDb.exec('ALTER TABLE cards ADD COLUMN release_at INTEGER');
    } catch (err) {
      // 'duplicate column name' = a fresh CREATE TABLE already had it (idempotent).
      // 'no such table' = a legacy fixture predating the cards table (matches the
      // v4→v5 active_workers guard); STATE_DDL is not re-run on migrated DBs.
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
    }
    // Hot-path indexes for the dispatch/gate/fan-out queries (idempotent; guarded
    // against the legacy no-cards-table fixtures the ALTER above tolerates). Must
    // run AFTER the release_at ALTER — idx_cards_dispatch references that column.
    try {
      stateDb.exec('CREATE INDEX IF NOT EXISTS idx_cards_dispatch ON cards(run_id, status, id, release_at)');
      stateDb.exec('CREATE INDEX IF NOT EXISTS idx_cards_parent ON cards(run_id, parent_id, id)');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('no such table')) throw err;
    }
    stateDb.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
  }

  // Hot-path partial index (the pre-public ingress-deduplication review), run UNCONDITIONALLY: STATE_DDL only
  // executes on a fresh DB (v0), so an index added there never reaches an
  // already-migrated state DB (same gap idx_active_workers_station had, worked
  // around above by adding it inside the v5→v6 branch). This one has no single
  // migration branch to hang off, so self-heal it here instead, on every open.
  // Guarded like the v8→v9 ALTERs: 'no such table'/'no such column' cover legacy
  // fixtures that pre-date ingress_events or its run_id column.
  try {
    stateDb.exec(
      `CREATE INDEX IF NOT EXISTS idx_ingress_events_run_id
       ON ingress_events(run_id, received_at DESC) WHERE flow_path IS NOT NULL`,
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!msg.includes('no such table') && !msg.includes('no such column')) throw err;
  }

  // Sweep-path partial index (the pre-public run-slot and HITL review) — same unconditional self-heal
  // rationale and guards as idx_ingress_events_run_id above.
  try {
    stateDb.exec(
      `CREATE INDEX IF NOT EXISTS idx_ingress_events_redrivable
       ON ingress_events(spawn_state, received_at) WHERE spawn_state IN ('accepted', 'failed')`,
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!msg.includes('no such table') && !msg.includes('no such column')) throw err;
  }

  // ── Journal DB ───────────────────────────────────────────────────────────
  // Same wrap as the state DB above (a pre-public engine review finding 4a).
  let journalDb: Database;
  try {
    journalDb = new Database(journalDbPath);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    throw new Error(`unable to open journal database file '${journalDbPath}': ${msg}`);
  }
  // WAL mode first — this persists to the file so concurrent state-DB locks
  // never block journal appends.
  withBusyRetry(() => journalDb.exec('PRAGMA journal_mode = WAL'), COLD_START_RETRY);
  withBusyRetry(() => journalDb.exec('PRAGMA busy_timeout = 5000'), COLD_START_RETRY);

  // Journal DB — v5→v6 migrations (MUST run before JOURNAL_DDL):
  //
  // JOURNAL_DDL contains two composite indexes that reference run_id on both
  // `journal` and `card_log`:
  //   CREATE INDEX … idx_journal_run_card ON journal(run_id, card_id)
  //   CREATE INDEX … idx_card_log_run_card ON card_log(run_id, card_id)
  // On an existing v5 journal DB neither table has run_id yet, so those index
  // statements would fail with "no such column: run_id". Both migrations must
  // complete BEFORE the JOURNAL_DDL exec below.

  // (a) journal — ADDITIVE ALTER: the journal table is a plain append log with
  // id AUTOINCREMENT; it has no UNIQUE/PK constraint involving run_id, so an
  // ALTER ADD COLUMN is sufficient (no table-recreate needed).
  try {
    journalDb.exec(`ALTER TABLE journal ADD COLUMN run_id TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}'`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    // 'duplicate column name' → already migrated or JOURNAL_DDL created it fresh; idempotent.
    // 'no such table'        → journal doesn't exist yet; JOURNAL_DDL will create it with run_id.
    if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
  }

  // (a') work_summaries — ADDITIVE ALTER (v7 follow-up to the v6 run-namespacing
  // work): same plain-append-log shape as journal, so the same idempotent ALTER
  // applies. Without this, deleteRun's work_summaries delete would leak rows
  // written by pre-v7 DBs under no run_id.
  try {
    journalDb.exec(`ALTER TABLE work_summaries ADD COLUMN run_id TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}'`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
  }

  // (a'') journal — WI-567 ADDITIVE ALTERs: harness/adapter identity, measured
  // wall-clock duration, and the explicit unknown-usage sentinel. Nullable (or
  // defaulted, for usage_unknown) and no backfill, so every pre-WI-567 span
  // (transform/deterministic) stays readable and its new columns simply read
  // back null/false rather than being masked.
  try {
    journalDb.exec('ALTER TABLE journal ADD COLUMN adapter TEXT');
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
  }
  try {
    journalDb.exec('ALTER TABLE journal ADD COLUMN duration_ms INTEGER');
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
  }
  try {
    journalDb.exec('ALTER TABLE journal ADD COLUMN usage_unknown INTEGER NOT NULL DEFAULT 0');
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
  }

  // (a''') journal — issue #5 ADDITIVE ALTERs: the cache split. Nullable with no
  // backfill, so a pre-existing row (whose input_tokens is a SUMMED total) reads
  // back null here rather than being silently reinterpreted as uncached input.
  // A query spanning the migration boundary can tell the two eras apart by
  // exactly that null.
  for (const column of [
    'ALTER TABLE journal ADD COLUMN cache_read_input_tokens INTEGER',
    'ALTER TABLE journal ADD COLUMN cache_creation_input_tokens INTEGER',
  ]) {
    try {
      journalDb.exec(column);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('duplicate column name') && !msg.includes('no such table')) throw err;
    }
  }

  // (b) card_log — TABLE-RECREATE: the UNIQUE changed from
  // (card_id, station, attempt, kind) to (run_id, card_id, station, attempt, kind).
  // SQLite cannot ALTER a UNIQUE constraint in place, so we recreate.
  // Guard: only recreate if the table exists but is still on the old schema.
  {
    const tableExistsJ = (name: string): boolean => {
      const row = journalDb
        .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")
        .get(name);
      return row !== undefined && row !== null;
    };
    const colExistsJ = (table: string, col: string): boolean => {
      const cols = journalDb.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[];
      return cols.some((c) => c.name === col);
    };
    // Only recreate if card_log exists but is missing run_id (old schema).
    if (tableExistsJ('card_log') && !colExistsJ('card_log', 'run_id')) {
      journalDb.exec(`
        CREATE TABLE IF NOT EXISTS card_log_v6 (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id        TEXT NOT NULL DEFAULT '${DEFAULT_RUN_ID}',
          card_id       TEXT NOT NULL,
          station       TEXT NOT NULL,
          attempt       INTEGER NOT NULL,
          kind          TEXT NOT NULL,
          source_lane   TEXT,
          dest_lane     TEXT,
          reason_class  TEXT,
          verdict       TEXT,
          findings_json TEXT,
          return_to     TEXT,
          reason        TEXT,
          UNIQUE(run_id, card_id, station, attempt, kind)
        )
      `);
      journalDb.exec(`
        INSERT OR IGNORE INTO card_log_v6
          (run_id, card_id, station, attempt, kind, source_lane, dest_lane, reason_class,
           verdict, findings_json, return_to, reason)
        SELECT '${DEFAULT_RUN_ID}', card_id, station, attempt, kind, source_lane, dest_lane,
               reason_class, verdict, findings_json, return_to, reason
        FROM card_log
      `);
      journalDb.exec('DROP TABLE IF EXISTS card_log');
      journalDb.exec('ALTER TABLE card_log_v6 RENAME TO card_log');
      journalDb.exec('CREATE INDEX IF NOT EXISTS idx_card_log_card ON card_log(card_id)');
    }
  }

  // Always run JOURNAL_DDL — every statement is CREATE TABLE/INDEX IF NOT EXISTS,
  // making this fully idempotent. Running it unconditionally self-heals existing
  // journals that pre-date a new table added in a later version (e.g. card_log).
  // The v5→v6 migrations above ensure both journal and card_log already have
  // run_id before the composite indexes in this DDL reference that column.
  journalDb.exec(JOURNAL_DDL);

  return new ConduitDBImpl(stateDb, journalDb);
}
