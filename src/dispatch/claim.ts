/**
 * Atomic claim + heartbeat lease + lease-based reconcile (WI-294).
 *
 * This is the single linearization point for dispatch (SPEC §7 rev-1 C6,
 * SPEC §11 lease semantics). Every function that mutates active_workers
 * or cards.status operates through the raw state Database so it can use
 * `BEGIN IMMEDIATE` — the only SQLite mode that prevents double-claim
 * under concurrent process contention.
 *
 * WIP occupancy is always measured from active_workers (COUNT(*) for
 * a station), never from cards.status. This correctly counts cards in
 * claimed, working, and done_pending_ack states that still hold a slot.
 *
 * `now` is injected epoch-seconds everywhere — no Date.now() calls —
 * so lease expiry is deterministic in tests.
 */

import { DEFAULT_RUN_ID, type ConduitDB } from '../persistence/db';
import { withBusyRetry } from '../persistence/busy-retry';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** Input for a single claim attempt. */
export interface ClaimRequest {
  /** The card to claim. Must be status='ready'. */
  cardId: string;
  /** Station the card is being dispatched to. */
  station: string;
  /** Identifier for the worker process claiming this card. */
  workerId: string;
  /** Maximum concurrent cards allowed in this station. */
  wipCap: number;
  /** Current epoch-seconds (injected for deterministic lease expiry). */
  now: number;
  /** How many seconds from `now` the lease is valid. */
  leaseSeconds: number;
  /** OS PID of the claiming worker process (stored for dead-PID detection). */
  pid?: number;
  /**
   * Run id that owns this card. Omit (or pass undefined) to use DEFAULT_RUN_ID.
   * All guards and writes are scoped to this run so concurrent runs sharing a
   * DB never cross-contaminate WIP counts, slot checks, or card status flips.
   */
  runId?: string;
}

/** Why a claim failed. Never set when ok=true. */
export type ClaimFailReason = 'not_ready' | 'wip_cap' | 'slot_occupied';

/** Result of a single claim attempt. */
export interface ClaimResult {
  /** True when the card was successfully claimed. */
  ok: boolean;
  /**
   * Present only when ok=false.
   * - not_ready:     card.status ≠ 'ready' (deps unsatisfied or wrong state)
   * - wip_cap:       active_workers count for the station >= wipCap
   * - slot_occupied: this (cardId, station) pair already has an active worker
   */
  reason?: ClaimFailReason;
}

// ---------------------------------------------------------------------------
// Internal row types (SQLite results)
// ---------------------------------------------------------------------------

interface CardStatusRow {
  status: string;
}

interface WipCountRow {
  count: number;
}

interface ActiveWorkerRow {
  worker_id: string;
  lease_until: number;
}

// ---------------------------------------------------------------------------
// attemptClaim — the single linearization point (AC1–AC4)
// ---------------------------------------------------------------------------

/**
 * Attempt to claim a card for a station worker.
 *
 * All three preconditions (card ready, station under WIP, slot free) are
 * checked inside a single `BEGIN IMMEDIATE` transaction. This serializes
 * concurrent claims so that two processes racing for the last slot produce
 * exactly one success and one failure (AC4).
 *
 * On success: cards.status → 'claimed' AND active_workers row inserted,
 * committed atomically. On failure: nothing is written.
 */
export function attemptClaim(db: ConduitDB, req: ClaimRequest): ClaimResult {
  const stateDb = db.getStateDb();

  // BEGIN IMMEDIATE acquires the write reservation at the start of the
  // transaction. Any concurrent `BEGIN IMMEDIATE` blocks until we COMMIT or
  // ROLLBACK, so the WIP recheck inside the transaction is serialized.
  const runId = req.runId ?? DEFAULT_RUN_ID;

  // Wrapped in withBusyRetry (the original run-lock and busy-retry work part 2): under sustained overlap from
  // concurrent `conduit run` processes sharing this DB, a writer that cannot
  // acquire the BEGIN IMMEDIATE reservation within busy_timeout throws
  // SQLITE_BUSY. Retrying with backoff lets transient contention wait instead
  // of aborting this process's tick.
  //
  // LOAD-BEARING for every withBusyRetry(...transaction...) in this file:
  // .immediate() means SQLITE_BUSY is thrown at BEGIN, BEFORE the closure body
  // runs, so a retried attempt re-runs the whole closure from scratch and any
  // side effects in the body (e.g. reconcile's `interrupted` / reclaim's
  // `reclaimed` array pushes) never double-fire. Do NOT switch these to
  // deferred transactions — a busy at COMMIT would retry a body that already
  // ran, duplicating those side effects.
  return withBusyRetry(() =>
    stateDb
      .transaction((): ClaimResult => {
        // Guard 1: slot must be free for this (run, card, station) triple.
        // Checked first so that a duplicate-claim on an already-claimed card
        // returns 'slot_occupied' rather than 'not_ready' (the card's status
        // will be 'claimed' after the first success, not 'ready').
        const existingSlot = stateDb
          .prepare(
            'SELECT worker_id FROM active_workers WHERE run_id = $run_id AND card_id = $card_id AND station = $station',
          )
          .get({ $run_id: runId, $card_id: req.cardId, $station: req.station });

        if (existingSlot) {
          return { ok: false, reason: 'slot_occupied' };
        }

        // Guard 2: card must be in 'ready' status (deps satisfied).
        const cardRow = stateDb
          .prepare('SELECT status FROM cards WHERE run_id = $run_id AND id = $id')
          .get({ $run_id: runId, $id: req.cardId }) as CardStatusRow | undefined;

        if (!cardRow || cardRow.status !== 'ready') {
          return { ok: false, reason: 'not_ready' };
        }

        // Guard 3: station must be under WIP cap within this run (measured from
        // active_workers, never from cards.status per spec). Scoped to the run
        // so another run's workers at the same station do not starve this one.
        const { count: wipCount } = stateDb
          .prepare(
            'SELECT COUNT(*) AS count FROM active_workers WHERE run_id = $run_id AND station = $station',
          )
          .get({ $run_id: runId, $station: req.station }) as WipCountRow;

        if (wipCount >= req.wipCap) {
          return { ok: false, reason: 'wip_cap' };
        }

        // All guards passed — reserve the slot and flip the card status.
        stateDb
          .prepare(
            `INSERT INTO active_workers (run_id, card_id, station, worker_id, started_at, lease_until, pid)
             VALUES ($run_id, $card_id, $station, $worker_id, $started_at, $lease_until, $pid)`,
          )
          .run({
            $run_id: runId,
            $card_id: req.cardId,
            $station: req.station,
            $worker_id: req.workerId,
            $started_at: req.now,
            $lease_until: req.now + req.leaseSeconds,
            $pid: req.pid ?? null,
          });

        // `release_at` is CLEARED with the claim: the gate it described has been
        // passed, and this is the transaction that passes it. Nothing else ever
        // cleared the column, so a card that parked once carried a stale past
        // value for the rest of its life — harmless to the release gate itself
        // (planTick compares `> now`), but every OTHER reader of the column has
        // to special-case it. The ingress parked sweep is the one that bit:
        // it takes MIN(release_at) over a run's ready cards to decide when a
        // parked run is due, so one stale value made a genuinely parked run
        // read as due immediately and churn a resume per tick.
        stateDb
          .prepare("UPDATE cards SET status = 'claimed', release_at = NULL WHERE run_id = $run_id AND id = $id")
          .run({ $run_id: runId, $id: req.cardId });

        return { ok: true };
      })
      .immediate(),
  );
}

// ---------------------------------------------------------------------------
// beginWork — claimed → working, sets the initial heartbeat lease (AC5/AC6)
// ---------------------------------------------------------------------------

/**
 * Transition a claimed card to 'working' and stamp the heartbeat lease.
 *
 * Called by the worker process after it starts executing so the controller
 * can distinguish an active worker (lease_until > now) from a stalled one
 * that needs reconciliation.
 */
export function beginWork(
  db: ConduitDB,
  cardId: string,
  station: string,
  now: number,
  leaseSeconds: number,
  runId: string = DEFAULT_RUN_ID,
): void {
  const stateDb = db.getStateDb();

  // Guard-before-operate: verify the card is in 'claimed' status before writing.
  // A card not yet claimed (e.g. status='ready') must NEVER be flipped to 'working'
  // — it would have no active_workers row, making it permanently invisible to
  // reconcile (which uses an INNER JOIN on active_workers).
  const card = stateDb
    .prepare("SELECT status FROM cards WHERE run_id = $run_id AND id = $id")
    .get({ $run_id: runId, $id: cardId }) as { status: string } | undefined;

  if (!card || card.status !== 'claimed') {
    throw new Error(
      `beginWork precondition failed: card '${cardId}' must be in 'claimed' status ` +
        `before transitioning to 'working' (found: ${card?.status ?? 'not found'})`,
    );
  }

  withBusyRetry(() =>
    stateDb
      .transaction(() => {
        stateDb
          .prepare("UPDATE cards SET status = 'working' WHERE run_id = $run_id AND id = $id")
          .run({ $run_id: runId, $id: cardId });

        stateDb
          .prepare(
            'UPDATE active_workers SET lease_until = $lease_until WHERE run_id = $run_id AND card_id = $card_id AND station = $station',
          )
          .run({ $run_id: runId, $lease_until: now + leaseSeconds, $card_id: cardId, $station: station });
      })
      .immediate(),
  );
}

// ---------------------------------------------------------------------------
// renewLease — push lease_until forward so the controller sees the worker alive
// ---------------------------------------------------------------------------

/**
 * Extend the heartbeat lease for an active worker.
 *
 * Returns true when the update succeeded (the slot exists).
 * Returns false when there is no active worker for (cardId, station) —
 * the caller should treat this as "lease already expired; do not continue".
 */
export function renewLease(
  db: ConduitDB,
  cardId: string,
  station: string,
  now: number,
  leaseSeconds: number,
  runId: string = DEFAULT_RUN_ID,
): boolean {
  const stateDb = db.getStateDb();

  const result = stateDb
    .prepare(
      'UPDATE active_workers SET lease_until = $lease_until WHERE run_id = $run_id AND card_id = $card_id AND station = $station',
    )
    .run({ $run_id: runId, $lease_until: now + leaseSeconds, $card_id: cardId, $station: station });

  return result.changes > 0;
}

// ---------------------------------------------------------------------------
// isWorkerAlive — lease_until > now (strictly after expiry is not-alive)
// ---------------------------------------------------------------------------

/**
 * Report whether the worker holding (cardId, station) has a valid lease.
 *
 * A worker is alive when lease_until > now. At the exact expiry instant
 * (lease_until === now) the worker is considered dead so reconcile will
 * catch it on the same tick.
 */
export function isWorkerAlive(
  db: ConduitDB,
  cardId: string,
  station: string,
  now: number,
  runId: string = DEFAULT_RUN_ID,
): boolean {
  const stateDb = db.getStateDb();

  const row = stateDb
    .prepare(
      'SELECT lease_until FROM active_workers WHERE run_id = $run_id AND card_id = $card_id AND station = $station',
    )
    .get({ $run_id: runId, $card_id: cardId, $station: station }) as { lease_until: number } | undefined;

  if (!row) return false;
  return row.lease_until > now;
}

// ---------------------------------------------------------------------------
// reconcile — find working cards with expired leases → interrupted (AC6)
// ---------------------------------------------------------------------------

/**
 * Find every card with status='working' whose active_workers lease has
 * expired (lease_until <= now), flip them to status='interrupted', and
 * release their active_workers slots.
 *
 * Returns the ids of all interrupted cards so the controller can
 * re-hydrate them.
 *
 * Typically called on kernel restart before dispatching new work.
 */
export function reconcile(db: ConduitDB, now: number, runId: string = DEFAULT_RUN_ID): { interrupted: string[] } {
  const stateDb = db.getStateDb();
  const interrupted: string[] = [];

  withBusyRetry(() =>
    stateDb
      .transaction(() => {
        // Find expired working cards using a JOIN so we read both tables
        // consistently inside the same transaction. Scoped to runId so two
        // concurrent runs sharing the same card id never reclaim each other's slots.
        const expiredRows = stateDb
          .prepare(
            `SELECT c.id
             FROM cards c
             JOIN active_workers aw ON aw.card_id = c.id AND aw.run_id = c.run_id
             WHERE c.run_id = $runId
               AND c.status = 'working'
               AND aw.lease_until <= $now`,
          )
          .all({ $runId: runId, $now: now }) as { id: string }[];

        for (const row of expiredRows) {
          stateDb
            .prepare("UPDATE cards SET status = 'interrupted' WHERE run_id = $runId AND id = $id")
            .run({ $runId: runId, $id: row.id });

          stateDb
            .prepare('DELETE FROM active_workers WHERE run_id = $runId AND card_id = $card_id')
            .run({ $runId: runId, $card_id: row.id });

          interrupted.push(row.id);
        }
      })
      .immediate(),
  );

  return { interrupted };
}

// ---------------------------------------------------------------------------
// reclaimOrphanedWorkers — UNCONDITIONAL reclaim on startup/resume
// ---------------------------------------------------------------------------

/**
 * Reclaim every in-flight card (`claimed` or `working`) regardless of its lease,
 * flipping it to `interrupted` and releasing its active_workers slot.
 *
 * Why this exists separately from `reconcile`:
 *   `reconcile` is the LIVE-controller tool — it detects a *stalled* worker
 *   mid-run via lease expiry (lease_until <= now), because within one running
 *   process a still-valid lease means the worker really is alive.
 *
 *   On `conduit resume`, however, the process that held those leases is GONE —
 *   the prior run crashed. Every `claimed`/`working` card is therefore orphaned
 *   by definition, even if its (long, e.g. 600 s) lease has not yet expired. A
 *   lease-based reconcile would no-op on an immediate resume and leave the card
 *   stuck in `working`, never re-dispatched. This function makes resume sound:
 *   any in-flight worker from a dead process is reclaimed unconditionally.
 *
 * The optional `isPidAlive` predicate exists ONLY for a hypothetical live-control
 * reuse where the process generation is known. IMPORTANT: a bare PID-existence
 * check is UNSOUND — the OS recycles PIDs, so a dead worker's PID may now belong
 * to an unrelated process and a naive predicate would wrongly keep its card
 * pinned forever. A correct predicate MUST verify process IDENTITY, which is why
 * the stored slot `startedAt` is passed alongside the pid (compare it against the
 * OS process start-time, not mere existence). The production resume path
 * (cmdResume) deliberately passes NO predicate, so it always reclaims — the only
 * sound choice when the owning process is known dead.
 *
 * Returns the ids of all reclaimed cards (so the caller can re-hydrate them).
 */
export function reclaimOrphanedWorkers(
  db: ConduitDB,
  now: number,
  isPidAlive?: (pid: number, startedAt: number) => boolean,
  runId?: string,
): { reclaimed: string[] } {
  void now; // lease is intentionally ignored — resume means the owner is dead.
  const stateDb = db.getStateDb();
  const reclaimed: string[] = [];

  withBusyRetry(() =>
    stateDb
      .transaction(() => {
        // Any card still in an in-flight execution state holds (or held) a slot
        // owned by the now-dead process. `done_pending_ack` is excluded: the
        // synchronous executor never persists a card there (work → next is one
        // atomic step), so a row in that state is not produced by this engine.
        const runFilter = runId !== undefined ? 'AND c.run_id = $run_id' : '';
        const orphanRows = stateDb
          .prepare(
            `SELECT c.id, c.run_id, aw.pid, aw.started_at AS startedAt
             FROM cards c
             LEFT JOIN active_workers aw ON aw.card_id = c.id AND aw.run_id = c.run_id
             WHERE c.status IN ('claimed', 'working') ${runFilter}`,
          )
          .all(runId !== undefined ? { $run_id: runId } : {}) as {
            id: string;
            run_id: string;
            pid: number | null;
            startedAt: number | null;
          }[];

        for (const row of orphanRows) {
          // When isPidAlive is supplied, only reclaim slots whose PID is dead.
          // The predicate receives the slot's startedAt so it can verify process
          // IDENTITY (not bare existence) and reject a recycled PID. See the warning
          // in the doc comment above.
          if (
            isPidAlive !== undefined &&
            row.pid !== null &&
            isPidAlive(row.pid, row.startedAt ?? 0)
          ) {
            continue;
          }

          stateDb
            .prepare("UPDATE cards SET status = 'interrupted' WHERE run_id = $run_id AND id = $id")
            .run({ $run_id: row.run_id, $id: row.id });

          stateDb
            .prepare('DELETE FROM active_workers WHERE run_id = $run_id AND card_id = $card_id')
            .run({ $run_id: row.run_id, $card_id: row.id });

          reclaimed.push(row.id);
        }
      })
      .immediate(),
  );

  return { reclaimed };
}

// ---------------------------------------------------------------------------
// activeWorkerCount — WIP metric (AC2)
// ---------------------------------------------------------------------------

/**
 * Count the number of active worker slots held for a station.
 *
 * This is the authoritative WIP measurement — derived from active_workers,
 * never from cards.status, so it correctly counts cards that are claimed,
 * working, or done_pending_ack but still hold a slot.
 */
export function activeWorkerCount(
  db: ConduitDB,
  station: string,
  runId: string = DEFAULT_RUN_ID,
): number {
  const stateDb = db.getStateDb();

  const { count } = stateDb
    .prepare(
      'SELECT COUNT(*) AS count FROM active_workers WHERE run_id = $run_id AND station = $station',
    )
    .get({ $run_id: runId, $station: station }) as WipCountRow;

  return count;
}

// ---------------------------------------------------------------------------
// getActiveWorker — slot inspection (used in AC1–AC3 assertions)
// ---------------------------------------------------------------------------

/**
 * Return the worker and lease metadata for an active slot, or null if
 * the slot does not exist.
 */
export function getActiveWorker(
  db: ConduitDB,
  cardId: string,
  station: string,
  runId: string = DEFAULT_RUN_ID,
): { workerId: string; leaseUntil: number } | null {
  const stateDb = db.getStateDb();

  const row = stateDb
    .prepare(
      'SELECT worker_id, lease_until FROM active_workers WHERE run_id = $run_id AND card_id = $card_id AND station = $station',
    )
    .get({ $run_id: runId, $card_id: cardId, $station: station }) as ActiveWorkerRow | undefined;

  if (!row) return null;
  return { workerId: row.worker_id, leaseUntil: row.lease_until };
}
