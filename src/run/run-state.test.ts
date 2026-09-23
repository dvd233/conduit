/**
 * Tests for getRunState (WI-480).
 *
 * getRunState(db, runId) is a read-only query that returns a run's current
 * state derived from its cards and run registry record:
 *
 *   { status: 'not_found' }
 *   { status: 'running' }
 *   { status: 'held', heldCards: Array<{ cardId: string; reason: string }> }
 *   { status: 'terminal', outcome: string }
 *
 * AC-1: run with at least one non-terminal, non-held card → 'running'
 * AC-2: run with a held card → 'held' + heldCards with reason from card_log
 * AC-3: run whose every card is terminal → 'terminal' with run registry outcome
 * AC-4: unknown run id → 'not_found' (not throw, not false 'terminal')
 * AC-5: cross-run isolation — run B's cards never affect run A's reported state
 */
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { openConduitDB, DEFAULT_RUN_ID, type ConduitDB } from '../persistence/db';
import { getRunState, getRunParkedRelease, formatParkedRun, type RunStateResult } from './run-state';
import type { Card } from '../types/kernel';

let db: ConduitDB;

beforeEach(() => {
  db = openConduitDB({ stateDbPath: ':memory:', journalDbPath: ':memory:' });
});

afterEach(() => {
  db.close();
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCard(runId: string, id: string, overrides: Partial<Card> = {}): Card {
  return {
    run_id: runId,
    id,
    parent_id: null,
    lane: 'brief',
    status: 'ready',
    attempt: 0,
    wave: 0,
    owned_paths: [],
    rework_count: 0,
    ...overrides,
  };
}

function seedRun(runId: string, outcome?: string): void {
  db.insertRun({
    run_id: runId,
    flow: 'studio',
    input_fingerprint: `fp-${runId}`,
    status: outcome ? 'done' : 'running',
    outcome,
  });
}

function holdCard(card: Card, reason: string): void {
  // Write card in held status
  const stateDb = db.getStateDb();
  stateDb
    .prepare("UPDATE cards SET status = 'held' WHERE run_id = $r AND id = $id")
    .run({ $r: card.run_id, $id: card.id });

  // Append the two card_log entries the executor writes on hold:
  // 1. entered_lane with reasonClass='hold'
  db.appendCardLog({
    runId: card.run_id,
    cardId: card.id,
    station: card.lane,
    attempt: card.attempt,
    kind: 'entered_lane',
    sourceLane: card.lane,
    destLane: card.lane,
    reasonClass: 'hold',
  });
  // 2. terminal entry carrying the hold reason string
  db.appendCardLog({
    runId: card.run_id,
    cardId: card.id,
    station: card.lane,
    attempt: card.attempt,
    kind: 'terminal',
    reason,
  });
}

/** Move `card` to a terminal lane, so no unfinished card remains to be parked. */
function terminalCard(card: Card, lane: 'done' | 'scrap'): void {
  const stateDb = db.getStateDb();
  stateDb
    .prepare("UPDATE cards SET status = 'complete', lane = $lane WHERE run_id = $r AND id = $id")
    .run({ $r: card.run_id, $id: card.id, $lane: lane });
}

// ---------------------------------------------------------------------------
// AC-4: unknown run id → 'not_found'
// ---------------------------------------------------------------------------

describe('getRunState — unknown run id (AC-4)', () => {
  it('returns not_found for a run id that was never registered', () => {
    const result = getRunState(db, 'no-such-run');
    expect(result.status).toBe('not_found');
  });

  it('does not throw for an unknown run id', () => {
    expect(() => getRunState(db, 'phantom-run')).not.toThrow();
  });

  it('returns not_found even when other runs exist', () => {
    seedRun('run-real');
    db.insertCard(makeCard('run-real', 'c1'));
    const result = getRunState(db, 'run-ghost');
    expect(result.status).toBe('not_found');
  });

  it('not_found result does not include heldCards or outcome fields', () => {
    const result = getRunState(db, 'no-such-run') as Extract<RunStateResult, { status: 'not_found' }>;
    expect('heldCards' in result).toBe(false);
    expect('outcome' in result).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// AC-1: run with at least one non-terminal, non-held card → 'running'
// ---------------------------------------------------------------------------

describe('getRunState — running (AC-1)', () => {
  it('returns running when a card is in a non-terminal status', () => {
    seedRun('run-go');
    db.insertCard(makeCard('run-go', 'c1', { status: 'ready' }));
    expect(getRunState(db, 'run-go').status).toBe('running');
  });

  it('returns running for status=working', () => {
    seedRun('run-w');
    db.insertCard(makeCard('run-w', 'c1', { status: 'working' }));
    expect(getRunState(db, 'run-w').status).toBe('running');
  });

  it('returns running for status=claimed', () => {
    seedRun('run-claimed');
    db.insertCard(makeCard('run-claimed', 'c1', { status: 'claimed' }));
    expect(getRunState(db, 'run-claimed').status).toBe('running');
  });

  it('returns running for status=waiting', () => {
    seedRun('run-wait');
    db.insertCard(makeCard('run-wait', 'c1', { status: 'waiting' }));
    expect(getRunState(db, 'run-wait').status).toBe('running');
  });

  it('returns running for status=awaiting_children', () => {
    seedRun('run-fan');
    db.insertCard(makeCard('run-fan', 'c1', { status: 'awaiting_children' }));
    expect(getRunState(db, 'run-fan').status).toBe('running');
  });

  it('returns running when at least one card is active even if others are terminal', () => {
    seedRun('run-mixed');
    const c1 = makeCard('run-mixed', 'c1', { status: 'ready' });
    const c2 = makeCard('run-mixed', 'c2', { lane: 'done', status: 'complete' });
    db.insertCard(c1);
    db.insertCard(c2);
    terminalCard(c2, 'done');
    expect(getRunState(db, 'run-mixed').status).toBe('running');
  });
});

// ---------------------------------------------------------------------------
// AC-2: run with a held card → 'held' with reason from card_log
// ---------------------------------------------------------------------------

describe('getRunState — held (AC-2)', () => {
  it('returns held when a card has status=held', () => {
    seedRun('run-held');
    const card = makeCard('run-held', 'ch1', { status: 'ready', lane: 'build' });
    db.insertCard(card);
    holdCard(card, 'needs human review');
    expect(getRunState(db, 'run-held').status).toBe('held');
  });

  it('includes the held card id in heldCards', () => {
    seedRun('run-hc');
    const card = makeCard('run-hc', 'card-held', { status: 'ready', lane: 'brief' });
    db.insertCard(card);
    holdCard(card, 'integrity check failed');
    const result = getRunState(db, 'run-hc') as Extract<RunStateResult, { status: 'held' }>;
    expect(result.heldCards.some((h) => h.cardId === 'card-held')).toBe(true);
  });

  it('includes the hold reason from the card_log terminal entry', () => {
    seedRun('run-reason');
    const card = makeCard('run-reason', 'c-reason', { status: 'ready', lane: 'build' });
    db.insertCard(card);
    holdCard(card, 'owned path violation detected');
    const result = getRunState(db, 'run-reason') as Extract<RunStateResult, { status: 'held' }>;
    const entry = result.heldCards.find((h) => h.cardId === 'c-reason');
    expect(entry).not.toBeUndefined();
    expect(entry!.reason).toBe('owned path violation detected');
  });

  it('reports all held cards when multiple cards are held', () => {
    seedRun('run-multi-held');
    const c1 = makeCard('run-multi-held', 'h1', { status: 'ready', lane: 'brief' });
    const c2 = makeCard('run-multi-held', 'h2', { status: 'ready', lane: 'build' });
    db.insertCard(c1);
    db.insertCard(c2);
    holdCard(c1, 'reason A');
    holdCard(c2, 'reason B');

    const result = getRunState(db, 'run-multi-held') as Extract<RunStateResult, { status: 'held' }>;
    expect(result.heldCards).toHaveLength(2);
    const ids = result.heldCards.map((h) => h.cardId);
    expect(ids).toContain('h1');
    expect(ids).toContain('h2');
  });

  it('held takes precedence over running when some cards are active and one is held', () => {
    seedRun('run-held-mix');
    const active = makeCard('run-held-mix', 'active', { status: 'ready' });
    const held = makeCard('run-held-mix', 'stuck', { status: 'ready', lane: 'build' });
    db.insertCard(active);
    db.insertCard(held);
    holdCard(held, 'budget exceeded');

    const result = getRunState(db, 'run-held-mix');
    expect(result.status).toBe('held');
  });
});

// ---------------------------------------------------------------------------
// AC-3: run whose every card is terminal → 'terminal' with outcome
// ---------------------------------------------------------------------------

describe('getRunState — terminal (AC-3)', () => {
  it('returns terminal when all cards are in done lane', () => {
    seedRun('run-done', 'success');
    const c1 = makeCard('run-done', 'c1');
    db.insertCard(c1);
    terminalCard(c1, 'done');
    expect(getRunState(db, 'run-done').status).toBe('terminal');
  });

  it('returns terminal when all cards are in scrap lane', () => {
    seedRun('run-scrap', 'scrapped');
    const c1 = makeCard('run-scrap', 'c1');
    db.insertCard(c1);
    terminalCard(c1, 'scrap');
    expect(getRunState(db, 'run-scrap').status).toBe('terminal');
  });

  it('includes the run outcome in the terminal result', () => {
    seedRun('run-outcome', 'success');
    const c1 = makeCard('run-outcome', 'c1');
    db.insertCard(c1);
    terminalCard(c1, 'done');
    const result = getRunState(db, 'run-outcome') as Extract<RunStateResult, { status: 'terminal' }>;
    expect(result.outcome).toBe('success');
  });

  it('includes a scrapped outcome for a fully scrapped run', () => {
    seedRun('run-scrapped', 'scrapped');
    const c1 = makeCard('run-scrapped', 'c1');
    db.insertCard(c1);
    terminalCard(c1, 'scrap');
    const result = getRunState(db, 'run-scrapped') as Extract<RunStateResult, { status: 'terminal' }>;
    expect(result.outcome).toBe('scrapped');
  });

  it('returns terminal only when ALL cards are terminal (not just some)', () => {
    seedRun('run-not-done-yet', 'success');
    const done = makeCard('run-not-done-yet', 'c-done');
    const active = makeCard('run-not-done-yet', 'c-active', { status: 'working' });
    db.insertCard(done);
    db.insertCard(active);
    terminalCard(done, 'done');
    // active is still working — not terminal
    expect(getRunState(db, 'run-not-done-yet').status).toBe('running');
  });

  it('returns terminal when a run has no cards (registry exists, no cards seeded)', () => {
    seedRun('run-no-cards', 'success');
    const result = getRunState(db, 'run-no-cards');
    expect(result.status).toBe('terminal');
  });
});

// ---------------------------------------------------------------------------
// AC-5: cross-run isolation — run B never affects run A's state
// ---------------------------------------------------------------------------

describe('getRunState — cross-run isolation (AC-5)', () => {
  it("run B's held card does not make run A appear held", () => {
    seedRun('run-A');
    seedRun('run-B');

    const aCard = makeCard('run-A', 'a1', { status: 'ready' });
    const bCard = makeCard('run-B', 'b1', { status: 'ready', lane: 'build' });
    db.insertCard(aCard);
    db.insertCard(bCard);
    holdCard(bCard, 'run B issue');

    expect(getRunState(db, 'run-A').status).toBe('running');
  });

  it("run B's active card does not make run A appear running when A is terminal", () => {
    seedRun('run-A2', 'success');
    seedRun('run-B2');

    const aCard = makeCard('run-A2', 'a1');
    const bCard = makeCard('run-B2', 'b1', { status: 'working' });
    db.insertCard(aCard);
    db.insertCard(bCard);
    terminalCard(aCard, 'done');

    expect(getRunState(db, 'run-A2').status).toBe('terminal');
  });

  it("run B's non-terminal cards do not appear in run A's heldCards list", () => {
    seedRun('run-A3');
    seedRun('run-B3');

    const aHeld = makeCard('run-A3', 'ah1', { status: 'ready', lane: 'brief' });
    const bHeld = makeCard('run-B3', 'bh1', { status: 'ready', lane: 'brief' });
    db.insertCard(aHeld);
    db.insertCard(bHeld);
    holdCard(aHeld, 'A hold reason');
    holdCard(bHeld, 'B hold reason');

    const result = getRunState(db, 'run-A3') as Extract<RunStateResult, { status: 'held' }>;
    expect(result.heldCards.every((h) => h.cardId === 'ah1')).toBe(true);
    expect(result.heldCards.some((h) => h.cardId === 'bh1')).toBe(false);
  });

  it('DEFAULT_RUN_ID state is independent of a custom run id', () => {
    seedRun(DEFAULT_RUN_ID, 'success');
    seedRun('run-custom');

    const defaultCard = makeCard(DEFAULT_RUN_ID, 'dc1');
    const customCard = makeCard('run-custom', 'cc1', { status: 'working' });
    db.insertCard(defaultCard);
    db.insertCard(customCard);
    terminalCard(defaultCard, 'done');

    expect(getRunState(db, DEFAULT_RUN_ID).status).toBe('terminal');
    expect(getRunState(db, 'run-custom').status).toBe('running');
  });
});

// ---------------------------------------------------------------------------
// Issue #7 — a run halted only because every card is parked behind a provider
// reset is PARKED: nothing failed, and it is resumable once the gate opens.
// ---------------------------------------------------------------------------

/**
 * Park a ready card behind a release gate, the way the executor's rate-limit
 * park does it: `release_at` stamped AND an `entered_lane` card_log entry
 * classed 'rate_limited'. Both halves matter — the column alone cannot say why
 * a card is gated, because the fan-out stagger stamps the very same column.
 * See `staggerCard` below for the other writer.
 */
function parkCard(card: Card, releaseAt: number): void {
  db.getStateDb()
    .prepare("UPDATE cards SET status = 'ready', release_at = $at WHERE run_id = $r AND id = $id")
    .run({ $at: releaseAt, $r: card.run_id, $id: card.id });
  db.appendCardLog({
    runId: card.run_id,
    kind: 'entered_lane',
    cardId: card.id,
    station: card.lane,
    attempt: card.attempt,
    sourceLane: card.lane,
    destLane: card.lane,
    reasonClass: 'rate_limited',
  });
}

/**
 * Gate a ready card the way the v10 fan-out cache-warming stagger does: the
 * same `release_at` column, but a lane move that is ordinary forward progress.
 * A run holding only these is scheduled, NOT parked behind a provider cap.
 */
function staggerCard(card: Card, releaseAt: number): void {
  db.getStateDb()
    .prepare("UPDATE cards SET status = 'ready', release_at = $at WHERE run_id = $r AND id = $id")
    .run({ $at: releaseAt, $r: card.run_id, $id: card.id });
  db.appendCardLog({
    runId: card.run_id,
    kind: 'entered_lane',
    cardId: card.id,
    station: card.lane,
    attempt: card.attempt,
    sourceLane: 'plan',
    destLane: card.lane,
    reasonClass: 'forward',
  });
}

describe('getRunParkedRelease — the one predicate behind outcome=parked (issue #7)', () => {
  const NOW = 1000;

  it('is parked when every unfinished card is ready behind a FUTURE release_at', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    parkCard(makeCard('r', 'c1'), NOW + 300);
    expect(getRunParkedRelease(db, 'r', NOW)).toEqual({ releaseAt: NOW + 300 });
  });

  it('names the SOONEST gate when several cards are parked', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    db.insertCard(makeCard('r', 'c2'));
    parkCard(makeCard('r', 'c1'), NOW + 900);
    parkCard(makeCard('r', 'c2'), NOW + 300);
    expect(getRunParkedRelease(db, 'r', NOW)?.releaseAt).toBe(NOW + 300);
  });

  it('ignores cards already at done — finished work is not waiting on anything', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    db.insertCard(makeCard('r', 'c2'));
    terminalCard(makeCard('r', 'c1'), 'done');
    parkCard(makeCard('r', 'c2'), NOW + 300);
    expect(getRunParkedRelease(db, 'r', NOW)).toEqual({ releaseAt: NOW + 300 });
  });

  it('is NOT parked when any card was scrapped — something went wrong, not just waited', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    db.insertCard(makeCard('r', 'c2'));
    parkCard(makeCard('r', 'c1'), NOW + 300);
    terminalCard(makeCard('r', 'c2'), 'scrap');
    expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
  });

  it('is NOT parked when any card is held — a human is owed a decision first', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    db.insertCard(makeCard('r', 'c2'));
    parkCard(makeCard('r', 'c1'), NOW + 300);
    holdCard(makeCard('r', 'c2'), 'needs judgment');
    expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
  });

  it('is NOT parked when the gate is already in the PAST — a dispatchable card that stopped is a stall', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    parkCard(makeCard('r', 'c1'), NOW - 1);
    expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
  });

  it('is NOT parked when a card is ready with no gate at all', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
  });

  it('is NOT parked when the run is complete', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    terminalCard(makeCard('r', 'c1'), 'done');
    expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
  });

  it("does not read another run's parked cards", () => {
    seedRun('a');
    seedRun('b');
    db.insertCard(makeCard('b', 'c1'));
    parkCard(makeCard('b', 'c1'), NOW + 300);
    expect(getRunParkedRelease(db, 'a', NOW)).toBeNull();
  });

  // Cards blocked on OTHER cards are scheduling, not failure: a fan-out
  // parent waits on its children, a dependent waits on its deps. If the card
  // they wait on is parked, the whole run is merely waiting on the provider.
  it('is parked when a fan-out parent is awaiting_children and its child is parked — with the CHILD gate', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'root', { lane: 'plan', status: 'awaiting_children' }));
    db.insertCard(makeCard('r', 'child', { parent_id: 'root', lane: 'cwork' }));
    parkCard(makeCard('r', 'child'), NOW + 300);
    expect(getRunParkedRelease(db, 'r', NOW)).toEqual({ releaseAt: NOW + 300 });
  });

  it('is parked when a waiting dependent sits behind a parked sibling', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    db.insertCard(makeCard('r', 'c2', { status: 'waiting' }));
    parkCard(makeCard('r', 'c1'), NOW + 300);
    expect(getRunParkedRelease(db, 'r', NOW)).toEqual({ releaseAt: NOW + 300 });
  });

  it('is NOT parked when cards wait with NOTHING scheduled — a wait on nothing is a stall', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'root', { lane: 'plan', status: 'awaiting_children' }));
    db.insertCard(makeCard('r', 'c2', { status: 'waiting' }));
    expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
  });

  it.each(['claimed', 'working', 'interrupted', 'done_pending_ack'] as const)(
    'is NOT parked while a sibling is %s — work is in flight, not waiting',
    (status) => {
      seedRun('r');
      db.insertCard(makeCard('r', 'c1'));
      db.insertCard(makeCard('r', 'c2', { status }));
      parkCard(makeCard('r', 'c1'), NOW + 300);
      expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
    },
  );
});

describe('getRunParkedRelease — a gate is only a park when the card_log says so', () => {
  const NOW = 1000;

  it('is NOT parked when the only gate is a fan-out stagger, not a provider cap', () => {
    // The halt that produced this shape was a budget blowout, and the operator
    // has to see it as one: recording it 'parked' prints "parked behind a
    // provider rate limit", suppresses the stuck-card summary, and hands the
    // run to the ingress listener's unattended resume.
    seedRun('r');
    db.insertCard(makeCard('r', 'parent', { status: 'awaiting_children', lane: 'plan' }));
    db.insertCard(makeCard('r', 'c1', { parent_id: 'parent' }));
    db.insertCard(makeCard('r', 'c2', { parent_id: 'parent' }));
    staggerCard(makeCard('r', 'c1', { parent_id: 'parent' }), NOW + 30);
    staggerCard(makeCard('r', 'c2', { parent_id: 'parent' }), NOW + 60);
    expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
  });

  it('names the soonest RATE-LIMITED gate, ignoring an earlier stagger gate', () => {
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    db.insertCard(makeCard('r', 'c2'));
    staggerCard(makeCard('r', 'c1'), NOW + 30);
    parkCard(makeCard('r', 'c2'), NOW + 300);
    // MIN(release_at) would say 1030 and resume the run while it is still
    // capped; the cap is what the run is actually waiting on.
    expect(getRunParkedRelease(db, 'r', NOW)).toEqual({ releaseAt: NOW + 300 });
  });

  it('reads the LATEST lane move — a card whose last move was forward is not parked', () => {
    // card_log is UNIQUE(run_id, card_id, station, attempt, kind), so a second
    // move for the same card must differ in attempt to be recorded at all.
    seedRun('r');
    db.insertCard(makeCard('r', 'c1'));
    parkCard(makeCard('r', 'c1'), NOW + 300);
    staggerCard(makeCard('r', 'c1', { attempt: 1 }), NOW + 300);
    expect(getRunParkedRelease(db, 'r', NOW)).toBeNull();
  });
});

describe('getRunState — parked (issue #7)', () => {
  it('reports parked, with the gate time, when the runs row was halted as parked', () => {
    db.insertRun({ run_id: 'r', flow: '/flows/vertical.yaml', input_fingerprint: 'fp', status: 'halted', outcome: 'parked' });
    db.insertCard(makeCard('r', 'c1'));
    parkCard(makeCard('r', 'c1'), 1300);
    expect(getRunState(db, 'r', 1000)).toEqual({ status: 'parked', releaseAt: 1300, flow: '/flows/vertical.yaml' });
  });

  it('does NOT trust a stale parked row while a resume has cards in flight', () => {
    // After a parked exit the row stays halted/parked until the resumed process
    // exits. `run status` mid-resume must read the cards, not the row.
    db.insertRun({ run_id: 'r', flow: '/flows/vertical.yaml', input_fingerprint: 'fp', status: 'halted', outcome: 'parked' });
    db.insertCard(makeCard('r', 'c1', { status: 'working' }));
    expect(getRunState(db, 'r', 1000)).toEqual({ status: 'running' });
  });

  it('does NOT report parked once the gate has passed and the card is merely ready', () => {
    db.insertRun({ run_id: 'r', flow: '/flows/vertical.yaml', input_fingerprint: 'fp', status: 'halted', outcome: 'parked' });
    db.insertCard(makeCard('r', 'c1'));
    parkCard(makeCard('r', 'c1'), 1300);
    expect(getRunState(db, 'r', 1300)).toEqual({ status: 'running' });
  });

  it('still reports a plain halt as terminal/held, never parked', () => {
    db.insertRun({ run_id: 'r', flow: '/flows/vertical.yaml', input_fingerprint: 'fp', status: 'halted', outcome: 'halted' });
    db.insertCard(makeCard('r', 'c1'));
    terminalCard(makeCard('r', 'c1'), 'scrap');
    expect(getRunState(db, 'r')).toEqual({ status: 'terminal', outcome: 'halted' });
  });
});

describe('formatParkedRun — what the operator needs to come back', () => {
  it('names the gate as ISO-8601 UTC and the exact resume command', () => {
    const line = formatParkedRun('job-7', '/flows/vertical.yaml', 1_788_328_800);
    expect(line).toContain('2026-09-02T06:00:00.000Z');
    expect(line).toContain('conduit resume /flows/vertical.yaml --run job-7');
    expect(line).toMatch(/provider rate limit/);
  });
});
