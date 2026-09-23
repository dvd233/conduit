/**
 * Slot-gated `conduit resume` (the pre-public run-slot and HITL review; issue #7).
 *
 * A resume is a real process against the same serial model box as a launch, so
 * it draws from the listener's run-slot pool — but two callers want different
 * behaviour when the pool is saturated:
 *
 *   - a HITL reply (runGatedHitlResume) BYPASSES the gate: a human's pick
 *     resumes work that was already admitted once, arrives at human latency,
 *     and queueing it behind a photo backlog reads as a broken reply loop.
 *   - a parked-run resume (ingress/parked.ts) must NOT bypass: a provider rate
 *     limit is exactly the moment a stampede of resumes is wrong, and nothing
 *     is waiting on the other end.
 *
 * Both suppress a resume for a run whose resume is already in flight (the
 * double-driver failure mode — the run lease would reject the loser anyway,
 * so suppression only saves the doomed spawn).
 */
import type { ConduitDB } from '../persistence/db';
import type { RunSlots } from './run-slots';

export interface GatedResumeOptions {
  /** Slot registration id, e.g. `hitl-resume:<runId>`. */
  slotId: string;
  runId: string;
  /** ingress_log source for the suppression entry. */
  source: string;
  /** Run anyway when every slot is busy (unregistered — see the header). */
  bypassWhenSaturated: boolean;
}

export type GatedResumeStart =
  /** The resume is running; `done` settles when it finishes and the slot (if any) is released. */
  | { outcome: 'ran'; done: Promise<void> }
  /** A resume for this run is already in flight — suppressed and logged. */
  | { outcome: 'duplicate' }
  /** No free slot and no bypass — not started; the caller retries on a later sweep. */
  | { outcome: 'full' };

/**
 * Acquire (or bypass) a slot SYNCHRONOUSLY and start the resume. The
 * acquisition is reported immediately so a sweep can move on while the
 * resumed run executes; `done` is for callers that want to wait it out.
 */
export function startGatedResume(
  slots: RunSlots,
  db: ConduitDB,
  opts: GatedResumeOptions,
  resume: () => Promise<void>,
): GatedResumeStart {
  const acquisition = slots.tryAcquire(opts.slotId);
  if (acquisition === 'duplicate') {
    db.appendIngressLog({
      source: opts.source,
      eventId: null,
      outcome: 'duplicate',
      reason: `a resume for run '${opts.runId}' is already in flight — suppressed`,
    });
    return { outcome: 'duplicate' };
  }
  if (acquisition === 'full' && !opts.bypassWhenSaturated) return { outcome: 'full' };

  // `resume()` is invoked inside the try, not just awaited: a closure that
  // throws SYNCHRONOUSLY never produces a promise, so a bare
  // `resume().finally(...)` would propagate past the release and strand the
  // slot for the process lifetime (run slots are never persisted or reaped).
  // The inline version this replaced used try/finally around `await resume()`,
  // which covered that; keep the guarantee rather than relying on both callers
  // happening to be `async`.
  let running: Promise<void>;
  try {
    running = resume();
  } catch (err) {
    if (acquisition === 'acquired') slots.release(opts.slotId);
    throw err;
  }
  const done = running.finally(() => {
    // A bypassed resume was never registered, so there is nothing to free.
    if (acquisition === 'acquired') slots.release(opts.slotId);
  });
  return { outcome: 'ran', done };
}

/**
 * Run a HITL-reply resume with OPPORTUNISTIC slot participation — a deliberate
 * decision from the pre-public run-slot and HITL review, not an accident of wiring:
 *
 *   - Slot free   → the resume claims it (`hitl-resume:<runId>`), so run-slot
 *     accounting stays honest in the common case.
 *   - Saturated   → the resume proceeds ANYWAY (bypass). Under saturation
 *     `max_concurrent_runs` can therefore be exceeded by in-flight resumes —
 *     bounded by the number of parked runs. (Bypassed resumes are not
 *     registered, so saturation-time duplicates still fall through to the run
 *     lease — same protection, one step later.)
 *   - A resume for the SAME run already in flight → suppressed (logged as
 *     'duplicate').
 */
export async function runGatedHitlResume(
  slots: RunSlots,
  db: ConduitDB,
  runId: string,
  resume: () => Promise<void>,
): Promise<void> {
  const started = startGatedResume(
    slots,
    db,
    { slotId: `hitl-resume:${runId}`, runId, source: 'slack-hitl-reply', bypassWhenSaturated: true },
    resume,
  );
  if (started.outcome === 'ran') await started.done;
}
