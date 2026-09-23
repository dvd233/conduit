/**
 * The listener's one alert-channel rule.
 *
 * The re-drive sweep (ingress/recovery.ts) and the parked-resume sweep
 * (ingress/parked.ts) both have to answer "which channel hears about this
 * flow?", and they used to answer it with two identical private copies that
 * were free to drift. Both now call the single implementation here; these
 * tests pin the three branches that implementation owes its callers.
 */
import { describe, it, expect } from 'bun:test';
import { resolveAlertChannel, UNATTRIBUTED_FLOW_ID, type RedriveAlerting } from './alert-channel';

/** An alerting seam whose channel table is all these tests care about. */
function alerting(channels: Record<string, string>, globalAlertChannel: string): RedriveAlerting {
  return { alert: async () => {}, channels, globalAlertChannel };
}

describe('resolveAlertChannel — the flow egress target, else the listener-global one', () => {
  it("prefers the flow's own resolved channel", () => {
    const alerts = alerting({ flowA: '#a', flowB: '#b' }, '#ops');
    expect(resolveAlertChannel(alerts, 'flowA')).toBe('#a');
    expect(resolveAlertChannel(alerts, 'flowB')).toBe('#b');
  });

  it('falls back to the global channel for a flow that declares no egress', () => {
    expect(resolveAlertChannel(alerting({ flowA: '#a' }, '#ops'), 'flowB')).toBe('#ops');
  });

  it('falls back to the global channel for an unattributed (pre-v9) row', () => {
    // flow_id is null on a row accepted before the attribution columns existed.
    // Silence is the bug being fixed, so it still alerts — globally.
    expect(resolveAlertChannel(alerting({ flowA: '#a' }, '#ops'), null)).toBe('#ops');
  });

  it('resolves to empty only when there is no alerting at all', () => {
    // No seam means nothing reads the channel; the empty string is never sent.
    expect(resolveAlertChannel(undefined, 'flowA')).toBe('');
    expect(resolveAlertChannel(undefined, null)).toBe('');
  });

  it('names the unattributed flow explicitly rather than guessing an owner', () => {
    expect(UNATTRIBUTED_FLOW_ID).toBe('unknown');
  });
});
