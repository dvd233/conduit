/**
 * The listener's one alert-channel rule (#8, #7).
 *
 * Two independent sweeps alert on failure — the re-drive path (ingress/recovery.ts)
 * and the parked-resume path (ingress/parked.ts) — and both have to answer the
 * same question: given a flow, which channel hears about it? The rule is the
 * flow's own resolved egress target, falling back to the listener-global one.
 *
 * It lives here rather than in either caller because `recovery.ts` already
 * imports `parked.ts` at runtime; exporting the helper from either side would
 * make that edge circular. A neutral module keeps the single implementation
 * without one.
 */
import type { AlertSeam } from './spawn';

/** Failure-alert seam plus the channel table the rule below reads. */
export interface RedriveAlerting {
  alert: AlertSeam;
  /** flowId → resolved alert channel (flow egress[0].target ?? globalAlertChannel). */
  channels: Record<string, string>;
  /** Listener-global fallback target. */
  globalAlertChannel: string;
}

/**
 * flow_id on a pre-v9 ingress_events row is null — the attribution columns did
 * not exist when it was accepted. Alert anyway (silence is the bug being fixed
 * here) with an explicit placeholder rather than guessing an owning flow.
 */
export const UNATTRIBUTED_FLOW_ID = 'unknown';

/**
 * The channel rule: the flow's first egress target, else the listener-global
 * channel. Empty only when there is no alerting at all, in which case nothing
 * reads it.
 */
export function resolveAlertChannel(
  alerts: RedriveAlerting | undefined,
  flowId: string | null,
): string {
  if (alerts === undefined) return '';
  return (flowId !== null ? alerts.channels[flowId] : undefined) ?? alerts.globalAlertChannel;
}
