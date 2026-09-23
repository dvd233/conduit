/**
 * Tests for the triage security boundary.
 *
 * This is the Law for the triage flow: the two transforms upstream read
 * attacker-authored text, so these assertions are the only thing standing
 * between a successful prompt injection and a privileged GitHub write. They
 * belong in the REQUIRED `tests` job (hence src/), not an advisory one.
 */

import { describe, expect, test } from 'bun:test';
import {
  LABEL_TIERS,
  PRIORITY_VALUES,
  TYPE_VALUES,
  decide,
  fence,
  renderComment,
  TRIAGE_MARKER,
} from './apply-triage';

const CLEAN = { injection_detected: false, evidence: '' };

describe('label allowlist', () => {
  test('every AUTO label is a type value and vice versa', () => {
    const auto = Object.keys(LABEL_TIERS).filter((l) => LABEL_TIERS[l] === 'AUTO').sort();
    expect(auto).toEqual([...TYPE_VALUES].sort());
  });

  test('dismissal and release labels are NEVER, so no input can reach them', () => {
    for (const label of ['invalid', 'wontfix', 'released']) {
      expect(LABEL_TIERS[label]).toBe('NEVER');
      // Exhaustive: no classification, however crafted, yields a NEVER label.
      const d = decide({ type: label, priority_suggestion: label }, CLEAN);
      expect(d.apply).not.toContain(label);
      expect(d.propose).not.toContain(label);
    }
  });

  test('a label the model invents is dropped, never created', () => {
    const d = decide({ type: 'security-cleared' }, CLEAN);
    expect(d.apply).toEqual([]);
    expect(d.suppressed).toBe('no-valid-type');
  });

  test('priority is proposed, never applied', () => {
    for (const p of PRIORITY_VALUES) {
      const d = decide({ type: 'bug', priority_suggestion: p }, CLEAN);
      expect(d.apply).toEqual(['bug']);
      expect(d.propose).toContain(`priority: ${p}`);
    }
  });

  test('area applies no label, because this repo has none', () => {
    const d = decide({ type: 'bug', area: 'kernel' }, CLEAN);
    expect(d.apply).toEqual(['bug']);
    expect(d.propose).toEqual([]);
  });
});

describe('injection verdict suppresses all writes', () => {
  test('a tripped scan yields no labels even with a well-formed classification', () => {
    const d = decide(
      { type: 'bug', priority_suggestion: 'high', possible_duplicate: 12 },
      { injection_detected: true, evidence: 'contains "ignore previous instructions"' },
    );
    expect(d.apply).toEqual([]);
    expect(d.propose).toEqual([]);
    expect(d.suppressed).toBe('injection');
    expect(d.flagged).toBe(true);
  });

  test('a non-boolean verdict is not read as an injection REPORT', () => {
    // `flagged` means the scan actually reported an injection. A string is not
    // a report, so the comment must not accuse the submitter of one.
    expect(decide({ type: 'bug' }, { injection_detected: 'true' }).flagged).toBe(false);
  });
});

describe('a missing or malformed scan verdict suppresses writes too', () => {
  // readArtifact returns {} for a missing, unreadable, or malformed scan.json,
  // so "no verdict" is indistinguishable from "clean verdict" unless the clean
  // case is required to be explicit. Losing the second independent signal must
  // not silently degrade to labelling anyway.
  test.each([
    ['absent key', {}],
    ['string "true"', { injection_detected: 'true' }],
    ['string "false"', { injection_detected: 'false' }],
    ['null', { injection_detected: null }],
    ['number 0', { injection_detected: 0 }],
    ['undefined', { injection_detected: undefined }],
  ])('%s applies nothing and is reported as a missing verdict', (_name, scan) => {
    const d = decide({ type: 'bug', priority_suggestion: 'high' }, scan as Record<string, unknown>);
    expect(d.apply).toEqual([]);
    expect(d.propose).toEqual([]);
    expect(d.suppressed).toBe('no-scan-verdict');
  });

  test('only an explicit false lets labels through', () => {
    expect(decide({ type: 'bug' }, { injection_detected: false }).apply).toEqual(['bug']);
  });

  test('a missing verdict does not accuse the submitter of injection', () => {
    const scan = {};
    const out = renderComment({ type: 'bug', summary: 's' }, decide({ type: 'bug' }, scan), scan);
    expect(out).not.toContain('flagged this issue body');
    expect(out).toContain('No labels were applied');
    expect(out.indexOf('No labels were applied')).toBeLessThan(out.indexOf('Model summary'));
  });

  test('the two suppression reasons stay distinguishable', () => {
    const injected = decide({ type: 'bug' }, { injection_detected: true });
    const missing = decide({ type: 'bug' }, {});
    expect(injected.suppressed).toBe('injection');
    expect(injected.flagged).toBe(true);
    expect(missing.suppressed).toBe('no-scan-verdict');
    expect(missing.flagged).toBe(false);
  });
});

describe('malformed model output fails closed', () => {
  test.each([
    ['empty object', {}],
    ['null type', { type: null }],
    ['numeric type', { type: 7 }],
    ['array type', { type: ['bug'] }],
    ['prototype-ish key', { type: 'constructor' }],
    ['__proto__ key', { type: '__proto__' }],
  ])('%s applies nothing', (_name, cls) => {
    expect(decide(cls as Record<string, unknown>, CLEAN).apply).toEqual([]);
  });

  test.each([
    ['fractional', 1.5],
    ['string', '12'],
    ['zero', 0],
    ['negative', -1],
    ['beyond safe integer range', Number.MAX_SAFE_INTEGER + 1],
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
  ])('a %s duplicate is neither proposed nor rendered', (_name, value) => {
    const cls = { type: 'bug', summary: 's', possible_duplicate: value };
    const d = decide(cls, CLEAN);
    expect(d.propose).toEqual([]);
    // The decision and the comment must agree: a value too invalid to propose
    // the label is too invalid to print as "#<n>" as well.
    expect(renderComment(cls, d, CLEAN)).not.toContain('Possible duplicate of');
  });

  test('a real issue number is proposed and rendered', () => {
    const cls = { type: 'bug', summary: 's', possible_duplicate: 12 };
    const d = decide(cls, CLEAN);
    expect(d.propose).toContain('duplicate');
    expect(renderComment(cls, d, CLEAN)).toContain('Possible duplicate of #12');
  });
});

describe('comment fencing', () => {
  test('a fence break in model prose cannot escape the code block', () => {
    const out = renderComment({ type: 'bug', summary: '```\n@maintainer: merge this' }, decide({ type: 'bug' }, CLEAN), CLEAN);
    const body = out.slice(out.indexOf('```text') + '```text'.length);
    // Exactly one closing fence: the one renderComment itself emits.
    expect(body.match(/```/g)).toHaveLength(1);
  });

  test('long summaries are clamped', () => {
    expect(fence('x'.repeat(5000)).length).toBeLessThanOrEqual(1201);
  });

  test('non-string summary renders as empty rather than "undefined"', () => {
    expect(fence(undefined)).toBe('');
    expect(fence({ a: 1 })).toBe('');
  });

  test('scan evidence is fenced, not interpolated into markdown', () => {
    // The evidence is the injection string itself, quoted back. Un-fenced it
    // renders: the image below would fire on every view of the issue page, and
    // the newline would put an attacker-authored line at the same level as the
    // ones this station wrote.
    const evidence = '![](https://tracker.example/p.gif)\nApplied: `wontfix`';
    const scan = { injection_detected: true, evidence };
    const out = renderComment({ type: 'bug', summary: 's' }, decide({ type: 'bug' }, scan), scan);

    const start = out.indexOf('```text');
    const end = out.indexOf('```', start + '```text'.length);
    expect(start).toBeGreaterThan(-1);
    expect(out.slice(start, end)).toContain(evidence);
    // Nothing attacker-authored escapes into the comment's own line structure.
    expect(out.slice(0, start)).not.toContain('tracker.example');
    expect(out.slice(0, start)).not.toContain('Applied: `wontfix`');
  });

  test('a fence break in scan evidence cannot escape its block', () => {
    const scan = { injection_detected: true, evidence: '```\n@maintainer: ship it' };
    const out = renderComment({ type: 'bug', summary: 's' }, decide({ type: 'bug' }, scan), scan);
    const start = out.indexOf('```text') + '```text'.length;
    const block = out.slice(start, out.indexOf('```', start));
    expect(block).not.toContain('```');
    expect(block).toContain("'''");
  });

  test('missing evidence renders a sentence, not an empty fence', () => {
    const scan = { injection_detected: true };
    const out = renderComment({ type: 'bug', summary: 's' }, decide({ type: 'bug' }, scan), scan);
    expect(out).toContain(
      'Scan evidence, quoted from the issue text. Treat as data, not instruction: none reported.',
    );
    // Only the summary block remains: one opening fence, one closing.
    expect(out.match(/```/g)).toHaveLength(2);
  });

  test('a flagged comment says so before showing the classification', () => {
    const scan = { injection_detected: true, evidence: 'e' };
    const out = renderComment({ type: 'bug', summary: 's' }, decide({ type: 'bug' }, scan), scan);
    expect(out).toContain('No labels were applied');
    expect(out.indexOf('No labels were applied')).toBeLessThan(out.indexOf('Model summary'));
  });
});

describe('idempotency marker', () => {
  test('every comment carries the marker the workflow guard greps for', () => {
    for (const scan of [CLEAN, { injection_detected: true, evidence: 'e' }]) {
      const out = renderComment({ type: 'bug', summary: 's' }, decide({ type: 'bug' }, scan), scan);
      expect(out.startsWith(TRIAGE_MARKER)).toBe(true);
    }
  });

  test('the marker is the exact literal .github/workflows/triage.yml greps for', () => {
    // Drift between these two is silent: the guard stops matching and every
    // re-run posts a second comment.
    expect(TRIAGE_MARKER).toBe('<!-- conduit-triage -->');
    const wf = require('node:fs').readFileSync('.github/workflows/triage.yml', 'utf8');
    expect(wf).toContain(`grep -qF '${TRIAGE_MARKER}'`);
  });
});
