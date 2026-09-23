/**
 * Contract tests for the SHIPPED triage flow config, not a fixture.
 *
 * These assert two properties of `triage/flow.yaml` that no other test can
 * catch, because both fail silently at runtime rather than loudly at load:
 *
 *   1. Both transforms actually RECEIVE the submission. A transform has no
 *      filesystem: its prompt is built by substituting `{{artifact}}`
 *      placeholders against that station's DECLARED inputs (src/flow/render.ts),
 *      and `conduit run --input` seeds its artifact at the entry station's
 *      FIRST declared input (src/cli/main.ts). A station with `inputs: []` and
 *      a prompt that merely mentions issue.json in prose produces a model call
 *      containing no issue text at all, and a model asked to classify nothing
 *      invents an answer. Nothing throws, so only an assertion finds it.
 *
 *   2. Both stations stay `kind: transform`. The run container holds
 *      GITHUB_TOKEN and CONDUIT_API_KEY while they execute, over anonymous
 *      attacker-authored text. That is safe for one reason: a transform is one
 *      model call with no tools and no loop (SPEC §4), so there is nothing an
 *      issue body can talk it into running. `kind: agentic` here would turn an
 *      unreviewed issue body into code execution against a token that can
 *      write to this repo.
 */

import { describe, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { loadFlow } from '../flow/load';
import { renderPrompt } from '../flow/render';
import type { FlowConfig } from '../types/kernel';

/** Repo root, resolved from this file rather than from the process CWD. */
const REPO_ROOT = join(import.meta.dir, '..', '..');
const TRIAGE_DIR = join(REPO_ROOT, 'triage');
const FLOW_PATH = join(TRIAGE_DIR, 'flow.yaml');

/** The two model-facing stations. `apply` is deterministic and has no prompt. */
const TRANSFORM_STATIONS: Array<'classify' | 'scan'> = ['classify', 'scan'];

/** Matches `{{name}}`, the placeholder syntax renderPrompt substitutes. */
const PLACEHOLDER_RE = /\{\{([^{}\s]+)\}\}/g;

function loadTriageFlow(): FlowConfig {
  const result = loadFlow(FLOW_PATH);
  if (!result.ok) {
    throw new Error(`triage/flow.yaml failed to load: ${JSON.stringify(result.errors)}`);
  }
  return result.flow;
}

/**
 * Read a station's prompt template. The loader resolves `prompt_file` against
 * the flow's own directory, so the result is absolute when the flow was loaded
 * by absolute path and repo-relative when it was not. Handle both.
 */
function readPrompt(promptFile: string): string {
  return readFileSync(isAbsolute(promptFile) ? promptFile : join(REPO_ROOT, promptFile), 'utf-8');
}

describe('triage/flow.yaml — shipped config', () => {
  test('loads and validates', () => {
    expect(loadTriageFlow().name).toBe('triage');
  });

  test('classify and scan are transform stations (load-bearing: no tools next to a write token)', () => {
    const flow = loadTriageFlow();
    for (const id of TRANSFORM_STATIONS) {
      expect(flow.stations[id]?.kind).toBe('transform');
    }
  });

  test('apply is the only station that can write, and it runs no model', () => {
    const apply = loadTriageFlow().stations['apply'];
    expect(apply?.kind).toBe('deterministic');
    expect(apply?.effectful).toBe(true);
  });

  describe.each(TRANSFORM_STATIONS)('%s receives the submission', (id) => {
    test('declares issue.json as an input', () => {
      expect(loadTriageFlow().stations[id]?.inputs).toContain('issue.json');
    });

    test('references {{issue.json}}, and every placeholder is a declared input', () => {
      const station = loadTriageFlow().stations[id];
      expect(station?.prompt_file).toBeTruthy();
      const template = readPrompt(station!.prompt_file!);
      const placeholders = [...template.matchAll(PLACEHOLDER_RE)].map((m) => m[1]);

      // A prompt with no placeholder renders verbatim: the model would be
      // asked to classify text it was never shown.
      expect(placeholders.length).toBeGreaterThan(0);
      expect(placeholders).toContain('issue.json');

      // renderPrompt REJECTS a placeholder that is not a declared input, so an
      // undeclared one is a load-time-clean, dispatch-time-fatal flow.
      const declared = new Set(station?.inputs ?? []);
      for (const name of placeholders) {
        expect(declared.has(name)).toBe(true);
      }
    });

    test('renders the submission text into the prompt handed to the model', () => {
      const station = loadTriageFlow().stations[id];
      const template = readPrompt(station!.prompt_file!);

      // Render against a scratch root holding a representative issue.json,
      // which is exactly what the engine does at dispatch.
      const scratch = mkdtempSync(join(tmpdir(), 'conduit-triage-prompt-'));
      const payload = JSON.stringify({
        kind: 'issue',
        title: 'CONDUIT_TEST_TITLE_MARKER',
        body: 'CONDUIT_TEST_BODY_MARKER',
        number: 1,
      });
      writeFileSync(join(scratch, 'issue.json'), payload);

      try {
        const rendered = renderPrompt(template, station?.inputs ?? [], scratch);
        expect(rendered).toContain('CONDUIT_TEST_TITLE_MARKER');
        expect(rendered).toContain('CONDUIT_TEST_BODY_MARKER');
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
    });
  });
});
