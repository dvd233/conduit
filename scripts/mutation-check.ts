#!/usr/bin/env bun
/**
 * Mutation check for the harness usage FOLD SITES.
 *
 * WHY THIS EXISTS
 * ---------------
 * Issue #26's bug was not a missing feature. The code invoked a billed harness
 * critic, received a `HarnessResult` carrying real token and cost figures, and
 * then dropped it on the floor — while every surrounding test stayed green,
 * because nothing asserted that the number reached a budget. A fold site is
 * exactly the kind of line that can be deleted without breaking a type, a
 * schema, or any test that reads a journal row: the row is still written, the
 * verdict is still correct, and only `tokensSpent` is quietly wrong.
 *
 * That is not hypothetical. While building #26 an early version of this suite
 * passed with `foldHarnessUsage(thrownUsage.tokens)` deleted, because the
 * assertion read the JOURNAL row (still written by the untouched line below
 * the fold) rather than the run's accumulator. The test named the right
 * behaviour and proved a different one.
 *
 * So the guarantee this script enforces is narrow and mechanical:
 *
 *     for each fold site: delete it, and some named test MUST fail.
 *
 * A fold site that survives deletion is unprotected, whatever its tests claim.
 *
 * WHAT IT IS NOT
 * --------------
 * Not a general mutation-testing run (no operator zoo, no mutation score, no
 * whole-file coverage). The manifest is hand-written and deliberately small:
 * five lines that move money into a budget. A broad mutation run over this
 * codebase would take a long time and produce mostly equivalent mutants, which
 * is how a check earns a permanent skip. This one runs in seconds and every
 * failure is actionable.
 *
 * Nor is it a substitute for reading the tests. It proves a test is LOAD-
 * BEARING; it says nothing about whether it asserts the right thing.
 *
 * USAGE
 * -----
 *   bun scripts/mutation-check.ts              # all mutants, guard tests only
 *   bun scripts/mutation-check.ts --list       # print the manifest, run nothing
 *   bun scripts/mutation-check.ts --only gate-critic-fold [--only ...]
 *   bun scripts/mutation-check.ts --full       # run the whole src/ suite per mutant
 *   bun scripts/mutation-check.ts --json       # machine-readable result
 *
 * EXIT CODES
 * ----------
 *   0  every mutant was killed by its guards
 *   1  a mutant SURVIVED (an unprotected fold site), or the manifest is stale
 *
 * A STALE MANIFEST IS A FAILURE, NOT A SKIP. If a `find` string no longer
 * occurs exactly once, the fold site moved or was renamed and this script can
 * no longer see it. Silently skipping it would reproduce the exact failure
 * mode the script exists to catch, so it exits 1 and says which entry to fix.
 *
 * SAFETY
 * ------
 * Each target file's exact bytes are snapshotted (in memory AND to a temp dir)
 * before any edit and restored in a `finally`, plus on SIGINT/SIGTERM. If the
 * process is killed in a way that runs neither, the temp-dir path printed at
 * startup holds the originals. Restoration is verified byte-for-byte before
 * the script reports success.
 */
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, basename } from 'node:path';

/** One fold site, and the tests that claim to protect it. */
interface Mutant {
  /** Stable id for --only and for CI logs. */
  id: string;
  /** Source file containing the fold site. */
  file: string;
  /**
   * The exact source text to delete. MUST occur exactly once in `file` — see
   * the stale-manifest note above.
   */
  find: string;
  /** What replaces it. A comment, so line-based tooling still sees a line. */
  replace: string;
  /**
   * Test files that must contain at least one failure once `find` is gone.
   * Discovered empirically (mutate, run the whole suite, record what broke),
   * not guessed — keep them that way when adding an entry.
   */
  guards: string[];
  /** What is silently wrong if this fold is missing. Shown on survival. */
  why: string;
}

const MUTANTS: Mutant[] = [
  {
    id: 'gate-critic-fold',
    file: 'src/controller/executor.ts',
    find: 'foldHarnessUsage(usage.tokens);',
    replace: '/* MUTATION-CHECK: gate-critic fold deleted */',
    guards: ['src/controller/executor-harness-gate.test.ts'],
    why:
      "a harness gate critic's spend never reaches the run/wave budget — issue " +
      "#26's original bug. The verdict and the journal row stay correct, so " +
      'only the budget is wrong, and only on runs that use an agentic critic.',
  },
  {
    id: 'park-throw-fold',
    file: 'src/controller/executor.ts',
    find: 'foldHarnessUsage(parkedUsage.tokens);',
    replace: '/* MUTATION-CHECK: rate-limit-park fold deleted */',
    guards: ['src/controller/executor-harness-journal.test.ts'],
    why:
      'a rate-limited invocation that was already billed before the provider ' +
      'refused is written off. Parks consume no execution attempt, so an ' +
      'unbounded amount of real spend can accumulate invisibly.',
  },
  {
    id: 'maker-throw-fold',
    file: 'src/controller/executor.ts',
    find: 'foldHarnessUsage(thrownUsage.tokens);',
    replace: '/* MUTATION-CHECK: maker-throw fold deleted */',
    guards: ['src/controller/executor-harness-journal.test.ts'],
    why:
      'a maker invocation that threw AFTER being billed (a timeout, most ' +
      'often) costs real money and counts as zero. This is the mutant that ' +
      'survived an early version of the #26 suite.',
  },
  {
    id: 'maker-success-fold',
    file: 'src/controller/executor.ts',
    find: 'foldHarnessUsage(reportedTokens);',
    replace: '/* MUTATION-CHECK: maker-success fold deleted */',
    guards: [
      'src/controller/executor-harness-journal.test.ts',
      'src/controller/executor-harness.test.ts',
      'src/controller/executor-harness-liveness.test.ts',
    ],
    why:
      'the ordinary successful harness maker call — the single largest line ' +
      'item in any agentic run — does not count against the budget at all.',
  },
  {
    id: 'subflow-fold',
    file: 'src/controller/executor.ts',
    find: 'if (result.tokens !== undefined) foldHarnessUsage(result.tokens);',
    replace: '/* MUTATION-CHECK: subflow fold deleted */',
    guards: ['src/controller/executor-subflow.test.ts'],
    why:
      "a child subflow's entire rolled-up spend is invisible to the parent " +
      'run, so a parent budget bounds only the work it does directly (FR-18).',
  },
];

// ---------------------------------------------------------------------------

interface Args {
  list: boolean;
  full: boolean;
  json: boolean;
  only: Set<string>;
}

function parseArgs(argv: string[]): Args {
  const args: Args = { list: false, full: false, json: false, only: new Set() };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--list') args.list = true;
    else if (a === '--full') args.full = true;
    else if (a === '--json') args.json = true;
    else if (a === '--only') {
      const v = argv[++i];
      if (v === undefined) fatal('--only requires a mutant id');
      args.only.add(v);
    } else fatal(`unknown flag '${a}' (see the header for usage)`);
  }
  const ids = new Set(MUTANTS.map((m) => m.id));
  for (const id of args.only) {
    if (!ids.has(id)) fatal(`--only '${id}' is not a mutant id. Known: ${[...ids].join(', ')}`);
  }
  return args;
}

function fatal(msg: string): never {
  console.error(`mutation-check: ${msg}`);
  process.exit(1);
}

/** Run a test command, returning only whether it failed. Output is captured. */
function runTests(paths: string[]): { failed: boolean; output: string } {
  const proc = Bun.spawnSync(['bun', 'test', ...paths], {
    stdout: 'pipe',
    stderr: 'pipe',
    env: { ...process.env, FORCE_COLOR: '0' },
  });
  const output = `${new TextDecoder().decode(proc.stdout)}${new TextDecoder().decode(proc.stderr)}`;
  return { failed: proc.exitCode !== 0, output };
}

/** Test names bun reported as failing, for a readable kill report. */
function failingTestNames(output: string): string[] {
  const names: string[] = [];
  for (const line of output.split('\n')) {
    const m = /^\(fail\)\s+(.*?)(?:\s+\[[\d.]+m?s\])?$/.exec(line.trim());
    if (m?.[1] !== undefined) names.push(m[1]);
  }
  return names;
}

interface Result {
  id: string;
  killed: boolean;
  guards: string[];
  killedBy: string[];
  /**
   * The mutated run exited non-zero but reported no `(fail)` line — the
   * mutation BROKE the run (a parse error, a crash) instead of failing an
   * assertion. Counting that as a kill would be a false negative: it would
   * report the fold site protected on the strength of a run that never got
   * far enough to check it.
   */
  broke: boolean;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const selected = args.only.size > 0 ? MUTANTS.filter((m) => args.only.has(m.id)) : MUTANTS;

  if (args.list) {
    for (const m of selected) {
      console.log(`${m.id}\n  site:   ${m.file} -> ${m.find}\n  guards: ${m.guards.join(', ')}\n  risk:   ${m.why}\n`);
    }
    return;
  }

  // Snapshot every target file BEFORE anything is mutated, so a failure
  // partway through still restores all of them.
  const files = [...new Set(selected.map((m) => m.file))];
  const snapshots = new Map<string, string>();
  const backupDir = mkdtempSync(join(tmpdir(), 'conduit-mutation-'));
  for (const f of files) {
    const original = readFileSync(f, 'utf-8');
    snapshots.set(f, original);
    writeFileSync(join(backupDir, basename(f)), original);
  }

  const restore = (): void => {
    for (const [f, original] of snapshots) writeFileSync(f, original);
  };
  const onSignal = (): never => {
    restore();
    console.error('\nmutation-check: interrupted; sources restored.');
    process.exit(130);
  };
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);

  const results: Result[] = [];
  try {
    if (!args.json) console.error(`mutation-check: originals backed up to ${backupDir}`);

    // A mutant that "fails the guards" proves nothing if the guards fail
    // clean. Establish the baseline once, over the union of every guard.
    const allGuards = [...new Set(selected.flatMap((m) => m.guards))];
    const baseline = runTests(args.full ? ['src/'] : allGuards);
    if (baseline.failed) {
      restore();
      console.error(baseline.output.split('\n').slice(-25).join('\n'));
      fatal('the guard tests FAIL on unmutated sources. Fix them first; until they pass, "fails when mutated" means nothing.');
    }

    for (const m of selected) {
      const original = snapshots.get(m.file)!;
      const occurrences = original.split(m.find).length - 1;
      if (occurrences !== 1) {
        restore();
        fatal(
          `manifest entry '${m.id}' is STALE: its find string occurs ${occurrences} times in ${m.file} ` +
            `(expected exactly 1). The fold site moved or was renamed — update MUTANTS in this file. ` +
            `Skipping it would reproduce the bug this script exists to catch.`,
        );
      }

      for (const g of m.guards) {
        if (!existsSync(g)) {
          restore();
          fatal(`mutant '${m.id}' names a guard that does not exist: ${g}. A missing path makes 'bun test' fail for the wrong reason, which would report this fold site protected when nothing checks it.`);
        }
      }

      writeFileSync(m.file, original.replace(m.find, m.replace));
      const run = runTests(args.full ? ['src/'] : m.guards);
      writeFileSync(m.file, original);

      const killedBy = run.failed ? failingTestNames(run.output) : [];
      const broke = run.failed && killedBy.length === 0;
      results.push({ id: m.id, killed: run.failed && !broke, guards: m.guards, killedBy, broke });

      if (!args.json) {
        if (broke) {
          console.log(`  BROKE     ${m.id}  (non-zero exit, but no test reported a failure)`);
          console.log(`    The mutation stopped the run rather than failing an assertion, so this`);
          console.log(`    proves nothing about the fold site. Make the replacement a valid statement.`);
          console.log(run.output.split('\n').slice(-15).join('\n'));
        } else if (run.failed) {
          const shown = killedBy.slice(0, 2).map((n) => `\n    ${n}`).join('');
          console.log(`  KILLED    ${m.id}  (${killedBy.length} failing)${shown}`);
        } else {
          console.log(`  SURVIVED  ${m.id}`);
          console.log(`    deleted: ${m.file} -> ${m.find}`);
          console.log(`    guards:  ${m.guards.join(', ')} — all still pass with the fold gone`);
          console.log(`    risk:    ${m.why}`);
        }
      }
    }
  } finally {
    restore();
    for (const [f, original] of snapshots) {
      if (readFileSync(f, 'utf-8') !== original) {
        console.error(`mutation-check: FAILED TO RESTORE ${f}. Original is at ${join(backupDir, basename(f))}`);
        process.exit(1);
      }
    }
  }

  const broke = results.filter((r) => r.broke);
  const survived = results.filter((r) => !r.killed && !r.broke);
  if (args.json) {
    console.log(JSON.stringify({ mutants: results, survived: survived.length, broke: broke.length }, null, 2));
  } else {
    if (survived.length > 0) {
      console.log(`\n${survived.length}/${results.length} fold site(s) UNPROTECTED: ${survived.map((r) => r.id).join(', ')}`);
      console.log('Add an assertion that reads the run/wave ACCUMULATOR (tokensSpent), not a journal row —');
      console.log('the journal row is written by a different line and survives the fold being deleted.');
    }
    if (broke.length > 0) {
      console.log(`\n${broke.length}/${results.length} mutant(s) BROKE the run instead of failing a test: ${broke.map((r) => r.id).join(', ')}`);
      console.log('Fix the manifest entry — an inconclusive mutant is not a passing one.');
    }
    if (survived.length === 0 && broke.length === 0) {
      console.log(`\nAll ${results.length} fold sites are protected: deleting any one fails a named test.`);
    }
  }
  process.exit(survived.length > 0 || broke.length > 0 ? 1 : 0);
}

await main();
