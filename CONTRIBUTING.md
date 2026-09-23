# Contributing to Conduit

Thanks for helping improve Conduit. Bug reports, focused feature proposals,
documentation fixes, and tests are welcome.

## Development setup

Conduit uses [Bun](https://bun.sh/) and TypeScript.

```bash
git clone https://github.com/theaiteam-dev/conduit
cd conduit
bun install
bun test src/ --pass-with-no-tests
bun run typecheck
```

Copy `.env.example` to `.env` only when a test or example needs a real model
gateway. Never commit credentials, runtime databases, journals, or generated
flow artifacts.

Optionally, enable the documentation hook. Some documents in this repo are
normative — SPEC.md states what the kernel must do — and
[`drift`](https://github.com/fiberplane/drift) binds them to the code they
govern:

```bash
# Pinned to the version CI installs — see .github/workflows/docs.yml. `sig` is
# an AST fingerprint on a pre-1.0 tool, so a different local build can disagree
# with CI about which anchors are stale. Bump both together, on purpose.
curl -fsSL https://drift.fp.dev/install.sh | sh -s -- --version v0.10.1
git config core.hooksPath .githooks
```

(`brew install fiberplane/tap/drift` works too, but the tap tracks latest and
offers no versioned formula, so it can drift out of step with CI.)

The hook runs at commit time (`bun run docs:check` runs the same thing by hand;
`bun run docs:check:all` reports the whole corpus and gates nothing), scoped to
your staged files — additions,
modifications, deletions and both sides of a rename, since a binding whose
target was moved or removed is exactly what needs flagging — and tells you when
a change leaves bound prose unvouched-for. It is skipped entirely when `drift`
is not installed, so it never blocks a contributor who has not set it up; CI
runs the same check as an advisory `docs` job.

## Making a change

1. Open an issue for substantial behavior or design changes.
2. Keep pull requests focused and explain the user-visible effect.
3. Add or update tests for behavioral changes.
4. Run `bun test src/ --pass-with-no-tests` and `bun run typecheck`.
5. Run `bun run test:blackbox` when changing CLI, ingress, persistence, or
   process-boundary behavior.
6. Run `bun run test:mutation` when touching harness usage accounting. It
   deletes each `foldHarnessUsage` call site in turn and requires a named test
   to fail; a fold site that survives deletion is unprotected no matter how
   green the suite is. It takes about three seconds, runs in the required CI
   gate, and `--list` explains each site. If you move or rename a fold site,
   update the manifest in `scripts/mutation-check.ts` in the same change — a
   stale entry fails the check rather than skipping it.
7. Update documentation and `CHANGELOG.md` when behavior changes.
8. If `drift check` reports a stale anchor, re-read the section it names and
   either correct the prose or re-stamp it with
   `drift link <doc> --doc-is-still-accurate`. Re-stamping asserts you read it.

By contributing, you agree that your contribution may be distributed under the
MIT License.
