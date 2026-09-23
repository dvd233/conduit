#!/usr/bin/env bash
# Check out and exercise a pull request without running its code on this host.
#
# THE THREAT. A pull request is a patch plus a promise. Reviewing the patch is
# reading; exercising it is execution, and `bun install` + `bun test` on a fork
# branch runs the author's code as you, with your ~/.claude credentials, your gh
# token, your ssh keys and every other repository under your home directory in
# reach. Nothing about that is exotic — a test file is arbitrary code, which is
# the whole reason tests are useful.
#
# FOUR PLACES fork code executes, in the order they fire:
#   1. `.githooks/pre-commit` — this repo sets core.hooksPath to a TRACKED
#      directory, so a pull request can edit the hook and your next commit in
#      that checkout runs it. Checkout itself is inert; the commit after is not.
#      Handled here by never putting the branch in your working clone: the
#      quarantine is its own repository, with hooksPath pointed at nothing.
#   2. `bun install` — the root package.json's lifecycle scripts run
#      unconditionally, and the pull request owns package.json AND bun.lock, so
#      --frozen-lockfile proves nothing about either. Handled by
#      --ignore-scripts plus the risk gate below.
#   3. `bun test` — arbitrary code, no mitigation possible except isolation.
#   4. Opening the tree in an editor — .vscode/settings.json can point
#      typescript.tsdk at a tsserver inside the repo. Do not open the
#      quarantine directory in your editor; read the diff on GitHub.
#
# THE CONTAINMENT is rootless podman, not docker: docker on this class of box
# runs a root daemon, so a container escape or a bind-mounted socket is root on
# the host. Rootless podman maps the container's root to your unprivileged uid.
# The only host path the container can see is the quarantine directory, which
# holds nothing but the pull request's own tree.
#
# NETWORK IS ON for install and OFF for everything after. Install genuinely
# needs the registry; a test suite does not, and `--network=none` is what turns
# a successful exfiltration into a failed connect.
#
# Usage: scripts/review-pr.sh <PR#> [--allow-risky] [--shell] [--keep]
# Exit:  0 = install, typecheck and tests all passed inside the sandbox
#        1 = a step failed, or the risk gate tripped without --allow-risky
#        2 = usage or environment problem (no podman, no gh, unknown PR)

set -euo pipefail

# Built locally, once, and cached. The stock bun image ships no git and no
# python3, and this suite shells out to both — a sandbox missing them reports
# failures the pull request did not cause, which is worse than no sandbox: it
# trains you to wave red runs through. The tag carries the bun version so a
# bump here rebuilds rather than silently reusing the old one.
readonly BUN_VERSION="1.3.11" # keep in step with .github/workflows/test.yml
readonly BUN_IMAGE="conduit-pr-sandbox:bun-${BUN_VERSION}"
readonly ROOT_DIR="${CONDUIT_PR_REVIEW_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/conduit-pr-review}"

# TWO lists, because they fail differently and the reader does different things
# with each. Both are globs, matched against the pull request's changed files.
#
# EXEC — runs OUTSIDE the sandbox, on your machine or in CI with real
# credentials. The containment below does not reach these at all, so a change
# to one is read before anything is executed.
readonly EXEC_PATHS=(
  'package.json' 'bun.lock' 'bunfig.toml'
  '.githooks/*' '.github/*' 'scripts/*'
  '.vscode/*' '.devcontainer/*' '.idea/*'
  'Dockerfile' '.dockerignore' 'docker-compose*'
)

# PROMPT — read by a MODEL rather than executed. This repository is an LLM
# runtime: these files become prompts, skills and flow definitions, and the
# containment below is irrelevant to them because nothing here is a sandbox
# escape. The attack is text that redirects a model at run time, and it looks
# like ordinary prose in the diff. A sandbox cannot tell you anything about it;
# only reading it can.
#
# triage/ is the sharpest of these: triage.yml runs that flow against real
# issues with an `issues: write` token, on a schedule no reviewer gates.
readonly PROMPT_PATHS=(
  'triage/*' 'examples/*' 'fixtures/*'
  '*.flow.yaml' '*/prompts/*' '*/SKILL.md'
)

die() { printf '\033[31merror:\033[0m %s\n' "$1" >&2; exit "${2:-2}"; }
note() { printf '\033[36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarn:\033[0m %s\n' "$*" >&2; }

pr=""
allow_risky=false
want_shell=false
keep=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-risky) allow_risky=true ;;
    --shell) want_shell=true ;;
    --keep) keep=true ;;
    -h|--help) sed -n '/^# Usage:/,/^#        2 =/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) die "unknown flag: $1" ;;
    *) [[ -z "$pr" ]] || die "expected one PR number, got '$pr' and '$1'"; pr="$1" ;;
  esac
  shift
done

[[ "$pr" =~ ^[0-9]+$ ]] || die "usage: scripts/review-pr.sh <PR#> [--allow-risky] [--shell] [--keep]"
command -v podman >/dev/null || die "podman is required (rootless). docker's root daemon is not an acceptable substitute here."
command -v gh >/dev/null || die "gh is required to resolve the pull request"

[[ "$(podman info --format '{{.Host.Security.Rootless}}' 2>/dev/null)" == "true" ]] \
  || die "podman is running rootful; this script exists to avoid giving fork code a root daemon"

# ---------------------------------------------------------------------------
# Sandbox image, built from a Containerfile held HERE rather than read from the
# repository: a Containerfile in the tree is a file the pull request can edit,
# and this image is the thing that is supposed to contain it.
#
# The git identity is baked in and HOME is /tmp because --userns=keep-id leaves
# the container's HOME unwritable; a test that commits would otherwise fail on
# configuration rather than on the code under review.
# ---------------------------------------------------------------------------
if ! podman image exists "$BUN_IMAGE"; then
  note "building $BUN_IMAGE (one time)"
  podman build -q -t "$BUN_IMAGE" -f - . >/dev/null <<CONTAINERFILE || die "sandbox image build failed"
FROM docker.io/oven/bun:${BUN_VERSION}
RUN apt-get update \
 && apt-get install -y --no-install-recommends git python3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ENV HOME=/tmp \
    GIT_AUTHOR_NAME=pr-sandbox GIT_AUTHOR_EMAIL=sandbox@localhost \
    GIT_COMMITTER_NAME=pr-sandbox GIT_COMMITTER_EMAIL=sandbox@localhost
CONTAINERFILE
fi

# ---------------------------------------------------------------------------
# Resolve the pull request. head_sha is pinned ONCE and used everywhere after:
# a pull request can gain a commit between the metadata read and the fetch, and
# reviewing one tree while executing another is the failure this avoids.
# ---------------------------------------------------------------------------
note "resolving PR #$pr"
meta="$(gh pr view "$pr" --json number,title,headRefOid,baseRefOid,isCrossRepository,author 2>/dev/null)" \
  || die "no such pull request: #$pr"

head_sha="$(jq -r .headRefOid <<<"$meta")"
base_sha="$(jq -r .baseRefOid <<<"$meta")"
title="$(jq -r .title <<<"$meta")"
author="$(jq -r .author.login <<<"$meta")"
fork="$(jq -r .isCrossRepository <<<"$meta")"

printf '\n  \033[1m#%s\033[0m %s\n  by %s%s\n  head %s\n\n' \
  "$pr" "$title" "$author" "$([[ $fork == true ]] && echo '  (fork)' || echo '')" "$head_sha"

# ---------------------------------------------------------------------------
# Risk gate. Read the diff of anything that executes outside the sandbox before
# the sandbox is even built, because the sandbox does not contain those files.
# ---------------------------------------------------------------------------
changed="$(gh api "repos/{owner}/{repo}/pulls/$pr/files" --paginate --jq '.[].filename')"
exec_hits=() prompt_hits=()
while IFS= read -r f; do
  [[ -n "$f" ]] || continue
  for p in "${EXEC_PATHS[@]}";   do [[ "$f" == $p ]] && { exec_hits+=("$f");   break; }; done
  for p in "${PROMPT_PATHS[@]}"; do [[ "$f" == $p ]] && { prompt_hits+=("$f"); break; }; done
done <<<"$changed"

if (( ${#exec_hits[@]} )); then
  warn "changes code that runs OUTSIDE the sandbox (your machine, or CI with real secrets):"
  printf '         %s\n' "${exec_hits[@]}" >&2
fi
if (( ${#prompt_hits[@]} )); then
  warn "changes model-facing text (prompt / skill / flow). Read it as an INSTRUCTION,"
  warn "not as data — the sandbox has nothing to say about prompt injection:"
  printf '         %s\n' "${prompt_hits[@]}" >&2
fi
if (( ${#exec_hits[@]} + ${#prompt_hits[@]} )); then
  printf '\n       gh pr diff %s -- %s\n\n' "$pr" "${exec_hits[*]} ${prompt_hits[*]}" >&2
  $allow_risky || die "refusing to continue; pass --allow-risky once you have read them" 1
  warn "--allow-risky given; continuing"
fi

# ---------------------------------------------------------------------------
# Quarantine checkout. A FRESH repository, not a worktree and not a clone of
# yours: a worktree writes into your .git, and a --shared clone would hand the
# container a path into your object store. This one has no remotes you push to,
# no hooks, and nothing in it you have ever committed.
# ---------------------------------------------------------------------------
# `git -C` does not override the environment: GIT_DIR, GIT_WORK_TREE and the
# rest still win over it. This script runs `git init` and `git fetch` into the
# quarantine path, so an inherited GIT_DIR would point that quarantine at the
# caller's repository instead — and being invoked from a hook or a git wrapper
# is exactly the situation this script exists to survive. Clear them before the
# first git command below.
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE \
      GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_PREFIX

origin_url="$(git -C "$(dirname "$0")/.." remote get-url origin)"

# quarantine <dir> <sha> — a shallow, hookless checkout of exactly that commit.
quarantine() {
  local dir="$1" sha="$2"
  rm -rf "$dir"
  git init -q "$dir"
  git -C "$dir" config core.hooksPath /var/empty
  git -C "$dir" fetch -q --depth=1 "$origin_url" "$sha"
  git -C "$dir" checkout -q FETCH_HEAD
  local actual; actual="$(git -C "$dir" rev-parse HEAD)"
  [[ "$actual" == "$sha" ]] || die "fetched $actual but expected $sha"
}

work="$ROOT_DIR/pr-$pr"
base_work="$ROOT_DIR/base-$base_sha"
mkdir -p "$ROOT_DIR"
trap '$keep || rm -rf "$work"' EXIT

note "fetching $head_sha into $work"
quarantine "$work" "$head_sha"

# ---------------------------------------------------------------------------
# The sandbox. Every flag here is load-bearing:
#   --rm                  the container is per-run; nothing survives to be reused
#   -v "$work":/src       the ONLY host path in reach, and it holds only the PR
#   --cap-drop=ALL        no capability is needed to run a test suite
#   no-new-privileges     a setuid binary inside the image cannot re-acquire any
#   --userns=keep-id      container writes land as you, not as a mapped root
#   --pids-limit/-m       a fork bomb or a leak stops at the container boundary
#   -w /src               never /, so a relative path cannot wander
# Env is NOT forwarded. Nothing in your shell belongs to this pull request.
# ---------------------------------------------------------------------------
sandbox() {
  local net="$1" tree="$2"; shift 2
  podman run --rm -i \
    --network="$net" \
    -v "$tree":/src:Z \
    -w /src \
    --cap-drop=ALL \
    --security-opt=no-new-privileges \
    --userns=keep-id \
    --pids-limit=512 \
    -m 4g \
    --tmpfs /tmp:rw,exec,size=2g \
    "$BUN_IMAGE" "$@"
}

note "install (network ON, lifecycle scripts OFF)"
sandbox bridge "$work" bun install --frozen-lockfile --ignore-scripts \
  || die "bun install failed inside the sandbox" 1

if $want_shell; then
  note "dropping into the sandbox (network OFF). The tree is at /src."
  podman run --rm -it --network=none -v "$work":/src:Z -w /src \
    --cap-drop=ALL --security-opt=no-new-privileges --userns=keep-id \
    --pids-limit=512 -m 4g --tmpfs /tmp:rw,exec,size=2g "$BUN_IMAGE" /bin/bash
  exit 0
fi

logs="$ROOT_DIR/logs"; mkdir -p "$logs"
failed=()

note "typecheck (network OFF)"
sandbox none "$work" bun run typecheck || failed+=(typecheck)

note "tests (network OFF)"
# set +e around the pipeline: under `set -e -o pipefail` a failing suite would
# abort the script here, and aborting is exactly wrong — a red run is the case
# the attribution pass below exists to interpret.
set +e
sandbox none "$work" bun run test 2>&1 | tee "$logs/pr-$pr.test.log"
tests_rc=${PIPESTATUS[0]}
set -e
(( tests_rc == 0 )) || failed+=(tests)

note "black-box (network OFF, advisory)"
sandbox none "$work" bun run test:blackbox || warn "blackbox failed — advisory, not a merge gate"

# ---------------------------------------------------------------------------
# A red run is not yet a verdict. This suite exercises process-group reaping,
# filesystem timestamps and spawned executables, and a rootless container is
# not the ubuntu runner CI uses — some tests fail here on an UNTOUCHED tree.
# So when the pull request's tests fail, the base commit is run through the
# IDENTICAL sandbox and the two failure sets are subtracted. What survives is
# attributable to the pull request; what cancels is the sandbox's own.
#
# Without this the script reports failures the author did not cause, and a
# reviewer who learns to wave those through is worse off than one who never
# ran it.
# ---------------------------------------------------------------------------
# Total by construction: grep exits 1 on no match and `set -o pipefail` would
# make that the pipeline's status, which `set -e` turns into an abort the
# moment this is used in a command substitution rather than a <(...).
fails_in() { grep -oP '^\(fail\) \K.*?(?= \[[0-9.]+m?s\]$|$)' "$1" 2>/dev/null | sort -u || true; }

if [[ " ${failed[*]} " == *" tests "* ]]; then
  note "tests failed — running base $base_sha through the same sandbox to attribute them"
  # A bare node_modules is not evidence of a usable baseline. An interrupted
  # install leaves a partial one behind, and this tree is deliberately
  # persistent — it is also where the previous baseline run's tests wrote. Both
  # make the baseline fail MORE than the base commit really does, and a
  # baseline that over-fails cancels real failures in the subtraction below.
  # So: reuse only a tree whose install ran to completion, and put it back to
  # the checked-out commit before reusing it.
  base_stamp="$ROOT_DIR/base-$base_sha.installed"
  if [[ ! -f "$base_stamp" || ! -d "$base_work/.git" ]]; then
    rm -f "$base_stamp"
    quarantine "$base_work" "$base_sha"
    sandbox bridge "$base_work" bun install --frozen-lockfile --ignore-scripts >/dev/null \
      || die "baseline install failed; cannot attribute the failures" 1
    # Written only after a clean install, and kept outside the tree so that
    # the reset below cannot remove the very marker that vouches for it.
    : >"$base_stamp"
  else
    note "reusing the installed baseline at $base_work (resetting it first)"
    git -C "$base_work" reset -q --hard
    git -C "$base_work" clean -qfd -e node_modules
  fi

  base_rc=0
  sandbox none "$base_work" bun run test >"$logs/base-$base_sha.test.log" 2>&1 || base_rc=$?

  pr_fails="$(fails_in "$logs/pr-$pr.test.log")"
  base_fails="$(fails_in "$logs/base-$base_sha.test.log")"
  new_fails="$(comm -23 <(printf '%s\n' "$pr_fails") <(printf '%s\n' "$base_fails"))"

  # Dropping `tests` from `failed` asserts that the sandbox caused all of it,
  # and that assertion needs evidence on BOTH sides: the base must actually
  # have failed, each log must carry parsed `(fail)` lines to subtract, and
  # nothing may survive the subtraction. Demanding all three is what stops a
  # suite that died WITHOUT printing a single `(fail)` line — a crash, an OOM,
  # a broken `test` script — from yielding an empty set on both sides,
  # subtracting to empty, and reporting the pull request clean. A false green
  # from the tool that exists to prevent one is the worst thing this script
  # could do, so ambiguity keeps the failure rather than explaining it away.
  if (( base_rc != 0 )) && [[ -n "$pr_fails" && -n "$base_fails" && -z "$new_fails" ]]; then
    warn "every parsed failure also fails on the base commit in this sandbox — none are attributable to PR #$pr"
    warn "logs: $logs/pr-$pr.test.log vs $logs/base-$base_sha.test.log"
    failed=("${failed[@]/tests/}")
  elif [[ -n "$new_fails" ]]; then
    printf '\n\033[31mfailures introduced by PR #%s:\033[0m\n' "$pr" >&2
    printf '  %s\n' "$new_fails" >&2
  else
    warn "the suite failed, but the logs do not show the base failing the same way"
    warn "keeping it against PR #$pr — check the logs yourself rather than trusting this"
    warn "logs: $logs/pr-$pr.test.log vs $logs/base-$base_sha.test.log"
  fi
fi

printf '\n'
failed=("${failed[@]}")
remaining=(); for f in "${failed[@]}"; do [[ -n "$f" ]] && remaining+=("$f"); done
$keep && note "tree kept at $work (delete it when done: rm -rf '$work')"
if (( ${#remaining[@]} )); then
  die "failed inside the sandbox: ${remaining[*]}" 1
fi
note "PR #$pr passed typecheck and tests in the sandbox"
