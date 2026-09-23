# Per-flow triage image, built FROM the engine image in the same job.
#
# Built from this repository, not pulled from another org. That is the whole
# reason this flow is auditable: src/triage/apply-triage.ts is the only station
# that can write to GitHub, and it ships in a reviewable diff here rather than
# baked into an image nobody in this repo can read.
ARG BASE=conduit-engine:local
FROM ${BASE}

USER root

# The flow ships as committed: model ids are literals in triage/flow.yaml, so a
# model change is a reviewable diff rather than a workflow env var.
COPY triage/flow.yaml /flow/flow.yaml
COPY triage/prompts /flow/prompts

# The engine writes artifacts (triage.json, scan.json, result.json) into the
# project root as the non-root `conduit` user.
RUN chown -R conduit:conduit /flow

ENV CONDUIT_PROJECT_ROOT=/flow
USER conduit
