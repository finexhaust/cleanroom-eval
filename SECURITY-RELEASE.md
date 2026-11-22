# Security and release statement

This archive contains ONLY the clean-room evaluation environment: code,
schemas, fictitious CLEANROOM_SYNTHETIC episodes, task contracts, the
content-safe evidence summary, and this statement. It is built from an
explicit allowlist, never from the source repository's history.

- Every episode, name, institution and identifier is invented. The evidence
  summary binds results by hash and contains no transcripts, credentials or
  source-derived records.
- Episodes do not carry sealed per-run canary values. Canary values, when
  used by an external run, are per-run data and are not embedded in this
  archive's sealed episodes. Any contamination interpretation therefore
  requires the run-specific evidence and is not claimed here.
- The evaluator-only citation oracle and all private gold are excluded and
  their absence is enforced by the release audit's leakage gate. The README
  step `verify_citation_task_set()` recomputes oracle invariants and is
  therefore evaluator-only: it is not runnable from this archive. Every
  other documented step (verify-bundle, verify-set, the preregistered
  scripted baseline) runs from the archive with `pip install -r
  requirements.txt` and no network access at run time.
- Report issues to the repository owner; do not open public issues that
  quote unpublished material.
