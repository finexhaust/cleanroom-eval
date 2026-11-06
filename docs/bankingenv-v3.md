# BankingEnv / cleanroom-eval 2.1.1

Document-grounded synthetic banking operations, dataset v3.1.
The two public repositories now ship the same runtime and development dataset
from one allowlisted build. The historical 40-case fixtures remain available
for reproduction; they are no longer the current benchmark.

## Dataset 3.1, what changed from 3.0

Runtime 2.1 ships dataset 3.1. The development split is regenerated from the
same development seed as 3.0 with one authored-policy clarification: the
evidence family's calculation rule now reads "count distinct valid proof
records from approved independent issuers; deduplicate by event_id", matching
what the evaluator has always scored. The 20 October smoke audit had found the
earlier wording ambiguous (unique proof records in the prose, unique issuers in
the evaluator) and quarantined the affected cases from difficulty claims. In
3.1, 168 of the 192 development cases are byte-identical to 3.0; the 24
evidence cases carry the clarified policy documents and new document hashes.
Every case's provenance records generator_version 3.1. The private evaluation
split is regenerated with a fresh seed and stays private. Runtime 2.1 also
ships the model-panel runner and report modules (`banking_v3.panel`,
`banking_v3.panel_report`) and the model-panel note; the open-weight
comparison recorded in that note was run on generation 3.0 and is not a
difficulty measurement for 3.1, which remains unmeasured.

## Dataset and task contract

The default generation produces 384 cases: 192 public development cases and
192 private compositional evaluation cases, across settlement, allocations,
reconciliation, margin, permissions, lifecycle, evidence and temporal workflows.
Every case has 7 or 8 hashed documents, including a current house policy, an
operational record, a control review, historical/future material and an
untrusted urgent request. There are 16 control combinations per family and
three numeric variants per combination. These are generated variants of eight
authored workflow primitives, not 384 independently authored tasks.

Each case requires eight rubric checks: authoritative evidence, calculation,
exception diagnosis, routing, approval, correct state change, inspection of
the resulting receipt, and closure. These checks are related; do not describe
them as eight independent decisions. The expected routing mix is 50% owned
escalation with no mutation, 25% approval then execution, and 25% execution
within authority. All successful outcomes retain a system receipt or ticket.

The arithmetic differs by family: outstanding settlement amount, allocation
residual, ledger adjustment, margin/MTA calculation, permission bitmask,
effective amendment, independent proof count, or contiguous sequence.
Policies and thresholds are fictional house rules, not assertions about law
or regulatory requirements. No private calibration material or external
source document is used to generate this release.

## Run

```sh
pip install -e .
python -m banking_v3 run --dataset banking_v3/data/development \
  --policy reference --out /tmp/banking-reference-run
```

All commands run through Prefect. Configure `PREFECT_API_URL` for your server;
otherwise Prefect uses its local server. Output directories must be new.
The reference policy knows the evaluator answer. It demonstrates mechanical
solvability and is never reported as an AI model score.

For a real model, configure `CLEANROOM_POLICY_BASE_URL`,
`CLEANROOM_POLICY_API_KEY`, and `CLEANROOM_POLICY_MODEL`, then:

```sh
python -m banking_v3 run --dataset banking_v3/data/development \
  --policy live --turn-limit 24 --out /tmp/banking-model-run
```

The runner freezes case IDs, grader/code digest, model identity, turn budget
and missing-result policy before the first call. Tool failures remain in the
trajectory. Provider failures remain in the denominator and cause a nonzero
CLI exit; they are not silently dropped or called model failures. Each run
saves per-case grades, transcripts, aggregate failure patterns and telemetry.
Credentials and HTTP error bodies never enter reports.

Regrade saved trajectories without trusting their recorded rewards:

```sh
python -m banking_v3 grade --dataset banking_v3/data/development \
  --transcripts /tmp/banking-model-run/trajectories --out /tmp/banking-regrade
```

Missing trajectories remain in the denominator as incomplete cases.

To generate a fresh dataset outside the repository:

```sh
python -m banking_v3 regenerate --out /tmp/banking-fresh-dataset
```

Only `development/` may be exported publicly. `evaluation/` and `evaluator/`
remain private, including the randomly generated evaluation seed, answers,
and reference trajectories. `receipt.json` contains publishable counts and
hash commitments. Regeneration does not overwrite prior runs.

## Evaluation boundary and split

The model receives a case brief, document index, its own working state and
seven tool schemas. It must read documents to access their contents. Tools
never return gold answers, scores or the next required action. Assessment
records a judgment without saying whether it is correct. Mutations enforce
versions, idempotency, policy authority, evidence availability and approval;
final grading independently checks the calculation and resulting state.
The model adapter receives only `observe()` and `step()` outputs; it must not
be given Python/filesystem access to the evaluator process or private files.
This is an API boundary, not a sandbox for arbitrary model-authored code.

Development and evaluation have disjoint case, document and world IDs and
disjoint combinations of stale policy, duplicate event, materiality and
missing evidence. Both share workflow primitives and renderers. This tests
compositional transfer, not unseen-family generalization. The public data is
contaminable; private seeds are not published. Cryptographic hashes bind bytes;
they do not establish realism, difficulty or independent review.

## Validation and honest status

Each generated answer is checked against separately constructed arithmetic.
Every case gets an evaluator-informed successful replay and single-fault
adversarial trajectories for wrong calculation, policy, diagnosis, routing,
evidence, approval, operation, owner, version, receipt and closure as applicable.
The receipt reports actual denominators and counts, with private case details
excluded. Mechanical checks are not independent human validation.

No model-difficulty or human-certification claim is made without a new receipt.
Legacy August scores (27/40 and 39/40) apply only to the historical v2 set.
The legacy `cleanroom_eval.certify` entry point is disabled because it used
scripted refusals in place of model measurements. There are no issued v3
certificates. Admission still requires real model runs and independent domain
review; a mechanically passing generated case is a development artifact.

## Reproduction and release

Run `python -m pytest banking_v3/tests -q`. The canonical release builder
exports explicit code/data directories, excludes evaluation seeds, private
answers and evaluator-only historical assets, and writes a byte manifest.
Both public repositories consume that same archive. Runtime version 2.1.1 and
dataset version 3.1 intentionally distinguish software from fixture versions.
Runtime 2.1.1 changes no code path and no case: it corrects
`banking_v3.__version__`, which 2.1.0 shipped as "2.0.0", and adds a test
that keeps it equal to the package version.

MIT. Every generated document is marked `CLEANROOM_SYNTHETIC`.
