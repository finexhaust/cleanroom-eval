# BankingEnv v3 model panel

The panel runner compares live models against the same frozen document packages
and deterministic environment. Private inputs and responses stay outside the
repository. The primary metric is all eight checks passing with no recorded
safety violation. This is a generated compositional holdout, not a collection of
192 independently authored banking workflows or human-certified tasks.

Install the pinned runtime with `pip install -r requirements-banking-panel.txt`.
Run public smoke tests first:

```sh
python -m banking_v3.panel --dataset /private/package/development \
  --output /private/panel/smoke --config /private/panel/config.json --smoke
```

Run the private split with the same committed implementation and configuration:

```sh
python -m banking_v3.panel --dataset /private/package/evaluation \
  --output /private/panel/evaluation --config /private/panel/config.json
```

Both commands are Prefect flows. Use a systemd user unit and a frozen runtime
checkout for long runs. Do not edit the runtime during a run. The output directory
holds a singleton lock, immutable protocol plan, exact provider response hashes,
per-turn checkpoints, transcripts, replay-verified grades and an aggregate report.
Restarting the same command reuses saved responses; it never resamples failed
answers. Changed input/protocol commitments are rejected. Terminal provider errors
remain in the assigned denominator and are reported separately from model failures.
No case is silently dropped. Incomplete campaigns must report the missing count.

Configuration contains model names, HTTPS endpoints, credential variable names,
reasoning effort, workers per model, action budget, output budget per case and per
turn. Existing local credentials are read into memory; none are saved in plans or
transcripts. Model inputs are the public initial observation followed by verbatim
assistant action text and simulator tool responses. The environment validates
names and arguments. No hidden evaluator values, grading feedback, answer-based
retries or model-based judges are used. A single outer Markdown fence may be
removed before JSON parsing, identically for all arms.

Output token budgets include provider-reported reasoning tokens. Requested maximum
effort is vendor-specific, not a claim of equal inference compute. The runner uses
the provider's sampling defaults. Native function-calling and reasoning-state
continuation are not enabled: this measures the common JSON-action interface.

Publish only aggregate metrics and abstract failure descriptions. Keep case
inputs, evaluator seeds, gold answers and complete transcripts private. A model
result is not independent practitioner review; that work remains outstanding.

The 2026-10-20 run retains the direct DeepSeek V4 Pro arm's HTTP 402 interruption
and adds DeepSeek V4.1 Flash through Neuralwatt as a distinct full-set arm. The
report accepts a separately committed supplemental plan only when the dataset,
code, prompts and action/token limits match. It never replaces an existing model
identity or imputes successful answers. Provider-blocked arms are shown as
unranked and excluded from cross-model case selection.

The public smoke audit found an authored-policy ambiguity in the evidence family:
unique proof records in the prose versus unique issuers in the evaluator. Report
raw 192-case metrics unchanged and a separate 168-case sensitivity analysis. Do
not sell the affected cases as demonstrated model difficulty. Dataset 3.1
clarifies the policy wording; the comparison in this note was run on 3.0.

Protocol references: DeepSeek's [JSON output contract](https://api-docs.deepseek.com/guides/json_mode/)
requires JSON mode and an example in the prompt. Its [thinking-mode documentation](https://api-docs.deepseek.com/guides/thinking_mode/)
says prior reasoning is ignored for requests without native tools. The runner
uses that text-only action protocol. Neuralwatt's model listing snapshot retains
accepted effort mappings; MiMo's requested max maps to high, its highest supported
level. This is a common bounded interface comparison, not equal inference compute
or a vendor-optimized native-tool leaderboard.

## Measured comparison, 20 October 2026

All 192 private cases were assigned to each working arm. Every one of the 24
ambiguous evidence cases is excluded from the primary comparison, including
cases with other errors. The original raw scores remain in the
[aggregate artifact](evaluation/bankingenv-v3-model-comparison-20261020.json).

| Model | Complete / 168 | Completion | Mean of eight checks | Control-breach cases |
|---|---:|---:|---:|---:|
| MiMo V2.6 Pro | 154 | 91.7% | 96.9% | 2 |
| GLM 5.3 | 153 | 91.1% | 94.3% | 5 |
| DeepSeek V4.1 Flash | 146 | 86.9% | 95.7% | 6 |
| Kimi K3 | 145 | 86.3% | 96.4% | 13 |

MiMo leads GLM by one case, not evidence of a general capability advantage.
GLM's 15 retained-set failures comprise ten response-token-limit failures and
five approval-control breaches. Kimi has thirteen attempts before required
approval and nine wrong-calculation closures (overlapping categories). MiMo and
Flash close eight and fifteen cases respectively with wrong calculations.
The primary metric retains an unauthorized attempt even if the agent later
obtains approval and closes the case correctly.

Of the 168 unambiguous variants, 121 pass on every working model, 46 distinguish
models, and one fails on every model. The 47 failing variants collapse to 28
family/control-condition groups; 13 groups include control breaches or wrong
executed state. These are prioritized for lab review. Numeric variants are not
independent authored problems.

Two selected cases received two additional samples per model. In the margin
approval case, Kimi attempted execution before approval in all three trials. In
the trade-amendment case, Flash passed zero of three trials: two wrong-value
closures and one response-token-limit failure. These are post-hoc repeatability
checks, not an unbiased estimate or a best-of replacement for primary scores.
Case-level examples and transcripts remain in the private report.

All 960 primary trajectories (including the interrupted direct Pro arm) and 16
selected repeats were independently replayed. Audit Prefect run:
`470086d7-77a3-48a7-a7c0-279cb885b757`. Public export:
`ad5e842c-a0ff-4f6e-aac4-4f6e404fb16d`; private identifier and seed scan passed.
The code has 164 passing focused tests.

Access exclusions: OpenAI returned exhausted-credit errors; Anthropic returned
401 and xAI 403; no Gemini generation credential was configured. Direct DeepSeek
V4 Pro produced 44 non-provider-error episodes before/around interruption, with
27 completions; 148 episodes ended in HTTP 402 and this arm is unranked. Kimi had
one transient provider-budget error in an already-quarantined evidence case.
There are no provider-error cases among the 168 compared cases for the four
working arms. Independent banking review remains open; the revised proof-counting
wording ships as dataset 3.1.
