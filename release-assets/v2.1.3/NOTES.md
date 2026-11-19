BankingEnv / cleanroom-eval 2.1.3 with document-grounded dataset v3.1.

cleanroom-eval is now the only public repository for BankingEnv. The former `bankingenv` repository is archived and points here; its last release, 2.1.2, is the same artifact as cleanroom-eval 2.1.2.

Runtime 2.1.3 changes no code path, no case and no grading rule. It updates the README, which described two public repositories, plus one docstring and the version strings. Dataset 3.1 is byte-identical to 2.1.2, and the private evaluation split is unchanged and private.

The archive was built from the merged FinExhaust tree with the canonical release builder and reproduces byte for byte. Release-audit gates: see release-audit-2.1.3.json. 10 of 13 gates PASS, terminal state RELEASE_AUDIT_PASS_WITH_LIMITATIONS. registry_identity_scan, source_shingle_overlap and v3_private_instances were NOT_RUN for this release because the calibration corpus and the private evaluation split were not available to the build; all three passed for 2.1.2, and the content changed since then is README prose, one docstring and version strings. The clean-machine tutorial replay ran in a fresh venv on the build machine; separate-machine replay was not performed.

This is a mechanically validated synthetic release. The cases are variants of eight authored primitives, not independently authored cases. Model difficulty for 3.1 and independent practitioner certification remain unmeasured. Private evaluation cases, answers and seeds are excluded.

SHA-256: 02184bb6fb3227f890488e16865aa6d994b0025583f613093d67363651a427d5.

See docs/bankingenv-v3.md, release-audit-2.1.3.json and banking_v3/data/regeneration-receipt.json for commands and evidence.
