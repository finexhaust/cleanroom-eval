"""One allowlisted, audited release tree for the public cleanroom-eval distribution."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import argparse
import hashlib
import tarfile
import tempfile

from prefect import flow
from prefect.runtime import flow_run

from .contract import load_json, load_package, write_json
from .evaluate import reference_rollout


@flow(name="bankingenv-v3-release", persist_result=False, log_prints=True)
def export_release(output: str, calibration_root: str | None = None, dataset_root: str | None = None,
                   release_tag: str | None = None, repository_commit: str | None = None):
    from cleanroom_eval.release import build_release_candidate as build
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError("release output must be empty")
    archive = build.build_archive(root)
    members = build._archive_members(archive)
    if calibration_root:
        from cleanroom_eval.release import registry_identity
        registry_identity.DEFAULT_REGISTRY = Path(calibration_root) / "knowledge-graph/profiles/corpus-1-2-3-4-5/entity_registry.jsonl"
        registry_identity.RECORD_SOURCES_DIR = Path(calibration_root) / "raw"
    gates = {
        "secret_scan": build.gate_secret_scan(members),
        "internal_identifier_scan": build.gate_internal_identifier_scan(members),
        "registry_identity_scan": build.gate_registry_identity_scan(members),
        "source_shingle_overlap": build.gate_source_shingle_overlap(members, Path(calibration_root) / "raw" if calibration_root else Path("/nonexistent")),
        "private_gold_leak": build.gate_private_gold_leak(members),
        "path_metadata_scan": build.gate_path_metadata_scan(members),
        "license_inventory": build.gate_license_inventory(members),
        "deterministic_rebuild": build.gate_deterministic_rebuild(archive),
        "manifest_verification": build.gate_manifest_verification(members),
        "import_smoke": build.gate_import_smoke(archive),
        "clean_machine_tutorial": build.gate_tutorial_replay(archive),
    }
    with tempfile.TemporaryDirectory() as scratch:
        with tarfile.open(archive) as tar:
            tar.extractall(scratch, filter="data")
        _, cases = load_package(Path(scratch) / "banking_v3/data/development")
        passed = sum(reference_rollout(case)[0]["complete"] for case in cases)
        gates["v3_development_replay"] = {"status": "PASS" if passed == len(cases) else "FAIL", "complete": passed, "cases": len(cases)}
    # Check exact private instance identifiers and seed, not just filenames.
    private_hits = []
    if dataset_root:
        private = Path(dataset_root)
        _, held = load_package(private / "evaluation")
        seed = load_json(private / "evaluator/reproduction.json")["evaluation_seed"]
        needles = [seed] + [c["id"] for c in held] + [d["id"] for c in held for d in c["documents"]]
        for info, blob in members:
            text = blob.decode("utf-8", errors="replace")
            if any(needle in text for needle in needles):
                private_hits.append(info.name)
        gates["v3_private_instances"] = {"status": "FAIL" if private_hits else "PASS", "files_with_hits": private_hits, "instances_checked": len(held)}
    else:
        gates["v3_private_instances"] = {"status": "NOT_RUN", "reason": "private dataset not supplied"}
    build.scrub_not_run_reasons(gates)
    report = {"schema": "bankingenv.release-audit/v3", "prefect_flow_run_id": str(flow_run.id),
              "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
              "file_count": len(members), "gates": gates}
    gates["postrelease_artifact_scan"] = build.gate_postrelease_artifact_scan(
        report, build.REPO_ROOT / "cleanroom_eval/evidence/gates-2026-08")
    report["not_run_gates"] = sorted(name for name, gate in gates.items() if gate.get("status") == "NOT_RUN")
    # #13: the published audit names its release, its commit, its generation
    # time and its own digest.
    report["release_tag"] = release_tag
    report["repository_commit"] = repository_commit or build._git_head()
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report["terminal_state"] = build.terminal_state(gates)
    report["audit_sha256"] = build.audit_self_digest(report)
    write_json(root / "release-audit.json", report)
    print({"gates": {key: value["status"] for key, value in gates.items()}, "terminal_state": report["terminal_state"]})
    if any(value["status"] == "FAIL" for value in gates.values()):
        raise ValueError("release audit failed; archive must not be published")
    with tarfile.open(archive) as tar:
        tar.extractall(root / "tree", filter="data")
    (root / "EXPORT-OK").write_text(report["archive_sha256"] + "\n")
    return report


@flow(name="bankingenv-v3-stage-development", persist_result=False)
def stage_development(dataset_root: str, destination: str):
    """Copy only validated public development inputs and aggregate evidence."""
    from shutil import copytree
    from .flow import code_commitment
    dataset = Path(dataset_root)
    receipt = load_json(dataset / "receipt.json")
    if receipt["code_sha256"] != code_commitment():
        raise ValueError("generation code changed; regenerate before staging")
    manifest, _ = load_package(dataset / "development")
    if manifest["case_index_sha256"] != receipt["splits"]["development"]["case_index_sha256"]:
        raise ValueError("receipt does not bind development dataset")
    dest = Path(destination)
    if dest.exists() and any(dest.iterdir()):
        raise ValueError("staging destination must be empty")
    dest.mkdir(parents=True, exist_ok=True)
    copytree(dataset / "development", dest / "development")
    write_json(dest / "regeneration-receipt.json", receipt)
    return {"cases": manifest["case_count"], "private_cases_exported": 0}


RECEIPT_PATH = Path(__file__).resolve().parents[1] / "banking_v3/data/regeneration-receipt.json"


def check_receipt(path: Path = RECEIPT_PATH) -> dict:
    """#15: the receipt must bind the code this repository ships.

    ``code_sha256`` records the generator at dataset-generation time and is
    historical. ``shipped_code_sha256`` records the code shipped beside the
    dataset and must be updated by any push that changes banking_v3/*.py.
    """
    from .flow import code_commitment

    receipt = load_json(path)
    shipped = receipt.get("shipped_code_sha256")
    if not isinstance(shipped, str) or shipped != code_commitment():
        raise ValueError(
            "regeneration receipt does not bind the shipped generator: update "
            "shipped_code_sha256 in banking_v3/data/regeneration-receipt.json "
            "in the same push that changes banking_v3 code"
        )
    return {"shipped_code_sha256": shipped}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export", help="build the release archive and audit (#13)")
    export.add_argument("--output", required=True)
    export.add_argument("--calibration-root", default=None)
    export.add_argument("--dataset-root", default=None)
    export.add_argument("--tag", default=None, help="release tag, e.g. v2.1.4")
    export.add_argument("--commit", default=None, help="40-character release commit")
    receipt = commands.add_parser("check-receipt", help="verify the receipt binds the shipped code (#15)")
    receipt.add_argument("--path", default=str(RECEIPT_PATH))
    args = parser.parse_args()
    if args.command == "export":
        export_release(
            output=args.output, calibration_root=args.calibration_root,
            dataset_root=args.dataset_root, release_tag=args.tag,
            repository_commit=args.commit,
        )
    else:
        print(check_receipt(Path(args.path)))


if __name__ == "__main__":
    main()
