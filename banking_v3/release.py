"""One allowlisted, audited release tree for the public cleanroom-eval distribution."""
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import hashlib
import re
import subprocess
import tarfile
import tempfile

from prefect import flow
from prefect.runtime import flow_run

from .contract import digest, load_json, load_package, write_json
from .evaluate import reference_rollout


def _seal_audit(report):
    report["audit_payload_sha256"] = digest(report)
    return report


@flow(name="bankingenv-v3-release", persist_result=False, log_prints=True)
def export_release(
    output: str,
    tag: str,
    commit: str,
    calibration_root: str | None = None,
    dataset_root: str | None = None,
):
    from cleanroom_eval.release import build_release_candidate as build

    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z.]+)?", tag):
        raise ValueError("tag must look like v1.2.3")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("commit must be a full 40-character SHA")
    head = subprocess.run(
        ["git", "-C", str(build.REPO_ROOT), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if head != commit:
        raise ValueError(f"checkout {head} does not match release commit {commit}")
    status = subprocess.run(
        ["git", "-C", str(build.REPO_ROOT), "status", "--porcelain"],
        check=True, capture_output=True, text=True,
    ).stdout
    if status:
        raise ValueError("release checkout must be clean")

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
        "tagged_tree": build.gate_tagged_tree(members, commit),
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
    report = {"schema": "bankingenv.release-audit/v4", "tag": tag, "commit": commit,
              "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
              "prefect_flow_run_id": str(flow_run.id),
              "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
              "file_count": len(members), "gates": gates, "terminal_state": build.terminal_state(gates)}
    _seal_audit(report)
    write_json(root / "release-audit.json", report)
    print({"gates": {key: value["status"] for key, value in gates.items()}, "terminal_state": report["terminal_state"]})
    if any(value["status"] == "FAIL" for value in gates.values()):
        raise ValueError("release audit failed; archive must not be published")
    from cleanroom_eval.contract import build_release_manifest
    manifest = build_release_manifest(
        root=root,
        relative_paths=[archive.name, "release-audit.json"],
        release_id=f"cleanroom-{tag.lower()}",
        repository_commit=commit,
    )
    write_json(root / "release-manifest.json", manifest)
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
