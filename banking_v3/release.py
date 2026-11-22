"""One allowlisted, audited release tree for the public cleanroom-eval distribution."""
from __future__ import annotations

from pathlib import Path
import hashlib
import json
import re
import subprocess
import tarfile
import tempfile

from prefect import flow
from prefect.runtime import flow_run

from .contract import load_json, load_package, write_json
from .evaluate import reference_rollout


@flow(name="bankingenv-v3-release", persist_result=False, log_prints=True)
def export_release(
    output: str,
    calibration_root: str | None = None,
    dataset_root: str | None = None,
    release_tag: str = "unreleased",
    repository_commit: str | None = None,
):
    from cleanroom_eval.release import build_release_candidate as build
    from cleanroom_eval.contract import build_release_manifest
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError("release output must be empty")
    if release_tag != "unreleased" and not re.fullmatch(
        r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-.][0-9A-Za-z.]+)?", release_tag
    ):
        raise ValueError("release_tag must be unreleased or a semantic version tag")
    if repository_commit is None:
        repository_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=build.REPO_ROOT,
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", repository_commit):
        raise ValueError("repository_commit must be a full 40-character SHA")
    checked_out_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=build.REPO_ROOT,
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    if checked_out_commit != repository_commit:
        raise ValueError("release checkout differs from the requested commit")
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
        "result_limits": build.gate_result_limits(members),
        "import_smoke": build.gate_import_smoke(archive),
        "clean_machine_tutorial": build.gate_tutorial_replay(archive),
    }
    try:
        with tempfile.TemporaryDirectory() as scratch:
            with tarfile.open(archive) as tar:
                tar.extractall(scratch, filter="data")
            _, cases = load_package(Path(scratch) / "banking_v3/data/development")
            passed = sum(reference_rollout(case)[0]["complete"] for case in cases)
            gates["v3_development_replay"] = {"status": "PASS" if passed == len(cases) else "FAIL", "complete": passed, "cases": len(cases)}
    except Exception:
        gates["v3_development_replay"] = {
            "status": "NOT_RUN",
            "reason": "development replay verifier unavailable",
        }
    # Check exact private instance identifiers and seed, not just filenames.
    private_hits = []
    if dataset_root:
        try:
            private = Path(dataset_root)
            _, held = load_package(private / "evaluation")
            seed = load_json(private / "evaluator/reproduction.json")["evaluation_seed"]
            needles = [seed] + [c["id"] for c in held] + [d["id"] for c in held for d in c["documents"]]
            for info, blob in members:
                text = blob.decode("utf-8", errors="replace")
                if any(needle in text for needle in needles):
                    private_hits.append(info.name)
            gates["v3_private_instances"] = {"status": "FAIL" if private_hits else "PASS", "files_with_hits": private_hits, "instances_checked": len(held)}
        except Exception:
            gates["v3_private_instances"] = {
                "status": "NOT_RUN",
                "reason": "private dataset verifier unavailable",
            }
    else:
        gates["v3_private_instances"] = {"status": "NOT_RUN", "reason": "private dataset not supplied"}
    tree = root / "tree"
    tree.mkdir()
    with tarfile.open(archive) as tar:
        tar.extractall(tree, filter="data")
    payloads = [info.name for info, _ in members if info.name != "RELEASE-MANIFEST.sha256"]
    release_manifest = build_release_manifest(
        root=tree,
        relative_paths=payloads,
        release_id=f"cleanroom-{release_tag}",
        repository_commit=repository_commit,
        release_tag=release_tag,
    )
    write_json(root / "release-manifest.json", release_manifest)
    report = {"schema": "bankingenv.release-audit/v3", "prefect_flow_run_id": str(flow_run.id),
              "release_tag": release_tag, "repository_commit": repository_commit,
              "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
              "release_manifest_sha256": release_manifest["manifest_payload_sha256"],
              "file_count": len(members), "gates": gates, "terminal_state": build.terminal_state(gates)}
    report["gates"]["release_metadata_scan"] = build.gate_release_metadata_scan(report, members)
    report["terminal_state"] = build.terminal_state(report["gates"])
    report["not_run_gates"] = sorted(
        name for name, gate in report["gates"].items() if gate["status"] == "NOT_RUN"
    )
    report["audit_sha256"] = hashlib.sha256(
        json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    write_json(root / "release-audit.json", report)
    print({
        "gates": {key: value["status"] for key, value in report["gates"].items()},
        "terminal_state": report["terminal_state"],
    })
    if report["terminal_state"] in ("RELEASE_AUDIT_FAIL", "RELEASE_AUDIT_NOT_RUN"):
        raise ValueError("release audit is incomplete or failed; archive must not be published")
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
