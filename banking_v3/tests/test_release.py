from pathlib import Path
import json
import subprocess
import sys
import tarfile

import pytest

from cleanroom_eval import certify, contract
from cleanroom_eval.release import build_release_candidate as build
from banking_v3.release import _seal_audit


def test_no_legacy_refusal_certificate_can_be_issued(tmp_path):
    with pytest.raises(certify.CertificationError, match="RefusePolicy"):
        certify.run_certification(prereg_path=tmp_path, sealed_path=tmp_path,
            evidence_path=tmp_path, runs_root=tmp_path, out_dir=tmp_path)
    assert not list(tmp_path.iterdir())


def test_public_release_allows_v3_runtime_but_not_private_data():
    names = [name for _, name in build._iter_allowlist()]
    assert "banking_v3/environment.py" in names
    assert "banking_v3/generate.py" in names
    assert not any("banking_v3/data/evaluation/" in name or "/evaluator/" in name for name in names)
    assert 'version = "2.1.3"' in build.PYPROJECT
    assert '"banking_v3"' in build.PYPROJECT


def test_runtime_version_literal_matches_package_version():
    """banking_v3.__version__ must equal the version the release pyproject declares."""
    import re
    import banking_v3

    declared = re.search(r'^version = "([^"]+)"$', build.PYPROJECT, re.M).group(1)
    assert banking_v3.__version__ == declared


def test_release_audit_self_digest_excludes_its_own_field():
    report = _seal_audit({"schema": "bankingenv.release-audit/v4", "tag": "v1.2.3"})
    digest = report["audit_payload_sha256"]
    assert digest == contract.digest(
        {key: value for key, value in report.items() if key != "audit_payload_sha256"}
    )
    report["tag"] = "v1.2.4"
    assert digest != contract.digest(
        {key: value for key, value in report.items() if key != "audit_payload_sha256"}
    )


def test_release_manifest_verifier_can_require_commit(tmp_path, monkeypatch):
    (tmp_path / "archive.tar.gz").write_bytes(b"release archive")
    manifest = contract.build_release_manifest(
        root=tmp_path,
        relative_paths=["archive.tar.gz"],
        release_id="cleanroom-v1.2.3",
        repository_commit="a" * 40,
    )
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(
        sys, "argv",
        ["contract", "verify-release", "--root", str(tmp_path),
         "--manifest", str(manifest_path), "--commit", "a" * 40],
    )
    assert contract._main() == 0

    monkeypatch.setattr(sys, "argv", sys.argv[:-1] + ["b" * 40])
    with pytest.raises(contract.ContractError, match="different commit"):
        contract._main()


def test_export_release_command_is_documented_by_cli(monkeypatch, capsys):
    from banking_v3 import __main__

    monkeypatch.setattr(sys, "argv", ["banking_v3", "export-release", "--help"])
    with pytest.raises(SystemExit) as exc:
        __main__.main()
    assert exc.value.code == 0
    assert "--commit" in capsys.readouterr().out


def test_tagged_tree_gate_rejects_archive_source_drift(tmp_path, monkeypatch):
    source = tmp_path / "sample.txt"
    source.write_bytes(b"from tagged tree")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "sample.txt"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-qm", "tagged source"], check=True)
    commit = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    monkeypatch.setattr(build, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(build, "_iter_allowlist", lambda: [(source, "sample.txt")])
    monkeypatch.setattr(build, "GENERATED_FILES", {})
    member = tarfile.TarInfo("sample.txt")
    manifest_member = tarfile.TarInfo("RELEASE-MANIFEST.sha256")

    archive_members = [(member, b"from tagged tree"), (manifest_member, b"")]
    assert build.gate_tagged_tree(archive_members, commit)["status"] == "PASS"
    archive_members[0] = (member, b"modified archive")
    assert build.gate_tagged_tree(archive_members, commit)["status"] == "FAIL"
