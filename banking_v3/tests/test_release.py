from pathlib import Path
import hashlib
import json
import subprocess

import pytest

from cleanroom_eval import certify
from cleanroom_eval.release import build_release_candidate as build
from cleanroom_eval.status_gate import validate_status_matrix, validate_local_markdown_links


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


def test_status_matrix_and_local_result_links_are_gate_checked():
    assert "independent_review" in validate_status_matrix()
    assert validate_local_markdown_links() == []


def test_evidence_manifest_is_bound_to_current_repository_and_runner_files():
    root = Path(__file__).resolve().parents[2]
    manifest = json.loads((root / "cleanroom_eval/evidence/gates-2026-08/evidence-manifest.json").read_text())
    commit = manifest["repository_commit"]
    subprocess.run(
        ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
        cwd=root, check=True,
    )
    for rel, expected in manifest["runner_code_sha256"].items():
        actual = hashlib.sha256((root / rel).read_bytes()).hexdigest()
        assert actual == expected
