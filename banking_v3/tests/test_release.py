import tarfile
from pathlib import Path

import pytest

from cleanroom_eval import certify
from cleanroom_eval.release import build_release_candidate as build


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


def test_release_output_scans_cover_audit_report_and_evidence_pack():
    evidence = tarfile.TarInfo(
        "cleanroom_eval/evidence/gates-2026-08/evidence.json"
    )
    members = [(evidence, b'{"detail":"/home/faz/private/report"}')]

    scans = build.gate_release_output_scans(
        members, b'{"detail":"/home/faz/private/audit"}'
    )

    assert scans["internal_identifier_scan"]["status"] == "FAIL"
    assert {
        hit["file"] for hit in scans["internal_identifier_scan"]["hits"]
    } == {
        "cleanroom_eval/evidence/gates-2026-08/evidence.json",
        "release-audit.json",
    }


def test_release_output_scans_reject_unsafe_evidence_pack_paths():
    evidence = tarfile.TarInfo("../evidence-pack.json")

    scans = build.gate_release_output_scans([(evidence, b"{}")], b"{}")

    assert scans["path_metadata_scan"]["status"] == "FAIL"
    assert scans["path_metadata_scan"]["problems"][0]["entry"] == "../evidence-pack.json"
