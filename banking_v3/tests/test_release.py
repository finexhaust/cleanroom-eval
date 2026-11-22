from pathlib import Path

import pytest

from cleanroom_eval import certify
from cleanroom_eval.release import build_release_candidate as build


def _passing_gates():
    return {
        name: {"status": "PASS"}
        for name in (
            "private_gold_leak",
            "secret_scan",
            "internal_identifier_scan",
            "registry_identity_scan",
            "source_shingle_overlap",
            "license_inventory",
            "deterministic_rebuild",
            "import_smoke",
            "clean_machine_tutorial",
        )
    }


def test_no_legacy_refusal_certificate_can_be_issued(tmp_path):
    with pytest.raises(certify.CertificationError, match="RefusePolicy"):
        certify.run_certification(prereg_path=tmp_path, sealed_path=tmp_path,
            evidence_path=tmp_path, runs_root=tmp_path, out_dir=tmp_path)
    assert not list(tmp_path.iterdir())


def test_terminal_state_names_gates_that_did_not_run():
    gates = _passing_gates()
    gates["clean_machine_tutorial"]["limitation"] = "separate-machine replay not performed"
    assert build.terminal_state(gates) == "RELEASE_AUDIT_PASS_WITH_LIMITATIONS"

    gates["source_shingle_overlap"] = {"status": "NOT_RUN"}
    gates["v3_private_instances"] = {"status": "NOT_RUN"}

    assert build.terminal_state(gates) == (
        "RELEASE_AUDIT_GATES_NOT_RUN:source_shingle_overlap,v3_private_instances"
    )


def test_not_run_gate_reason_does_not_expose_machine_path(monkeypatch):
    import builtins

    original_import = builtins.__import__

    def import_without_private_module(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "cleanroom_eval.release" and "registry_identity" in fromlist:
            raise ImportError("missing /private/verify_cast_world.py")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", import_without_private_module)
    gate = build.gate_registry_identity_scan([])

    assert gate["status"] == "NOT_RUN"
    assert "/" not in gate["reason"]
    assert "\\" not in gate["reason"]
    assert "ImportError" not in gate["reason"]


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
