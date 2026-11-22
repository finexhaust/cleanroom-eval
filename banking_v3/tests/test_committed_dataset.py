from pathlib import Path
from shutil import copytree

import pytest

import banking_v3.flow as flow
from banking_v3.contract import load_json, load_package, write_json


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "banking_v3" / "data"


def verify_dataset(data):
    receipt = load_json(data / "regeneration-receipt.json")
    manifest, _ = load_package(data / "development")
    assert receipt["code_sha256"] == flow.code_commitment()
    assert receipt["splits"]["development"]["case_index_sha256"] == manifest["case_index_sha256"]


def test_committed_dataset_and_receipt():
    verify_dataset(DATA)


def test_committed_dataset_tampering_is_rejected(tmp_path):
    copytree(DATA, tmp_path / "data")
    case = next((tmp_path / "data" / "development" / "cases").glob("*.json"))
    case.write_text("{}")
    with pytest.raises(ValueError):
        verify_dataset(tmp_path / "data")


def test_receipt_dataset_mismatch_is_rejected(tmp_path):
    copytree(DATA, tmp_path / "data")
    receipt_path = tmp_path / "data" / "regeneration-receipt.json"
    receipt = load_json(receipt_path)
    receipt["splits"]["development"]["case_index_sha256"] = "0" * 64
    write_json(receipt_path, receipt)
    with pytest.raises(AssertionError):
        verify_dataset(tmp_path / "data")


def test_generator_change_invalidates_receipt(tmp_path, monkeypatch):
    for path in (ROOT / "banking_v3").glob("*.py"):
        (tmp_path / path.name).write_bytes(path.read_bytes())
    with (tmp_path / "generate.py").open("a") as generator:
        generator.write("\n# changed\n")
    monkeypatch.setattr(flow, "__file__", str(tmp_path / "flow.py"))
    with pytest.raises(AssertionError):
        verify_dataset(DATA)
