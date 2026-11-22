"""Validate published claims against the repository status matrix."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = REPO_ROOT / "status-matrix.json"
REQUIRED_LIMITS = (
    "independent_review",
    "model_difficulty",
    "human_certification",
    "standard_harness_execution",
    "embedded_episode_canaries",
)


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_status_matrix(matrix_path: Path = DEFAULT_MATRIX) -> list[str]:
    if not matrix_path.is_file():
        raise ValueError(f"status matrix missing: {matrix_path}")
    payload = json.loads(matrix_path.read_text(encoding="utf-8"))
    if payload.get("schema") != "cleanroom.status-matrix/v1":
        raise ValueError("invalid status-matrix schema")
    rows = payload.get("limits")
    if not isinstance(rows, list):
        raise ValueError("status-matrix limits must be a list")

    names = [row.get("limit") for row in rows if isinstance(row, dict)]
    missing = [name for name in REQUIRED_LIMITS if name not in names]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if missing:
        raise ValueError("dropped status-matrix rows: " + ", ".join(missing))
    if duplicates:
        raise ValueError("duplicate status-matrix rows: " + ", ".join(duplicates))

    for row in rows:
        evidence = row.get("evidence")
        if not evidence:
            raise ValueError(f"status-matrix row lacks evidence: {row.get('limit')}")
        artifact = (REPO_ROOT / evidence).resolve()
        try:
            artifact.relative_to(REPO_ROOT.resolve())
        except ValueError:
            raise ValueError(f"evidence escapes repository: {evidence}")
        if not artifact.is_file():
            raise ValueError(f"linked evidence missing: {evidence}")
    return names


def validate_local_markdown_links(root: Path = REPO_ROOT) -> list[str]:
    missing: list[str] = []
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in (".md", ".markdown"):
            continue
        text = path.read_text(encoding="utf-8")
        for marker in text.split("](")[1:]:
            target = marker.split(")", 1)[0].strip().split()[0]
            if not target or "://" in target or target.startswith("#"):
                continue
            target = target.split("#", 1)[0]
            artifact = (path.parent / target).resolve()
            try:
                artifact.relative_to(root.resolve())
            except ValueError:
                missing.append(f"{path.relative_to(root)} -> {target}")
                continue
            if not artifact.exists():
                missing.append(f"{path.relative_to(root)} -> {target}")
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    args = parser.parse_args()
    try:
        validate_status_matrix(args.matrix)
        missing = validate_local_markdown_links()
        if missing:
            raise ValueError("missing linked artifacts:\n" + "\n".join(missing))
    except (ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"STATUS GATE FAIL: {exc}")
    print("STATUS GATE PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
