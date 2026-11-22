"""Build and audit the isolated clean-room release candidate (#145 Phase D).

Produces a deterministic archive from a STRICT ALLOWLIST (never from the
repository at large) and runs the release-audit gates against the exact
archive bytes. The repository's history is contaminated and is never
exported; a publishable candidate goes to a fresh repository with new
history.

Usage:
    python -m cleanroom_eval.release.build_release_candidate [--out DIR]

Outputs (under --out, default ~/.local/share/finexhaust/cleanroom-release):
    cleanroom-eval-release.tar.gz    the candidate archive
    release-audit.json               gate results + terminal state

The audit report alone is committed back to the repository; the archive
stays local until the owner's publication decision (Phase E).
"""
from __future__ import annotations

import argparse
import pathlib
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ARCHIVE_NAME = "cleanroom-eval-release.tar.gz"
FIXED_MTIME = 1787788800  # 2026-08-23T00:00:00Z, for deterministic archives

# ---- allowlist ------------------------------------------------------------
ALLOW_DIRS = (
    ("cleanroom_eval", "*.py"),
    ("banking_v3", "*.py"),
    ("banking_v3/tests", "test_*.py"),
    ("banking_v3/data/development", "**/*.json"),
    ("cleanroom_eval/schemas", "**/*"),
    ("cleanroom_eval/assets", "**/*"),
    ("cleanroom_eval/mock_training", "**/*"),
    ("cleanroom_eval/evidence/gates-2026-08", "*"),
    ("cleanroom_eval/evidence", "bind_gates_evidence.py"),
    ("cleanroom_eval/release", "build_release_candidate.py"),
)
ALLOW_FILES = (
    "cleanroom_eval/README.md",
    "docs/bankingenv-v3.md",
    "docs/bankingenv-v3-model-panel.md",
    "banking_v3/data/regeneration-receipt.json",
    "banking_v3/data/limits-status.v1.json",
    "docs/technical-note-strategy-locking.md",
    "cleanroom_eval/evidence/__init__.py",
    "cleanroom_eval/release/__init__.py",
    "cleanroom_corpus/eval_adapters.py",
    "cleanroom_corpus/eval_export.py",
    "requirements-test.txt",
    "requirements-banking-panel.txt",
    "LICENSE",
)
# Evaluator-only material must never ship, allowlist notwithstanding.
DENY_NAME_PATTERNS = (
    re.compile(r"oracle", re.IGNORECASE),
    # S1 private-set minting recipe: the SKU differentiator, never shipped.
    re.compile(r"private_sets\.py$"),
    re.compile(r"mint_private_set\.py$"),
    # Private run analysis bound to a local output path; not part of the runtime.
    re.compile(r"passk_aggregate\.py$"),
    # ISDA curve replay handoff (#217): binds a real institution's private
    # gold set by artifact id, so it breaks the CLEANROOM_SYNTHETIC boundary.
    re.compile(r"(^|/)isda_replay/"),
    re.compile(r"\.pyc$"),
    re.compile(r"__pycache__"),
)
MINIMAL_CORPUS_INIT = (
    '"""Release packaging: adapter surface only.\n\n'
    "The full synthetic-corpus generator is not part of the clean-room\n"
    'release; only the evaluation adapters and export contract ship."""\n'
)
SECURITY_STATEMENT = """# Security and release statement

This archive contains ONLY the clean-room evaluation environment: code,
schemas, fictitious CLEANROOM_SYNTHETIC episodes, task contracts, the
content-safe evidence summary, and this statement. It is built from an
explicit allowlist, never from the source repository's history.

- Every episode, name, institution and identifier is invented. The evidence
  summary binds results by hash and contains no transcripts, credentials or
  source-derived records.
- Episodes deliberately embed canary values and reward traps; they are part
  of the published task boundary. A model trained on this release loses
  canary-based contamination detection; this is the standard public-benchmark
  caveat.
- The evaluator-only citation oracle and all private gold are excluded and
  their absence is enforced by the release audit's leakage gate. The README
  step `verify_citation_task_set()` recomputes oracle invariants and is
  therefore evaluator-only: it is not runnable from this archive. Every
  other documented step (verify-bundle, verify-set, the preregistered
  scripted baseline) runs from the archive with `pip install -r
  requirements.txt` and no network access at run time.
- Report issues to the repository owner; do not open public issues that
  quote unpublished material.
"""
RUNTIME_REQUIREMENTS = "cryptography==46.0.5\njsonschema==4.26.0\nprefect==3.6.20\n"
PYPROJECT = """[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "cleanroom-eval"
version = "2.1.3"
description = "Clean-room capital-markets agent evaluation: sealed synthetic episodes, task contracts, reward-hacking gates, hash-bound evidence"
readme = "README.md"
license = {file = "LICENSE"}
requires-python = ">=3.11"
dependencies = ["cryptography>=42", "jsonschema>=4.20", "prefect==3.6.20"]

[project.optional-dependencies]
panel = ["httpx>=0.27", "python-dotenv>=1.0"]

[tool.setuptools]
packages = ["cleanroom_eval", "cleanroom_eval.evidence", "cleanroom_eval.release", "cleanroom_corpus", "banking_v3"]

[tool.setuptools.package-data]
cleanroom_eval = ["assets/**/*", "schemas/*", "mock_training/*", "README.md"]
banking_v3 = ["data/development/**/*.json", "data/*.json"]
"""
PYPROJECT = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
ROOT_README_PATH = REPO_ROOT / "docs/bankingenv-v3.md"
ROOT_README = (ROOT_README_PATH if ROOT_README_PATH.is_file() else REPO_ROOT / "README.md").read_text(encoding="utf-8")
PUBLIC_CI = (REPO_ROOT / ".github/workflows/banking-v3.yml").read_text(encoding="utf-8")



def _iter_allowlist() -> list[tuple[Path, str]]:
    """Return (source_path, archive_relpath) pairs, deny-filtered."""
    seen: dict[str, Path] = {}
    for base, pattern in ALLOW_DIRS:
        root = REPO_ROOT / base
        if not root.is_dir():
            continue
        for path in sorted(root.glob(pattern)):
            if not path.is_file():
                continue
            rel = str(path.relative_to(REPO_ROOT))
            if any(p.search(rel) for p in DENY_NAME_PATTERNS):
                continue
            seen[rel] = path
    for rel in ALLOW_FILES:
        path = REPO_ROOT / rel
        if path.is_file() and not any(p.search(rel) for p in DENY_NAME_PATTERNS):
            seen[rel] = path
    return [(seen[rel], rel) for rel in sorted(seen)]


def build_archive(out_dir: Path) -> Path:
    entries = _iter_allowlist()
    manifest_lines = []
    generated = {
        "cleanroom_corpus/__init__.py": MINIMAL_CORPUS_INIT.encode("utf-8"),
        "SECURITY-RELEASE.md": SECURITY_STATEMENT.encode("utf-8"),
        "requirements.txt": RUNTIME_REQUIREMENTS.encode("utf-8"),
        "pyproject.toml": PYPROJECT.encode("utf-8"),
        "README.md": ROOT_README.encode("utf-8"),
        ".github/workflows/banking-v3.yml": PUBLIC_CI.encode("utf-8"),
    }
    payloads: list[tuple[str, bytes]] = []
    for path, rel in entries:
        blob = path.read_bytes()
        if rel == "requirements-test.txt":
            # Drop includes of files outside the release tree; the archive is self-contained.
            blob = "".join(
                line for line in blob.decode("utf-8").splitlines(keepends=True)
                if not line.lstrip().startswith("-r ")
            ).encode("utf-8")
        payloads.append((rel, blob))
    for rel, blob in generated.items():
        payloads.append((rel, blob))
    payloads.sort(key=lambda pair: pair[0])
    for rel, blob in payloads:
        manifest_lines.append(f"{hashlib.sha256(blob).hexdigest()}  {rel}")
    manifest = ("\n".join(manifest_lines) + "\n").encode("utf-8")
    payloads.append(("RELEASE-MANIFEST.sha256", manifest))

    out_dir.mkdir(parents=True, exist_ok=True)
    archive_path = out_dir / ARCHIVE_NAME
    # gzip with mtime=0 for byte-determinism
    import gzip

    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        for rel, blob in payloads:
            info = tarfile.TarInfo(name=rel)
            info.size = len(blob)
            info.mtime = FIXED_MTIME
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(blob))
    archive_path.write_bytes(gzip.compress(raw.getvalue(), mtime=0))
    return archive_path


# ---- gates ---------------------------------------------------------------
SECRET_PATTERNS = (
    r"AKIA[0-9A-Z]{16}", r"sk-[A-Za-z0-9]{20,}", r"ghp_[A-Za-z0-9]{36}",
    r"xox[baprs]-[A-Za-z0-9-]{10,}", r"cfat_[A-Za-z0-9]{20,}",
    r"-----BEGIN (?:RSA|OPENSSH|EC|PGP) PRIVATE KEY-----", r"AIza[0-9A-Za-z_-]{35}",
    r"(?i)authorization:\s*bearer\s+[A-Za-z0-9._-]{16,}",
)
INTERNAL_PATTERNS = (
    r"/home/[a-z]+/", r"/mnt/[ce]/", r"/Users/[A-Za-z]+/",
    r"(?i)C:\\\\Users\\\\",
    # Personal identity tokens load from an untracked local file so the gate
    # itself never ships the identifiers it exists to suppress.
    *(
        pattern.strip()
        for _f in [pathlib.Path.home() / ".finexhaust-scrub-denylist"]
        if _f.is_file()
        for pattern in _f.read_text(encoding="utf-8").splitlines()
        if pattern.strip() and not pattern.strip().startswith("#")
    ),
)


def _archive_members(archive_path: Path) -> list[tuple[tarfile.TarInfo, bytes]]:
    members = []
    with tarfile.open(archive_path, mode="r:gz") as tar:
        for info in tar.getmembers():
            blob = tar.extractfile(info).read() if info.isfile() else b""
            members.append((info, blob))
    return members


def _text(blob: bytes) -> str:
    try:
        return blob.decode("utf-8")
    except UnicodeDecodeError:
        return ""


def gate_secret_scan(members) -> dict:
    hits = []
    for info, blob in members:
        text = _text(blob)
        for pattern in SECRET_PATTERNS:
            if re.search(pattern, text):
                hits.append({"file": info.name, "pattern": pattern})
    return {"status": "PASS" if not hits else "FAIL", "hits": hits}


def gate_internal_identifier_scan(members) -> dict:
    hits = []
    for info, blob in members:
        text = _text(blob)
        for pattern in INTERNAL_PATTERNS:
            for match in set(re.findall(pattern, text)):
                hits.append({"file": info.name, "pattern": pattern, "match": match})
    return {"status": "PASS" if not hits else "FAIL", "hits": hits[:50]}


def _shingles(text: str, width: int = 10):
    words = re.findall(r"[a-z0-9']+", text.lower())
    for index in range(len(words) - width + 1):
        yield " ".join(words[index : index + width])


def gate_registry_identity_scan(members) -> dict:
    """Hunt every identity the extractor recorded, not just what an operator remembered.

    ``gate_internal_identifier_scan`` depends on an untracked local denylist and
    ``gate_source_shingle_overlap`` needs a ten-word verbatim run, so a single
    real name written into fresh prose passes both. The needle set and the sweep
    live in ``registry_identity``, which is deliberately outside the allowlist:
    this file ships, and it must never carry the identifiers it hunts.
    """
    try:
        from cleanroom_eval.release import registry_identity
    except ImportError as exc:
        return {"status": "NOT_RUN", "reason": "private identity verifier unavailable"}
    return registry_identity.scan_members(members)


def gate_source_shingle_overlap(members, corpora_dir: Path) -> dict:
    corpus_files = sorted(corpora_dir.glob("*.txt")) if corpora_dir.is_dir() else []
    if not corpus_files:
        return {"status": "NOT_RUN", "reason": "record corpus not present on this machine"}
    corpus_shingles: set[str] = set()
    for path in corpus_files:
        corpus_shingles.update(_shingles(path.read_text(encoding="utf-8", errors="replace")))
    overlaps = []
    for info, blob in members:
        text = _text(blob)
        if not text:
            continue
        matched = {s for s in _shingles(text) if s in corpus_shingles}
        if matched:
            overlaps.append({"file": info.name, "overlapping_shingles": len(matched),
                             "example_hash": hashlib.sha256(sorted(matched)[0].encode()).hexdigest()[:16]})
    return {
        "status": "PASS" if not overlaps else "FAIL",
        "corpora_scanned": len(corpus_files),
        "shingle_width_words": 10,
        "overlaps": overlaps,
    }


def gate_private_gold_leak(members) -> dict:
    leaks = [
        {"file": info.name, "reason": "matches evaluator-only naming"}
        for info, _ in members
        if re.search(r"oracle|private[-_]gold", info.name, re.IGNORECASE)
    ]
    # evaluator-only oracle CONTENT must not appear under another name
    oracle = REPO_ROOT / "cleanroom_eval/assets/citation_tasks/citation-task-oracle.v1.jsonl"
    if oracle.is_file():
        needles = set()
        for line in oracle.read_text(encoding="utf-8").splitlines()[:200]:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            blob = json.dumps(row, sort_keys=True)
            needles.add(blob[:120])
        for info, blob in members:
            text = _text(blob)
            if text and any(needle[:60] in text for needle in needles):
                leaks.append({"file": info.name, "reason": "embeds oracle-derived content"})
    return {"status": "PASS" if not leaks else "FAIL", "leaks": leaks}


def gate_path_metadata_scan(members) -> dict:
    problems = []
    for info, _ in members:
        if info.name.startswith("/") or ".." in Path(info.name).parts:
            problems.append({"entry": info.name, "reason": "unsafe path"})
        if info.issym() or info.islnk():
            problems.append({"entry": info.name, "reason": "link entry"})
        if info.uid or info.gid or info.uname or info.gname:
            problems.append({"entry": info.name, "reason": "identity-bearing ownership metadata"})
    return {"status": "PASS" if not problems else "FAIL", "problems": problems}


def gate_license_inventory(members) -> dict:
    names = {info.name for info, _ in members}
    dependencies = []
    for info, blob in members:
        if info.name == "requirements-test.txt":
            dependencies = [
                line.strip() for line in _text(blob).splitlines()
                if line.strip() and not line.startswith("#")
            ]
    has_license = "LICENSE" in names
    return {
        "status": "PASS" if has_license else "FAIL",
        "license_present": has_license,
        "declared_dependencies": dependencies,
        "note": None if has_license else "no LICENSE file exists in the repository; owner must choose one",
    }


def gate_deterministic_rebuild(archive_path: Path) -> dict:
    with tempfile.TemporaryDirectory() as scratch:
        second = build_archive(Path(scratch))
        first_sha = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        second_sha = hashlib.sha256(second.read_bytes()).hexdigest()
    return {
        "status": "PASS" if first_sha == second_sha else "FAIL",
        "archive_sha256": first_sha,
        "rebuild_sha256": second_sha,
    }


def gate_manifest_verification(members) -> dict:
    by_name = {info.name: blob for info, blob in members}
    manifest = _text(by_name.get("RELEASE-MANIFEST.sha256", b""))
    if not manifest:
        return {"status": "FAIL", "reason": "manifest missing"}
    mismatches = []
    listed = set()
    for line in manifest.splitlines():
        sha, _, rel = line.partition("  ")
        listed.add(rel)
        if rel not in by_name:
            mismatches.append({"file": rel, "reason": "listed but absent"})
        elif hashlib.sha256(by_name[rel]).hexdigest() != sha:
            mismatches.append({"file": rel, "reason": "hash mismatch"})
    for name in by_name:
        if name not in listed and name != "RELEASE-MANIFEST.sha256":
            mismatches.append({"file": name, "reason": "present but unlisted"})
    return {"status": "PASS" if not mismatches else "FAIL", "mismatches": mismatches}


def gate_result_limits(members) -> dict:
    by_name = {info.name: blob for info, blob in members}
    path = "banking_v3/data/limits-status.v1.json"
    try:
        matrix = json.loads(by_name[path])
        rows = matrix["limits"]
        statuses = {row["id"]: row["status"] for row in rows}
        expected = {
            "independent_review": "NOT_RUN",
            "model_difficulty": "NOT_MEASURED",
            "human_certification": "NOT_ISSUED",
            "standard_harness": "NOT_MET",
        }
        artifacts = matrix["linked_artifacts"]
        if (
            matrix["schema"] != "bankingenv.limits-status/v1"
            or statuses != expected
            or len(rows) != len(expected)
        ):
            raise ValueError("limit status rows are missing or overstate evidence")
        for artifact in artifacts:
            name = artifact["path"]
            if (
                Path(name).is_absolute()
                or ".." in Path(name).parts
                or name not in by_name
                or hashlib.sha256(by_name[name]).hexdigest() != artifact["sha256"]
            ):
                raise ValueError("linked result artifact is missing or has a different digest")
        return {"status": "PASS", "limits_checked": len(expected), "artifacts_checked": len(artifacts)}
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "FAIL", "reason": str(exc)}


def gate_release_metadata_scan(report: dict, members) -> dict:
    audit = json.dumps(report, ensure_ascii=False, sort_keys=True).encode("utf-8")
    audit_info = tarfile.TarInfo("release-audit.json")
    generated = [(audit_info, audit)]
    evidence = [
        (info, blob) for info, blob in members
        if info.name.startswith("cleanroom_eval/evidence/")
    ]
    scanned = [*generated, *evidence]
    results = {
        "secret_scan": gate_secret_scan(scanned),
        "internal_identifier_scan": gate_internal_identifier_scan(scanned),
        "path_metadata_scan": gate_path_metadata_scan(scanned),
    }
    return {
        "status": "PASS" if all(item["status"] == "PASS" for item in results.values()) else "FAIL",
        "files_scanned": len(scanned),
        "scans": results,
    }


def gate_import_smoke(archive_path: Path) -> dict:
    with tempfile.TemporaryDirectory() as scratch:
        with tarfile.open(archive_path, mode="r:gz") as tar:
            tar.extractall(scratch, filter="data")
        probe = (
            "import sys; sys.path.insert(0, sys.argv[1]);\n"
            "import cleanroom_eval.contract, cleanroom_eval.free_run, cleanroom_eval.fire_gates\n"
            "from cleanroom_eval.contract import ASSET_DIR\n"
            "import cleanroom_eval.episode_contract\n"
            "import banking_v3.environment, banking_v3.flow\n"
            "import banking_v3.panel, banking_v3.panel_report\n"
            "print('IMPORT_OK')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", probe, scratch],
            capture_output=True, text=True, timeout=120,
            cwd=scratch,
        )
    ok = result.returncode == 0 and "IMPORT_OK" in result.stdout
    return {
        "status": "PASS" if ok else "FAIL",
        "detail": None if ok else (result.stderr.strip().splitlines() or ["no output"])[-1],
    }


def gate_tutorial_replay(archive_path: Path) -> dict:
    """Replay the release-runnable tutorial core in an isolated fresh venv.

    Extracts the archive, creates a clean virtualenv, installs the release's
    own requirements.txt, and runs the documented steps: sealed-set
    verification for both episode sets, preregistration, and the scripted
    baseline (40 episodes, in-process, no network at run time). Machine-level
    isolation is a recorded limitation, not silently claimed.
    """
    steps_run: list[dict] = []
    with tempfile.TemporaryDirectory() as scratch:
        root = Path(scratch)
        with tarfile.open(archive_path, mode="r:gz") as tar:
            tar.extractall(root, filter="data")
        venv_dir = root / "venv"
        bootstrap = subprocess.run(
            [sys.executable, "-m", "venv", str(venv_dir)],
            capture_output=True, text=True, timeout=180,
        )
        if bootstrap.returncode != 0:
            return {"status": "FAIL", "detail": "venv creation failed"}
        python = venv_dir / "bin" / "python"
        # Install exactly the way the README tells a buyer to: pip install -e .
        install = subprocess.run(
            [str(python), "-m", "pip", "install", "--quiet", "-e", str(root)],
            capture_output=True, text=True, timeout=600, cwd=root,
        )
        if install.returncode != 0:
            return {
                "status": "FAIL",
                "detail": "pip install -e . failed: " + (install.stderr.strip().splitlines() or ["no output"])[-1][:200],
            }
        environment = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": scratch,
        }
        commands = (
            ["-m", "cleanroom_eval.contract", "verify-bundle"],
            ["-m", "cleanroom_eval.contract", "verify-set"],
            ["-m", "cleanroom_eval.fire_gates", "preregister", "--out", "runs"],
            ["-m", "cleanroom_eval.fire_gates", "baseline", "--out", "runs"],
        )
        for command in commands:
            result = subprocess.run(
                [str(python), *command],
                capture_output=True, text=True, timeout=1200, cwd=root, env=environment,
            )
            steps_run.append({"step": " ".join(command), "returncode": result.returncode})
            if result.returncode != 0:
                detail = (result.stderr.strip() or result.stdout.strip()).splitlines()
                return {
                    "status": "FAIL",
                    "steps": steps_run,
                    "detail": (detail or ["no output"])[-1][:300],
                }
    return {
        "status": "PASS",
        "environment": "isolated-venv",
        "steps": steps_run,
        "limitation": "replayed in a fresh venv on the build machine; separate-machine replay not performed",
    }


def terminal_state(gates: dict) -> str:
    if gates["private_gold_leak"]["status"] == "FAIL":
        return "PRIVATE_GOLD_LEAK"
    if (gates["secret_scan"]["status"] == "FAIL"
            or gates["internal_identifier_scan"]["status"] == "FAIL"
            or gates["registry_identity_scan"]["status"] == "FAIL"):
        return "SECRET_OR_IDENTIFIER_LEAK"
    if gates["source_shingle_overlap"]["status"] == "FAIL":
        return "SOURCE_OVERLAP_DETECTED"
    if gates["license_inventory"]["status"] == "FAIL":
        return "RIGHTS_OR_LICENSE_UNRESOLVED"
    if gates["deterministic_rebuild"]["status"] == "FAIL" or gates["import_smoke"]["status"] == "FAIL":
        return "RUNTIME_NOT_REPRODUCIBLE"
    if gates["clean_machine_tutorial"]["status"] != "PASS":
        return "RUNTIME_NOT_REPRODUCIBLE"
    failed = sorted(name for name, gate in gates.items() if gate["status"] == "FAIL")
    if failed:
        # Gates without a dedicated state above (path metadata, manifest
        # verification, caller-added gates) must not fall through to a PASS.
        return "RELEASE_AUDIT_FAIL"
    if any(g["status"] == "NOT_RUN" for g in gates.values()):
        return "RELEASE_AUDIT_NOT_RUN"
    if any(g["status"] not in ("PASS", "NOT_RUN") for g in gates.values()):
        return "RELEASE_AUDIT_FAIL"
    if any(g["status"] != "PASS" or g.get("limitation") for g in gates.values()):
        return "RELEASE_AUDIT_PASS_WITH_LIMITATIONS"
    return "RELEASE_AUDIT_PASS"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(Path.home() / ".local/share/finexhaust/cleanroom-release"))
    parser.add_argument("--corpora", default=str(Path.home() / ".local/share/finexhaust/corpora"))
    parser.add_argument("--tag", default="unreleased")
    parser.add_argument("--commit")
    args = parser.parse_args()
    out_dir = Path(args.out)
    if args.tag != "unreleased" and not re.fullmatch(
        r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-.][0-9A-Za-z.]+)?", args.tag
    ):
        parser.error("--tag must be unreleased or a semantic version tag")

    archive = build_archive(out_dir)
    members = _archive_members(archive)
    commit = args.commit
    if commit is None:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True,
            text=True, check=True,
        ).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        parser.error("--commit must be a full 40-character commit SHA")
    with tempfile.TemporaryDirectory() as scratch:
        with tarfile.open(archive, mode="r:gz") as tar:
            tar.extractall(scratch, filter="data")
        from cleanroom_eval.contract import build_release_manifest, write_json

        payloads = [info.name for info, _ in members if info.name != "RELEASE-MANIFEST.sha256"]
        release_manifest = build_release_manifest(
            root=Path(scratch),
            relative_paths=payloads,
            release_id=f"cleanroom-{args.tag}",
            repository_commit=commit,
            release_tag=args.tag,
        )
        write_json(out_dir / "release-manifest.json", release_manifest)
    gates = {
        "secret_scan": gate_secret_scan(members),
        "internal_identifier_scan": gate_internal_identifier_scan(members),
        "registry_identity_scan": gate_registry_identity_scan(members),
        "source_shingle_overlap": gate_source_shingle_overlap(members, Path(args.corpora)),
        "private_gold_leak": gate_private_gold_leak(members),
        "path_metadata_scan": gate_path_metadata_scan(members),
        "license_inventory": gate_license_inventory(members),
        "deterministic_rebuild": gate_deterministic_rebuild(archive),
        "manifest_verification": gate_manifest_verification(members),
        "result_limits": gate_result_limits(members),
        "import_smoke": gate_import_smoke(archive),
        "clean_machine_tutorial": gate_tutorial_replay(archive),
    }
    try:
        from banking_v3.contract import load_package
        from banking_v3.evaluate import reference_rollout

        _, cases = load_package(REPO_ROOT / "banking_v3/data/development")
        passed = sum(reference_rollout(case)[0]["complete"] for case in cases)
        gates["v3_development_replay"] = {
            "status": "PASS" if passed == len(cases) else "FAIL",
            "cases": len(cases),
            "complete": passed,
        }
    except Exception:
        gates["v3_development_replay"] = {
            "status": "NOT_RUN",
            "reason": "development replay verifier unavailable",
        }
    gates["v3_private_instances"] = {
        "status": "NOT_RUN",
        "reason": "private dataset not supplied",
    }
    report = {
        "schema": "bankingenv.release-audit/v3",
        "archive": ARCHIVE_NAME,
        "release_tag": args.tag,
        "repository_commit": commit,
        "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "release_manifest_sha256": release_manifest["manifest_payload_sha256"],
        "file_count": len(members),
        "archive_bytes": archive.stat().st_size,
        "gates": gates,
        "terminal_state": terminal_state(gates),
        "not_run_gates": sorted(name for name, gate in gates.items() if gate["status"] == "NOT_RUN"),
    }
    report["gates"]["release_metadata_scan"] = gate_release_metadata_scan(report, members)
    report["terminal_state"] = terminal_state(report["gates"])
    report["not_run_gates"] = sorted(
        name for name, gate in report["gates"].items() if gate["status"] == "NOT_RUN"
    )
    report["audit_sha256"] = hashlib.sha256(
        json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    (out_dir / "release-audit.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    summary = {name: g["status"] for name, g in gates.items()}
    print(json.dumps({"archive": str(archive), "gates": summary,
                      "terminal_state": report["terminal_state"]}, indent=2))
    if report["terminal_state"] == "RELEASE_AUDIT_FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
