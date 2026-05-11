"""
sync_requirements.py — Regenerate requirements.txt from imports in active code dirs.

AST-scans .py files in TARGET_DIRS, maps top-level imports to installed
distributions via importlib.metadata, pins to installed versions.

Usage:
    python utils/sync_requirements.py
"""
import ast
import sys
from importlib.metadata import packages_distributions, version, PackageNotFoundError
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_DIRS = ["scripts", "api", "scraper", "utils", "ai_sentiment", "ml", "finbert_service"]

# Runtime-only deps (not imported in code, but required to run something).
EXTRA_PACKAGES = ["uvicorn"]

# Local top-level modules — anything importable from repo root that isn't a dep.
LOCAL_MODULES = {p.stem for p in REPO_ROOT.glob("*.py")} | {
    d.name for d in REPO_ROOT.iterdir() if d.is_dir() and (d / "__init__.py").exists()
} | set(TARGET_DIRS) | {"edgar", "pr_detection", "prn_classifier"}  # scripts/ modules imported by name


def top_level_imports(py_file: Path) -> set[str]:
    """Extract top-level module names from import statements in one file."""
    try:
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return set()
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                names.add(node.module.split(".")[0])
    return names


def main():
    stdlib = sys.stdlib_module_names
    mod_to_dist = packages_distributions()  # {"cv2": ["opencv-python"], "yaml": ["PyYAML"], ...}

    imports: set[str] = set()
    for d in TARGET_DIRS:
        root = REPO_ROOT / d
        if not root.is_dir():
            continue
        for py in root.rglob("*.py"):
            imports |= top_level_imports(py)

    pkgs: dict[str, str] = {}
    unresolved: list[str] = []

    for extra in EXTRA_PACKAGES:
        try:
            pkgs[extra.lower()] = f"{extra}=={version(extra)}"
        except PackageNotFoundError:
            unresolved.append(extra)

    for mod in sorted(imports):
        if mod in stdlib or mod in LOCAL_MODULES or mod.startswith("_"):
            continue
        dists = mod_to_dist.get(mod)
        if not dists:
            unresolved.append(mod)
            continue
        for dist in dists:
            try:
                pkgs[dist.lower()] = f"{dist}=={version(dist)}"
            except PackageNotFoundError:
                unresolved.append(dist)

    out_path = REPO_ROOT / "requirements.txt"
    out_path.write_text("\n".join(sorted(pkgs.values(), key=str.lower)) + "\n", encoding="utf-8")
    print(f"Wrote {len(pkgs)} packages -> {out_path}")
    if unresolved:
        print(f"Unresolved imports (likely local or not installed): {sorted(set(unresolved))}")


if __name__ == "__main__":
    main()
