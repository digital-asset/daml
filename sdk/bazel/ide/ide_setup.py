import os
import shutil
import sys
from pathlib import Path

HLS_BIN = Path("bazel/ide/hls/bin")
HLS_WRAPPER = HLS_BIN / "haskell-language-server-wrapper"
BAZEL_RECORD = Path("bazel/ide/.bazel-path")


def workspace() -> Path:
    directory = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if not directory:
        sys.exit("error: run this through Bazel, for example `bazelisk run //:ide_setup`")
    return Path(directory)


def write_text(path: Path, content: str) -> bool:
    if path.exists() and path.read_text() == content:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_name(path.name + ".tmp")
    staging.write_text(content)
    staging.replace(path)
    return True


def backup(path: Path) -> Path:
    copy = path.with_name(path.name + ".bak")
    shutil.copy2(path, copy)
    return copy


def record_bazel(root: Path) -> str | None:
    found = shutil.which("bazelisk") or shutil.which("bazel")
    if found:
        write_text(root / BAZEL_RECORD, found + "\n")
    return found


def install_generated_file(path: Path, content: str, signature: str) -> str:
    if not path.exists():
        write_text(path, content)
        return "created"
    current = path.read_text()
    if current == content:
        return "unchanged"
    if signature not in current:
        return "conflict"
    backup(path)
    write_text(path, content)
    return "updated"


def remove_generated_file(path: Path, content: str, signature: str) -> str:
    if not path.exists():
        return "absent"
    current = path.read_text()
    if signature not in current:
        return "conflict"
    if current != content:
        backup(path)
    path.unlink()
    return "removed"


def announce(root: Path, bazel: str | None) -> None:
    print(f"Haskell language server for this checkout: {root / HLS_WRAPPER}")
    if bazel:
        print(f"Editors started outside a shell will run Bazel through: {bazel}")
    else:
        print("warning: neither bazelisk nor bazel is on PATH; editors must find one on their own PATH")
