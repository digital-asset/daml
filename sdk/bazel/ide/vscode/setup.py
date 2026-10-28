import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

import ide_setup

EXTENSION = "haskell.haskell"
EDITOR_CLIS = ("codium", "code")
SETTINGS = {
    "haskell.manageHLS": "PATH",
    "haskell.serverExecutablePath": "${workspaceFolder}/" + ide_setup.HLS_WRAPPER.as_posix(),
}


def load(path: Path) -> dict | None:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text())
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def merge_settings(settings: dict, remove: bool) -> dict:
    merged = dict(settings)
    for key, value in SETTINGS.items():
        if not remove:
            merged[key] = value
        elif merged.get(key) == value:
            del merged[key]
    return merged


def merge_recommendations(extensions: dict, remove: bool) -> dict:
    merged = dict(extensions)
    recommendations = list(merged.get("recommendations", []))
    if remove:
        recommendations = [r for r in recommendations if r != EXTENSION]
    elif EXTENSION not in recommendations:
        recommendations.append(EXTENSION)
    if recommendations:
        merged["recommendations"] = recommendations
    else:
        merged.pop("recommendations", None)
    return merged


def update(path: Path, merge, remove: bool, snippet: dict) -> bool:
    current = load(path)
    if current is None:
        print(f"skipped {path}: it is not plain JSON (comments or trailing commas?)")
        print("  merge these entries by hand:" if not remove else "  remove these entries by hand:")
        for line in json.dumps(snippet, indent=4).splitlines():
            print(f"  {line}")
        return False
    merged = merge(current, remove)
    if merged == current or (not merged and not path.exists()):
        print(f"unchanged {path}")
        return True
    if path.exists():
        print(f"backed up {path} to {ide_setup.backup(path)}")
    ide_setup.write_text(path, json.dumps(merged, indent=4) + "\n")
    print(f"updated {path}")
    return True


def install_extension() -> None:
    clis = [cli for cli in EDITOR_CLIS if shutil.which(cli)]
    if not clis:
        print(f"no {' or '.join(EDITOR_CLIS)} command on PATH; install {EXTENSION} from the editor")
    for cli in clis:
        subprocess.run([cli, "--install-extension", EXTENSION], check=False)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="bazelisk run //:vscode_setup --")
    parser.add_argument("--remove", action="store_true", help="undo the workspace changes made by this setup")
    parser.add_argument("--install-extension", action="store_true", help=f"also install {EXTENSION} via codium/code")
    args = parser.parse_args(argv)

    root = ide_setup.workspace()
    vscode = root / ".vscode"
    ok = update(vscode / "settings.json", merge_settings, args.remove, SETTINGS)
    ok = update(vscode / "extensions.json", merge_recommendations, args.remove, {"recommendations": [EXTENSION]}) and ok
    if args.remove:
        return 0 if ok else 1

    if args.install_extension:
        install_extension()
    print()
    ide_setup.announce(root, ide_setup.record_bazel(root))
    print(f"Open {root} as the folder in VS Code or VSCodium and accept the {EXTENSION} recommendation.")
    print("The first start builds the HLS cradle; progress shows in Output > Haskell.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
