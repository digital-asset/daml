# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import subprocess
import sys

from python.runfiles import runfiles

hlint_config = ".hlint.yaml"
haskell_patterns = ["*.hs", "*.lhs"]
git = os.environ.get("GIT", "git")
utf8_locale = "en_US.UTF-8" if sys.platform == "darwin" else "C.UTF-8"


def workspace_directory():
    if "BUILD_WORKSPACE_DIRECTORY" in os.environ:
        return os.environ["BUILD_WORKSPACE_DIRECTORY"]
    module_file = runfiles.Create().Rlocation("_main/MODULE.bazel")
    return os.path.dirname(os.path.realpath(module_file))


def tracked_haskell_files():
    result = subprocess.run(
        [git, "ls-files", "-z", "--", *haskell_patterns],
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        sys.exit(f"git ls-files failed:\n{result.stderr}")
    return [path for path in result.stdout.split("\0") if path]


def run_hlint(paths):
    hlint = runfiles.Create().Rlocation(os.environ["HLINT"])
    result = subprocess.run(
        [hlint, f"--hint={hlint_config}", "-j", "--json", "--no-exit-code", *paths],
        capture_output=True,
        encoding="utf-8",
        env={**os.environ, "LANG": utf8_locale, "LC_ALL": utf8_locale},
    )
    if result.returncode != 0:
        sys.exit(f"hlint failed:\n{result.stderr}")
    return json.loads(result.stdout)


def indented(text, prefix):
    return "\n".join((prefix + line).rstrip() for line in text.splitlines())


def describe(hint):
    lines = [
        f'  {hint["file"]}:{hint["startLine"]}:{hint["startColumn"]}: {hint["severity"]}: {hint["hint"]}',
        "    Found:",
        indented(hint["from"], "      "),
    ]
    if hint["to"] is not None:
        lines += ["    Perhaps:", indented(hint["to"], "      ")]
    lines += [indented(f"Note: {note}", "    ") for note in hint["note"]]
    return "\n".join(lines)


def main():
    if sys.argv[1:2] != ["check"]:
        sys.exit("usage: hlint check [PATH...]")

    os.chdir(workspace_directory())
    paths = sys.argv[2:] or tracked_haskell_files()
    if not paths:
        print("No Haskell files to check.")
        return 0

    hints = run_hlint(paths)

    if not hints:
        print(f"No hlint hints in {len(paths)} checked paths.")
        return 0

    files = {hint["file"] for hint in hints}
    print(f"hlint check failed: {len(hints)} hints in {len(files)} files.")
    print()
    print(f"Hints ({len(hints)}):")
    for hint in hints:
        print(describe(hint))
        print()
    print(f"Fix them by hand, or ignore them in {hlint_config}.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
