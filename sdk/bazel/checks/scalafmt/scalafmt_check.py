# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import sys
from urllib.parse import urlparse
from urllib.request import url2pathname

from python.runfiles import runfiles

scalafmt_config = ".scalafmt.conf"
scala_patterns = ["*.scala", "*.sc"]
update_command = "bazelisk run //bazel/checks/scalafmt -- update"
git = os.environ.get("GIT", "git")
exit_code_ok = 0
exit_code_unformatted = 1


def workspace_directory():
    if "BUILD_WORKSPACE_DIRECTORY" in os.environ:
        return os.environ["BUILD_WORKSPACE_DIRECTORY"]
    module_file = runfiles.Create().Rlocation("_main/MODULE.bazel")
    return os.path.dirname(os.path.realpath(module_file))


def tracked_scala_files():
    result = subprocess.run(
        [git, "ls-files", "-z", "--", *scala_patterns],
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        sys.exit(f"git ls-files failed:\n{result.stderr}")
    return [path for path in result.stdout.split("\0") if path]


def run_scalafmt(arguments, accepted_exit_codes):
    files = runfiles.Create()
    binary = files.Rlocation(os.environ["SCALAFMT"])
    result = subprocess.run(
        [binary, f"--config={scalafmt_config}", "--non-interactive", "--respect-project-filters", *arguments],
        capture_output=True,
        encoding="utf-8",
        env={**os.environ, **files.EnvVars()},
    )
    if result.returncode not in accepted_exit_codes:
        sys.exit(f"scalafmt failed:\n{result.stdout}{result.stderr}")
    return result.stdout


def display_path(listed):
    path = url2pathname(urlparse(listed).path) if listed.startswith("file:") else listed
    relative = os.path.relpath(path)
    return path if relative.startswith(os.pardir) else relative


def unformatted_files(paths):
    output = run_scalafmt(["--list", *paths], {exit_code_ok, exit_code_unformatted})
    return [display_path(line) for line in output.splitlines() if line]


def print_list(title, paths):
    print(f"{title} ({len(paths)}):")
    for path in paths:
        print(f"  {path}")
    print()


def main():
    if sys.argv[1:2] not in (["check"], ["update"]):
        sys.exit("usage: scalafmt <check|update> [PATH...]")
    check_only = sys.argv[1] == "check"

    os.chdir(workspace_directory())
    paths = sys.argv[2:] or tracked_scala_files()
    if not paths:
        print("No Scala files to check.")
        return 0

    unformatted = unformatted_files(paths)

    if not unformatted:
        print(f"All {len(paths)} checked Scala files are formatted.")
        return 0

    if check_only:
        print(f"scalafmt check failed: {len(unformatted)} of {len(paths)} Scala files are not formatted.")
        print()
        print_list("Not formatted", unformatted)
        print("To fix them, run:")
        print(update_command)
        return 1

    run_scalafmt(unformatted, {exit_code_ok})
    print(f"Reformatted {len(unformatted)} of {len(paths)} Scala files.")
    print()
    print_list("Reformatted", unformatted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
