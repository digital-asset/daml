# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import sys

from python.runfiles import runfiles

java_patterns = ["*.java"]
update_command = "bazelisk run //bazel/checks/google-java-format -- update"
git = os.environ.get("GIT", "git")


def workspace_directory():
    if "BUILD_WORKSPACE_DIRECTORY" in os.environ:
        return os.environ["BUILD_WORKSPACE_DIRECTORY"]
    module_file = runfiles.Create().Rlocation("_main/MODULE.bazel")
    return os.path.dirname(os.path.realpath(module_file))


def tracked_java_files():
    result = subprocess.run(
        [git, "ls-files", "-z", "--", *java_patterns],
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        sys.exit(f"git ls-files failed:\n{result.stderr}")
    return [path for path in result.stdout.split("\0") if path]


def run_google_java_format(arguments):
    files = runfiles.Create()
    binary = files.Rlocation(os.environ["GOOGLE_JAVA_FORMAT"])
    result = subprocess.run(
        [binary, *arguments],
        capture_output=True,
        encoding="utf-8",
        env={**os.environ, **files.EnvVars()},
    )
    if result.returncode != 0:
        sys.exit(f"google-java-format failed:\n{result.stdout}{result.stderr}")
    return result.stdout


def unformatted_files(paths):
    return run_google_java_format(["--dry-run", *paths]).splitlines()


def print_list(title, paths):
    print(f"{title} ({len(paths)}):")
    for path in paths:
        print(f"  {path}")
    print()


def main():
    if sys.argv[1:2] not in (["check"], ["update"]):
        sys.exit("usage: google-java-format <check|update> [PATH...]")
    check_only = sys.argv[1] == "check"

    os.chdir(workspace_directory())
    paths = sys.argv[2:] or tracked_java_files()
    if not paths:
        print("No Java files to check.")
        return 0

    unformatted = unformatted_files(paths)

    if not unformatted:
        print(f"All {len(paths)} checked Java files are formatted.")
        return 0

    if check_only:
        print(f"google-java-format check failed: {len(unformatted)} of {len(paths)} Java files are not formatted.")
        print()
        print_list("Not formatted", unformatted)
        print("To fix them, run:")
        print(update_command)
        return 1

    run_google_java_format(["--replace", *unformatted])
    print(f"Reformatted {len(unformatted)} of {len(paths)} Java files.")
    print()
    print_list("Reformatted", unformatted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
