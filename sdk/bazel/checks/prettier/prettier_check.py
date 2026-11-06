# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import sys

from python.runfiles import runfiles

typescript_patterns = ["*.ts", "*.tsx"]
update_command = "bazelisk run //bazel/checks/prettier -- update"
git = os.environ.get("GIT", "git")
exit_code_ok = 0
exit_code_unformatted = 1


def workspace_directory():
    if "BUILD_WORKSPACE_DIRECTORY" in os.environ:
        return os.environ["BUILD_WORKSPACE_DIRECTORY"]
    module_file = runfiles.Create().Rlocation("_main/MODULE.bazel")
    return os.path.dirname(os.path.realpath(module_file))


def tracked_typescript_files():
    result = subprocess.run(
        [git, "ls-files", "-z", "--", *typescript_patterns],
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        sys.exit(f"git ls-files failed:\n{result.stderr}")
    return [path for path in result.stdout.split("\0") if path]


def prettier_environment(files):
    environment = {**os.environ, **files.EnvVars()}
    environment["BAZEL_BINDIR"] = "."
    return environment


def run_prettier(arguments, accepted_exit_codes):
    files = runfiles.Create()
    binary = files.Rlocation(os.environ["PRETTIER"])
    result = subprocess.run(
        [binary, "--no-color", *arguments],
        capture_output=True,
        encoding="utf-8",
        env=prettier_environment(files),
    )
    if result.returncode not in accepted_exit_codes:
        sys.exit(f"prettier failed:\n{result.stdout}{result.stderr}")
    return result.stdout


def unformatted_files(paths):
    output = run_prettier(["--list-different", *paths], {exit_code_ok, exit_code_unformatted})
    return [line for line in output.splitlines() if line]


def print_list(title, paths):
    print(f"{title} ({len(paths)}):")
    for path in paths:
        print(f"  {path}")
    print()


def main():
    if sys.argv[1:2] not in (["check"], ["update"]):
        sys.exit("usage: prettier <check|update> [PATH...]")
    check_only = sys.argv[1] == "check"

    os.chdir(workspace_directory())
    paths = sys.argv[2:] or tracked_typescript_files()
    if not paths:
        print("No TypeScript files to check.")
        return 0

    unformatted = unformatted_files(paths)

    if not unformatted:
        print(f"All {len(paths)} checked TypeScript files are formatted.")
        return 0

    if check_only:
        print(f"prettier check failed: {len(unformatted)} of {len(paths)} TypeScript files are not formatted.")
        print()
        print_list("Not formatted", unformatted)
        print("To fix them, run:")
        print(update_command)
        return 1

    run_prettier(["--write", *unformatted], {exit_code_ok})
    print(f"Reformatted {len(unformatted)} of {len(paths)} TypeScript files.")
    print()
    print_list("Reformatted", unformatted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
