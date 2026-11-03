# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import sys

from python.runfiles import runfiles

lockfile_name = "pnpm-lock.yaml"
lockfiles_resolved_at_test_time = {
    "language-support/js/codegen/tests/ts/pnpm-lock.yaml",
}
update_command = "bazelisk run //bazel/checks/pnpm -- update"
git = os.environ.get("GIT", "git")


def workspace_directory():
    if "BUILD_WORKSPACE_DIRECTORY" in os.environ:
        return os.environ["BUILD_WORKSPACE_DIRECTORY"]
    module_file = runfiles.Create().Rlocation("_main/MODULE.bazel")
    return os.path.dirname(os.path.realpath(module_file))


def list_lockfiles():
    tracked = subprocess.check_output(
        [git, "ls-files", "--", f"*{lockfile_name}"],
        encoding="utf-8",
    ).splitlines()
    return sorted(
        f for f in tracked
        if os.path.basename(f) == lockfile_name
        and f not in lockfiles_resolved_at_test_time
    )


def pnpm_environment():
    environment = dict(os.environ)
    environment.update(runfiles.Create().EnvVars())
    environment["BAZEL_BINDIR"] = "."
    environment["npm_config_update_notifier"] = "false"
    return environment


def run_pnpm(lockfile, check_only):
    pnpm = runfiles.Create().Rlocation(os.environ["PNPM"])
    project_directory = os.path.abspath(os.path.dirname(lockfile))
    frozen = ["--frozen-lockfile"] if check_only else []
    return subprocess.run(
        [pnpm, "--dir", project_directory, "install", "--lockfile-only", *frozen],
        env=pnpm_environment(),
        capture_output=True,
        encoding="utf-8",
    )


def read_file(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def print_list(title, lockfiles):
    print(f"{title} ({len(lockfiles)}):")
    for lockfile in lockfiles:
        print(f"  {lockfile}")
    print()


def print_pnpm_output(failures):
    for lockfile, output in failures:
        print(f"pnpm output for {lockfile}:")
        for line in output.strip().splitlines():
            print(f"  {line}")
        print()


def report(lockfiles, failures, updated, check_only):
    if failures:
        verb = "check" if check_only else "update"
        print(f"pnpm lockfile {verb} failed for {len(failures)} of {len(lockfiles)} lockfiles.")
        print()
        print_list("Not up to date with package.json", [lockfile for lockfile, _ in failures])
        print_pnpm_output(failures)
        if check_only:
            print("To fix them, run:")
            print(update_command)
        return

    if updated:
        print(f"Updated {len(updated)} of {len(lockfiles)} pnpm lockfiles.")
        print()
        print_list("Updated", updated)
        return

    print(f"All {len(lockfiles)} pnpm lockfiles are up to date with their package.json files.")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("check", "update"):
        sys.exit("usage: pnpm <check|update>")
    check_only = sys.argv[1] == "check"

    os.chdir(workspace_directory())
    lockfiles = list_lockfiles()
    failures = []
    updated = []
    for lockfile in lockfiles:
        before = read_file(lockfile)
        result = run_pnpm(lockfile, check_only)
        if result.returncode != 0:
            failures.append((lockfile, result.stdout + result.stderr))
        elif read_file(lockfile) != before:
            updated.append(lockfile)

    report(lockfiles, failures, updated, check_only)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
