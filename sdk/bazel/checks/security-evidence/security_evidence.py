# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import difflib
import os
import subprocess
import sys

from python.runfiles import runfiles

evidence_file = "security-evidence.md"
evidence_marker = "TEST_EVIDENCE:"
check_directory = "bazel/checks/security-evidence"
update_command = "bazelisk run //bazel/checks/security-evidence -- update"
git = os.environ.get("GIT", "git")


def workspace_directory():
    if "BUILD_WORKSPACE_DIRECTORY" in os.environ:
        return os.environ["BUILD_WORKSPACE_DIRECTORY"]
    module_file = runfiles.Create().Rlocation("_main/MODULE.bazel")
    return os.path.dirname(os.path.realpath(module_file))


def find_evidence_markers():
    result = subprocess.run(
        [git, "grep", "--line-number", "--fixed-strings", evidence_marker,
         "--", ".", f":(exclude){check_directory}"],
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode not in (0, 1):
        sys.exit(f"git grep failed:\n{result.stderr}")
    return result.stdout


def generate_evidence(markers):
    generator = runfiles.Create().Rlocation(os.environ["EVIDENCE_GENERATOR"])
    result = subprocess.run(
        [generator],
        input=markers,
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        sys.exit(result.stderr)
    return result.stdout


def read_current_evidence():
    if not os.path.exists(evidence_file):
        return ""
    with open(evidence_file, encoding="utf-8") as f:
        return f.read()


def print_diff(current, generated):
    diff = difflib.unified_diff(
        current.splitlines(keepends=True),
        generated.splitlines(keepends=True),
        fromfile=f"{evidence_file} (current)",
        tofile=f"{evidence_file} (generated)",
    )
    sys.stdout.writelines(diff)
    print()


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("check", "update"):
        sys.exit("usage: security-evidence <check|update>")
    check_only = sys.argv[1] == "check"

    os.chdir(workspace_directory())
    markers = find_evidence_markers()
    marker_count = len(markers.splitlines())
    generated = generate_evidence(markers)
    current = read_current_evidence()

    if generated == current:
        print(f"{evidence_file} is up to date with {marker_count} TEST_EVIDENCE markers.")
        return 0

    if check_only:
        print(f"{evidence_file} is out of date with {marker_count} TEST_EVIDENCE markers.")
        print()
        print_diff(current, generated)
        print("To fix it, run:")
        print(update_command)
        return 1

    with open(evidence_file, "w", encoding="utf-8") as f:
        f.write(generated)
    print(f"Updated {evidence_file} from {marker_count} TEST_EVIDENCE markers.")
    print()
    print_diff(current, generated)
    return 0


if __name__ == "__main__":
    sys.exit(main())
