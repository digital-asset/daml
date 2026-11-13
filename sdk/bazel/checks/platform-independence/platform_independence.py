# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import difflib
import os
import sys

from python.runfiles import runfiles

dar_name = "platform-independence.dar"
update_command = "bazelisk run //bazel/checks/platform-independence -- update"


def read_runfile(variable):
    path = runfiles.Create().Rlocation(os.environ[variable])
    with open(path, encoding="utf-8") as f:
        return f.read()


def print_diff(golden, generated, golden_source):
    diff = difflib.unified_diff(
        golden.splitlines(keepends=True),
        generated.splitlines(keepends=True),
        fromfile=f"{golden_source} (golden)",
        tofile=f"{dar_name} (generated)",
    )
    sys.stdout.writelines(diff)
    print()


def write_golden(golden_source, generated):
    if "BUILD_WORKSPACE_DIRECTORY" not in os.environ:
        sys.exit(f"update writes to the source tree; run it as: {update_command}")
    destination = os.path.join(os.environ["BUILD_WORKSPACE_DIRECTORY"], golden_source)
    with open(destination, "w", encoding="utf-8", newline="\n") as f:
        f.write(generated)


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("check", "update"):
        sys.exit("usage: platform-independence <check|update>")
    check_only = sys.argv[1] == "check"

    golden_source = os.environ["GOLDEN_HASH_SOURCE"]
    golden = read_runfile("GOLDEN_HASH")
    generated = read_runfile("GENERATED_HASH")
    file_count = len(generated.splitlines())

    if generated == golden:
        print(f"{dar_name} matches {golden_source} ({file_count} files).")
        return 0

    if check_only:
        print(f"{dar_name} does not match {golden_source}.")
        print()
        print_diff(golden, generated, golden_source)
        print("If the change is intended, run:")
        print(update_command)
        return 1

    write_golden(golden_source, generated)
    print(f"Updated {golden_source} ({file_count} files).")
    print()
    print_diff(golden, generated, golden_source)
    return 0


if __name__ == "__main__":
    sys.exit(main())
