# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import difflib


def main():
    file1 = sys.argv[1]
    file2 = sys.argv[2]

    with open(file1) as f:
        content1 = f.read()
    with open(file2) as f:
        content2 = f.read()

    if content1 == content2:
        print("daml-lf-versions.json is up to date.")
        sys.exit(0)

    print("ERROR: daml-lf-versions.json is out of date!")
    print("Run 'bazel run //daml-lf:update-daml-lf-versions' to update it.")
    print()
    print("=== Checked in ===")
    print(content1)
    print("=== Generated (from jar) ===")
    print(content2)
    print("=== Diff ===")
    diff = difflib.unified_diff(
        content1.splitlines(keepends=True),
        content2.splitlines(keepends=True),
        fromfile="checked-in",
        tofile="generated",
    )
    print("".join(diff))
    sys.exit(1)


if __name__ == "__main__":
    main()


