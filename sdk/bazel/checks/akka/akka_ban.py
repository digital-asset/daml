# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sys

from python.runfiles import runfiles

banned_name = "akka"


def path_in_repository(rlocation_path):
    return rlocation_path.split("/", 1)[1]


def find_references(rlocation_path):
    path = runfiles.Create().Rlocation(rlocation_path)
    name = path_in_repository(rlocation_path)
    with open(path, encoding="utf-8") as f:
        return [
            f"{name}:{line_number}: {line.strip()}"
            for line_number, line in enumerate(f, start=1)
            if banned_name in line
        ]


def main():
    if sys.argv[1:] != ["check"]:
        sys.exit("usage: akka check")

    lockfiles = os.environ["MAVEN_LOCKFILES"].split()
    names = ", ".join(path_in_repository(lockfile) for lockfile in lockfiles)
    references = [reference for lockfile in lockfiles for reference in find_references(lockfile)]

    if not references:
        print(f"No akka references in {names}.")
        return 0

    print(f"Akka check failed: {len(references)} akka references in {names}.")
    print()
    print(f"Akka references ({len(references)}):")
    for reference in references:
        print(f"  {reference}")
    print()
    print("Akka 2.7+ is not open source (BSL 1.1); depend on Apache Pekko (org.apache.pekko) instead.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
