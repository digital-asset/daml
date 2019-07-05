#!/usr/bin/env python3
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import argparse
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=str)
    parser.add_argument("destination", type=str)
    args = parser.parse_args()

    create_symlinks(args.source, args.destination)


def create_symlinks(source_dir, dest_dir):
    print(os.getcwd(), source_dir, dest_dir)
    os.makedirs(dest_dir, exist_ok=True)
    for item in os.listdir(source_dir):
        if item in ["BUILD", "BUILD.bazel", "WORKSPACE"]:
            # Skip nested BUILD files as they confuse Bazel.
            continue
        relpath = os.path.relpath(os.path.join(source_dir, item), dest_dir)
        print("ln -s {} {}".format(relpath, os.path.join(dest_dir, item)))
        os.symlink(relpath, os.path.join(dest_dir, item))


if __name__ == "__main__":
    main()

