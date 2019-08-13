#!/usr/bin/env python3
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import argparse
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=str)
    args = parser.parse_args()

    list_contents(args.source)


def list_contents(source_dir):
    for item in os.listdir(source_dir):
        if item in ["BUILD", "BUILD.bazel", "WORKSPACE"]:
            # Skip nested BUILD files as they confuse Bazel.
            continue
        print(item)


if __name__ == "__main__":
    main()

