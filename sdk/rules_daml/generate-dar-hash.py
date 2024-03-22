# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from argparse import ArgumentParser
from zipfile import ZipFile
from pathlib import PurePath
from hashlib import sha256


def parse_args():
    parser = ArgumentParser(description="generate-dar-hash")
    parser.add_argument('dar', metavar='DAR', type=str)
    return parser.parse_args()


def entry(filename, bytes):
    extension = PurePath(filename).suffix
    if extension in ['.hi', '.hie']:
        # hie/hi files may differ, so we ignore their contents.
        hash = 64 * '0'
    else:
        if extension in ['.daml']:
            # on text files, newlines will be encoded differently depending
            # on platform, so we canonicalize them here
            bytes = bytes.replace(b'\r\n', b'\n')
        hash = sha256(bytes).hexdigest()
    return f"{hash}  {filename}"


def main():
    args = parse_args()
    with ZipFile(args.dar) as dar:
        for filename in dar.namelist():
            print(entry(filename, dar.read(filename)))


if __name__ == '__main__':
    main()
