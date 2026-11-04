# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

HLS_VERSION = "2.4.0.0"

HLS_URL = "https://downloads.haskell.org/~hls/haskell-language-server-{version}/haskell-language-server-{version}-{triple}.tar.xz"

HLS_BINDISTS = {
    ("linux", "amd64"): {
        "triple": "x86_64-linux-deb10",
        "sha256": "ac26c6d8648308247929102ed4a9fccbfb8d6245aa304b741b8317556b3110a9",
    },
    ("linux", "aarch64"): {
        "triple": "aarch64-linux-ubuntu20",
        "sha256": "2f7ef3e1ae1b40438e2d95218139f2e6985b3df15bc133aedab20c4686cfda20",
    },
    ("darwin", "amd64"): {
        "triple": "x86_64-apple-darwin",
        "sha256": "aecb93090ded96d76a11aad3cc4849c5c00faad9491856a020218aa6083409d8",
    },
}

HLS_SOURCE_PLATFORMS = [
    ("darwin", "aarch64"),
]

HLS_SOURCE = {
    "url": "https://github.com/haskell/haskell-language-server/archive/refs/tags/{version}.tar.gz".format(version = HLS_VERSION),
    "sha256": "67bbfae1275aabbfdb26869bc6df91feb58e03427cb76df89f74b864dbb5d57b",
    "strip_prefix": "haskell-language-server-{version}".format(version = HLS_VERSION),
}
