# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

CABAL_INSTALL_VERSION = "3.10.3.0"

_BASE_URL = "https://downloads.haskell.org/~cabal/cabal-install-{v}/cabal-install-{v}".format(
    v = CABAL_INSTALL_VERSION,
)

_PLATFORMS = {
    "linux-x86_64": {
        "url": _BASE_URL + "-x86_64-linux-deb10.tar.xz",
        "sha256": "1d7a7131402295b01f25be5373fde095a404c45f9b5a5508fb7474bb0d3d057a",
    },
    "linux-aarch64": {
        "url": _BASE_URL + "-aarch64-linux-deb10.tar.xz",
        "sha256": "92d341620c60294535f03098bff796ef6de2701de0c4fcba249cde18a2923013",
    },
    "darwin-x86_64": {
        "url": _BASE_URL + "-x86_64-darwin.tar.xz",
        "sha256": "3aed78619b2164dd61eb61afb024073ae2c50f6655fa60fcc1080980693e3220",
    },
    "darwin-aarch64": {
        "url": _BASE_URL + "-aarch64-darwin.tar.xz",
        "sha256": "f4f606b1488a4b24c238f7e09619959eed89c550ed8f8478b350643f652dc08c",
    },
    "windows-x86_64": {
        "url": _BASE_URL + "-x86_64-windows.zip",
        "sha256": "b651ca732998eba5c0e54f4329c147664a7fb3fe3e74eac890c31647ce1e179a",
    },
}

def _cabal_install_impl(repository_ctx):
    os_name = repository_ctx.os.name
    os_arch = repository_ctx.os.arch

    if "linux" in os_name:
        arch = "aarch64" if os_arch == "aarch64" else "x86_64"
        platform_key = "linux-" + arch
    elif "mac" in os_name:
        arch = "aarch64" if os_arch == "aarch64" else "x86_64"
        platform_key = "darwin-" + arch
    elif "windows" in os_name:
        platform_key = "windows-x86_64"
    else:
        fail("Unsupported OS for cabal-install: " + os_name)

    platform = _PLATFORMS[platform_key]
    repository_ctx.download_and_extract(
        url = platform["url"],
        sha256 = platform["sha256"],
    )

    cabal_bin = "cabal.exe" if "windows" in os_name else "cabal"
    repository_ctx.file(
        "BUILD.bazel",
        'exports_files(["{}"], visibility = ["//visibility:public"])\n'.format(cabal_bin),
    )

_cabal_install = repository_rule(
    implementation = _cabal_install_impl,
)

def _impl(module_ctx):
    _cabal_install(name = "cabal")

cabal = module_extension(implementation = _impl)
