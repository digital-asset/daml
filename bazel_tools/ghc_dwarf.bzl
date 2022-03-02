# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# This repository rule exists so that we can switch the GHC attribute
# name in `WORKSPACE` as we cannot use `select` theer.

_ghc_dwarf_bzl_template = """
enable_ghc_dwarf = {GHC_DWARF}
"""

def _ghc_dwarf_impl(repository_ctx):
    enable_dwarf = repository_ctx.os.environ.get("GHC_DWARF", "") != ""
    substitutions = {
        "GHC_DWARF": enable_dwarf,
    }
    repository_ctx.file(
        "ghc_dwarf.bzl",
        _ghc_dwarf_bzl_template.format(**substitutions),
        False,
    )
    repository_ctx.file(
        "BUILD",
        "",
        False,
    )

ghc_dwarf = repository_rule(
    implementation = _ghc_dwarf_impl,
    local = True,
)
