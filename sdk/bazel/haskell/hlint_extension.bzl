# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@rules_haskell//haskell:cabal.bzl", "stack_snapshot")
load("//bazel/versions:hlint.version.bzl", "HLINT_STACK_PACKAGES", "HLINT_STACK_SNAPSHOT")

_GMP = str(Label("@gmp//:gmp_cc_lib"))

_SNAPSHOT_JSON = Label("//bazel/haskell:hlint_snapshot.json")

def _pinned_packages(module_ctx):
    module_ctx.watch(_SNAPSHOT_JSON)
    if not module_ctx.path(_SNAPSHOT_JSON).exists:
        return []
    return json.decode(module_ctx.read(_SNAPSHOT_JSON))["resolved"].keys()

def _impl(module_ctx):
    pinned_packages = _pinned_packages(module_ctx)
    stack_snapshot(
        name = "hlint_stackage",
        components = {
            "attoparsec": [
                "lib:attoparsec",
                "lib:attoparsec-internal",
            ],
            "hlint": [
                "lib",
                "exe",
            ],
        },
        components_dependencies = {
            "attoparsec": json.encode({"lib:attoparsec": ["lib:attoparsec-internal"]}),
        },
        extra_deps = {package: [_GMP] for package in pinned_packages},
        haddock = False,
        label_builder = lambda label: Label(label),
        packages = HLINT_STACK_PACKAGES,
        snapshot = HLINT_STACK_SNAPSHOT,
        stack = Label("@stack//:stack"),
        stack_snapshot_json = _SNAPSHOT_JSON,
    )

hlint = module_extension(implementation = _impl)
