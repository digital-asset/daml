# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:haskell.bzl", "da_haskell_test")
load("//bazel_tools/sh:sh.bzl", "sh_inline_test")

def damlc_compile_test(
        name,
        srcs,
        main,
        damlc = "//compiler/damlc",
        stack_limit = "",
        heap_limit = "",
        enable_scenarios = False,
        **kwargs):
    stack_opt = "-K" + stack_limit if stack_limit else ""
    heap_opt = "-M" + heap_limit if heap_limit else ""
    sh_inline_test(
        name = name,
        data = [damlc, main] + srcs,
        cmd = """\
DAMLC=$$(canonicalize_rlocation $(rootpath {damlc}))
MAIN=$$(canonicalize_rlocation $(rootpath {main}))

TMP=$$(mktemp -d)
function cleanup() {{
  rm -rf "$$TMP"
}}
trap cleanup EXIT

$$DAMLC compile {scenarios} $$MAIN -o $$TMP/out +RTS -s {stack_opt} {heap_opt}
""".format(
            damlc = damlc,
            main = main,
            stack_opt = stack_opt,
            heap_opt = heap_opt,
            scenarios = "--enable-scenarios=yes" if enable_scenarios else "",
        ),
        **kwargs
    )
