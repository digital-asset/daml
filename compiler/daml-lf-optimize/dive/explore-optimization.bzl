# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//rules_daml:daml.bzl", "daml_compile")

def _inspect_dar(base):
    name = base + "-inspect"
    dar = base + ".dar"
    pp = "xx-" + base + ".pp"  # prefix with xx so these list together at the end
    native.genrule(
        name = name,
        srcs = [
            dar,
            "//compiler/damlc",
        ],
        outs = [pp],
        cmd = "$(location //compiler/damlc) inspect $(location :" + dar + ") > $@",
    )

def daml_explore_optimization(example):
    daml = "daml/" + example + ".daml"
    original = example + "-A"
    optimized = example + "-B"

    daml_compile(
        name = original,
        srcs = [daml],
    )
    daml_compile(
        name = optimized,
        srcs = [daml],
        run_optimizer = True,
    )

    _inspect_dar(original)
    _inspect_dar(optimized)
