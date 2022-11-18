# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@io_bazel_rules_scala//scala:advanced_usage/providers.bzl", "ScalaRulePhase")
load("@io_bazel_rules_scala//scala:advanced_usage/scala.bzl", "make_scala_binary")

_write_scalatest_runpath_phase = {
    "attrs": {
        "tests": attr.label_list(allow_files = True),
    },
    "phase_providers": [
        "@com_github_digital_asset_daml//test-evidence:phase_write_scalatest_runpath",
    ],
}

def phase_write_scalatest_runpath(ctx, p):
    test_jars = [jar.short_path for jar in ctx.files.tests]

    runpath_file = ctx.actions.declare_file("%s.runpath" % ctx.label.name)

    ctx.actions.write(
        output = runpath_file,
        content = "\n".join(test_jars),
    )

    runfiles_ext = [runpath_file]

    return struct(
        runfiles = depset(runfiles_ext),
    )

test_evidence_binary = make_scala_binary(_write_scalatest_runpath_phase)

def _write_scalatest_runpath_impl(ctx):
    return [
        ScalaRulePhase(
            custom_phases = [
                ("first", "", "write_scalatest_runpath_phase", phase_write_scalatest_runpath),
            ],
        ),
    ]

write_scalatest_runpath = rule(
    implementation = _write_scalatest_runpath_impl,
)
