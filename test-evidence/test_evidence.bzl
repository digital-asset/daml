# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@io_bazel_rules_scala//scala:advanced_usage/providers.bzl", "ScalaRulePhase")
load("@io_bazel_rules_scala//scala:advanced_usage/scala.bzl", "make_scala_binary")

TestSuiteInfo = provider(
    fields = {
        "tests": "tests the suite runs",
    },
)

def _test_suite_info_aspect_impl(target, ctx):
    if ctx.rule.kind == "test_suite":
        tests = ctx.rule.attr.tests
    else:
        tests = []
    return [TestSuiteInfo(tests = tests)]

_test_suite_info_aspect = aspect(implementation = _test_suite_info_aspect_impl, attr_aspects = ["tests"])

_write_scalatest_runpath_phase = {
    "attrs": {
        "tests": attr.label_list(allow_files = True, aspects = [_test_suite_info_aspect]),
    },
    "phase_providers": [
        "@com_github_digital_asset_daml//test-evidence:phase_write_scalatest_runpath",
    ],
}

def phase_write_scalatest_runpath(ctx, p):
    runfiles_ext = []
    test_jars = []

    for target in ctx.attr.tests:
        if TestSuiteInfo in target:
            for suite_target in target[TestSuiteInfo].tests:
                files = suite_target.files.to_list()
                if files:
                    test_jar = files[0]
                    test_jars.append(test_jar.short_path)
                    runfiles_ext.append(test_jar)
                elif TestSuiteInfo in suite_target:
                    for sub_target in suite_target[TestSuiteInfo].tests:
                        files = sub_target.files.to_list()
                        test_jar = files[0]
                        test_jars.append(test_jar.short_path)
                        runfiles_ext.append(test_jar)

        files = target.files.to_list()

        if files:
            test_jars.append(files[0].short_path)

    runpath_file = ctx.actions.declare_file("%s.runpath" % ctx.label.name)

    runfiles_ext.append(runpath_file)
    runfiles_ext.extend(ctx.files.tests)

    compile_jars = [target[JavaInfo].compile_jars for target in ctx.attr.tests if JavaInfo in target]
    runtime_jars = [target[JavaInfo].transitive_runtime_jars for target in ctx.attr.tests if JavaInfo in target]

    for target in ctx.attr.tests:
        if JavaInfo in target:
            deps = target[JavaInfo].transitive_runtime_jars

    ctx.actions.write(
        output = runpath_file,
        content = "\n".join(test_jars),
    )

    return struct(
        runfiles = depset(runfiles_ext),
        transitive_runtime_jars = depset(ctx.files.tests, transitive = runtime_jars + compile_jars),
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
