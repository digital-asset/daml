# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _integrity_test_impl(ctx):
    wrapper = ctx.actions.declare_file(ctx.label.name + "_wrapper.sh")
    ctx.actions.write(
        output = wrapper,
        content = """#!/usr/bin/env bash
set -eux
{checker} $(rlocation "$TEST_WORKSPACE/{dump}")
""".format(
            checker = ctx.executable.checker.short_path,
            dump = ctx.file.dump.short_path,
        ),
        is_executable = True,
    )
    runfiles = ctx.runfiles(
        files = [wrapper, ctx.file.dump],
    )
    runfiles = runfiles.merge(ctx.attr.checker[DefaultInfo].default_runfiles)
    return DefaultInfo(
        executable = wrapper,
        files = depset([wrapper]),
        runfiles = runfiles,
    )

integrity_test = rule(
    implementation = _integrity_test_impl,
    test = True,
    executable = True,
    attrs = {
        "checker": attr.label(cfg = "host", executable = True),
        "dump": attr.label(allow_single_file = True),
    },
)
