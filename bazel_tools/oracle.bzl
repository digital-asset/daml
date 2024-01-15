# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _impl(ctx):
    # Generates an empty BUILD file, because we do not need to build anything.
    ctx.file(
        "BUILD",
        content = """exports_files(["index.bzl"])""",
        executable = False,
    )

    testing = ctx.os.environ.get("DAML_ORACLE_TESTING", default = "false") == "true"

    ctx.file(
        "index.bzl",
        content = """
oracle_testing = {testing}
oracle_tags = ["oracle"] + ([] if oracle_testing else ["manual"])
""".format(
            testing = testing,
        ),
        executable = False,
    )

oracle_configure = repository_rule(
    environ = ["DAML_ORACLE_TESTING"],
    implementation = _impl,
    attrs = {},
)
