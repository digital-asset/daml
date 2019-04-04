# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

_java_home_runtime_build_template = """
java_runtime(
    name = "{name}",
    java_home = "{java_home}",
    visibility = ["//visibility:public"],
)
"""

def _java_home_runtime_impl(ctx):
    java_home = ctx.os.environ.get("JAVA_HOME", default = "")
    if java_home == "":
        fail("Environment variable JAVA_HOME is empty.")
    build_content = _java_home_runtime_build_template.format(
        name = "javabase",
        java_home = ctx.path(java_home),
    )
    ctx.file("BUILD", content=build_content, executable=False)

java_home_runtime = repository_rule(
    implementation = _java_home_runtime_impl,
)
"""Define a java_runtime pointing to the JAVA_HOME environment variable."""
