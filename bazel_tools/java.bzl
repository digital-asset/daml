# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:pom_file.bzl", "pom_file")
load("@com_github_google_bazel_common//tools/javadoc:javadoc.bzl", "javadoc_library")

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
    ctx.file("BUILD", content = build_content, executable = False)

java_home_runtime = repository_rule(
    implementation = _java_home_runtime_impl,
)
"""Define a java_runtime pointing to the JAVA_HOME environment variable."""

def _wrap_rule(rule, name = "", **kwargs):
    rule(name = name, **kwargs)

def da_java_library(name, deps, srcs, data = [], resources = [], resource_jars = [], resource_strip_prefix = None,
                    tags = [], visibility = None, exports = []):

    root_packages = None
    for tag in tags:
       if tag.startswith("javadoc_root_packages="):
            root_packages = [ tag[len("javadoc_root_packages="):] ]

    native.java_library(
        name = name,
        deps = deps,
        srcs = srcs,
        data = data,
        resources = resources,
        resource_jars = resource_jars,
        resource_strip_prefix = resource_strip_prefix,
        visibility = visibility,
        exports = exports
    )
    if (root_packages):
        javadoc_library(
            name = name + "_javadoc",
            deps = deps,
            srcs = srcs,
            root_packages = root_packages
        )
    pom_file(
        name = name + "_pom",
        tags = tags,
        target = ":" + name,
        visibility = ["//visibility:public"],
    )

def da_java_binary(name, **kwargs):
    _wrap_rule(native.java_binary, name, **kwargs)
    pom_file(
        name = name + "_pom",
        target = ":" + name,
        visibility = ["//visibility:public"],
    )

def da_java_proto_library(name, **kwargs):
    _wrap_rule(native.java_proto_library, name, **kwargs)
    pom_file(
        name = name + "_pom",
        target = ":" + name,
        visibility = ["//visibility:public"],
    )
