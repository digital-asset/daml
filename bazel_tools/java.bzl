# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:pom_file.bzl", "pom_file")
load("@os_info//:os_info.bzl", "is_windows")
load("@com_github_google_bazel_common//tools/javadoc:javadoc.bzl", "javadoc_library")
load("//bazel_tools:pkg.bzl", "pkg_empty_zip")

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
    doc = "Define a `java_runtime` pointing to the JAVA_HOME environment variable.",
    environ = ["JAVA_HOME"],
)

def da_java_library(
        name,
        deps,
        srcs,
        data = [],
        resources = [],
        resource_jars = [],
        resource_strip_prefix = None,
        tags = [],
        visibility = None,
        exports = [],
        **kwargs):
    root_packages = None
    for tag in tags:
        if tag.startswith("javadoc_root_packages="):
            root_packages = tag[len("javadoc_root_packages="):].split(":")

    native.java_library(
        name = name,
        deps = deps,
        srcs = srcs,
        data = data,
        resources = resources,
        resource_jars = resource_jars,
        resource_strip_prefix = resource_strip_prefix,
        tags = tags,
        visibility = visibility,
        exports = exports,
        **kwargs
    )
    pom_file(
        name = name + "_pom",
        tags = tags,
        target = ":" + name,
        visibility = ["//visibility:public"],
    )

    # Disable the building of Javadoc on Windows as the rule fails to
    # find the sources under Windows.
    if root_packages and is_windows == False:
        javadoc_library(
            name = name + "_javadoc",
            deps = deps + [name],
            srcs = srcs,
            root_packages = root_packages,
        )

def da_java_proto_library(name, **kwargs):
    native.java_proto_library(name = name, **kwargs)
    pom_file(
        name = name + "_pom",
        target = ":" + name,
        visibility = ["//visibility:public"],
    )

    # Create empty javadoc JAR for uploading proto jars to Maven Central
    # we don't need to create an empty zip for sources, because java_proto_library
    # creates a sources jar as a side effect automatically
    pkg_empty_zip(
        name = name + "_javadoc",
        out = name + "_javadoc.jar",
    )
