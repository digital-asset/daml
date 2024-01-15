# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:pom_file.bzl", "pom_file")
load("@os_info//:os_info.bzl", "is_windows")
load("@com_github_google_bazel_common//tools/javadoc:javadoc.bzl", "javadoc_library")
load("@io_tweag_rules_nixpkgs//nixpkgs:nixpkgs.bzl", "nixpkgs_package")
load("//bazel_tools:pkg.bzl", "pkg_empty_zip")
load("//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dadew_tool_home", "dadew_where")

def _dadew_java_configure_impl(repository_ctx):
    ps = repository_ctx.which("powershell")
    dadew = dadew_where(repository_ctx, ps)
    java_home = dadew_tool_home(dadew, repository_ctx.attr.dadew_path)
    repository_ctx.file("BUILD.bazel", executable = False, content = """
load("@rules_java//java:defs.bzl", "java_runtime")
java_runtime(
    name = "runtime",
    java_home = r"{java_home}",
    visibility = ["//visibility:public"],
)
toolchain(
    name = "toolchain",
    toolchain = ":runtime",
    toolchain_type = "@bazel_tools//tools/jdk:runtime_toolchain_type",
    exec_compatible_with = [
        "@platforms//cpu:x86_64",
        "@platforms//os:windows",
    ],
    target_compatible_with = [
        "@platforms//cpu:x86_64",
        "@platforms//os:windows",
    ],
)
""".format(
        java_home = java_home.replace("\\", "/"),
    ))

_dadew_java_configure = repository_rule(
    implementation = _dadew_java_configure_impl,
    attrs = {
        "dadew_path": attr.string(
            mandatory = True,
            doc = "The installation path of the JDK within dadew.",
        ),
    },
    configure = True,
    local = True,
    doc = """\
Define a Java runtime provided by dadew.

Creates a `java_runtime` that uses the JDK installed by dadew.
""",
)

def dadew_java_configure(name, dadew_path):
    _dadew_java_configure(
        name = name,
        dadew_path = dadew_path,
    )
    native.register_toolchains("@{}//:toolchain".format(name))

def da_java_library(
        name,
        deps,
        srcs,
        data = [],
        resources = [],
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
