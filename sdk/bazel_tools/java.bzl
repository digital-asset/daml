# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:pom_file.bzl", "pom_file")
load("@os_info//:os_info.bzl", "is_windows")
load("@google_bazel_common//tools/javadoc:javadoc.bzl", "javadoc_library")
load("//bazel_tools:pkg.bzl", "pkg_empty_zip")

def dadew_java_configure(name, dadew_path):
    _ignore = (name, dadew_path)
    fail("dadew_java_configure is removed with dev_env_tool cleanup; WORKSPACE dadew flow is unsupported in this branch.")

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
