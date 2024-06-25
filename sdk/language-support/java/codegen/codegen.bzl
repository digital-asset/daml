# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:pkg.bzl", "pkg_empty_zip")
load("//bazel_tools:pom_file.bzl", "pom_file")
load("//language-support/java:javaopts.bzl", "da_java_bindings_javacopts")

def mangle(name):
    return ".".join(name.rsplit("-", 1))

def mangle_for_java(name):
    return name.replace(".", "_")

def dar_to_java(**kwargs):
    base_name = kwargs["name"]

    dar = kwargs["src"]
    visibility = kwargs.get("visibility", ["//visibility:private"])

    src_out = base_name + "-srcs"
    src_jar = base_name + "-src.jar"

    package_prefix = kwargs.get("package_prefix", "")

    native.genrule(
        name = src_jar,
        srcs = [dar],
        outs = [mangle(base_name) + ".srcjar"],
        cmd = """
            $(execpath //language-support/java/codegen:codegen) -o {gen_out} -d com.daml.ledger.javaapi.TestDecoder {gen_in}
            $(JAVABASE)/bin/jar -cf $@ -C {gen_out} .
        """.format(
            gen_in = "$(location %s)=%s" % (dar, package_prefix),
            gen_out = src_out,
        ),
        toolchains = ["@bazel_tools//tools/jdk:current_java_runtime"],
        tools = ["//language-support/java/codegen:codegen"],
    )

    native.java_library(
        name = base_name,
        javacopts = da_java_bindings_javacopts,
        srcs = [
            ":%s" % src_jar,
        ],
        deps = [
            "//canton:bindings-java",
        ],
        tags = kwargs.get("tags", []),
        visibility = visibility,
    )

    # Create empty javadoc JAR for uploading proto jars to Maven Central
    # we don't need to create an empty zip for sources, because dar_to_java
    # creates a sources jar as a side effect automatically
    pkg_empty_zip(
        name = base_name + "_javadoc",
        out = base_name + "_javadoc.jar",
    )

    if "tags" in kwargs:
        for tag in kwargs["tags"]:
            if tag.startswith("maven_coordinates="):
                pom_file(
                    name = base_name + "_pom",
                    target = ":" + base_name,
                )
                break

test_exclusions = {
    "2.1": ["src/it/daml/Tests/ContractKeys.daml", "src/it/daml/Tests/SimpleInterface.daml"],
}
