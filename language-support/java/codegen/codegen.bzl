# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

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
    src_jar = base_name + "-srcjar"
    lib = base_name + ".jar"

    package_prefix = kwargs.get("package_prefix", "")

    native.genrule(
        name = src_jar,
        srcs = [dar],
        outs = [mangle(src_jar)],
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
        name = lib,
        javacopts = da_java_bindings_javacopts,
        srcs = [
            ":%s" % src_jar,
        ],
        deps = [
            "//language-support/java/bindings:bindings-java",
        ],
        visibility = visibility,
    )

test_exclusions = {
    "1.6": ["src/it/daml/Tests/GenMapTest.daml", "src/it/daml/Tests/NumericTest.daml"],
    "1.7": ["src/it/daml/Tests/GenMapTest.daml"],
    "1.8": ["src/it/daml/Tests/GenMapTest.daml"],
}
