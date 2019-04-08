# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def mangle(name):
    return ".".join(name.rsplit("-", 1))

def mangle_for_java(name):
    return name.replace(".", "_")

def dar_to_java(**kwargs):
    base_name = kwargs["name"]

    dar = kwargs["src"]

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
            $(execpath @local_jdk//:bin/jar) -cf $@ -C {gen_out} .
        """.format(
            gen_in = "$(location %s)=%s" % (dar, package_prefix),
            gen_out = src_out
        ),
        tools = [
            "//language-support/java/codegen:codegen",
            "@local_jdk//:bin/jar"
        ],
    )

    native.java_library(
        name = lib,
        srcs = [
            ":%s" % src_jar,
        ],
        deps = [
            "//language-support/java/bindings:bindings-java",
        ],
    )
