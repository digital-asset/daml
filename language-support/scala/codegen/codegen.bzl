# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def dar_to_scala(**kwargs):
    """
    Generate scala code from provided DAR files.

    This macro does not try to compile the generated classes it just outputs the `srcjar` with generated files.
    """
    name = kwargs["name"]
    dars = kwargs["srcs"]
    package_prefix = kwargs["package_prefix"]
    verbosity = kwargs.get("verbosity", "2")
    srcjar_out = kwargs["srcjar_out"]

    src_out = name + "-srcs"

    cmd = """
        $(execpath //language-support/scala/codegen:codegen-main) --output-directory={gen_out} --verbosity={verbosity} {gen_in}
        $(execpath @bazel_tools//tools/jdk:jar) -cf $@ -C {gen_out} .
    """.format(
        verbosity = verbosity,
        gen_in = dars_with_package_prefix(dars, package_prefix),
        gen_out = src_out,
    )

    native.genrule(
        name = name,
        srcs = dars,
        outs = [srcjar_out],
        cmd = cmd,
        tools = [
            "//language-support/scala/codegen:codegen-main",
            "@bazel_tools//tools/jdk:jar",
        ],
    )

def dar_with_package_prefix(dar, package_prefix):
    return "$(location %s)=%s" % (dar, package_prefix)

def dars_with_package_prefix(dars, package_prefix):
    arr = []
    for d in dars:
        arr.append(dar_with_package_prefix(d, package_prefix))
    return " ".join(arr)
