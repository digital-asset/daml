# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# This is a mixture of the javadoc_library rule in
# https://github.com/google/bazel-common and the one in
# https://github.com/stackb/rules_proto.

# The main two factors for why we need this are

# 1. We only have a source jar for java_proto_library.
# The rules in bazel-common do not support this.

# 2. We need to get the source jar via the JavaInfo provider.
# The rules in rules_proto do not support this.

# We probably want to clean this up and upstream it at some point.

def _javadoc_library(ctx):
    transitive_deps = []
    for dep in ctx.attr.deps:
        if JavaInfo in dep:
            transitive_deps.append(dep[JavaInfo].transitive_deps)
        elif hasattr(dep, "java"):
            transitive_deps.append(dep.java.transitive_deps)

    classpath = depset([], transitive = transitive_deps).to_list()

    java_home = str(ctx.attr._jdk[java_common.JavaRuntimeInfo].java_home)

    javadoc_command = [
        java_home + "/bin/javadoc",
        # this is an ugly hack to provide all directories ending with `java` as a "source root"
        '-sourcepath $(find . -type d -name "*java" -printf "%P:").',
        " ".join(ctx.attr.root_packages),
        "-use",
        "-subpackages",
        ":".join(ctx.attr.root_packages),
        "-encoding UTF8",
        "-classpath",
        ":".join([jar.path for jar in classpath]),
        "-notimestamp",
        "-d tmp",
        "-Xdoclint:-missing",
        "-quiet",
    ]

    if ctx.attr.doctitle:
        javadoc_command.append('-doctitle "%s"' % ctx.attr.doctitle)

    if ctx.attr.exclude_packages:
        javadoc_command.append("-exclude %s" % ":".join(ctx.attr.exclude_packages))

    for link in ctx.attr.external_javadoc_links:
        javadoc_command.append("-linkoffline {0} {0}".format(link))

    if ctx.attr.bottom_text:
        javadoc_command.append("-bottom '%s'" % ctx.attr.bottom_text)

    jar_command = "%s/bin/jar cf %s -C tmp ." % (java_home, ctx.outputs.jar.path)

    unjar_params = []
    srcs = []
    for src in ctx.attr.srcs:
        # this is for when the provided src is a java_library target
        if JavaInfo in src:
            for jar in src[JavaInfo].source_jars:
                unjar_params.append(jar.path)
                srcs.append(jar)

        elif DefaultInfo in src:
            srcs.extend(src[DefaultInfo].files.to_list())
    unjar_command = "%s/bin/jar xf %s" % (java_home, " ".join(unjar_params))

    ctx.actions.run_shell(
        inputs = srcs + classpath + ctx.files._jdk,
        command = "%s && %s && %s" % (unjar_command, " ".join(javadoc_command), jar_command),
        outputs = [ctx.outputs.jar],
    )

javadoc_library = rule(
    attrs = {
        "srcs": attr.label_list(
            allow_empty = False,
            allow_files = True,
        ),
        "deps": attr.label_list(),
        "doctitle": attr.string(default = ""),
        "root_packages": attr.string_list(),
        "exclude_packages": attr.string_list(),
        "android_api_level": attr.int(default = -1),
        "bottom_text": attr.string(default = ""),
        "external_javadoc_links": attr.string_list(),
        "_jdk": attr.label(
            default = Label("@bazel_tools//tools/jdk:current_java_runtime"),
            providers = [java_common.JavaRuntimeInfo],
        ),
    },
    outputs = {"jar": "%{name}.jar"},
    implementation = _javadoc_library,
)
