# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:java.bzl", "da_java_library")
load("//bazel_tools:javadoc_library.bzl", "javadoc_library")
load("//bazel_tools:pkg.bzl", "pkg_empty_zip")
load("//bazel_tools:pom_file.bzl", "pom_file")
load("//bazel_tools:scala.bzl", "scala_source_jar", "scaladoc_jar")
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@os_info//:os_info.bzl", "is_windows")
load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("@scala_version//:index.bzl", "scala_major_version_suffix")

# taken from rules_proto:
# https://github.com/stackb/rules_proto/blob/f5d6eea6a4528bef3c1d3a44d486b51a214d61c2/compile.bzl#L369-L393
def get_plugin_runfiles(tool, plugin_runfiles):
    """Gather runfiles for a plugin.
    """
    files = []
    if not tool:
        return files

    info = tool[DefaultInfo]
    if not info:
        return files

    if info.files:
        files += info.files.to_list()

    if info.default_runfiles:
        runfiles = info.default_runfiles
        if runfiles.files:
            files += runfiles.files.to_list()

    if info.data_runfiles:
        runfiles = info.data_runfiles
        if runfiles.files:
            files += runfiles.files.to_list()

    if plugin_runfiles:
        for target in plugin_runfiles:
            files += target.files.to_list()

    return files

def _proto_gen_impl(ctx):
    sources_out = ctx.actions.declare_directory(ctx.attr.name + "-sources")

    descriptor_set_delim = "\\;" if _is_windows(ctx) else ":"
    descriptors = [depset for src in ctx.attr.srcs for depset in src[ProtoInfo].transitive_descriptor_sets.to_list()]
    args = [
        "--descriptor_set_in=" + descriptor_set_delim.join([depset.path for depset in descriptors]),
        "--{}_out={}".format(ctx.attr.plugin_name, sources_out.path),
        "--{}_opt={}".format(ctx.attr.plugin_name, ",".join(ctx.attr.plugin_options)),
    ]
    plugins = []
    plugin_runfiles = []
    if ctx.attr.plugin_name not in ["java", "python"]:
        plugins = [ctx.executable.plugin_exec]
        plugin_runfiles = get_plugin_runfiles(ctx.attr.plugin_exec, ctx.attr.plugin_runfiles)
        args += [
            "--plugin=protoc-gen-{}={}".format(ctx.attr.plugin_name, ctx.executable.plugin_exec.path),
        ]

    inputs = []

    for src in ctx.attr.srcs:
        src_root = src[ProtoInfo].proto_source_root
        for direct_source in src[ProtoInfo].direct_sources:
            path = ""

            # in some cases the paths of src_root and direct_source are only partially
            # overlapping. the following for loop finds the maximum overlap of these two paths
            for i in range(len(src_root) + 1):
                if direct_source.path.startswith(src_root[-i:]):
                    path = direct_source.path[i:]
                else:
                    # this noop is needed to make bazel happy
                    noop = ""

            path = direct_source.short_path if not path else path
            path = path[1:] if path.startswith("/") else path

            inputs += [path]

    args += inputs

    posix = ctx.toolchains["@rules_sh//sh/posix:toolchain_type"]
    ctx.actions.run_shell(
        mnemonic = "ProtoGen",
        outputs = [sources_out],
        inputs = descriptors + [ctx.executable.protoc] + plugin_runfiles,
        command = posix.commands["mkdir"] + " -p " + sources_out.path + " && " + ctx.executable.protoc.path + " " + " ".join(args),
        tools = plugins,
    )

    # since we only have the output directory of the protoc compilation,
    # we need to find all the files below sources_out and add them to the zipper args file
    zipper_args_file = ctx.actions.declare_file(ctx.label.name + ".zipper_args")
    ctx.actions.run_shell(
        mnemonic = "CreateZipperArgsFile",
        outputs = [zipper_args_file],
        inputs = [sources_out],
        command = "{find} -L {src_path} -type f | {sed} -E 's#^{src_path}/(.*)$#\\1={src_path}/\\1#' | {sort} > {args_file}".format(
            find = posix.commands["find"],
            sed = posix.commands["sed"],
            sort = posix.commands["sort"],
            src_path = sources_out.path,
            args_file = zipper_args_file.path,
        ),
        progress_message = "zipper_args_file %s" % zipper_args_file.path,
    )

    # Call zipper to create srcjar
    zipper_args = ctx.actions.args()
    zipper_args.add("c")
    zipper_args.add(ctx.outputs.out.path)
    zipper_args.add("@%s" % zipper_args_file.path)
    ctx.actions.run(
        executable = ctx.executable._zipper,
        inputs = [sources_out, zipper_args_file],
        outputs = [ctx.outputs.out],
        arguments = [zipper_args],
        progress_message = "srcjar %s" % ctx.outputs.out.short_path,
    )

proto_gen = rule(
    implementation = _proto_gen_impl,
    attrs = {
        "srcs": attr.label_list(providers = [ProtoInfo]),
        "plugin_name": attr.string(),
        "plugin_exec": attr.label(
            cfg = "host",
            executable = True,
        ),
        "plugin_options": attr.string_list(),
        "plugin_runfiles": attr.label_list(
            default = [],
            allow_files = True,
        ),
        "protoc": attr.label(
            default = Label("@com_google_protobuf//:protoc"),
            cfg = "host",
            allow_files = True,
            executable = True,
        ),
        "_zipper": attr.label(
            default = Label("@bazel_tools//tools/zip:zipper"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
    },
    outputs = {
        "out": "%{name}.srcjar",
    },
    output_to_genfiles = True,
    toolchains = ["@rules_sh//sh/posix:toolchain_type"],
)

def _is_windows(ctx):
    return ctx.configuration.host_path_separator == ";"

def _maven_tags(group, artifact_prefix, artifact_suffix):
    if group and artifact_prefix:
        artifact = artifact_prefix + "-" + artifact_suffix
        return ["maven_coordinates=%s:%s:__VERSION__" % (group, artifact)]
    else:
        return []

def _proto_scala_srcs(name, grpc):
    return [
        ":%s" % name,
        "//bazel_tools/scalapb:scalapb-configuration",
    ] + ([
        "@go_googleapis//google/rpc:code_proto",
        "@go_googleapis//google/rpc:errdetails_proto",
        "@go_googleapis//google/rpc:status_proto",
        "@com_github_grpc_grpc//src/proto/grpc/health/v1:health_proto_descriptor",
    ] if grpc else [])

def _proto_scala_deps(grpc, proto_deps, java_conversions):
    return [
        "@maven//:com_google_api_grpc_proto_google_common_protos",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_thesamet_scalapb_lenses_{}".format(scala_major_version_suffix),
        "@maven//:com_thesamet_scalapb_scalapb_runtime_{}".format(scala_major_version_suffix),
    ] + ([
        "@maven//:com_thesamet_scalapb_scalapb_runtime_grpc_{}".format(scala_major_version_suffix),
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
    ] if grpc else []) + [
        "%s_scala" % label
        for label in proto_deps
    ] + ([
        "@maven//:io_grpc_grpc_services",
    ] if java_conversions else [])

def proto_jars(
        name,
        srcs,
        visibility = None,
        strip_import_prefix = "",
        grpc = False,
        java_conversions = False,
        deps = [],
        proto_deps = [],
        java_deps = [],
        scala_deps = [],
        javadoc_root_packages = [],
        maven_group = None,
        maven_artifact_prefix = None,
        maven_artifact_proto_suffix = "proto",
        maven_artifact_java_suffix = "java-proto",
        maven_artifact_scala_suffix = "scala-proto"):
    # NOTE (MK) An empty string flattens the whole structure which is
    # rarely what you want, see https://github.com/bazelbuild/rules_pkg/issues/82
    tar_strip_prefix = "." if not strip_import_prefix else strip_import_prefix

    # Tarball containing the *.proto files.
    pkg_tar(
        name = "%s_tar" % name,
        srcs = srcs,
        extension = "tar.gz",
        strip_prefix = tar_strip_prefix,
        visibility = [":__subpackages__", "//release:__subpackages__"],
    )

    # JAR and source JAR containing the *.proto files.
    da_java_library(
        name = "%s_jar" % name,
        srcs = None,
        deps = None,
        runtime_deps = ["%s_jar" % label for label in proto_deps],
        resources = srcs,
        resource_strip_prefix = "%s/%s/" % (native.package_name(), strip_import_prefix),
        tags = _maven_tags(maven_group, maven_artifact_prefix, maven_artifact_proto_suffix),
        visibility = visibility,
    )

    # An empty Javadoc JAR for uploading the source proto JAR to Maven Central.
    pkg_empty_zip(
        name = "%s_jar_javadoc" % name,
        out = "%s_jar_javadoc.jar" % name,
    )

    # Compiled protobufs.
    proto_library(
        name = name,
        srcs = srcs,
        strip_import_prefix = strip_import_prefix,
        visibility = visibility,
        deps = deps + proto_deps,
    )

    # JAR and source JAR containing the generated Java bindings.
    native.java_proto_library(
        name = "%s_java" % name,
        tags = _maven_tags(maven_group, maven_artifact_prefix, maven_artifact_java_suffix),
        visibility = visibility,
        deps = [":%s" % name],
    )

    if maven_group and maven_artifact_prefix:
        pom_file(
            name = "%s_java_pom" % name,
            tags = _maven_tags(maven_group, maven_artifact_prefix, maven_artifact_java_suffix),
            target = ":%s_java" % name,
            visibility = visibility,
        )

    if javadoc_root_packages:
        javadoc_library(
            name = "%s_java_javadoc" % name,
            srcs = [":%s_java" % name],
            root_packages = javadoc_root_packages,
            deps = ["@maven//:com_google_protobuf_protobuf_java"],
        ) if not is_windows else None
    else:
        # An empty Javadoc JAR for uploading the compiled proto JAR to Maven Central.
        pkg_empty_zip(
            name = "%s_java_javadoc" % name,
            out = "%s_java_javadoc.jar" % name,
        )

    # JAR containing the generated Scala bindings.
    proto_gen(
        name = "%s_scala_sources" % name,
        srcs = _proto_scala_srcs(name, grpc, java_conversions),
        plugin_exec = "//scala-protoc-plugins/scalapb:protoc-gen-scalapb",
        plugin_name = "scalapb",
        plugin_options = (["grpc"] if grpc else []) + (["java_conversions"] if java_conversions else []),
    )

    all_scala_deps = _proto_scala_deps(grpc, proto_deps, java_conversions)

    scala_library(
        name = "%s_scala" % name,
        srcs = [":%s_scala_sources" % name],
        tags = _maven_tags(maven_group, maven_artifact_prefix, maven_artifact_scala_suffix),
        unused_dependency_checker_mode = "error",
        visibility = visibility,
        deps = all_scala_deps + (["{}_java".format(name)] if java_conversions else []),
        exports = all_scala_deps,
    )

    scala_source_jar(
        name = "%s_scala_src" % name,
        srcs = [":%s_scala_sources" % name],
    )

    scaladoc_jar(
        name = "%s_scala_scaladoc" % name,
        srcs = [":%s_scala_sources" % name],
        tags = ["scaladoc"],
        deps = [],
    ) if is_windows == False else None

    if maven_group and maven_artifact_prefix:
        pom_file(
            name = "%s_scala_pom" % name,
            target = ":%s_scala" % name,
            visibility = visibility,
        )
