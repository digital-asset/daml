# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:pkg.bzl", "pkg_tar")

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
    src_descs = [src[ProtoInfo].direct_descriptor_set for src in ctx.attr.srcs]
    dep_descs = [dep[ProtoInfo].direct_descriptor_set for dep in ctx.attr.deps]
    descriptors = src_descs + dep_descs

    sources_out = ctx.actions.declare_directory(ctx.attr.name + "-sources")

    descriptor_set_delim = "\;" if _is_windows(ctx) else ":"

    args = []
    args += [
        "--descriptor_set_in=" + descriptor_set_delim.join([d.path for d in descriptors]),
    ]
    args += [
        "--{}_out={}:{}".format(ctx.attr.plugin_name, ",".join(ctx.attr.plugin_options), sources_out.path),
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
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers = [ProtoInfo]),
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
