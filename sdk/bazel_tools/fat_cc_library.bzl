# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain")
load("@os_info//:os_info.bzl", "is_darwin", "is_windows")

def _fat_cc_library_impl(ctx):
    input_lib = ctx.attr.input_lib
    cc_info = input_lib[CcInfo]
    static_libs = []

    # For now we assume that we have static PIC libs for all libs.
    # It should be possible to extend this but we do not have a need
    # for it so far and it would complicate things.
    for input in cc_info.linking_context.linker_inputs.to_list():
        for lib in input.libraries:
            static_lib = None
            if lib.pic_static_library:
                static_lib = lib.pic_static_library
            elif lib.static_library:
                # On Windows and MacOS we don't seem to have `pic_static_library`s available.
                static_lib = lib.static_library
            else:
                fail("No (PIC) static library found for '{}'.".format(str(lib)))
            static_libs += [static_lib]

    dyn_lib = ctx.outputs.dynamic_library
    static_lib = ctx.outputs.static_library

    toolchain = find_cpp_toolchain(ctx)
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )

    compiler = cc_common.get_tool_for_action(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.c_compile,
    )
    ctx.actions.run(
        mnemonic = "CppLinkFatDynLib",
        outputs = [dyn_lib],
        executable = compiler,
        tools = toolchain.all_files.to_list(),
        arguments =
            ["-o", dyn_lib.path, "-shared"] +
            ctx.attr.whole_archive_flag +
            [f.path for f in static_libs] +
            ctx.attr.no_whole_archive_flag +
            # Some libs seems to depend on libstdc++ implicitely
            (["-lc++", "-lc++abi"] if is_darwin else ["-lstdc++"]) +
            (["-framework", "CoreFoundation"] if is_darwin else []) +
            # On Windows we have some extra deps.
            (["-lws2_32"] if is_windows else []),
        inputs = static_libs,
        env = {"PATH": ""},
    )

    archive_list_content = "\n".join([lib.path for lib in static_libs])
    archive_list = ctx.actions.declare_file(ctx.label.name + "_archives")
    ctx.actions.write(archive_list, archive_list_content)

    ar = toolchain.ar_executable

    if ar.find("libtool") >= 0:
        # We are on MacOS where ar_executable is actually libtool, see
        # https://github.com/bazelbuild/bazel/issues/5127.
        ctx.actions.run(
            mnemonic = "CppLinkFatStaticLib",
            outputs = [static_lib],
            executable = ar,
            inputs = static_libs,
            arguments =
                ["-no_warning_for_no_symbols", "-static", "-o", static_lib.path] +
                [f.path for f in static_libs],
        )
    else:
        command = """\
set -euo pipefail
AR=$1
OUT=$2
LIBS=$3

DIR=$(mktemp -d)
trap "rm -rf $DIR" EXIT

OBJS=$DIR/objs.txt
> $OBJS

while IFS= read -r archive; do
  XDIR=$DIR/$(basename $archive .a)
  mkdir -p $XDIR

  $AR x $archive --output $XDIR

  $AR t $archive | while IFS= read -r object; do
    echo $XDIR/$object >> $OBJS
  done
done < $LIBS

$AR rcs $OUT @$OBJS
        """
        ctx.actions.run_shell(
            mnemonic = "CppLinkFatStaticLib",
            outputs = [static_lib],
            inputs = [archive_list] + static_libs,
            command = command,
            arguments = [ar, static_lib.path, archive_list.path],
        )

    fat_lib = cc_common.create_library_to_link(
        actions = ctx.actions,
        feature_configuration = feature_configuration,
        cc_toolchain = toolchain,
        dynamic_library = dyn_lib,
        static_library = static_lib,
    )

    linker_input = cc_common.create_linker_input(
        libraries = depset([fat_lib]),
        owner = ctx.label,
    )

    new_linking_context = cc_common.create_linking_context(
        linker_inputs = depset([linker_input]),
    )
    new_cc_info = CcInfo(
        linking_context = new_linking_context,
        compilation_context = cc_info.compilation_context,
    )
    return [new_cc_info]

# Shared libraries built with Bazel do not declare their dependencies on other libraries properly.
# Instead that dependency is tracked in Bazel internally. This breaks the GHCi linker if
# RTLD_LAZY doesn’t work which happens quite often. To make matters worse, we also cannot use
# linkstatic = True, since the GHCi linker cannot handle some relocations.
# To work around this mess, we create fat libraries that do not have additional dependencies.
# See https://github.com/tweag/rules_haskell/issues/720
fat_cc_library = rule(
    _fat_cc_library_impl,
    attrs = dict({
        "input_lib": attr.label(),
        "_cc_toolchain": attr.label(
            default = Label("@bazel_tools//tools/cpp:current_cc_toolchain"),
        ),
        "whole_archive_flag": attr.string_list(
            # ld on MacOS doesn’t understand --whole-archive
            default = ["-Wl,-all_load"] if is_darwin else ["-Wl,--whole-archive"],
        ),
        "no_whole_archive_flag": attr.string_list(
            default = [] if is_darwin else ["-Wl,--no-whole-archive"],
        ),
    }),
    fragments = ["cpp"],
    outputs = {
        "dynamic_library": "lib%{name}.dll" if is_windows else "lib%{name}.so",
        "static_library": "lib%{name}.a",
    },
    toolchains = ["@bazel_tools//tools/cpp:toolchain_type"],
)
