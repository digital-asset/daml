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
    link_args = ctx.actions.args()
    link_args.add_all(["-o", dyn_lib, "-shared"])
    link_args.add_all(ctx.attr.whole_archive_flag)
    link_args.add_all(static_libs)
    link_args.add_all(ctx.attr.no_whole_archive_flag)
    # Some libs seems to depend on libstdc++ implicitely
    link_args.add_all(["-lc++", "-lc++abi"] if is_darwin else ["-lstdc++"])
    link_args.add_all(["-framework", "CoreFoundation"] if is_darwin else [])
    # On Windows we have some extra deps.
    link_args.add_all(["-lws2_32"] if is_windows else [])
    link_args.use_param_file("@%s")
    ctx.actions.run(
        mnemonic = "CppLinkFatDynLib",
        outputs = [dyn_lib],
        executable = compiler,
        tools = toolchain.all_files.to_list(),
        arguments = [link_args],
        inputs = static_libs,
        env = {"PATH": ""},
    )

    mri_script_content = "\n".join(
        ["create {}".format(static_lib.path)] +
        ["addlib {}".format(lib.path) for lib in static_libs] +
        ["save", "end"],
    ) + "\n"

    mri_script = ctx.actions.declare_file(ctx.label.name + "_mri")
    ctx.actions.write(mri_script, mri_script_content)

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
        # Note [MRI Script Tilde]
        #
        # The MRI script (`ar -M`) started failing with bzlmod enabled because
        # some of the provided archives had `~` characters in their path due to
        # Bazel bzlmod name mangling. The reason is that ar's MRI script format
        # does not support `~` characters in paths, see [lexer
        # specification](https://sourceware.org/git/?p=binutils-gdb.git;a=blob;f=binutils/arlex.l;h=d6508c4f120bbf0656c7ecf486571e21f253dd14;hb=HEAD#l81).
        #
        # The workaround is to replace all library paths with `~` in the file
        # name by copies without `~` in the file name. Symlinks alone are
        # insufficient as the same `~` restriction applies to the resolved
        # path.
        command = """
set -euo pipefail

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

AR="$1"
MRI="$2"
NEW_MRI="$TEMP_DIR/$(basename "$MRI")"

while IFS= read -r line; do
  if [[ "$line" =~ ^[[:space:]]*addlib[[:space:]]+(.*)$ ]]; then
    lib_path="${BASH_REMATCH[1]}"

    if [[ "$lib_path" == *"~"* ]]; then
      # Create copy without tilde, see Note [MRI Script Tilde]
      copy_name=$(basename "$lib_path" | tr '~' '_')
      copy_path=$(mktemp -d -p "$TEMP_DIR")/$copy_name
      cp "$lib_path" "$copy_path"
      echo "addlib $copy_path" >> "$NEW_MRI"
    else
      echo "$line" >> "$NEW_MRI"
    fi
  else
    echo "$line" >> "$NEW_MRI"
  fi
done < "$MRI"

"$AR" -M < "$NEW_MRI"
"""
        ctx.actions.run_shell(
            mnemonic = "CppLinkFatStaticLib",
            outputs = [static_lib],
            inputs = [mri_script] + static_libs,
            command = command,
            arguments = [ar, mri_script.path],
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
