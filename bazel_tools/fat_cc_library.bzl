# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_darwin")

def _fat_cc_library_impl(ctx):
    input_lib = ctx.attr.input_lib
    cc_info = input_lib[CcInfo]
    pic_static_libs = []
    # For now we assume that we have static PIC libs for all libs.
    # It should be possible to extend this but we do not have a need
    # for it so far and it would complicate things.
    for lib in cc_info.linking_context.libraries_to_link:
        if not lib.pic_static_library:
            fail("No PIC static lib for: " + lib)
        pic_static_libs += [lib.pic_static_library]

    dyn_lib = ctx.outputs.dynamic_library
    static_lib = ctx.outputs.static_library

    toolchain = ctx.attr._cc_toolchain[cc_common.CcToolchainInfo]
    feature_configuration = cc_common.configure_features(cc_toolchain = toolchain)

    ctx.actions.run(
        mnemonic = "CppLinkFatDynLib",
        outputs = [dyn_lib],
        # toolchain.compiler_executable() fails on MacOS, see https://github.com/bazelbuild/bazel/issues/7105
        executable = ctx.executable.cc_compiler,
        arguments =
            ["-o", dyn_lib.path, "-shared"] +
            # Some libs seems to depend on libstdc++ implicitely
            ["-lstdc++"] +
            ctx.attr.whole_archive_flag +
            [f.path for f in pic_static_libs] +
            ctx.attr.no_whole_archive_flag,
        inputs = pic_static_libs,
    )

    mri_script_content = "\n".join(
        ["create {}".format(static_lib.path)] +
        ["addlib {}".format(lib.path) for lib in pic_static_libs] +
        ["save", "end"]
    ) + "\n"

    mri_script = ctx.actions.declare_file(ctx.label.name + "_mri")
    ctx.actions.write(mri_script, mri_script_content)

    ar = toolchain.ar_executable()

    if ar.find("libtool") >= 0:
        # We are on MacOS where ar_executable is actually libtool, see
        # https://github.com/bazelbuild/bazel/issues/5127.
        ctx.actions.run(
            mnemonic = "CppLinkFatStaticLib",
            outputs = [static_lib],
            executable = ar,
            inputs = pic_static_libs,
            arguments =
                ["-no_warning_for_no_symbols", "-static", "-o", static_lib.path] +
                [f.path for f in pic_static_libs]
        )
    else:
        ctx.actions.run_shell(
            mnemonic = "CppLinkFatStaticLib",
            outputs = [static_lib],
            inputs = [mri_script] + pic_static_libs,
            command = "{ar} -M < {mri_script}".format(ar = ar, mri_script = mri_script.path),
        )

    fat_lib = cc_common.create_library_to_link(
        actions = ctx.actions,
        feature_configuration = feature_configuration,
        cc_toolchain = toolchain,
        dynamic_library = dyn_lib,
        static_library = static_lib,
    )

    new_linking_context = cc_common.create_linking_context(
        libraries_to_link = [fat_lib],
    )
    new_cc_info = CcInfo(
        linking_context = new_linking_context,
        compilation_context = cc_info.compilation_context,
    )
    return struct(
        # cc is a legacy provider so it needs to be handled differently.
        # Hopefully, rules_haskell will stop depending onit at somepoint and
        # we can stop providing both cc and CcInfo.
        cc = input_lib.cc,
        providers = [new_cc_info],
    )

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
        "cc_compiler": attr.label(
            allow_files = True,
            executable =True,
            cfg = "host",
            default =
                # bin/cc is gcc on Darwin which fails to find libc++
                Label("@nixpkgs_cc_toolchain//:bin/clang")
                if is_darwin else Label("@nixpkgs_cc_toolchain//:bin/cc"),
        ),
        "whole_archive_flag": attr.string_list(
            # ld on MacOS doesn’t understand --whole-archive
            default = ["-Wl,-all_load"] if is_darwin else ["-Wl,--whole-archive"],
        ),
        "no_whole_archive_flag": attr.string_list(
            default = [] if is_darwin else ["-Wl,--no-whole-archive"],
        ),
    }),
    outputs = {
        "dynamic_library": "lib%{name}.so",
        "static_library": "lib%{name}.a",
    },
)
