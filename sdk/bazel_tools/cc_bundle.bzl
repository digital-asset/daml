# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain", "use_cpp_toolchain")

def _cc_bundle_impl(ctx):
    cc_info = ctx.attr.lib[CcInfo]
    toolchain = find_cpp_toolchain(ctx)
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )

    static_libs = []
    for input in cc_info.linking_context.linker_inputs.to_list():
        for lib in input.libraries:
            static = lib.pic_static_library or lib.static_library
            if not static:
                fail("No (PIC) static library found for '{}'.".format(lib))
            static_libs.append(static)

    whole_archived = [
        cc_common.create_library_to_link(
            actions = ctx.actions,
            feature_configuration = feature_configuration,
            cc_toolchain = toolchain,
            static_library = lib,
            alwayslink = True,
        )
        for lib in static_libs
    ]
    link_input = cc_common.create_linking_context(
        linker_inputs = depset([cc_common.create_linker_input(
            owner = ctx.label,
            libraries = depset(whole_archived),
        )]),
    )
    link_out = cc_common.link(
        actions = ctx.actions,
        name = ctx.label.name,
        feature_configuration = feature_configuration,
        cc_toolchain = toolchain,
        linking_contexts = [link_input],
        output_type = "dynamic_library",
    )

    return [CcInfo(
        compilation_context = cc_info.compilation_context,
        linking_context = cc_common.create_linking_context(
            linker_inputs = depset([cc_common.create_linker_input(
                owner = ctx.label,
                libraries = depset([link_out.library_to_link]),
            )]),
        ),
    )]

# Merges a cc_library and its transitive static closure into one self-contained
# shared library. Bazel underlinks shared libraries (deps tracked internally,
# not declared), which the GHCi linker cannot resolve; a dependency-free bundle
# sidesteps it. See https://github.com/tweag/rules_haskell/issues/720.
cc_bundle = rule(
    _cc_bundle_impl,
    attrs = {
        "lib": attr.label(mandatory = True, providers = [CcInfo]),
        "_cc_toolchain": attr.label(
            default = Label("@bazel_tools//tools/cpp:current_cc_toolchain"),
        ),
    },
    fragments = ["cpp"],
    toolchains = use_cpp_toolchain(),
)
