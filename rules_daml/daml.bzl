# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@build_environment//:configuration.bzl", "ghc_version")

daml_provider = provider(doc = "DAML provider", fields = {
    "dalf": "The DAML-LF file.",
    "dar": "The packaged archive.",
})

def _daml_impl_compile_dalf(ctx):
    # Call damlc compile
    compile_args = ctx.actions.args()
    compile_args.add("compile")
    compile_args.add(ctx.file.main_src)
    compile_args.add("--output", ctx.outputs.dalf.path)
    if ctx.attr.target:
        compile_args.add("--target", ctx.attr.target)
    ctx.actions.run(
        inputs = depset([ctx.file.main_src] + ctx.files.srcs),
        outputs = [ctx.outputs.dalf],
        arguments = [compile_args],
        progress_message = "Compiling DAML into DAML-LF archive %s" % ctx.outputs.dalf.short_path,
        executable = ctx.executable.damlc,
    )

def _daml_impl_package_dar(ctx):
    # Call damlc package
    package_args = ctx.actions.args()
    package_args.add("package")
    package_args.add(ctx.file.main_src)
    package_args.add(ctx.attr.name)
    if ctx.attr.target:
        package_args.add("--target", ctx.attr.target)
    package_args.add("--output")
    package_args.add(ctx.outputs.dar.path)
    ctx.actions.run(
        inputs = [ctx.file.main_src] + ctx.files.srcs,
        outputs = [ctx.outputs.dar],
        arguments = [package_args],
        progress_message = "Creating DAR package %s" % ctx.outputs.dar.basename,
        executable = ctx.executable.damlc,
    )

def _daml_compile_impl(ctx):
    _daml_impl_compile_dalf(ctx)
    _daml_impl_package_dar(ctx)

    # DAML provider
    daml = daml_provider(
        dalf = ctx.outputs.dalf,
        dar = ctx.outputs.dar,
    )
    return [daml]

def _daml_compile_outputs_impl(name):
    patterns = {
        "dalf": "{name}.dalf",
        "dar": "{name}.dar",
    }
    return {
        k: v.format(name = name)
        for (k, v) in patterns.items()
    }

daml_compile = rule(
    implementation = _daml_compile_impl,
    attrs = {
        "main_src": attr.label(
            allow_single_file = [".daml"],
            mandatory = True,
            doc = "The main DAML file that will be passed to the compiler.",
        ),
        "srcs": attr.label_list(
            allow_files = [".daml"],
            default = [],
            doc = "Other DAML files that compilation depends on.",
        ),
        "target": attr.string(doc = "DAML-LF version to output"),
        "damlc": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("//compiler/damlc"),
        ),
    },
    executable = False,
    outputs = _daml_compile_outputs_impl,
)

def _daml_test_impl(ctx):
    script = """
      set -eou pipefail

      DAMLC=$(rlocation $TEST_WORKSPACE/{damlc})
      rlocations () {{ for i in $@; do echo $(rlocation $TEST_WORKSPACE/$i); done; }}

      $DAMLC test --files $(rlocations "{files}")
    """.format(
        damlc = ctx.executable.damlc.short_path,
        files = " ".join([f.short_path for f in ctx.files.srcs]),
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )
    damlc_runfiles = ctx.attr.damlc[DefaultInfo].data_runfiles
    runfiles = ctx.runfiles(
        collect_data = True,
        files = ctx.files.srcs,
    ).merge(damlc_runfiles)
    return [DefaultInfo(runfiles = runfiles)]

daml_test = rule(
    implementation = _daml_test_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".daml"],
            default = [],
            doc = "DAML source files to test.",
        ),
        "damlc": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("//compiler/damlc"),
        ),
    },
    test = True,
)

def _daml_doctest_impl(ctx):
    script = """
      set -eou pipefail
      DAMLC=$(rlocation $TEST_WORKSPACE/{damlc})
      CPP=$(rlocation $TEST_WORKSPACE/{cpp})
      rlocations () {{ for i in $@; do echo $(rlocation $TEST_WORKSPACE/$i); done; }}
      $DAMLC doctest {flags} --cpp $CPP --package-name {package_name}-{version} $(rlocations "{files}")
    """.format(
        damlc = ctx.executable.damlc.short_path,
        # we end up with "../hpp/hpp" while we want "external/hpp/hpp"
        # so we just do the replacement ourselves.
        cpp = ctx.executable.cpp.short_path.replace("..", "external"),
        package_name = ctx.attr.package_name,
        flags = " ".join(ctx.attr.flags),
        version = ghc_version,
        files = " ".join([
            f.short_path
            for f in ctx.files.srcs
            if all([not f.short_path.endswith(ignore) for ignore in ctx.attr.ignored_srcs])
        ]),
    )
    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )
    damlc_runfiles = ctx.attr.damlc[DefaultInfo].data_runfiles
    cpp_runfiles = ctx.attr.cpp[DefaultInfo].data_runfiles
    runfiles = ctx.runfiles(
        collect_data = True,
        files = ctx.files.srcs,
    ).merge(damlc_runfiles).merge(cpp_runfiles)
    return [DefaultInfo(runfiles = runfiles)]

daml_doc_test = rule(
    implementation = _daml_doctest_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".daml"],
            default = [],
            doc = "DAML source files that should be tested.",
        ),
        "ignored_srcs": attr.string_list(
            default = [],
            doc = "DAML source files that should be ignored.",
        ),
        "damlc": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("//compiler/damlc"),
        ),
        "cpp": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("@hpp//:hpp"),
        ),
        "flags": attr.string_list(
            default = [],
            doc = "Flags for damlc invokation.",
        ),
        "package_name": attr.string(),
    },
    test = True,
)
