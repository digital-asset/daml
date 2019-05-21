# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:paths.bzl", "paths")

daml_provider = provider(doc = "DAML provider", fields = {
    "dalf": "The DAML-LF file.",
    "dar": "The packaged archive.",
    "srcjar": "The generated Scala sources as srcjar.",
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

def _daml_outputs_impl(name):
    patterns = {
        "dalf": "{name}.dalf",
        "dar": "{name}.dar",
        "srcjar": "{name}.srcjar",
    }
    return {
        k: v.format(name = name)
        for (k, v) in patterns.items()
    }

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

# TODO(JM): The daml_compile() is same as daml(), but without the codegen bits.
# All of this needs a cleanup once we understand the needs for daml related rules.
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
            default = Label("//daml-foundations/daml-tools/da-hs-damlc-app"),
        ),
    },
    executable = False,
    outputs = _daml_compile_outputs_impl,
)

def _daml_test_impl(ctx):
    script = """
      {damlc} test {files}
    """.format(damlc = ctx.executable.damlc.short_path, files = " ".join([f.short_path for f in ctx.files.srcs]))

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )
    runfiles = ctx.runfiles(files = ctx.files.srcs + [ctx.executable.damlc])
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
            default = Label("//daml-foundations/daml-tools/da-hs-damlc-app"),
        ),
    },
    test = True,
)

_daml_binary_script_template = """
#!/usr/bin/env sh
{java} -jar {sandbox} $@ {dar}
"""

def _daml_binary_impl(ctx):
    script = _daml_binary_script_template.format(
        java = ctx.executable._java.short_path,
        sandbox = ctx.file._sandbox.short_path,
        dar = ctx.file.dar.short_path,
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(
        files = [ctx.file.dar, ctx.file._sandbox, ctx.executable._java],
    )

    return [DefaultInfo(runfiles = runfiles)]

daml_binary = rule(
    implementation = _daml_binary_impl,
    attrs = {
        "dar": attr.label(
            allow_single_file = [".dar"],
            mandatory = True,
            doc = "The DAR to execute in the sandbox.",
        ),
        "_sandbox": attr.label(
            cfg = "target",
            allow_single_file = [".jar"],
            default = Label("//ledger/sandbox:sandbox-binary_deploy.jar"),
        ),
        "_java": attr.label(
            executable = True,
            cfg = "target",
            allow_files = True,
            default = Label("@bazel_tools//tools/jdk:java"),
        ),
    },
    executable = True,
)
"""
Executable target that runs the DAML sandbox on the given DAR package.

Example:
  ```
  daml_binary(
      name = "example-exec",
      dar = ":dar-out/com/digitalasset/sample/example/0.1/example-0.1.dar",
  )
  ```

  This target can be executed as follows:

  ```
  $ bazel run //:example-exec
  ```

  Command-line arguments can be passed to the sandbox as follows:

  ```
  $ bazel run //:example-exec -- --help
  ```
"""

def _daml_compile_dalf_output_impl(name):
    return {"dalf": name + ".dalf"}

dalf_compile = rule(
    implementation = _daml_impl_compile_dalf,
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
            default = Label("//daml-foundations/daml-tools/da-hs-damlc-app"),
        ),
    },
    executable = False,
    outputs = _daml_compile_dalf_output_impl,
)
"""
Stripped down version of daml_compile that does not package DALFs into DARs
"""

daml_sandbox_version = "6.0.0"
