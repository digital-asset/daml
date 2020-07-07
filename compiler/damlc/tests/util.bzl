# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:haskell.bzl", "da_haskell_test")

def _damlc_compile_test_impl(ctx):
    stack_opt = "-K" + ctx.attr.stack_limit if ctx.attr.stack_limit else ""
    heap_opt = "-M" + ctx.attr.heap_limit if ctx.attr.heap_limit else ""
    script = """
      set -eou pipefail

      DAMLC=$(rlocation $TEST_WORKSPACE/{damlc})
      MAIN=$(rlocation $TEST_WORKSPACE/{main})

      TMP=$(mktemp -d)
      function cleanup() {{
        rm -rf "$TMP"
      }}
      trap cleanup EXIT

      $DAMLC compile $MAIN -o $TMP/out +RTS -s {stack_opt} {heap_opt}
    """.format(
        damlc = ctx.executable.damlc.short_path,
        main = ctx.files.main[0].short_path,
        stack_opt = stack_opt,
        heap_opt = heap_opt,
    )
    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    # To ensure the files needed by the script are available, we put them in
    # the runfiles.
    runfiles = ctx.runfiles(
        files =
            ctx.files.srcs + ctx.files.main +
            [ctx.executable.damlc],
    )
    return [DefaultInfo(
        runfiles = runfiles,
    )]

damlc_compile_test = rule(
    implementation = _damlc_compile_test_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "main": attr.label(allow_files = True),
        "damlc": attr.label(
            default = Label("//compiler/damlc"),
            executable = True,
            cfg = "target",
            allow_files = True,
        ),
        "stack_limit": attr.string(),
        "heap_limit": attr.string(),
    },
    test = True,
)
