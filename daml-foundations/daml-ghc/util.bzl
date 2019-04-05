# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load ("//bazel_tools:haskell.bzl", "da_haskell_test")

def _daml_ghc_compile_test_impl(ctx):
    stack_opt = "-K" + ctx.attr.stack_limit if ctx.attr.stack_limit else ""
    heap_opt = "-M" + ctx.attr.heap_limit if ctx.attr.heap_limit else ""
    script = """
      {damlc} compile {main} -o /dev/null +RTS {stack_opt} {heap_opt}
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
    runfiles = ctx.runfiles(files =
        ctx.files.srcs + ctx.files.main
      + [ctx.executable.damlc]
    )
    return [DefaultInfo(
      runfiles = runfiles
    )]

daml_ghc_compile_test = rule(
  implementation = _daml_ghc_compile_test_impl,
  attrs = {
    "srcs": attr.label_list(allow_files = True),
    "main": attr.label(allow_files = True),
    "damlc": attr.label(
      default = Label("//daml-foundations/daml-tools/da-hs-damlc-app:da-hs-damlc-app"),
      executable = True,
      cfg = "target",
      allow_files = True
    ),
    "stack_limit": attr.string(),
    "heap_limit": attr.string(),
  },
  test = True,
)

def daml_ghc_integration_test(name, main_function):
    da_haskell_test(
        name = name,
        srcs = ["src/DA/Test/GHC.hs"],
        src_strip_prefix = "src",
        main_function = main_function,
        data = [
        "//daml-foundations/daml-ghc/package-database:package-db"
        , "//compiler/scenario-service/server:scenario_service_jar"
        , "@jq//:bin"
        , ":tests"
        , ":bond-trading"
        ],
        deps = [
          ":daml-ghc-lib"
        , "//compiler/daml-lf-ast"
        , "//daml-lf/archive:daml_lf_haskell_proto"
        , "//libs-haskell/da-hs-pretty"
        , "//libs-haskell/da-hs-base"
        , "//libs-haskell/prettyprinter-syntax"
        , "//compiler/haskell-ide-core"
        , "//libs-haskell/da-hs-language-server"
        ],
        hazel_deps = [
          "aeson",
          "base",
          "bytestring",
          "containers",
          "deepseq",
          "directory",
          "dlist",
          "extra",
          "filepath",
          "ghc-lib",
          "ghc-lib-parser",
          "lens",
          "lens-aeson",
          "managed",
          "optparse-applicative",
          "process",
          "shake",
          "tagged",
          "tasty",
          "tasty-hunit",
          "text",
          "time",
        ],
        visibility = ["//visibility:public"],
        # TODO fix flakiness, see #990
        flaky = True,
    )

