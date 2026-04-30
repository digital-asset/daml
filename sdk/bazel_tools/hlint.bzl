# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Highly inspired by https://github.com/tweag/rules_haskell/blob/master/haskell/lint.bzl
# but that code has a bug that it doesn't demand lint outputs recursively
#
# Use:
# haskell_hlint(
#     name = "haskell-app@hlint",
#     deps = ["haskell-app"],
# )

HaskellHLintInfo = provider(
    doc = "Provider that collects files produced by linters",
    fields = {
        "outputs": "List of linter log files.",
    },
)

def _collect_hlint_logs(deps):
    res = []
    for dep in deps:
        if HaskellHLintInfo in dep:
            res.extend(dep[HaskellHLintInfo].outputs)
    return res

def _haskell_hlint_rule_impl(ctx):
    return [DefaultInfo(
        files = depset(_collect_hlint_logs(ctx.attr.deps)),
    )]

def _haskell_hlint_aspect_impl(target, ctx):
    inputFiles = []
    inputPaths = []
    if hasattr(ctx.rule.attr, "srcs"):
        for src in ctx.rule.attr.srcs:
            for f in src.files.to_list():
                # We want to only do native Haskell source files, which
                # seems to involve ignoring these generated paths
                # (the f.is_source almost always returns True)
                if all([
                    f.path.endswith(".hs"),
                    f.path.startswith("external/") == False,
                    f.path.startswith("bazel-out/") == False,
                    f.path.startswith("nix/") == False,
                ]):
                    inputFiles.append(f)
                    inputPaths.append(f.path)

    if len(inputFiles) == 0:
        return []
    output = ctx.actions.declare_file(target.label.name + ".html")
    # `--no-exit-code` keeps HLint from failing the build on every reported
    # idea. Required while the snapshot pins ghc-lib-parser-ex 8.8.5.8 against
    # an LTS-19 (GHC 9.0.2) codebase: the older parser misreads modern
    # syntax (e.g. `let v :: T = e`) as a parse error, and those parse-error
    # ideas carry the GHC error message as their hint *name*, so they can't be
    # suppressed via `ignore:` rules in `.hlint.yaml`. The HTML report is still
    # produced and can be inspected. Revert once HLint / ghc-lib-parser-ex are
    # bumped to versions matching the resolver's GHC.
    args = ["--hint=" + ctx.files._hlint_yaml[0].path] + inputPaths + ["--report=" + output.path] + ["--verbose", "--no-exit-code"]
    # print(args)

    ctx.actions.run(
        inputs = ctx.files._hlint_yaml + inputFiles,
        outputs = [output],
        mnemonic = "HaskellHLint",
        progress_message = "HaskellHLint {}".format(ctx.label),
        executable = ctx.executable._hlint,
        arguments = args,
        # The Bazel sandbox strips env vars (`exec env -`), defaulting the
        # locale to C. Source files contain Unicode (e.g. curly quotes in
        # comments); HLint's GHC runtime then crashes when writing the parse
        # report with `<stdout>: commitBuffer: invalid argument (invalid
        # character)`. Force a UTF-8 locale so output encoding works.
        env = {"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
    )

    outputs = [output]
    for dep in ctx.rule.attr.deps:
        if HaskellHLintInfo in dep:
            outputs.extend(dep[HaskellHLintInfo].outputs)
    lint_info = HaskellHLintInfo(outputs = outputs)
    output_files = OutputGroupInfo(default = outputs)
    return [lint_info, output_files]

haskell_hlint_aspect = aspect(
    _haskell_hlint_aspect_impl,
    attr_aspects = ["deps"],
    attrs = {
        "_hlint": attr.label(
            executable = True,
            cfg = "host",
            allow_single_file = True,
            default = Label("@stackage-exe//hlint"),
        ),
        "_hlint_yaml": attr.label(
            allow_single_file = True,
            default = Label("//:.hlint.yaml"),
        ),
    },
)

haskell_hlint = rule(
    _haskell_hlint_rule_impl,
    attrs = {
        "deps": attr.label_list(
            aspects = [haskell_hlint_aspect],
            doc = "List of Haskell targets to lint.",
        ),
    },
)
