# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@io_bazel_rules_scala//scala:scala.bzl",
    "scala_binary",
    "scala_library",
    "scala_macro_library",
    "scala_test",
    "scala_test_suite",
)
load(
    "@io_bazel_rules_scala//jmh:jmh.bzl",
    "scala_benchmark_jmh",
)
load("//bazel_tools:pom_file.bzl", "pom_file")
load("@os_info//:os_info.bzl", "is_windows")

# This file defines common Scala compiler flags and plugins used throughout
# this repository. The initial set of flags is taken from the ledger-client
# project. If you find that additional flags are required for another project,
# consider whether all projects could benefit from these changes. If so, add
# them here.
#
# Use the macros `da_scala_*` defined in this file, instead of the stock rules
# `scala_*` from `rules_scala` in order for these default flags to take effect.

common_scalacopts = [
    # doesn't allow advance features of the language without explict import
    # (higherkinds, implicits)
    "-feature",
    "-target:jvm-1.8",
    "-encoding",
    "UTF-8",
    # more detailed information about type-erasure related warnings
    "-unchecked",
    # warn if using deprecated stuff
    "-deprecation",
    "-Xfuture",
    # better error reporting for pureconfig
    "-Xmacro-settings:materialize-derivations",
    "-Xfatal-warnings",
    # adapted args is a deprecated feature:
    # `def foo(a: (A, B))` can be called with `foo(a, b)`.
    # properly it should be `foo((a,b))`
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    # Warn about implicit conversion between numerical types
    "-Ywarn-numeric-widen",
    # Gives a warning for functions declared as returning Unit, but the body returns a value
    "-Ywarn-value-discard",
    "-Ywarn-unused-import",
    # unfortunately give false warning for the `(a, b) = someTuple`
    # line inside a for comprehension
    # "-Ywarn-unused"
]

plugin_deps = [
    "//3rdparty/jvm/org/wartremover:wartremover",
]

common_plugins = [
    "//external:jar/org/wartremover/wartremover_2_12",
]

plugin_scalacopts = [
    # do not enable wart remover for now, because we need to fix a lot of
    # test code, which didn't have wart remover enabled before
    "-Xplugin-require:wartremover",

    # This lists all wartremover linting passes.
    # The list of enabled ones is pretty arbitrary, please ping Francesco for
    # info
    "-P:wartremover:traverser:org.wartremover.warts.Any",
    "-P:wartremover:traverser:org.wartremover.warts.AnyVal",
    "-P:wartremover:traverser:org.wartremover.warts.ArrayEquals",
    # "-P:wartremover:traverser:org.wartremover.warts.AsInstanceOf",
    # "-P:wartremover:traverser:org.wartremover.warts.DefaultArguments",
    # "-P:wartremover:traverser:org.wartremover.warts.EitherProjectionPartial",
    "-P:wartremover:traverser:org.wartremover.warts.Enumeration",
    # "-P:wartremover:traverser:org.wartremover.warts.Equals",
    "-P:wartremover:traverser:org.wartremover.warts.ExplicitImplicitTypes",
    # "-P:wartremover:traverser:org.wartremover.warts.FinalCaseClass",
    # "-P:wartremover:traverser:org.wartremover.warts.FinalVal",
    # "-P:wartremover:traverser:org.wartremover.warts.ImplicitConversion",
    # "-P:wartremover:traverser:org.wartremover.warts.ImplicitParameter",
    # "-P:wartremover:traverser:org.wartremover.warts.IsInstanceOf",
    "-P:wartremover:traverser:org.wartremover.warts.JavaSerializable",
    "-P:wartremover:traverser:org.wartremover.warts.LeakingSealed",
    # "-P:wartremover:traverser:org.wartremover.warts.MutableDataStructures",
    # "-P:wartremover:traverser:org.wartremover.warts.NonUnitStatements",
    # "-P:wartremover:traverser:org.wartremover.warts.Nothing",
    # "-P:wartremover:traverser:org.wartremover.warts.Null",
    "-P:wartremover:traverser:org.wartremover.warts.Option2Iterable",
    # "-P:wartremover:traverser:org.wartremover.warts.OptionPartial",
    # "-P:wartremover:traverser:org.wartremover.warts.Overloading",
    "-P:wartremover:traverser:org.wartremover.warts.Product",
    # "-P:wartremover:traverser:org.wartremover.warts.PublicInference",
    # "-P:wartremover:traverser:org.wartremover.warts.Recursion",
    "-P:wartremover:traverser:org.wartremover.warts.Return",
    "-P:wartremover:traverser:org.wartremover.warts.Serializable",
    "-P:wartremover:traverser:org.wartremover.warts.StringPlusAny",
    # "-P:wartremover:traverser:org.wartremover.warts.Throw",
    # "-P:wartremover:traverser:org.wartremover.warts.ToString",
    # "-P:wartremover:traverser:org.wartremover.warts.TraversableOps",
    # "-P:wartremover:traverser:org.wartremover.warts.TryPartial",
    # "-P:wartremover:traverser:org.wartremover.warts.Var",
    # "-P:wartremover:traverser:org.wartremover.warts.While",
]

# delete items from lf_scalacopts as they are restored to common_scalacopts and plugin_scalacopts
# # calculate items to delete
# $ python
# ... copypaste ...
# >>> filter(set(common_scalacopts + plugin_scalacopts).__contains__, lf_scalacopts)
# []
# ^ means nothing to remove
lf_scalacopts = [
    "-Ywarn-unused",
]

def _wrap_rule(rule, name = "", scalacopts = [], plugins = [], **kwargs):
    rule(
        name = name,
        scalacopts = common_scalacopts + plugin_scalacopts + scalacopts,
        plugins = common_plugins + plugins,
        **kwargs
    )

def _wrap_rule_no_plugins(rule, scalacopts = [], **kwargs):
    rule(
        scalacopts = common_scalacopts + scalacopts,
        **kwargs
    )

def _strip_path_upto(path, upto):
    idx = path.find(upto)
    if idx >= 0:
        return [path[idx + len(upto):].strip("/")]
    else:
        return []

def _scala_source_jar_impl(ctx):
    zipper_args = [
        "%s=%s" % (new_path, src.path)
        for src in ctx.files.srcs
        for new_path in _strip_path_upto(src.path, ctx.attr.strip_upto)
    ]
    zipper_args_file = ctx.actions.declare_file(
        ctx.label.name + ".zipper_args",
    )

    manifest_file = ctx.actions.declare_file(
        ctx.label.name + "_MANIFEST.MF",
        sibling = zipper_args_file,
    )
    ctx.actions.write(manifest_file, "Manifest-Version: 1.0\n")
    zipper_args += ["META-INF/MANIFEST.MF=" + manifest_file.path + "\n"]

    ctx.actions.write(zipper_args_file, "\n".join(zipper_args))

    ctx.actions.run(
        executable = ctx.executable._zipper,
        inputs = ctx.files.srcs + [manifest_file, zipper_args_file],
        outputs = [ctx.outputs.out],
        arguments = ["c", ctx.outputs.out.path, "@" + zipper_args_file.path],
        mnemonic = "ScalaSourceJar",
    )

scala_source_jar = rule(
    implementation = _scala_source_jar_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),

        # The string to strip up to from source file paths.
        # E.g. "main/scala" strips "foo/src/main/scala/com/daml/Foo.scala"
        # to "com/daml/Foo.scala".
        # Files not matching are not included in the source jar,
        # so you may end up with empty source jars.
        # TODO(JM): Add support for multiple options.
        "strip_upto": attr.string(default = "main/scala"),
        "_zipper": attr.label(
            default = Label("@bazel_tools//tools/zip:zipper"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
    },
    outputs = {
        "out": "%{name}.jar",
    },
)

def _create_scala_source_jar(**kwargs):
    # Try to not create empty source jars. We may still end up
    # with them if the "strip_upto" does not match any of the
    # paths.
    if len(kwargs["srcs"]) > 0:
        scala_source_jar(
            name = kwargs["name"] + "_src",
            srcs = kwargs["srcs"],
        )

def da_scala_library(name, **kwargs):
    """
    Define a Scala library.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_library` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_docs].

    [rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala_library
    """
    _wrap_rule(scala_library, name, **kwargs)
    _create_scala_source_jar(name = name, **kwargs)

    if "tags" in kwargs:
        for tag in kwargs["tags"]:
            if tag.startswith("maven_coordinates="):
                pom_file(
                    name = name + "_pom",
                    target = ":" + name,
                )
                break

def da_scala_macro_library(**kwargs):
    """
    Define a Scala library that contains macros.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_macro_library` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_docs].

    [rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala_library
    """
    _wrap_rule(scala_macro_library, **kwargs)
    _create_scala_source_jar(**kwargs)

def da_scala_binary(name, **kwargs):
    """
    Define a Scala executable.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_binary` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_docs].

    [rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala_binary
    """
    _wrap_rule(scala_binary, name, **kwargs)

    if "tags" in kwargs:
        for tag in kwargs["tags"]:
            if tag.startswith("maven_coordinates="):
                pom_file(
                    name = name + "_pom",
                    target = ":" + name,
                )
                break

def da_scala_test(**kwargs):
    """
    Define a Scala executable that runs the unit tests in the given source files.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_test` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_docs].

    [rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala_test
    """
    _wrap_rule(scala_test, **kwargs)

def da_scala_test_suite(**kwargs):
    """
    Define a Scala test executable for each source file and bundle them into one target.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_test_suite` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_docs].

    [rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala_test_suite
    """
    _wrap_rule(scala_test_suite, use_short_names = is_windows, **kwargs)

# TODO make the jmh rule work with plugins -- probably
# just a matter of passing the flag in
def da_scala_benchmark_jmh(**kwargs):
    _wrap_rule_no_plugins(scala_benchmark_jmh, **kwargs)
