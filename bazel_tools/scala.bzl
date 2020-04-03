# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@io_bazel_rules_scala//scala:scala.bzl",
    "scala_binary",
    "scala_library",
    "scala_library_suite",
    "scala_test",
    "scala_test_suite",
)
load(
    "@io_bazel_rules_scala//jmh:jmh.bzl",
    "scala_benchmark_jmh",
)
load("//bazel_tools:pom_file.bzl", "pom_file")
load("@os_info//:os_info.bzl", "is_windows")
load("//bazel_tools:pkg.bzl", "pkg_empty_zip")

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
    # catch missing string interpolators
    "-Xlint:missing-interpolator",
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
    "@maven//:org_wartremover_wartremover_2_12",
]

common_plugins = [
    "@maven//:org_wartremover_wartremover_2_12",
]

plugin_scalacopts = [
    # do not enable wart remover for now, because we need to fix a lot of
    # test code, which didn't have wart remover enabled before
    "-Xplugin-require:wartremover",

    # This lists all wartremover linting passes.
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

default_compile_arguments = {
    "unused_dependency_checker_mode": "error",
}

default_initial_heap_size = "128m"
default_max_heap_size = "1g"
default_scalac_stack_size = "2m"

def _jvm_flags(initial_heap_size, max_heap_size):
    return ["-Xms{}".format(initial_heap_size), "-Xmx{}".format(max_heap_size)]

def _set_compile_jvm_flags(
        arguments,
        initial_heap_size = default_initial_heap_size,
        max_heap_size = default_max_heap_size,
        scalac_stack_size = default_scalac_stack_size):
    jvm_flags = _jvm_flags(initial_heap_size, max_heap_size)
    result = {}
    result.update(arguments)
    result.update({
        "scalac_jvm_flags": arguments.get("scalac_jvm_flags", []) + ["-Xss{}".format(scalac_stack_size)] + jvm_flags,
    })
    return result

def _set_jvm_flags(
        arguments,
        initial_heap_size = default_initial_heap_size,
        max_heap_size = default_max_heap_size,
        scalac_stack_size = default_scalac_stack_size):
    jvm_flags = _jvm_flags(initial_heap_size, max_heap_size)
    result = {}
    result.update(arguments)
    result.update({
        "scalac_jvm_flags": arguments.get("scalac_jvm_flags", []) + ["-Xss{}".format(scalac_stack_size)] + jvm_flags,
        "jvm_flags": arguments.get("jvm_flags", []) + jvm_flags,
    })
    return result

def _wrap_rule(rule, name = "", scalacopts = [], plugins = [], generated_srcs = [], **kwargs):
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
    manifest_file = ctx.actions.declare_file(
        ctx.label.name + "_MANIFEST.MF",
    )
    ctx.actions.write(manifest_file, "Manifest-Version: 1.0\n")

    zipper_args_file = ctx.actions.declare_file(
        ctx.label.name + ".zipper_args",
        sibling = manifest_file,
    )
    tmpsrcdirs = []

    zipper_args = ["META-INF/MANIFEST.MF=" + manifest_file.path]
    for src in ctx.files.srcs:
        # Check for common extensions that indicate that the file is a ZIP archive.
        if src.extension == "srcjar" or src.extension == "jar" or src.extension == "zip":
            tmpsrcdir = ctx.actions.declare_directory("%s_%s_tmpdir" % (ctx.label.name, src.owner.name))
            ctx.actions.run(
                executable = ctx.executable._zipper,
                inputs = [src],
                outputs = [tmpsrcdir],
                arguments = ["x", src.path, "-d", tmpsrcdir.path],
                mnemonic = "ScalaUnpackSourceJar",
            )
            tmpsrcdirs.append(tmpsrcdir)
        else:
            for new_path in _strip_path_upto(src.path, ctx.attr.strip_upto):
                zipper_args.append("%s=%s" % (new_path, src.path))

    posix = ctx.toolchains["@rules_sh//sh/posix:toolchain_type"]
    if len(tmpsrcdirs) > 0:
        tmpsrc_cmds = [
            "({find} -L {tmpsrc_path} -type f | {sed} -E 's#^{tmpsrc_path}/(.*)$#\\1={tmpsrc_path}/\\1#')".format(
                find = posix.commands["find"],
                sed = posix.commands["sed"],
                tmpsrc_path = tmpsrcdir.path,
            )
            for tmpsrcdir in tmpsrcdirs
        ]

        cmd = "(echo -e \"{src_paths}\" && {joined_tmpsrc_cmds}) | {sort} > {args_file}".format(
            src_paths = "\\n".join(zipper_args),
            joined_tmpsrc_cmds = " && ".join(tmpsrc_cmds),
            args_file = zipper_args_file.path,
            sort = posix.commands["sort"],
        )
        inputs = tmpsrcdirs + [manifest_file] + ctx.files.srcs
    else:
        cmd = "echo -e \"{src_paths}\" | {sort} > {args_file}".format(
            src_paths = "\\n".join(zipper_args),
            args_file = zipper_args_file.path,
            sort = posix.commands["sort"],
        )
        inputs = [manifest_file] + ctx.files.srcs

    ctx.actions.run_shell(
        mnemonic = "ScalaFindSourceFiles",
        outputs = [zipper_args_file],
        inputs = inputs,
        command = cmd,
        progress_message = "find_scala_source_files %s" % zipper_args_file.path,
    )

    ctx.actions.run(
        executable = ctx.executable._zipper,
        inputs = inputs + [zipper_args_file],
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
    toolchains = ["@rules_sh//sh/posix:toolchain_type"],
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

def _build_nosrc_jar(ctx):
    # this ensures the file is not empty
    manifest_path = ctx.actions.declare_file("%s_MANIFEST.MF" % ctx.label.name)
    ctx.actions.write(manifest_path, "Manifest-Version: 1.0")
    resources = "META-INF/MANIFEST.MF=%s\n" % manifest_path.path

    zipper_arg_path = ctx.actions.declare_file("%s_zipper_args" % ctx.label.name)
    ctx.actions.write(zipper_arg_path, resources)
    cmd = """
rm -f {jar_output}
{zipper} c {jar_output} @{path}
"""

    cmd = cmd.format(
        path = zipper_arg_path.path,
        jar_output = ctx.outputs.out.path,
        zipper = ctx.executable._zipper.path,
    )

    outs = [ctx.outputs.out]
    inputs = [manifest_path]

    ctx.actions.run_shell(
        inputs = inputs,
        tools = [ctx.executable._zipper, zipper_arg_path],
        outputs = outs,
        command = cmd,
        progress_message = "scala %s" % ctx.label,
        arguments = [],
    )

def _scaladoc_jar_impl(ctx):
    # Detect an actual scala source file rather than a srcjar or other label
    srcFiles = [
        src.path
        for src in ctx.files.srcs
        if src.is_source or src in ctx.files.generated_srcs
    ]

    if srcFiles != []:
        # The following plugin handling is lifted from a private library of 'rules_scala'.
        # https://github.com/bazelbuild/rules_scala/blob/1cffc5fcae1f553a7619b98bf7d6456d65081665/scala/private/rule_impls.bzl#L130
        pluginPaths = []
        for p in ctx.attr.plugins:
            if hasattr(p, "path"):
                pluginPaths.append(p)
            elif hasattr(p, "scala"):
                pluginPaths.extend([j.class_jar for j in p.scala.outputs.jars])
            elif hasattr(p, "java"):
                pluginPaths.extend([j.class_jar for j in p.java.outputs.jars])
                # support http_file pointed at a jar. http_jar uses ijar,
                # which breaks scala macros

            elif hasattr(p, "files"):
                pluginPaths.extend([f for f in p.files.to_list() if "-sources.jar" not in f.basename])

        transitive_deps = [dep[JavaInfo].transitive_deps for dep in ctx.attr.deps]
        classpath = depset([], transitive = transitive_deps).to_list()

        outdir = ctx.actions.declare_directory(ctx.label.name + "_tmpdir")

        args = ctx.actions.args()
        args.add_all(["-d", outdir.path])
        args.add("-classpath")
        args.add_joined(classpath, join_with = ":")
        args.add_joined(pluginPaths, join_with = ",", format_joined = "-Xplugin:%s")
        args.add_all(common_scalacopts)
        args.add_all(ctx.attr.scalacopts)
        args.add_all(srcFiles)

        ctx.actions.run(
            executable = ctx.executable._scaladoc,
            inputs = ctx.files.srcs + classpath + pluginPaths,
            outputs = [outdir],
            arguments = [args],
            mnemonic = "ScaladocGen",
        )

        # since we only have the output directory of the scaladoc generation we need to find
        # all the files below sources_out and add them to the zipper args file
        zipper_args_file = ctx.actions.declare_file(ctx.label.name + ".zipper_args")
        posix = ctx.toolchains["@rules_sh//sh/posix:toolchain_type"]
        ctx.actions.run_shell(
            mnemonic = "ScaladocFindOutputFiles",
            outputs = [zipper_args_file],
            inputs = [outdir],
            command = "{find} -L {src_path} -type f | {sed} -E 's#^{src_path}/(.*)$#\\1={src_path}/\\1#' | {sort} > {args_file}".format(
                find = posix.commands["find"],
                sed = posix.commands["sed"],
                sort = posix.commands["sort"],
                src_path = outdir.path,
                args_file = zipper_args_file.path,
            ),
            progress_message = "find_scaladoc_output_files %s" % zipper_args_file.path,
        )

        ctx.actions.run(
            executable = ctx.executable._zipper,
            inputs = ctx.files.srcs + classpath + [outdir, zipper_args_file],
            outputs = [ctx.outputs.out],
            arguments = ["c", ctx.outputs.out.path, "@" + zipper_args_file.path],
            mnemonic = "ScaladocJar",
        )
    else:
        _build_nosrc_jar(ctx)

scaladoc_jar = rule(
    implementation = _scaladoc_jar_impl,
    attrs = {
        "deps": attr.label_list(),
        "doctitle": attr.string(default = ""),
        "plugins": attr.label_list(default = []),
        "srcs": attr.label_list(allow_files = True),
        # generated source files that should still be included.
        "generated_srcs": attr.label_list(allow_files = True),
        "scalacopts": attr.string_list(),
        "_zipper": attr.label(
            default = Label("@bazel_tools//tools/zip:zipper"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
        "_scaladoc": attr.label(
            default = Label("@scala_nix//:bin/scaladoc"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
    },
    outputs = {
        "out": "%{name}.jar",
    },
    toolchains = ["@rules_sh//sh/posix:toolchain_type"],
)
"""
Generates a Scaladoc jar path/to/target/<name>.jar.

Arguments:
  srcs: source files to process
  deps: targets that contain references to other types referenced in Scaladoc.
  doctitle: title for Scalaadoc's index.html. Typically the name of the library
"""

def _create_scaladoc_jar(**kwargs):
    # Limit execution to Linux and MacOS
    if is_windows == False:
        plugins = []
        if "plugins" in kwargs:
            plugins = kwargs["plugins"]

        scaladoc_jar(
            name = kwargs["name"] + "_scaladoc",
            deps = kwargs.get("deps", []),
            plugins = plugins,
            srcs = kwargs["srcs"],
            scalacopts = kwargs.get("scalacopts", []),
            generated_srcs = kwargs.get("generated_srcs", []),
            tags = ["scaladoc"],
        )

def da_scala_library(name, **kwargs):
    """
    Define a Scala library.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_library` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_library_docs].

    [rules_scala_library_docs]: https://github.com/bazelbuild/rules_scala/blob/master/docs/scala_library.md
    """
    arguments = {}
    arguments.update(default_compile_arguments)
    arguments.update(kwargs)
    arguments = _set_compile_jvm_flags(arguments)
    _wrap_rule(scala_library, name, **arguments)
    _create_scala_source_jar(name = name, **arguments)
    _create_scaladoc_jar(name = name, **arguments)

    if "tags" in arguments:
        for tag in arguments["tags"]:
            if tag.startswith("maven_coordinates="):
                pom_file(
                    name = name + "_pom",
                    target = ":" + name,
                )
                break

def da_scala_library_suite(name, **kwargs):
    """
    Define a suite of Scala libraries as a single target.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_library_suite` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_library_suite_docs].

    [rules_scala_library_suite_docs]: https://github.com/bazelbuild/rules_scala/blob/master/docs/scala_library_suite.md
    """
    arguments = {}
    arguments.update(kwargs)
    arguments = _set_compile_jvm_flags(arguments)
    _wrap_rule(scala_library_suite, name, **arguments)
    _create_scala_source_jar(name = name, **arguments)
    _create_scaladoc_jar(name = name, **arguments)

    if "tags" in arguments:
        for tag in arguments["tags"]:
            if tag.startswith("maven_coordinates="):
                fail("Usage of maven_coordinates in da_scala_library_suite is NOT supported", "tags")
                break

def da_scala_binary(name, initial_heap_size = default_initial_heap_size, max_heap_size = default_max_heap_size, **kwargs):
    """
    Define a Scala executable.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_binary` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_docs].

    [rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala_binary
    """
    arguments = {}
    arguments.update(default_compile_arguments)
    arguments.update(kwargs)
    arguments = _set_jvm_flags(arguments, initial_heap_size = initial_heap_size, max_heap_size = max_heap_size)
    _wrap_rule(scala_binary, name, **arguments)

    if "tags" in arguments:
        for tag in arguments["tags"]:
            if tag.startswith("maven_coordinates="):
                pom_file(
                    name = name + "_pom",
                    target = ":" + name,
                )

                # Create empty Sources JAR for uploading to Maven Central
                pkg_empty_zip(
                    name = name + "_src",
                    out = name + "_src.jar",
                )

                # Create empty javadoc JAR for uploading deploy jars to Maven Central
                pkg_empty_zip(
                    name = name + "_javadoc",
                    out = name + "_javadoc.jar",
                )
                break

def da_scala_test(initial_heap_size = default_initial_heap_size, max_heap_size = default_max_heap_size, **kwargs):
    """
    Define a Scala executable that runs the unit tests in the given source files.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_test` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_docs].

    [rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala_test
    """
    arguments = {}
    arguments.update(default_compile_arguments)
    arguments.update(kwargs)
    arguments = _set_jvm_flags(arguments, initial_heap_size = initial_heap_size, max_heap_size = max_heap_size)
    _wrap_rule(scala_test, **arguments)

def da_scala_test_suite(initial_heap_size = default_initial_heap_size, max_heap_size = default_max_heap_size, **kwargs):
    """
    Define a Scala test executable for each source file and bundle them into one target.

    Applies common Scala options defined in `bazel_tools/scala.bzl`.
    And forwards to `scala_test_suite` from `rules_scala`.
    Refer to the [`rules_scala` documentation][rules_scala_docs].

    [rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala_test_suite
    """
    arguments = {}
    arguments.update(kwargs)
    arguments = _set_jvm_flags(arguments, initial_heap_size = initial_heap_size, max_heap_size = max_heap_size)
    _wrap_rule(scala_test_suite, use_short_names = is_windows, **arguments)

# TODO make the jmh rule work with plugins -- probably
# just a matter of passing the flag in
def da_scala_benchmark_jmh(**kwargs):
    _wrap_rule_no_plugins(scala_benchmark_jmh, **kwargs)
