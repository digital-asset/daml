# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:pom_file.bzl", "pom_file")
load("@os_info//:os_info.bzl", "is_windows")
load("@com_github_google_bazel_common//tools/javadoc:javadoc.bzl", "javadoc_library")
load("@io_tweag_rules_nixpkgs//nixpkgs:nixpkgs.bzl", "nixpkgs_package")
load("//bazel_tools:pkg.bzl", "pkg_empty_zip")
load("//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dadew_tool_home", "dadew_where")

_java_nix_file_content = """
let
  pkgs = import <nixpkgs> { config = {}; overlays = []; };
in

{ attrPath
, attrSet
, filePath
}:

let
  javaHome =
    if attrSet == null then
      pkgs.lib.attrByPath (pkgs.lib.splitString "." attrPath) null pkgs
    else
      pkgs.lib.attrByPath (pkgs.lib.splitString "." attrPath) null attrSet
    ;
  javaHomePath =
    if filePath == "" then
      "${javaHome}"
    else
      "${javaHome}/${filePath}"
    ;
in

assert javaHome != null;

pkgs.runCommand "bazel-nixpkgs-java-runtime"
  { executable = false;
    # Pointless to do this on a remote machine.
    preferLocalBuild = true;
    allowSubstitutes = false;
  }
  ''
    n=$out/BUILD.bazel
    mkdir -p "$(dirname "$n")"

    cat >>$n <<EOF
    load("@rules_java//java:defs.bzl", "java_runtime")
    java_runtime(
        name = "runtime",
        java_home = r"${javaHomePath}",
        visibility = ["//visibility:public"],
    )
    EOF
  ''
"""

def nixpkgs_java_configure(
        name = "nixpkgs_java_runtime",
        attribute_path = None,
        java_home_path = "",
        repository = None,
        repositories = {},
        nix_file = None,
        nix_file_content = "",
        nix_file_deps = None,
        nixopts = [],
        fail_not_supported = True,
        quiet = False):
    """Define a Java runtime provided by nixpkgs.

    Creates a `nixpkgs_package` for a `java_runtime` instance.

    Args:
      name: The name-prefix for the created external repositories.
      attribute_path: string, The nixpkgs attribute path for `jdk.home`.
      java_home_path: optional, string, The path to `JAVA_HOME` within the package.
      repository: See [`nixpkgs_package`](#nixpkgs_package-repository).
      repositories: See [`nixpkgs_package`](#nixpkgs_package-repositories).
      nix_file: optional, Label, Obtain the runtime from the Nix expression defined in this file. Specify only one of `nix_file` or `nix_file_content`.
      nix_file_content: optional, string, Obtain the runtime from the given Nix expression. Specify only one of `nix_file` or `nix_file_content`.
      nix_file_deps: See [`nixpkgs_package`](#nixpkgs_package-nix_file_deps).
      nixopts: See [`nixpkgs_package`](#nixpkgs_package-nixopts).
      fail_not_supported: See [`nixpkgs_package`](#nixpkgs_package-fail_not_supported).
      quiet: See [`nixpkgs_package`](#nixpkgs_package-quiet).
    """
    if attribute_path == None:
        fail("'attribute_path' is required.", "attribute_path")

    nix_expr = None
    if nix_file and nix_file_content:
        fail("Cannot specify both 'nix_file' and 'nix_file_content'.")
    elif nix_file:
        nix_expr = "import $(location {}) {{}}".format(nix_file)
        nix_file_deps = depset(direct = [nix_file] + nix_file_deps).to_list()
    elif nix_file_content:
        nix_expr = nix_file_content
    else:
        nix_expr = "null"

    nixopts = list(nixopts)
    nixopts.extend([
        "--argstr",
        "attrPath",
        attribute_path,
        "--arg",
        "attrSet",
        nix_expr,
        "--argstr",
        "filePath",
        java_home_path,
    ])

    kwargs = dict(
        repository = repository,
        repositories = repositories,
        nix_file_deps = nix_file_deps,
        nixopts = nixopts,
        fail_not_supported = fail_not_supported,
        quiet = quiet,
    )
    java_runtime = "@%s//:runtime" % name
    nixpkgs_package(
        name = name,
        nix_file_content = _java_nix_file_content,
        **kwargs
    )

def _dadew_java_configure_impl(repository_ctx):
    ps = repository_ctx.which("powershell")
    dadew = dadew_where(repository_ctx, ps)
    java_home = dadew_tool_home(dadew, repository_ctx.attr.dadew_path)
    repository_ctx.file("BUILD.bazel", executable = False, content = """
load("@rules_java//java:defs.bzl", "java_runtime")
java_runtime(
    name = "runtime",
    java_home = r"{java_home}",
    visibility = ["//visibility:public"],
)
""".format(
        java_home = java_home.replace("\\", "/"),
    ))

dadew_java_configure = repository_rule(
    implementation = _dadew_java_configure_impl,
    attrs = {
        "dadew_path": attr.string(
            mandatory = True,
            doc = "The installation path of the JDK within dadew.",
        ),
    },
    configure = True,
    local = True,
    doc = """\
Define a Java runtime provided by dadew.

Creates a `java_runtime` that uses the JDK installed by dadew.
""",
)

def da_java_library(
        name,
        deps,
        srcs,
        data = [],
        resources = [],
        resource_jars = [],
        resource_strip_prefix = None,
        tags = [],
        visibility = None,
        exports = [],
        **kwargs):
    root_packages = None
    for tag in tags:
        if tag.startswith("javadoc_root_packages="):
            root_packages = tag[len("javadoc_root_packages="):].split(":")

    native.java_library(
        name = name,
        deps = deps,
        srcs = srcs,
        data = data,
        resources = resources,
        resource_jars = resource_jars,
        resource_strip_prefix = resource_strip_prefix,
        tags = tags,
        visibility = visibility,
        exports = exports,
        **kwargs
    )
    pom_file(
        name = name + "_pom",
        tags = tags,
        target = ":" + name,
        visibility = ["//visibility:public"],
    )

    # Disable the building of Javadoc on Windows as the rule fails to
    # find the sources under Windows.
    if root_packages and is_windows == False:
        javadoc_library(
            name = name + "_javadoc",
            deps = deps + [name],
            srcs = srcs,
            root_packages = root_packages,
        )

def da_java_proto_library(name, **kwargs):
    native.java_proto_library(name = name, **kwargs)
    pom_file(
        name = name + "_pom",
        target = ":" + name,
        visibility = ["//visibility:public"],
    )

    # Create empty javadoc JAR for uploading proto jars to Maven Central
    # we don't need to create an empty zip for sources, because java_proto_library
    # creates a sources jar as a side effect automatically
    pkg_empty_zip(
        name = name + "_javadoc",
        out = name + "_javadoc.jar",
    )
