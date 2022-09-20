# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# A Daml package database contains a subdirectory for each Daml-LF
# version.  Each subdirectory contains a regular GHC package database
# and the DALF files. Note
# that the GHC package database also needs to be able to depend on the
# Daml-LF version since we want to make things in Daml conditional on
# the target version.

# We have the following rules:
#
# daml-package:
#   Inputs:
#   - A Daml source directory
#   - The root Daml file
#   - The target LF version
#   - A package database in the format described above.
#   Outputs:
#   - A DALF file for the target LF version
#   - A directory containing the interface files (and potentially source files)
#   - The package config file
#   - A way of querying the target LF version, e.g., by making it a provider.
#
# daml-package-db:
#   Inputs:
#   - A list of targets produced by daml-compile
#   Outputs:
#   - A directory containing the combined package database
load("@build_environment//:configuration.bzl", "ghc_version")

PACKAGE_CONF_TEMPLATE = """
name: {name}
version: __SDK_VERSION__
id: __ID__
copyright: 2020 Digital Asset Holdings
maintainer: Digital Asset
exposed: True
exposed-modules: {modules}
import-dirs: \\$topdir/__ID__
library-dirs: \\$topdir/__ID__
data-dir: \\$topdir/__ID__
depends: {depends}
"""

DamlPackage = provider(fields = ["daml_lf_version", "pkg_name", "pkg_name_version", "pkg_conf", "iface_dir", "dalf", "modules"])

# Compile a Daml file and create the GHC package database
# for it.
def _daml_package_rule_impl(ctx):
    name = ctx.attr.name
    pkg_name_version = ctx.actions.declare_file("".join([name, "_pkg_name_version"]))
    ctx.actions.run_shell(
        outputs = [pkg_name_version],
        command = """
        if [ "daml-prim" = {pkg_name} ]; then
          echo {pkg_name} > {pkg_name_version_file}
        else
          echo {pkg_name}-{version} > {pkg_name_version_file}
        fi
      """.format(
            pkg_name = ctx.attr.pkg_name,
            pkg_name_version_file = pkg_name_version.path,
            version = ghc_version,
        ),
    )
    dalf = ctx.actions.declare_file("{}.dalf".format(name))
    iface_dir = ctx.actions.declare_directory("{}_iface".format(name))
    package_config = ctx.actions.declare_file("{}.conf".format(name))

    # Construct mapping from module names to paths
    modules = {}
    for file in ctx.files.srcs:
        # FIXME(JM): HACK: the `[3:]` assumes we're in
        # compiler/damlc! Find a way to get the
        # base path...
        modules[".".join(file.path[:-5].split("/")[3:])] = file.path

    # Create the package conf file
    ctx.actions.run_shell(
        outputs = [package_config],
        inputs = [pkg_name_version],
        command = """
        echo "{content}" > {package_config}
        sed -i s/__ID__/`cat {pkg_name_version_file}`/ {package_config}
        sed -i s/__SDK_VERSION__/{version}/ {package_config}
          """.format(
            package_config = package_config.path,
            pkg_name_version_file = pkg_name_version.path,
            version = ghc_version,
            content = PACKAGE_CONF_TEMPLATE.format(
                name = ctx.attr.pkg_name,
                modules = " ".join(modules.keys()),
                depends = " ".join([dep[DamlPackage].pkg_name for dep in ctx.attr.dependencies]),
            ),
        ),
    )

    package_db_dir = ctx.attr.package_db[PackageDb].db_dir

    ctx.actions.run_shell(
        outputs = [dalf, iface_dir],
        inputs = ctx.files.srcs + [package_db_dir, pkg_name_version],
        tools = [ctx.executable.damlc_bootstrap, ctx.executable.cpp],
        progress_message = "Compiling " + name + ".daml to daml-lf " + ctx.attr.daml_lf_version,
        command = """
      set -eou pipefail
      PKG_NAME=`cat {pkg_name_version_file}`

      # We use a temp directory here to avoid issues due to the lack of sandboxing
      # on Windows.
      IFACE_DIR=$(mktemp -d)

      # Compile the dalf file
      {damlc_bootstrap} compile \
        --package-name $PKG_NAME \
        --package-db {package_db_dir} \
        --write-iface \
        --iface-dir $IFACE_DIR \
        --target {daml_lf_version} \
        --cpp {cpp} \
        --ghc-option=-Werror \
        --disable-warn-large-tuples=yes \
        -o {dalf_file} \
        {main}

      cp -a {pkg_root}/* {iface_dir}
      cp -a $IFACE_DIR/{pkg_root}/* {iface_dir}
      rm -rf $IFACE_DIR
    """.format(
            main = modules[ctx.attr.main],
            pkg_name_version_file = pkg_name_version.path,
            package_db_dir = package_db_dir.path,
            damlc_bootstrap = ctx.executable.damlc_bootstrap.path,
            dalf_file = dalf.path,
            cpp = ctx.executable.cpp.path,
            daml_lf_version = ctx.attr.daml_lf_version,
            iface_dir = iface_dir.path,
            pkg_root = ctx.attr.pkg_root,
        ),
    )

    return [
        DefaultInfo(files = depset([dalf, iface_dir, package_config])),
        DamlPackage(
            pkg_name = ctx.attr.pkg_name,
            pkg_name_version = pkg_name_version,
            daml_lf_version = ctx.attr.daml_lf_version,
            pkg_conf = package_config,
            iface_dir = iface_dir,
            dalf = dalf,
            modules = [k for k in modules],
        ),
    ]

daml_package_rule = rule(
    implementation = _daml_package_rule_impl,
    toolchains = ["@rules_haskell//haskell:toolchain"],
    attrs = {
        "pkg_name": attr.string(mandatory = True),
        "main": attr.string(default = "LibraryModules"),
        "srcs": attr.label(allow_files = True),
        "pkg_root": attr.string(),
        "package_db": attr.label(
            default = Label("//compiler/damlc/pkg-db"),
            executable = False,
            cfg = "host",
        ),
        "dependencies": attr.label_list(allow_files = False),
        "damlc_bootstrap": attr.label(
            default = Label("//compiler/damlc:damlc-bootstrap"),
            executable = True,
            cfg = "host",
        ),
        "cpp": attr.label(
            default = Label("@stackage-exe//hpp"),
            executable = True,
            cfg = "host",
        ),
        "daml_lf_version": attr.string(
            mandatory = True,
        ),
    },
)

PackageDb = provider(fields = ["db_dir", "pkgs"])

def _daml_package_db_impl(ctx):
    toolchain = ctx.toolchains["@rules_haskell//haskell:toolchain"]
    db_dir = ctx.actions.declare_directory(ctx.attr.name + "_dir")
    ctx.actions.run_shell(
        inputs = [inp for pkg in ctx.attr.pkgs for inp in [pkg[DamlPackage].pkg_conf, pkg[DamlPackage].iface_dir, pkg[DamlPackage].dalf, pkg[DamlPackage].pkg_name_version]],
        tools = [toolchain.tools.ghc_pkg],
        outputs = [db_dir],
        command =
            """
        set -eou pipefail
        shopt -s nullglob
        mkdir -p {db_dir}
        for ver in {daml_lf_versions}; do
            mkdir -p "{db_dir}/$ver/package.conf.d"
        done
        """.format(db_dir = db_dir.path, daml_lf_versions = " ".join(ctx.attr.daml_lf_versions)) +
            "".join(
                [
                    """
        PKG_NAME_VERSION=`cat {pkg_name_version_file}`
        mkdir -p "{db_dir}/{daml_lf_version}/$PKG_NAME_VERSION"
        cp {pkg_conf} "{db_dir}/{daml_lf_version}/package.conf.d/$PKG_NAME_VERSION.conf"
        cp -aL {iface_dir}/* "{db_dir}/{daml_lf_version}/$PKG_NAME_VERSION/"
        cp {dalf} "{db_dir}/{daml_lf_version}/$PKG_NAME_VERSION.dalf"
        """.format(
                        daml_lf_version = pkg[DamlPackage].daml_lf_version,
                        pkg_name_version_file = pkg[DamlPackage].pkg_name_version.path,
                        pkg_conf = pkg[DamlPackage].pkg_conf.path,
                        iface_dir = pkg[DamlPackage].iface_dir.path,
                        dalf = pkg[DamlPackage].dalf.path,
                        db_dir = db_dir.path,
                    )
                    for pkg in ctx.attr.pkgs
                ],
            ) +
            """
        for lf_version in "{db_dir}"/*; do
          {ghc_pkg} recache --package-db=$lf_version/package.conf.d --no-expand-pkgroot
        done
        """.format(
                db_dir = db_dir.path,
                ghc_pkg = toolchain.tools.ghc_pkg.path,
            ),
    )
    return [
        DefaultInfo(files = depset([db_dir]), runfiles = ctx.runfiles(files = [db_dir])),
        PackageDb(db_dir = db_dir, pkgs = [pkg[DamlPackage] for pkg in ctx.attr.pkgs]),
    ]

daml_package_db = rule(
    implementation = _daml_package_db_impl,
    toolchains = ["@rules_haskell//haskell:toolchain"],
    attrs = {
        "pkgs": attr.label_list(
            allow_files = False,
            providers = [DamlPackage],
        ),
        "daml_lf_versions": attr.string_list(mandatory = True),
    },
)
