# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# A DAML package database contains a subdirectory for each DAML-LF
# version.  Each subdirectory contains a regular GHC package database
# and the DALF files. Note
# that the GHC package database also needs to be able to depend on the
# DAML-LF version since we want to make things in DAML conditional on
# the target version.

# We have the following rules:
#
# daml-package:
#   Inputs:
#   - A DAML source directory
#   - The root DAML file
#   - The target LF version
#   - A package database in the format described above.
#   Outputs:
#   - A DALF file for the target LF version
#   - A tarball containing the interface files (and potentially source files)
#   - The package config file
#   - A way of querying the target LF version, e.g., by making it a provider.
#
# daml-package-db:
#   Inputs:
#   - A list of targets produced by daml-compile
#   Outputs:
#   - A tarball containing the combined package database

PACKAGE_CONF_TEMPLATE = """
name: {name}
version: 1.0.0
id: {name}
key: {name}
copyright: 2015-2018 Digital Asset Holdings
maintainer: Digital Asset
exposed: True
exposed-modules: {modules}
import-dirs: $topdir/{name}
library-dirs: $topdir/{name}
data-dir: $topdir/{name}
depends: {depends}
"""

DamlPackage = provider(fields = ["daml_lf_version", "pkg_name", "pkg_conf", "iface_tar", "dalf", "modules"])

# Compile a DAML file and create the GHC package database
# for it.
def _daml_package_rule_impl(ctx):
    name = ctx.attr.name

    # Construct mapping from module names to paths
    modules = {}
    for file in ctx.files.srcs:
        # FIXME(JM): HACK: the `[3:]` assumes we're in
        # daml-foundations/daml-ghc! Find a way to get the
        # base path...
        modules[".".join(file.path[:-5].split("/")[3:])] = file.path

    # Create the package conf file
    ctx.actions.write(
        output = ctx.outputs.config,
        content = PACKAGE_CONF_TEMPLATE.format(
            name = ctx.attr.pkg_name,
            modules = " ".join(modules.keys()),
            depends = " ".join([dep[DamlPackage].pkg_name for dep in ctx.attr.dependencies]),
        ),
    )

    package_db_tar = ctx.attr.package_db[PackageDb].tar

    ctx.actions.run_shell(
        outputs = [ctx.outputs.dalf, ctx.outputs.iface_tar],
        inputs = ctx.files.srcs + [package_db_tar],
        tools = [ctx.executable.damlc_bootstrap],
        progress_message = "Compiling " + name + ".daml to daml-lf " + ctx.attr.daml_lf_version,
        command = """
      set -eou pipefail
      # Since we don't have sandboxing on Windows, that directory might
      # exist from a previous build.
      rm -rf {package_db_name}
      mkdir -p {package_db_name}
      tar xf {db_tar} -C {package_db_name} --strip-components 1
      mkdir -p {package_db_name}/{daml_lf_version}

      # Compile the dalf file
      {damlc_bootstrap} compile \
        --package-name {pkg_name} \
        --package-db {package_db_name} \
        --write-iface \
        --target {daml_lf_version} \
        -o {dalf_file} \
        {main}

      tar cf {iface_tar} '--exclude=*.daml' -C $(dirname {pkg_root}) $(basename {pkg_root})
    """.format(
            main = modules[ctx.attr.main],
            name = name,
            package_db_name = "package_db_for_" + name,
            damlc_bootstrap = ctx.executable.damlc_bootstrap.path,
            dalf_file = ctx.outputs.dalf.path,
            daml_lf_version = ctx.attr.daml_lf_version,
            db_tar = package_db_tar.path,
            iface_tar = ctx.outputs.iface_tar.path,
            pkg_root = ctx.attr.pkg_root,
            pkg_name = ctx.attr.pkg_name,
        ),
    )

    return [
        DefaultInfo(),
        DamlPackage(
            pkg_name = ctx.attr.pkg_name,
            daml_lf_version = ctx.attr.daml_lf_version,
            pkg_conf = ctx.outputs.config,
            iface_tar = ctx.outputs.iface_tar,
            dalf = ctx.outputs.dalf,
            modules = [k for k in modules],
        ),
    ]

def _daml_package_outputs_impl(name):
    return {
        "dalf": name + ".dalf",
        "config": name + ".conf",
        "iface_tar": name + ".tar",
    }

daml_package_rule = rule(
    implementation = _daml_package_rule_impl,
    toolchains = ["@io_tweag_rules_haskell//haskell:toolchain"],
    attrs = {
        "pkg_name": attr.string(mandatory = True),
        "main": attr.string(default = "LibraryModules"),
        "srcs": attr.label(allow_files = True),
        "pkg_root": attr.string(),
        "package_db": attr.label(
            default = Label("//daml-foundations/daml-ghc/package-database:package-db"),
            executable = False,
            cfg = "host",
        ),
        "dependencies": attr.label_list(allow_files = False),
        "damlc_bootstrap": attr.label(
            default = Label("//daml-foundations/daml-tools/da-hs-damlc-app:damlc_bootstrap"),
            executable = True,
            cfg = "host",
        ),
        "daml_lf_version": attr.string(
            mandatory = True,
        ),
    },
    outputs = _daml_package_outputs_impl,
)

PackageDb = provider(fields = ["tar", "pkgs"])

def _daml_package_db_impl(ctx):
    toolchain = ctx.toolchains["@io_tweag_rules_haskell//haskell:toolchain"]
    ctx.actions.run_shell(
        inputs = [inp for pkg in ctx.attr.pkgs for inp in [pkg[DamlPackage].pkg_conf, pkg[DamlPackage].iface_tar, pkg[DamlPackage].dalf]],
        tools = [toolchain.tools.ghc_pkg],
        outputs = [ctx.outputs.tar],
        command =
            """
        set -eou pipefail
        shopt -s nullglob
        TMP_DIR=$(mktemp -d)
        PACKAGE_DB="$TMP_DIR/package_db"
        mkdir -p "$PACKAGE_DB"
        """ +
            "".join(
                [
                    """
        mkdir -p "$PACKAGE_DB/{daml_lf_version}/{pkg_name}"
        cp {pkg_conf} "$PACKAGE_DB/{daml_lf_version}/{pkg_name}.conf"
        tar xf {iface_tar} --strip-components=1 -C "$PACKAGE_DB/{daml_lf_version}/{pkg_name}/"
        cp {dalf} "$PACKAGE_DB/{daml_lf_version}/{pkg_name}.dalf"
        """.format(
                        daml_lf_version = pkg[DamlPackage].daml_lf_version,
                        pkg_name = pkg[DamlPackage].pkg_name,
                        pkg_conf = pkg[DamlPackage].pkg_conf.path,
                        iface_tar = pkg[DamlPackage].iface_tar.path,
                        dalf = pkg[DamlPackage].dalf.path,
                    )
                    for pkg in ctx.attr.pkgs
                ],
            ) +
            """
        for lf_version in "$PACKAGE_DB"/*; do
          {ghc_pkg} recache --package-db=$lf_version --no-expand-pkgroot
        done
        tar cf {db_tar} -C "$TMP_DIR" package_db
        """.format(
                db_tar = ctx.outputs.tar.path,
                ghc_pkg = toolchain.tools.ghc_pkg.path,
            ),
    )
    return [
        DefaultInfo(),
        PackageDb(tar = ctx.outputs.tar, pkgs = [pkg[DamlPackage] for pkg in ctx.attr.pkgs]),
    ]

def _daml_package_db_outputs_impl(name):
    return {
        "tar": name + ".tar",
    }

daml_package_db = rule(
    implementation = _daml_package_db_impl,
    toolchains = ["@io_tweag_rules_haskell//haskell:toolchain"],
    attrs = {
        "pkgs": attr.label_list(
            allow_files = False,
            providers = [DamlPackage],
        ),
    },
    outputs = _daml_package_db_outputs_impl,
)

# We might want to kill this rule at some point but I find the fact that
# I can just mess around with tarballs in the other rules way more convenient
# so given that we only use the bundling stuff once, this seems somewhat reasonable.
def _bundled_package_db_impl(ctx):
    db_files = [
        f
        for daml_lf_version in ctx.attr.daml_lf_versions
        for f in [
            ctx.actions.declare_file("{daml_lf_version}/package.cache".format(daml_lf_version = daml_lf_version)),
            ctx.actions.declare_file("{daml_lf_version}/package.cache.lock".format(daml_lf_version = daml_lf_version)),
        ]
    ]
    pkg_db = ctx.attr.pkg_db[PackageDb]
    root = ctx.actions.declare_file("ROOT")
    for pkg in pkg_db.pkgs:
        pkg_conf = ctx.actions.declare_file("{daml_lf_version}/{pkg_name}.conf".format(
            daml_lf_version = pkg.daml_lf_version,
            pkg_name = pkg.pkg_name,
        ))
        dalf = ctx.actions.declare_file("{daml_lf_version}/{pkg_name}.dalf".format(
            daml_lf_version = pkg.daml_lf_version,
            pkg_name = pkg.pkg_name,
        ))
        iface_files = [ctx.actions.declare_file("{daml_lf_version}/{pkg_name}/{module}.hi".format(daml_lf_version = pkg.daml_lf_version, pkg_name = pkg.pkg_name, module = module.replace(".", "/"))) for module in pkg.modules]
        db_files = db_files + [pkg_conf, dalf] + iface_files
    ctx.actions.run_shell(
        inputs = [pkg_db.tar],
        outputs = [root] + db_files,
        command =
            """
      touch {root_file}
      tar xf {db_tar} --strip-components=1 -C $(dirname {root_file})
    """.format(db_tar = pkg_db.tar.path, root_file = root.path),
    )
    return [
        DefaultInfo(files = depset(db_files), runfiles = ctx.runfiles(files = db_files)),
    ]

bundled_package_db = rule(
    implementation = _bundled_package_db_impl,
    attrs = {
        "pkg_db": attr.label(
            allow_files = False,
            providers = [PackageDb],
        ),
        "daml_lf_versions": attr.string_list(),
    },
)
