# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@rules_haskell//haskell:defs.bzl",
    "haskell_binary",
    "haskell_library",
    "haskell_repl",
    "haskell_test",
)
load(
    "@rules_haskell//haskell:c2hs.bzl",
    "c2hs_library",
)
load("//bazel_tools:hlint.bzl", "haskell_hlint")
load("@os_info//:os_info.bzl", "is_windows")

# This file defines common Haskell language extensions and compiler flags used
# throughout this repository. The initial set of flags is taken from the
# daml-foundations project. If you find that additional flags are required for
# another project, consider whether all projects could benefit from these
# changes. If so, add them here.
#
# Use the macros `da_haskell_*` defined in this file, instead of stock rules
# `haskell_*` from `rules_haskell` in order for these default flags to take
# effect.

common_haskell_exts = [
    "BangPatterns",
    "DeriveDataTypeable",
    "DeriveFoldable",
    "DeriveFunctor",
    "DeriveGeneric",
    "DeriveTraversable",
    "FlexibleContexts",
    "GeneralizedNewtypeDeriving",
    "LambdaCase",
    "NamedFieldPuns",
    "NumericUnderscores",
    "OverloadedStrings",
    "PackageImports",
    "RecordWildCards",
    "ScopedTypeVariables",
    "StandaloneDeriving",
    "TupleSections",
    "TypeApplications",
    "ViewPatterns",
    "ImportQualifiedPost",
]

common_haskell_flags = [
    "-Wall",
    "-Werror",
    "-Wincomplete-uni-patterns",
    "-Wno-name-shadowing",
    "-fno-omit-yields",
    "-fno-ignore-asserts",
]

# Passing these to libraries produces a (harmless but annoying) warning.
common_binary_haskell_flags = common_haskell_flags + [
    "-threaded",
    "-rtsopts",
    # run on two cores, disable idle & parallel GC
    "-with-rtsopts=-N2 -qg -I0",
]

def _wrap_rule(rule, common_flags, name = "", deps = [], hackage_deps = [], compiler_flags = [], **kwargs):
    ext_flags = ["-X%s" % ext for ext in common_haskell_exts]
    stackage_libs = ["@stackage//:{}".format(dep) for dep in hackage_deps]
    rule(
        name = name,
        compiler_flags = ext_flags + common_flags + compiler_flags,
        deps = stackage_libs + deps,
        **kwargs
    )

    # We don't fetch HLint on Windows
    if not is_windows:
        haskell_hlint(
            name = name + "@hlint",
            deps = [name],
            testonly = True,
        )

    # Load Main module on executable rules.
    repl_ghci_commands = []
    if "main_function" in kwargs:
        main_module = ".".join(kwargs["main_function"].split(".")[:-1])
        repl_ghci_commands = [
            ":m " + main_module,
        ]
    da_haskell_repl(
        name = name + "@ghci",
        deps = [name],
        repl_ghci_commands = repl_ghci_commands,
        testonly = kwargs.get("testonly", False),
        visibility = ["//visibility:public"],
    )

def da_haskell_library(**kwargs):
    """
    Define a Haskell library.

    Allows to define Hackage dependencies using `hackage_deps`,
    applies common Haskell options defined in `bazel_tools/haskell.bzl`
    and forwards to `haskell_library` from `rules_haskell`.
    Refer to the [`rules_haskell` documentation][rules_haskell_docs].

    [rules_haskell_docs]: https://api.haskell.build/

    Example:
        ```
        da_haskell_library(
            name = "example",
            src_strip_prefix = "src",
            srcs = glob(["src/**/*.hs"]),
            hackage_deps = [
                "base",
                "text",
            ],
            deps = [
                "//some/package:target",
            ],
            visibility = ["//visibility:public"],
        )
        ```
    """
    _wrap_rule(haskell_library, common_haskell_flags, **kwargs)

def da_haskell_binary(main_function = "Main.main", **kwargs):
    """
    Define a Haskell executable.

    Allows to define Hackage dependencies using `hackage_deps`,
    applies common Haskell options defined in `bazel_tools/haskell.bzl`
    and forwards to `haskell_binary` from `rules_haskell`.
    Refer to the [`rules_haskell` documentation][rules_haskell_docs].

    [rules_haskell_docs]: https://api.haskell.build/

    Example:
        ```
        da_haskell_binary(
            name = "example",
            src_strip_prefix = "src",
            srcs = glob(["src/**/*.hs"]),
            hackage_deps = [
                "base",
                "text",
            ],
            deps = [
                "//some/package:target",
            ],
            visibility = ["//visibility:public"],
        )
        ```
    """
    _wrap_rule(
        haskell_binary,
        common_binary_haskell_flags,
        # Make this argument explicit, so we can distinguish library and
        # executable rules in _wrap_rule.
        main_function = main_function,
        **kwargs
    )

def da_haskell_test(main_function = "Main.main", testonly = True, **kwargs):
    """
    Define a Haskell test suite.

    Allows to define Hackage dependencies using `hackage_deps`,
    applies common Haskell options defined in `bazel_tools/haskell.bzl`
    and forwards to `haskell_test` from `rules_haskell`.
    Refer to the [`rules_haskell` documentation][rules_haskell_docs].

    [rules_haskell_docs]: https://api.haskell.build/

    Example:
        ```
        da_haskell_test(
            name = "example",
            src_strip_prefix = "src",
            srcs = glob(["src/**/*.hs"]),
            hackage_deps = [
                "base",
                "text",
            ],
            deps = [
                "//some/package:target",
            ],
            visibility = ["//visibility:public"],
        )
        ```
    """
    _wrap_rule(
        haskell_test,
        common_binary_haskell_flags,
        # Make this argument explicit, so we can distinguish library and
        # executable rules in _wrap_rule.
        main_function = main_function,
        # Make this argument explicit, so we can distinguish test targets
        # in _wrap_rule.
        testonly = testonly,
        **kwargs
    )

def da_haskell_repl(**kwargs):
    """
    Define a Haskell repl.

    Applies common Haskell options defined in `bazel_tools/haskell.bzl`
    and forwards to `haskell_repl` from `rules_haskell`.
    Refer to the [`rules_haskell` documentation][rules_haskell_docs].

    [rules_haskell_docs]: https://api.haskell.build/

    Example:
        ```
        da_haskell_repl(
            name = "repl",
            # Load these packages and their dependencies by source.
            # Will only load packages from the local workspace by source.
            deps = [
                "//some/package:target_a",
                "//some/package:target_b",
            ],
            # (optional) only load targets matching these patterns by source.
            experimental_from_source = [
                "//some/package/...",
                "//some/other/package/...",
            ],
            # (optional) don't load targets matching these patterns by source.
            experimental_from_binary = [
                "//some/vendored/package/...",
            ],
            # (optional) Pass extra arguments to ghci.
            repl_ghci_args = [
                "-fexternal-interpreter",
            ],
            # (optional) Make runfiles available to the REPL, or not.
            collect_data = True,
        )
        ```

    """

    # Set default arguments
    with_defaults = dict(
        # Whether to make runfiles, such as the daml stdlib, available to the
        # REPL. Pass --define ghci_data=True to enable.
        collect_data = select({
            "//:ghci_data": True,
            "//conditions:default": False,
        }),
        experimental_from_binary = [
            # Workaround for https://github.com/tweag/rules_haskell/issues/1726
            "//bazel_tools/ghc-lib/...",
            "//nix/...",
        ],
        repl_ghci_args = [
            "-fexternal-interpreter",
            "-j",
            "+RTS",
            "-I0",
            "-n2m",
            "-A128m",
            "-qb0",
            "-RTS",
        ],
    )
    with_defaults.update(kwargs)

    # The common_haskell_exts and common_haskell_flags are already passed on
    # the library/binary/test rule wrappers. haskell_repl will pick these up
    # automatically, if such targets are loaded by source.
    # Right now we don't set any default flags.
    haskell_repl(**with_defaults)

def _sanitize_string_for_usage(s):
    res_array = []
    for idx in range(len(s)):
        c = s[idx]
        if c.isalnum() or c == ".":
            res_array.append(c)
        else:
            res_array.append("_")
    return "".join(res_array)

def c2hs_suite(name, hackage_deps, deps = [], srcs = [], c2hs_srcs = [], c2hs_src_strip_prefix = "", **kwargs):
    ts = []
    for file in c2hs_srcs:
        n = _sanitize_string_for_usage(file)
        c2hs_library(
            name = n,
            srcs = [file],
            deps = deps + [":" + t for t in ts],
            src_strip_prefix = c2hs_src_strip_prefix,
            # language-c fails to pass mingw’s intrinsic-impl.h header if
            # we do not unset this option.
            extra_args = ["-C-U__GCC_ASM_FLAG_OUTPUTS__"] if is_windows else [],
        )
        ts.append(n)
    da_haskell_library(
        name = name,
        srcs = [":" + t for t in ts] + srcs,
        deps = deps,
        hackage_deps = hackage_deps,
        **kwargs
    )

# Check is disabled on windows
def generate_and_track_cabal(name, exe_name = None, src_dir = None, exclude_deps = [], exclude_exports = []):
    generate_cabal(name, exe_name, src_dir, exclude_deps, exclude_exports)

    native.filegroup(
        name = name + "-golden-cabal",
        srcs = native.glob([name + ".cabal"]),
        visibility = ["//visibility:public"],
    )

    native.sh_test(
        name = name + "-cabal-file-matches",
        srcs = ["//bazel_tools:match-cabal-file"],
        args = [
            "$(location :%s-generated-cabal)" % name,
            "$(location :%s-golden-cabal)" % name,
            "$(POSIX_DIFF)",
        ],
        data = [
            ":%s-golden-cabal" % name,
            ":%s-generated-cabal" % name,
        ],
        toolchains = [
            "@rules_sh//sh/posix:make_variables",
        ],
        deps = [
            "@bazel_tools//tools/bash/runfiles",
        ],
    ) if not is_windows else None

def generate_cabal(name, exe_name = None, src_dir = None, exclude_deps = [], exclude_exports = []):
    full_path = "//%s:%s" % (native.package_name(), name)
    src_dir = "src" if src_dir == None else src_dir
    source_dir = src_dir if exe_name == None else "lib"

    native.genquery(
        name = name + "-deps",
        expression = "deps(%s, 1) except %s" % (full_path, full_path),
        opts = [
            "--output",
            "label_kind",
        ],
        scope = [":" + name],
        deps = [],
    )

    native.genrule(
        name = name + "-generated-cabal",
        srcs = [":%s-deps" % name],
        outs = [name + "-generated.cabal"],
        cmd = """
         cat << EOF > $@
cabal-version: 2.4
-- This file was autogenerated
name: {name}
build-type: Simple
version: 0.1.15.0
synopsis: {name}
license: Apache-2.0
author: Digital Asset
maintainer: Digital Asset
copyright: Digital Asset 2023
homepage: https://github.com/digital-asset/daml#readme
bug-reports: https://github.com/digital-asset/daml/issues

source-repository head
    type: git
    location: https://github.com/digital-asset/daml.git

EOF

if {has_exe}
then
         cat << EOF >> $@
executable {exe_name}
    main-is: Main.hs
    hs-source-dirs:
      src
    build-depends:
      {name},
      optparse-applicative,
EOF

    grep -v "\\-\\-" $(SRCS) | \
      sed -nE '''
s#^haskell_toolchain_library rule (@stackage//:([a-zA-Z0-9\\-]+))$$#\\2 \\1#g
s#^haskell_toolchain_library rule (@stackage//:([a-zA-Z0-9\\-]+))$$#\\2 \\1#g
s#^haskell_cabal_library rule (@stackage//:([a-zA-Z0-9\\-]+))$$#\\2 \\1#g
s#^_haskell_library rule (//[A-Za-z0-9/_\\-]+:daml_lf_dev_archive_haskell_proto)$$#daml-lf-proto-types \\1#g
s#^_haskell_library rule (//[A-Za-z0-9/_\\-]+:([A-Za-z0-9/_\\-]+))$$#\\2 \\1#g
s#^alias rule (@stackage//:([a-zA-Z0-9\\-]+))$$#\\2 \\1#g
T;p
        ''' | sort -f {dependency_filter} | awk '{{print "      -- " $$2; print "      " $$1 ","}}' >> $@

         cat << EOF >> $@
    default-language: Haskell2010
    default-extensions:
      {extensions}
EOF
fi

         cat << EOF >> $@
library
    default-language: Haskell2010
    hs-source-dirs: {source_dir}
    build-depends:
EOF

    grep -v "\\-\\-" $(SRCS) | \
      sed -nE '''
s#^haskell_toolchain_library rule (@stackage//:([a-zA-Z0-9\\-]+))$$#\\2 \\1#g
s#^haskell_toolchain_library rule (@stackage//:([a-zA-Z0-9\\-]+))$$#\\2 \\1#g
s#^haskell_cabal_library rule (@stackage//:([a-zA-Z0-9\\-]+))$$#\\2 \\1#g
s#^_haskell_library rule (//[A-Za-z0-9/_\\-]+:daml_lf_dev_archive_haskell_proto)$$#daml-lf-proto-types \\1#g
s#^_haskell_library rule (//[A-Za-z0-9/_\\-]+:([A-Za-z0-9/_\\-]+))$$#\\2 \\1#g
s#^alias rule (@stackage//:([a-zA-Z0-9\\-]+))$$#\\2 \\1#g
T;p
        ''' | sort -f {dependency_filter} | awk '{{print "      -- " $$2; print "      " $$1 ","}}'  >> $@

    echo '    exposed-modules:' >> $@

    grep -v "\\-\\-" $(SRCS) | \
       sed -nE 's#^source file //[A-Za-z0-9/_\\-]+:{source_dir}/([A-Za-z0-9/_\\-]+)\\.hs$$#      \\1#g;T;p' | \
       sed 's#/#\\.#g' | sort -f {export_filter} >> $@

    echo -ne '    default-extensions:\n      ' >> $@

    echo "{extensions}" >> $@


         """.format(
            name = name,
            exe_name = exe_name,
            has_exe = "false" if exe_name == None else "true",
            source_dir = source_dir,
            extensions = "\n      ".join(common_haskell_exts),
            dependency_filter = "| grep -v " + " ".join(["-e " + e for e in exclude_deps]) if exclude_deps else "",
            export_filter = "| grep -v " + " ".join(["-e " + e for e in exclude_exports]) if exclude_exports else "",
        ),
    )
