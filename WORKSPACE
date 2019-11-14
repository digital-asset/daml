workspace(
    name = "com_github_digital_asset_daml",
    managed_directories = {
        "@npm": ["node_modules"],
        "@daml_extension_deps": ["compiler/daml-extension/node_modules"],
        "@navigator_frontend_deps": ["navigator/frontend/node_modules"],
    },
)

load("//:util.bzl", "hazel_ghclibs", "hazel_github", "hazel_github_external", "hazel_hackage")

# NOTE(JM): Load external dependencies from deps.bzl.
# Do not put "http_archive" and similar rules into this file. Put them into
# deps.bzl. This allows using this repository as an external workspace.
# (though with the caviat that that user needs to repeat the relevant bits of
#  magic in this file, but at least right versions of external rules are picked).
load("//:deps.bzl", "daml_deps")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

daml_deps()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("@rules_haskell//haskell:repositories.bzl", "rules_haskell_dependencies")

rules_haskell_dependencies()

load("@rules_haskell//tools:repositories.bzl", "rules_haskell_worker_dependencies")

# We don't use the worker mode, but this is required for bazel query to function.
rules_haskell_worker_dependencies()

register_toolchains(
    "//:c2hs-toolchain",
)

load("//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dev_env_tool")
load(
    "@io_tweag_rules_nixpkgs//nixpkgs:nixpkgs.bzl",
    "nixpkgs_cc_configure",
    "nixpkgs_local_repository",
    "nixpkgs_package",
)
load("//bazel_tools:os_info.bzl", "os_info")

os_info(name = "os_info")

load("@os_info//:os_info.bzl", "is_linux", "is_windows")
load("//bazel_tools:ghc_dwarf.bzl", "ghc_dwarf")

ghc_dwarf(name = "ghc_dwarf")

load("@ghc_dwarf//:ghc_dwarf.bzl", "enable_ghc_dwarf")

# Configure msys2 POSIX toolchain provided by dadew.
load("//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dadew_sh_posix_configure")

dadew_sh_posix_configure() if is_windows else None

nixpkgs_local_repository(
    name = "nixpkgs",
    nix_file = "//nix:nixpkgs.nix",
    nix_file_deps = [
        "//nix:nixpkgs/default.nix",
        "//nix:nixpkgs/default.src.json",
    ],
)

dev_env_nix_repos = {
    "nixpkgs": "@nixpkgs",
}

# Bazel cannot automatically determine which files a Nix target depends on.
# rules_nixpkgs offers the nix_file_deps attribute for that purpose. It should
# list all files that a target depends on. This allows Bazel to rebuild the
# target using Nix if any of these files has been changed. Omitting files from
# this list can cause subtle bugs or cache misses when Bazel loads an outdated
# store path. You can use the following command to determine what files a Nix
# target depends on. E.g. for tools.curl
#
# $ nix-build -vv -A tools.curl nix 2>&1 \
#     | egrep '(evaluating file|copied source)' \
#     | egrep -v '/nix/store'
#
# Unfortunately there is no mechanism to automatically keep this list up to
# date at the moment. See https://github.com/tweag/rules_nixpkgs/issues/74.
common_nix_file_deps = [
    "//nix:bazel.nix",
    "//nix:nixpkgs.nix",
    "//nix:nixpkgs/default.nix",
    "//nix:nixpkgs/default.src.json",
]

# Use Nix provisioned cc toolchain
nixpkgs_cc_configure(
    nix_file = "//nix:bazel-cc-toolchain.nix",
    nix_file_deps = common_nix_file_deps + [
        "//nix:bazel-cc-toolchain.nix",
        "//nix:tools/bazel-cc-toolchain/default.nix",
    ],
    repositories = dev_env_nix_repos,
)

# Curl system dependency
nixpkgs_package(
    name = "curl_nix",
    attribute_path = "curl",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# Patchelf system dependency
nixpkgs_package(
    name = "patchelf_nix",
    attribute_path = "patchelf",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# netcat dependency
nixpkgs_package(
    name = "netcat_nix",
    attribute_path = "netcat-gnu",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "netcat_dev_env",
    nix_include = ["bin/nc"],
    nix_label = "@netcat_nix",
    nix_paths = ["bin/nc"],
    tools = ["nc"],
    win_include = ["usr/bin/nc.exe"],
    win_paths = ["usr/bin/nc.exe"],
    win_tool = "msys2",
)

# Tar & gzip dependency
nixpkgs_package(
    name = "tar_nix",
    attribute_path = "gnutar",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "tar_dev_env",
    nix_include = ["bin/tar"],
    nix_label = "@tar_nix",
    nix_paths = ["bin/tar"],
    tools = ["tar"],
    win_include = ["usr/bin/tar.exe"],
    win_paths = ["usr/bin/tar.exe"],
    win_tool = "msys2",
)

nixpkgs_package(
    name = "gzip_nix",
    attribute_path = "gzip",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "gzip_dev_env",
    nix_include = ["bin/gzip"],
    nix_label = "@gzip_nix",
    nix_paths = ["bin/gzip"],
    tools = ["gzip"],
    win_include = ["usr/bin/gzip.exe"],
    win_paths = ["usr/bin/gzip.exe"],
    win_tool = "msys2",
)

dev_env_tool(
    name = "mvn_dev_env",
    nix_include = ["bin/mvn"],
    nix_label = "@mvn_nix",
    nix_paths = ["bin/mvn"],
    tools = ["mvn"],
    win_include = [
        "bin",
        "boot",
        "conf",
        "lib",
    ],
    win_paths = ["bin/mvn"],
    win_tool = "maven-3.6.1",
)

nixpkgs_package(
    name = "awk_nix",
    attribute_path = "gawk",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

nixpkgs_package(
    name = "coreutils_nix",
    attribute_path = "coreutils",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

nixpkgs_package(
    name = "grpcurl_nix",
    attribute_path = "grpcurl",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

nixpkgs_package(
    name = "hlint_nix",
    attribute_path = "hlint",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps + [
        "//nix:overrides/hlint-2.1.15.nix",
        "//nix:overrides/haskell-src-exts-1.21.0.nix",
    ],
    repositories = dev_env_nix_repos,
)

nixpkgs_package(
    name = "zip_nix",
    attribute_path = "zip",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "zip_dev_env",
    nix_include = ["bin/zip"],
    nix_label = "@zip_nix",
    nix_paths = ["bin/zip"],
    tools = ["zip"],
    win_include = ["usr/bin/zip.exe"],
    win_paths = ["usr/bin/zip.exe"],
    win_tool = "msys2",
)

load(
    "@rules_haskell//haskell:ghc_bindist.bzl",
    "haskell_register_ghc_bindists",
)
load(
    "@rules_haskell//haskell:nixpkgs.bzl",
    "haskell_register_ghc_nixpkgs",
)

nixpkgs_package(
    name = "glibc_locales",
    attribute_path = "glibcLocales",
    build_file_content = """
package(default_visibility = ["//visibility:public"])
filegroup(
    name = "locale-archive",
    srcs = ["lib/locale/locale-archive"],
)
""",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
) if is_linux else None

nix_ghc_deps = common_nix_file_deps + [
    "//nix:ghc.nix",
    "//nix:with-packages-wrapper.nix",
    "//nix:overrides/ghc-8.6.5.nix",
    "//nix:overrides/ghc-8.6.3-binary.nix",
]

# This is used to get ghc-pkg on Linux.
nixpkgs_package(
    name = "ghc_nix",
    attribute_path = "ghc.ghc",
    build_file_content = """
package(default_visibility = ["//visibility:public"])
exports_files(glob(["lib/**/*"]))
""",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = nix_ghc_deps,
    repositories = dev_env_nix_repos,
) if not is_windows else None

common_ghc_flags = [
    # We default to -c opt but we also want -O1 in -c dbg builds
    # since we use them for profiling.
    "-O1",
    "-hide-package=ghc-boot-th",
    "-hide-package=ghc-boot",
]

# Used by Darwin and Linux
haskell_register_ghc_nixpkgs(
    attribute_path = "ghcStaticDwarf" if enable_ghc_dwarf else "ghcStatic",
    build_file = "@io_tweag_rules_nixpkgs//nixpkgs:BUILD.pkg",

    # -fexternal-dynamic-refs is required so that we produce position-independent
    # relocations against some functions (-fPIC alone isn’t sufficient).

    # -split-sections would allow us to produce significantly smaller binaries, e.g., for damlc,
    # the binary shrinks from 186MB to 83MB. -split-sections only works on Linux but
    # we get a similar behavior on Darwin by default.
    # However, we had to disable split-sections for now as it seems to interact very badly
    # with the GHCi linker to the point where :main takes several minutes rather than several seconds.
    compiler_flags = common_ghc_flags + [
        "-fexternal-dynamic-refs",
    ] + (["-g3"] if enable_ghc_dwarf else ["-optl-s"]),
    compiler_flags_select = {
        "@com_github_digital_asset_daml//:profiling_build": ["-fprof-auto"],
        "//conditions:default": [],
    },
    is_static = True,
    locale_archive = "@glibc_locales//:locale-archive",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = nix_ghc_deps,
    repl_ghci_args = [
        "-O0",
        "-fexternal-interpreter",
        "-Wwarn",
    ],
    repositories = dev_env_nix_repos,
    version = "8.6.5",
)

# Used by Windows
haskell_register_ghc_bindists(
    compiler_flags = common_ghc_flags,
    version = "8.6.5",
) if is_windows else None

nixpkgs_package(
    name = "jq",
    attribute_path = "jq",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "jq_dev_env",
    nix_include = ["bin/jq"],
    nix_label = "@jq",
    nix_paths = ["bin/jq"],
    tools = ["jq"],
    win_include = ["mingw64/bin"],
    win_include_as = {"mingw64/bin": "bin"},
    win_paths = ["bin/jq.exe"],
    win_tool = "msys2",
)

nixpkgs_package(
    name = "mvn_nix",
    attribute_path = "mvn",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

#node & npm
nixpkgs_package(
    name = "node_nix",
    attribute_path = "nodejs",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

nixpkgs_package(
    name = "npm_nix",
    attribute_path = "nodejs",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

#sass
nixpkgs_package(
    name = "sass_nix",
    attribute_path = "sass",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

#tex
nixpkgs_package(
    name = "texlive_nix",
    attribute_path = "texlive",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

#sphinx
nixpkgs_package(
    name = "sphinx_nix",
    attribute_path = "sphinx183",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

#Imagemagick
nixpkgs_package(
    name = "imagemagick_nix",
    attribute_path = "imagemagick",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

#Docker
nixpkgs_package(
    name = "docker_nix",
    attribute_path = "docker",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

#Javadoc
nixpkgs_package(
    name = "jdk_nix",
    attribute_path = "jdk8",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# This will not be needed after merge of the PR to bazel adding proper javadoc filegroups:
# https://github.com/bazelbuild/bazel/pull/7898
# `@javadoc_dev_env//:javadoc` could be then replaced with `@local_jdk//:javadoc` and the below removed
dev_env_tool(
    name = "javadoc_dev_env",
    nix_include = ["bin/javadoc"],
    nix_label = "@jdk_nix",
    nix_paths = ["bin/javadoc"],
    tools = ["javadoc"],
    win_include = [
        "bin",
        "include",
        "jre",
        "lib",
    ],
    win_paths = ["bin/javadoc.exe"],
    win_tool = "java-openjdk-8u201",
)

# This only makes sense on Windows so we just put dummy values in the nix fields.
dev_env_tool(
    name = "makensis_dev_env",
    nix_include = [""],
    nix_paths = ["bin/makensis.exe"],
    tools = ["makensis"],
    win_include = [
        "bin",
        "contrib",
        "include",
        "plugins",
        "stubs",
    ],
    win_paths = ["bin/makensis.exe"],
    win_tool = "nsis-3.04",
) if is_windows else None

# Scaladoc
nixpkgs_package(
    name = "scala_nix",
    attribute_path = "scala",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# Dummy target //external:python_headers.
# To avoid query errors due to com_google_protobuf.
# See https://github.com/protocolbuffers/protobuf/blob/d9ccd0c0e6bbda9bf4476088eeb46b02d7dcd327/util/python/BUILD
bind(
    name = "python_headers",
    actual = "@com_google_protobuf//util/python:python_headers",
)

load("@ai_formation_hazel//:hazel.bzl", "hazel_custom_package_github", "hazel_custom_package_hackage", "hazel_default_extra_libs", "hazel_repositories")
load("//hazel:packages.bzl", "core_packages", "packages")
load("//bazel_tools:haskell.bzl", "add_extra_packages")
load("@bazel_skylib//lib:dicts.bzl", "dicts")

# XXX: We do not have access to an integer-simple version of GHC on Windows.
# For the time being we build with GMP. See https://github.com/digital-asset/daml/issues/106
use_integer_simple = not is_windows

HASKELL_LSP_COMMIT = "4c4b588c3cb21c0509e2dd590db3f431f33ada11"

HASKELL_LSP_HASH = "80a3944306fb455fce36f7b3aafb8f0f8f6096a0bd3c46ed25cc0ff288d6413e"

GRPC_HASKELL_CORE_VERSION = "0.0.0.0"

GHC_LIB_VERSION = "8.8.1.20191120"

http_archive(
    name = "haskell_ghc__lib__parser",
    build_file = "//3rdparty/haskell:BUILD.ghc-lib-parser",
    sha256 = "89e5e8eadd66a90970b866a0669da28b1cd4fff2511a2dc09151a5b269bc0bc1",
    strip_prefix = "ghc-lib-parser-{}".format(GHC_LIB_VERSION),
    urls = ["https://digitalassetsdk.bintray.com/ghc-lib/ghc-lib-parser-{}.tar.gz".format(GHC_LIB_VERSION)],
)

# On Hackage but we need a custom build file to work around linker issues in GHCi.
http_archive(
    name = "haskell_grpc__haskell__core",
    build_file = "//3rdparty/haskell:BUILD.grpc-haskell-core",
    sha256 = "087527ec3841330b5328d123ca410901905d111529956821b724d92c436e6cdf",
    strip_prefix = "grpc-haskell-core-{}".format(GRPC_HASKELL_CORE_VERSION),
    urls = ["https://hackage.haskell.org/package/grpc-haskell-core-{}/grpc-haskell-core-{}.tar.gz".format(GRPC_HASKELL_CORE_VERSION, GRPC_HASKELL_CORE_VERSION)],
)

http_archive(
    name = "static_asset_d3plus",
    build_file_content = 'exports_files(["js/d3.min.js", "js/d3plus.min.js"])',
    sha256 = "7d31a500a4850364a966ac938eea7f2fa5ce1334966b52729079490636e7049a",
    strip_prefix = "d3plus.v1.9.8",
    type = "zip",
    urls = ["https://github.com/alexandersimoes/d3plus/releases/download/v1.9.8/d3plus.zip"],
)

hazel_repositories(
    core_packages = dicts.add(
        core_packages,
        {
            "integer-simple": "0.1.1.1",
            "Win32": "2.6.1.0",
        },
    ),
    exclude_packages = [
        "bindings-DSL",
        "clock",
        # Excluded since we build it via the http_archive line above.
        "grpc-haskell-core",
        "ghc-lib-parser",
        "ghc-paths",
        "ghcide",
        "streaming-commons",
        "wai-app-static",
        "zlib",
    ],
    extra_flags = {
        "blaze-textual": {"integer-simple": use_integer_simple},
        "cryptonite": {"integer-gmp": not use_integer_simple},
        "hashable": {"integer-gmp": not use_integer_simple},
        "integer-logarithms": {"integer-gmp": not use_integer_simple},
        "text": {"integer-simple": use_integer_simple},
        "scientific": {"integer-simple": use_integer_simple},
        "hlint": {"ghc-lib": True},  # Force dependency on ghc-lib-parser (don't use the ghc package).
    },
    extra_libs = dicts.add(
        hazel_default_extra_libs,
        {
            "z": "@com_github_madler_zlib//:z",
            "bz2": "@bzip2//:bz2",
            "grpc": "@com_github_grpc_grpc//:grpc",
            # The Bazel grpc lib seems to include gpr
            "gpr": "",
        },
    ),
    ghc_workspaces = {
        "k8": "@rules_haskell_ghc_nixpkgs",
        "darwin": "@rules_haskell_ghc_nixpkgs",
        # although windows is not quite supported yet
        "x64_windows": "@rules_haskell_ghc_windows_amd64",
    },
    packages = add_extra_packages(
        extra =

            # Read [Working on ghc-lib] for ghc-lib update instructions at
            # https://github.com/digital-asset/daml/blob/master/ghc-lib/working-on-ghc-lib.md.
            hazel_ghclibs(GHC_LIB_VERSION, "0000000000000000000000000000000000000000000000000000000000000000", "2433176141caffd066313876ef756e2fcb34dc96a809d40eec753a4248ba016e") +
            hazel_github_external("digital-asset", "hlint", "951fdb6d28d7eed8ea1c7f3be69da29b61fcbe8f", "f5fb4cf98cde3ecf1209857208369a63ba21b04313d570c41dffe9f9139a1d34") +
            # Not in stackage
            hazel_hackage(
                "proto3-wire",
                "1.1.0",
                "af5af81b8ced2cb21e81964ce13891b2474ba628ce343ca53dcd7ced17a51bb9",
            ) +
            # Not in stackage
            hazel_hackage(
                "proto3-suite",
                "0.4.0.0",
                "216fb8b5d92afc9df70512da2331e098e926239efd55e770802079c2a13bad5e",
            ) +
            # Not in stackage
            hazel_hackage(
                "grpc-haskell",
                "0.0.1.0",
                "acd048425e1717215db8323188c475c6f14fe2238b7d258b1eb46e8ed01381b2",
            ) +

            # Not in stackage
            hazel_hackage("bytestring-nums", "0.3.6", "bdca97600d91f00bb3c0f654784e3fbd2d62fcf4671820578105487cdf39e7cd") +
            # In Stackage but we want the latest version.
            hazel_hackage(
                "network",
                "2.8.0.1",
                "61f55dbfed0f0af721a8ea36079e9309fcc5a1be20783b44ae500d9e4399a846",
                patches = ["@ai_formation_hazel//third_party/haskell:network.patch"],
            ) +
            hazel_github_external(
                "alanz",
                "haskell-lsp",
                HASKELL_LSP_COMMIT,
                HASKELL_LSP_HASH,
            ) + hazel_github_external(
                "alanz",
                "haskell-lsp",
                HASKELL_LSP_COMMIT,
                HASKELL_LSP_HASH,
                name = "haskell-lsp-types",
                directory = "/haskell-lsp-types/",
            ) +
            # lsp-test’s cabal file relies on haskell-lsp reexporting haskell-lsp-types.
            # Hazel does not handle that for now, so we patch the cabal file
            # to add an explicit dependency on haskell-lsp-types.
            hazel_github_external(
                "digital-asset",
                "lsp-test",
                "95ef237e5b1c60385d20faefb5408f41908ad791",
                "1bdfb85dd2568025afec2c5565175f9897be1a52128f5470b350c0ddc164447d",
                patch_args = ["-p1"],
                patches = ["@com_github_digital_asset_daml//bazel_tools:haskell-lsp-test-no-reexport.patch"],
            ) + hazel_github_external(
                "mpickering",
                "hie-bios",
                "68c662ea1d0e7095ccf2a4e3d393fc524e769bfe",
                "065e54a01103c79c20a7e9ac3967cda9bdc027579bf1d0ca9a9d8023ff46dfb9",
                patch_args = ["-p1"],
                patches = ["@com_github_digital_asset_daml//bazel_tools:haskell-hie-bios.patch"],
            ) +
            hazel_hackage(
                "c2hs",
                "0.28.6",
                "91dd121ac565009f2fc215c50f3365ed66705071a698a545e869041b5d7ff4da",
                patch_args = ["-p1"],
                patches = ["@com_github_digital_asset_daml//bazel_tools:haskell-c2hs.patch"],
            ) + hazel_hackage(
                "bzlib-conduit",
                "0.3.0.2",
                "eb2c732b3d4ab5f7b367c51eef845e597ade19da52c03ee11954d35b6cfc4128",
                patch_args = ["-p1"],
                patches = ["@com_github_digital_asset_daml//3rdparty/haskell:bzlib-conduit.patch"],
            ),
        pkgs = packages,
    ),
)

hazel_custom_package_hackage(
    package_name = "ghc-paths",
    build_file = "@ai_formation_hazel//third_party/haskell:BUILD.ghc-paths",
    sha256 = "afa68fb86123004c37c1dc354286af2d87a9dcfb12ddcb80e8bd0cd55bc87945",
    version = "0.1.0.9",
)

hazel_custom_package_hackage(
    package_name = "clock",
    build_file = "//3rdparty/haskell:BUILD.clock",
    sha256 = "886601978898d3a91412fef895e864576a7125d661e1f8abc49a2a08840e691f",
    version = "0.7.2",
)

hazel_custom_package_hackage(
    package_name = "zlib",
    build_file = "//3rdparty/haskell:BUILD.zlib",
    sha256 = "0dcc7d925769bdbeb323f83b66884101084167501f11d74d21eb9bc515707fed",
    version = "0.6.2",
)

hazel_custom_package_hackage(
    package_name = "bindings-DSL",
    # Without a custom build file, packages depending on bindings-DSL
    # fail to find bindings.dsl.h.
    build_file = "//3rdparty/haskell:BUILD.bindings-DSL",
    sha256 = "63de32380c68d1cc5e9c7b3622d67832c786da21163ba0c8a4835e6dd169194f",
    version = "1.0.25",
)

hazel_custom_package_hackage(
    package_name = "streaming-commons",
    build_file = "//3rdparty/haskell:BUILD.streaming-commons",
    sha256 = "d8d1fe588924479ea7eefce8c6af77dfb373ee6bde7f4691bdfcbd782b36d68d",
    version = "0.2.1.0",
)

hazel_custom_package_github(
    package_name = "wai-app-static",
    build_file = "//3rdparty/haskell:BUILD.wai-app-static",
    github_repo = "wai",
    github_user = "nmattia-da",
    repo_sha = "05179164831432f207f3d43580c51161d519d191",
    strip_prefix = "wai-app-static",
)

GHCIDE_REV = "78aa9745798cfd730861e8c037cc481aa6b0dd43"

# We need a custom build file to depend on ghc-lib and ghc-lib-parser
hazel_custom_package_github(
    package_name = "ghcide",
    build_file = "//3rdparty/haskell:BUILD.ghcide",
    github_repo = "ghcide",
    github_user = "digital-asset",
    repo_sha = GHCIDE_REV,
)

load("//:bazel-haskell-deps.bzl", "daml_haskell_deps")

daml_haskell_deps()

load("//bazel_tools:java.bzl", "java_home_runtime")

java_home_runtime(name = "java_home")

# rules_go used here to compile a wrapper around the protoc-gen-scala plugin
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

nixpkgs_package(
    name = "go_nix",
    attribute_path = "go",
    build_file_content = """
    filegroup(
        name = "sdk",
        srcs = glob(["share/go/**"]),
        visibility = ["//visibility:public"],
    )
    """,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# A repository that generates the Go SDK imports, see
# ./bazel_tools/go_sdk/README.md
local_repository(
    name = "go_sdk_repo",
    path = "bazel_tools/go_sdk",
)

load("@io_bazel_rules_go//go:deps.bzl", "go_wrap_sdk")

# On Nix platforms we use the Nix provided Go SDK, on Windows we let Bazel pull
# an upstream one.
go_wrap_sdk(
    name = "go_sdk",
    root_file = "@go_nix//:share/go/ROOT",
) if not is_windows else None

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

# protoc-gen-doc repo
go_repository(
    name = "com_github_pseudomuto_protoc_gen_doc",
    commit = "0c4d666cfe1175663cf067963396a0b9b34f543f",
    importpath = "github.com/pseudomuto/protoc-gen-doc",
)

# protokit repo
go_repository(
    name = "com_github_pseudomuto_protokit",
    commit = "7037620bf27b13fcdc10b1b17ddef82540db670b",
    importpath = "github.com/pseudomuto/protokit",
)

load("//:bazel-java-deps.bzl", "install_java_deps")

install_java_deps()

load("@maven//:defs.bzl", "pinned_maven_install")

pinned_maven_install()

load(
    "@io_bazel_rules_scala//scala:scala.bzl",
    "scala_repositories",
)

scala_repositories((
    "2.12.6",
    {
        "scala_compiler": "3023b07cc02f2b0217b2c04f8e636b396130b3a8544a8dfad498a19c3e57a863",
        "scala_library": "f81d7144f0ce1b8123335b72ba39003c4be2870767aca15dd0888ba3dab65e98",
        "scala_reflect": "ffa70d522fc9f9deec14358aa674e6dd75c9dfa39d4668ef15bb52f002ce99fa",
    },
))

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

scala_register_toolchains()

load("@io_bazel_rules_scala//jmh:jmh.bzl", "jmh_repositories")

jmh_repositories()

dev_env_tool(
    name = "nodejs_dev_env",
    nix_include = [
        "bin",
        "include",
        "lib",
        "share",
    ],
    nix_label = "@node_nix",
    nix_paths = [],
    prefix = "nodejs_dev_env",
    tools = [],
    win_include = [
        ".",
    ],
    win_paths = [],
    win_tool = "nodejs-10.16.3",
)

# Setup the Node.js toolchain
load("@build_bazel_rules_nodejs//:defs.bzl", "node_repositories", "yarn_install")

node_repositories(
    package_json = ["//:package.json"],
    vendored_node = "@nodejs_dev_env",
)

yarn_install(
    name = "npm",
    package_json = "//:package.json",
    yarn_lock = "//:yarn.lock",
)

# Install all Bazel dependencies of the @npm packages
load("@npm//:install_bazel_dependencies.bzl", "install_bazel_dependencies")

install_bazel_dependencies()

load("@npm_bazel_typescript//:defs.bzl", "ts_setup_workspace")

ts_setup_workspace()

# TODO use fine-grained managed dependency
yarn_install(
    name = "daml_extension_deps",
    package_json = "//compiler/daml-extension:package.json",
    yarn_lock = "//compiler/daml-extension:yarn.lock",
)

# TODO use fine-grained managed dependency
yarn_install(
    name = "navigator_frontend_deps",
    package_json = "//navigator/frontend:package.json",
    yarn_lock = "//navigator/frontend:yarn.lock",
)

# Bazel Skydoc - Build rule documentation generator
load("@io_bazel_rules_sass//:package.bzl", "rules_sass_dependencies")

rules_sass_dependencies()

load("@io_bazel_rules_sass//:defs.bzl", "sass_repositories")

sass_repositories()

load("@io_bazel_skydoc//skylark:skylark.bzl", "skydoc_repositories")

skydoc_repositories()

# We usually use the _deploy_jar target to produce self-contained jars, but here we're using jar_jar because the size
# of codegen tool is substantially reduced (as shown below) and that the presence of JVM internal com.sun classes could
# theoretically stop the codegen running against JVMs other the OpenJDK 8 (the current JVM used for building).
load("@com_github_johnynek_bazel_jar_jar//:jar_jar.bzl", "jar_jar_repositories")

jar_jar_repositories()

# The following is advertised by rules_proto, but we define our own dependencies
# in dependencies.yaml. So all we need to do is replicate the binds here
# https://github.com/stackb/rules_proto/tree/master/java#java_grpc_library

# load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")
# grpc_java_repositories()

# Load the grpc deps last, since it won't try to load already loaded
# dependencies.
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@upb//bazel:workspace_deps.bzl", "upb_deps")

upb_deps()

load("@build_bazel_rules_apple//apple:repositories.bzl", "apple_rules_dependencies")

apple_rules_dependencies()

load("@com_github_bazelbuild_buildtools//buildifier:deps.bzl", "buildifier_dependencies")

buildifier_dependencies()

nixpkgs_package(
    name = "python3_nix",
    attribute_path = "python3",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

register_toolchains("//:nix_python_toolchain") if not is_windows else None

nixpkgs_package(
    name = "postgresql_nix",
    attribute_path = "postgresql_9_6",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "postgresql_dev_env",
    nix_include = [
        "bin",
        "include",
        "lib",
        "share",
    ],
    nix_label = "@postgresql_nix",
    nix_paths = [
        "bin/initdb",
        "bin/createdb",
        "bin/pg_ctl",
        "bin/postgres",
    ],
    tools = [
        "createdb",
        "initdb",
        "pg_ctl",
        "postgresql",
    ],
    win_include = [
        "mingw64/bin",
        "mingw64/include",
        "mingw64/lib",
        "mingw64/share",
    ],
    win_include_as = {
        "mingw64/bin": "bin",
        "mingw64/include": "include",
        "mingw64/lib": "lib",
        "mingw64/share": "share",
    },
    win_paths = [
        "bin/initdb.exe",
        "bin/createdb.exe",
        "bin/pg_ctl.exe",
        "bin/postgres.exe",
    ],
    win_tool = "msys2",
)

http_archive(
    name = "canton",
    build_file_content = """
package(default_visibility = ["//visibility:public"])

java_import(
    name = "lib",
    jars = glob(["lib/**"]),
)
""",
    sha256 = "e475dd93a165bd8299eb67684e99c7c5a82dc149cd92829a2103768cff1214ec",
    strip_prefix = "canton-0.4.1",
    urls = ["https://www.canton.io/releases/canton-0.4.1.tar.gz"],
)
