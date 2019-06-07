workspace(name = "com_github_digital_asset_daml")

load("//:util.bzl", "hazel_ghclibs", "hazel_github", "hazel_github_external", "hazel_hackage")

# NOTE(JM): Load external dependencies from deps.bzl.
# Do not put "http_archive" and similar rules into this file. Put them into
# deps.bzl. This allows using this repository as an external workspace.
# (though with the caviat that that user needs to repeat the relevant bits of
#  magic in this file, but at least right versions of external rules are picked).
load("//:deps.bzl", "daml_deps")

daml_deps()

load("@io_tweag_rules_haskell//haskell:repositories.bzl", "haskell_repositories")

haskell_repositories()

register_toolchains(
    "//:c2hs-toolchain",
)

load("//bazel_tools/dev_env_package:dev_env_package.bzl", "dev_env_package")
load("//bazel_tools/dev_env_package:dev_env_tool.bzl", "dev_env_tool")
load(
    "@io_tweag_rules_nixpkgs//nixpkgs:nixpkgs.bzl",
    "nixpkgs_cc_configure",
    "nixpkgs_local_repository",
    "nixpkgs_package",
)
load("//bazel_tools:os_info.bzl", "os_info")

os_info(name = "os_info")

load("@os_info//:os_info.bzl", "is_linux", "is_windows")

nixpkgs_local_repository(
    name = "nixpkgs",
    nix_file = "//nix:nixpkgs.nix",
    nix_file_deps = [
        "//nix:nixpkgs/nixos-19.03/default.nix",
        "//nix:nixpkgs/nixos-19.03/default.src.json",
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
    "//nix:nixpkgs/nixos-19.03/default.nix",
    "//nix:nixpkgs/nixos-19.03/default.src.json",
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
    nix_path = "bin/tar",
    tool = "tar",
    win_include = ["usr/bin/tar.exe"],
    win_path = "usr/bin/tar.exe",
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
    nix_path = "bin/gzip",
    tool = "gzip",
    win_include = ["usr/bin/gzip.exe"],
    win_path = "usr/bin/gzip.exe",
    win_tool = "msys2",
)

dev_env_tool(
    name = "mvn_dev_env",
    nix_include = ["bin/mvn"],
    nix_label = "@mvn_nix",
    nix_path = "bin/mvn",
    tool = "mvn",
    win_include = [
        "bin",
        "boot",
        "conf",
        "lib",
    ],
    win_path = "bin/mvn",
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
    nix_path = "bin/zip",
    tool = "zip",
    win_include = ["usr/bin/zip.exe"],
    win_path = "usr/bin/zip.exe",
    win_tool = "msys2",
)

load(
    "@io_tweag_rules_haskell//haskell:haskell.bzl",
    "haskell_register_ghc_bindists",
)
load(
    "@io_tweag_rules_haskell//haskell:nixpkgs.bzl",
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

# Used by Darwin and Linux
haskell_register_ghc_nixpkgs(
    attribute_path = "ghcStatic",
    build_file = "@io_tweag_rules_nixpkgs//nixpkgs:BUILD.pkg",

    # -fexternal-dynamic-refs is required so that we produce position-independent
    # relocations against some functions (-fPIC alone isn’t sufficient).

    # -split-sections would allow us to produce significantly smaller binaries, e.g., for damlc,
    # the binary shrinks from 186MB to 83MB. -split-sections only works on Linux but
    # we get a similar behavior on Darwin by default.
    # However, we had to disable split-sections for now as it seems to interact very badly
    # with the GHCi linker to the point where :main takes several minutes rather than several seconds.
    compiler_flags = [
        "-O1",
        "-fexternal-dynamic-refs",
        "-hide-package=ghc-boot-th",
        "-hide-package=ghc-boot",
    ],
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
    nix_path = "bin/jq",
    tool = "jq",
    win_include = ["mingw64/bin"],
    win_include_as = {"mingw64/bin": "bin"},
    win_path = "bin/jq.exe",
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
    nix_file_deps = common_nix_file_deps + [
        "//nix:overrides/sass/default.nix",
        "//nix:overrides/sass/Gemfile",
        "//nix:overrides/sass/Gemfile.lock",
        "//nix:overrides/sass/gemset.nix",
    ],
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
    nix_file_deps = common_nix_file_deps + [
        "//nix:tools/sphinx183/default.nix",
    ],
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
    nix_path = "bin/javadoc",
    tool = "javadoc",
    win_include = [
        "bin",
        "include",
        "jre",
        "lib",
    ],
    win_path = "bin/javadoc.exe",
    win_tool = "java-openjdk-8u201",
)

# This only makes sense on Windows so we just put dummy values in the nix fields.
dev_env_tool(
    name = "makensis_dev_env",
    nix_include = [""],
    nix_path = "bin/makensis.exe",
    tool = "makensis",
    win_include = [
        "bin",
        "contrib",
        "include",
        "plugins",
        "stubs",
    ],
    win_path = "bin/makensis.exe",
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

HASKELL_LSP_COMMIT = "1deb1eae5dd6851510d5d66e57bda7d27365d00c"

HASKELL_LSP_HASH = "8450b1f0872a6fc590492ef9b63c565d1424bc9eea193c62207292767338513a"

hazel_repositories(
    core_packages = dicts.add(
        core_packages,
        {
            "integer-simple": "0.1.1.1",

            # this is a core package, but not reflected in hazel/packages.bzl.
            "haskeline": "0.7.4.2",
            "Win32": "2.6.1.0",
        },
    ),
    exclude_packages = [
        "arx",
        "clock",
        "ghc-paths",
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
    },
    extra_libs = dicts.add(
        hazel_default_extra_libs,
        {
            "z": "@com_github_madler_zlib//:z",
            "ffi": "" if is_windows else "@libffi_nix//:ffi",
        },
    ),
    ghc_workspaces = {
        "k8": "@io_tweag_rules_haskell_ghc_nixpkgs",
        "darwin": "@io_tweag_rules_haskell_ghc_nixpkgs",
        # although windows is not quite supported yet
        "x64_windows": "@io_tweag_rules_haskell_ghc_windows_amd64",
    },
    packages = add_extra_packages(
        extra =
            # Read [Working on ghc-lib] for ghc-lib update instructions at
            # https://github.com/DACH-NY/daml/blob/master/ghc-lib/working-on-ghc-lib.md
            hazel_ghclibs("0.20190604.1", "283372061a51a6524f2c2940dc328985317cb5134a2beb8fd2a847a9e6e157d5", "522ab61c7fe386e8c8c2d396d063bc5b690e33b056defbb8304141890ec6786e") +
            hazel_github_external("awakesecurity", "proto3-wire", "43d8220dbc64ef7cc7681887741833a47b61070f", "1c3a7fbf4ab3308776675c6202583f9750de496757f3ad4815e81edd122d75e1") +
            hazel_github_external("awakesecurity", "proto3-suite", "dd01df7a3f6d0f1ea36125a67ac3c16936b53da0", "59ea7b876b14991347918eefefe24e7f0e064b5c2cc14574ac4ab5d6af6413ca") +
            hazel_hackage("happy", "1.19.10", "22eb606c97105b396e1c7dc27e120ca02025a87f3e44d2ea52be6a653a52caed") +
            hazel_hackage("bytestring-nums", "0.3.6", "bdca97600d91f00bb3c0f654784e3fbd2d62fcf4671820578105487cdf39e7cd") +
            hazel_hackage("semver", "0.3.4", "42dbdacb08f30ac8bf2f014981cb080737f793b89d57626cb7e2ab8c3d768e6b") +
            hazel_hackage(
                "network",
                "2.8.0.0",
                "c8905268b7e3b4cf624a40245bf11b35274a6dd836a5d4d531b5760075645303",
                patches = ["@ai_formation_hazel//third_party/haskell:network.patch"],
            ) +
            hazel_hackage("zip-archive", "0.3.3", "988adee77c806e0b497929b24d5526ea68bd3297427da0d0b30b99c094efc84d") +
            hazel_hackage("terminal-progress-bar", "0.4.0.1", "c5a9720fcbcd9d83f9551e431ee3975c61d7da6432aa687aef0c0e04e59ae277") +
            hazel_hackage("rope-utf16-splay", "0.3.1.0", "cbf878098355441ed7be445466fcb72d45390073a298b37649d762de2a7f8cc6") +
            hazel_hackage("unix-compat", "0.5.1", "a39d0c79dd906763770b80ba5b6c5cb710e954f894350e9917de0d73f3a19c52") +
            # This corresponds to our custom-methods branch that adds support for custom RPC methods
            # like daml/keepAlive. Once the corresponding PR has been merged
            # https://github.com/alanz/haskell-lsp/pull/171 we can switch back to upstream.
            hazel_github(
                "haskell-lsp",
                HASKELL_LSP_COMMIT,
                HASKELL_LSP_HASH,
            ) +
            hazel_github(
                "haskell-lsp",
                HASKELL_LSP_COMMIT,
                HASKELL_LSP_HASH,
                name = "haskell-lsp-types",
                directory = "/haskell-lsp-types/",
            ) +
            # This corresponds to our custom-methods branch which makes
            # lsp-test work with our custom-methods of haskell-lsp.
            hazel_github(
                "lsp-test",
                "a325a6860f38c9f533c33f5b37ee9b73580ae68b",
                "5553d30607ab8942c5aa77c4de08a0dafce345f5bf70b0cba4e47af4d55ba6b3",
            ),
        pkgs = packages,
    ),
)

hazel_custom_package_hackage(
    package_name = "ghc-paths",
    build_file = "@ai_formation_hazel//third_party/haskell:BUILD.ghc-paths",
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

hazel_custom_package_github(
    package_name = "arx",
    build_file = "//3rdparty/haskell:BUILD.arx",
    github_repo = "arx",
    github_user = "solidsnack",
    patch_args = ["-p1"],
    patches = ["@com_github_digital_asset_daml//bazel_tools:haskell-arx.patch"],
    repo_sha = "7561fed76bb613302d1ae104f0eb2ad13daa9fac",
)

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

nixpkgs_package(
    name = "libffi_nix",
    attribute_path = "libffi.dev",
    build_file_content = """
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "include",
    srcs = glob(["include/**/*.h"]),
)

cc_library(
    name = "ffi",
    hdrs = [":include"],
    strip_include_prefix = "include",
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

dev_env_package(
    name = "nodejs_dev_env",
    nix_label = "@node_nix",
    symlink_path = "nodejs_dev_env",
    win_tool = "nodejs-10.12.0",
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

# Setup TypeScript toolchain
load("@build_bazel_rules_typescript//:defs.bzl", "ts_setup_workspace")

ts_setup_workspace()

# TODO use fine-grained managed dependency
yarn_install(
    name = "daml_extension_deps",
    package_json = "//daml-foundations/daml-tools/daml-extension:package.json",
    yarn_lock = "//daml-foundations/daml-tools/daml-extension:yarn.lock",
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

load("//3rdparty:workspace.bzl", "maven_dependencies")

maven_dependencies()

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

load("@com_github_bazelbuild_buildtools//buildifier:deps.bzl", "buildifier_dependencies")

buildifier_dependencies()
