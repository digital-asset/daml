workspace(
    name = "com_github_digital_asset_daml",
)

# NOTE(JM): Load external dependencies from deps.bzl.
# Do not put "http_archive" and similar rules into this file. Put them into
# deps.bzl. This allows using this repository as an external workspace.
# (though with the caveat that that user needs to repeat the relevant bits of
#  magic in this file, but at least right versions of external rules are picked).
load("//:deps.bzl", "daml_deps")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

daml_deps()

load("@rules_haskell//haskell:repositories.bzl", "rules_haskell_dependencies")
load("@com_github_bazelbuild_remote_apis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "bazel_remote_apis_imports",
)

rules_haskell_dependencies()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

register_toolchains(
    "//:c2hs-toolchain",
)

load("//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dadew", "dev_env_tool")
load(
    "@io_tweag_rules_nixpkgs//nixpkgs:nixpkgs.bzl",
    "nixpkgs_cc_configure",
    "nixpkgs_local_repository",
    "nixpkgs_package",
    "nixpkgs_python_configure",
)
load("//bazel_tools:create_workspace.bzl", "create_workspace")
load("//bazel_tools:os_info.bzl", "os_info")

os_info(name = "os_info")

load("//bazel_tools:build_environment.bzl", "build_environment")

build_environment(name = "build_environment")

load("//bazel_tools:oracle.bzl", "oracle_configure")

oracle_configure(name = "oracle")

load("//bazel_tools:scala_version.bzl", "scala_version_configure")

scala_version_configure(name = "scala_version")

load(
    "@scala_version//:index.bzl",
    "scala_artifacts",
    "scala_major_version",
    "scala_major_version_suffix",
    "scala_version",
)

dadew(name = "dadew")

load("@os_info//:os_info.bzl", "is_darwin", "is_linux", "is_windows")
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
        "//nix:system.nix",
    ],
)

load("//nix:repositories.bzl", "common_nix_file_deps", "dev_env_nix_repos")
load("//bazel_tools:damlc_legacy.bzl", "damlc_legacy")

damlc_legacy(
    name = "damlc_legacy",
    sha256 = {
        "linux": "dd1c7f2d34f3eac631c7edc1637c9b3e93c341561d41828b4f0d8e897effa90f",
        "windows": "f458b8d2612887915372aad61766120e34c0fdc6a65eb37cdb1a8efc58e14de3",
        "macos": "63141d7168e883c0b8c212dca6198f5463f82aa82bbbc51d8805ce7e474300e4",
    },
    version = "1.18.0-snapshot.20211117.8399.0.a05a40ae",
)

# Use Nix provisioned cc toolchain
nixpkgs_cc_configure(
    nix_file = "//nix:bazel-cc-toolchain.nix",
    nix_file_deps = common_nix_file_deps + [
        "//nix:tools/bazel-cc-toolchain/default.nix",
    ],
    repositories = dev_env_nix_repos,
) if not is_windows else None

nixpkgs_python_configure(repository = "@nixpkgs") if not is_windows else None

# Curl system dependency
nixpkgs_package(
    name = "curl_nix",
    attribute_path = "curl",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# Sysctl system dependency
nixpkgs_package(
    name = "sysctl_nix",
    attribute_path = "sysctl",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# Toxiproxy dependency
nixpkgs_package(
    name = "toxiproxy_nix",
    attribute_path = "toxiproxy",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "toxiproxy_dev_env",
    nix_include = ["bin/toxiproxy-cmd"],
    nix_label = "@toxiproxy_nix",
    nix_paths = ["bin/toxiproxy-cmd"],
    tools = ["toxiproxy"],
    win_include = ["toxiproxy-server-windows-amd64.exe"],
    win_paths = ["toxiproxy-server-windows-amd64.exe"],
    win_tool = "toxiproxy",
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

nixpkgs_package(
    name = "openssl_nix",
    attribute_path = "openssl",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "openssl_dev_env",
    nix_include = ["bin/openssl"],
    nix_label = "@openssl_nix",
    nix_paths = ["bin/openssl"],
    tools = ["openssl"],
    win_include = [
        "usr/bin",
        "usr/ssl",
    ],
    win_paths = ["usr/bin/openssl.exe"],
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

nixpkgs_package(
    name = "patch_nix",
    attribute_path = "gnupatch",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

dev_env_tool(
    name = "patch_dev_env",
    nix_include = ["bin/patch"],
    nix_label = "@patch_nix",
    nix_paths = ["bin/patch"],
    tools = ["patch"],
    win_include = ["usr/bin/patch.exe"],
    win_paths = ["usr/bin/patch.exe"],
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
    nix_file_deps = common_nix_file_deps,
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

nixpkgs_package(
    name = "jekyll_nix",
    attribute_path = "jekyll",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
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

# This is used to get ghc-pkg on Linux.
nixpkgs_package(
    name = "ghc_nix",
    attribute_path = "ghc",
    build_file_content = """
package(default_visibility = ["//visibility:public"])
exports_files(glob(["lib/**/*"]))
""",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
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
    attribute_path = "ghcDwarf" if enable_ghc_dwarf else "ghc",
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
    ] + (["-g3"] if enable_ghc_dwarf else ([
        "-optl-unexported_symbols_list=*",
        "-optc-mmacosx-version-min=10.14",
        "-opta-mmacosx-version-min=10.14",
        "-optl-mmacosx-version-min=10.14",
    ] if is_darwin else ["-optl-s"])),
    compiler_flags_select = {
        "@com_github_digital_asset_daml//:profiling_build": ["-fprof-auto"],
        "//conditions:default": [],
    },
    locale_archive = "@glibc_locales//:locale-archive",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repl_ghci_args = [
        "-O0",
        "-fexternal-interpreter",
        "-Wwarn",
    ],
    repositories = dev_env_nix_repos,
    version = "9.0.2",
)

# Used by Windows
haskell_register_ghc_bindists(
    compiler_flags = common_ghc_flags,
    version = "9.0.2",
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
    attribute_path = "nodejsNested",
    build_file_content = 'exports_files(glob(["node_nix/**"]))',
    fail_not_supported = False,
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
    attribute_path = "sphinx-exts",
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
    attribute_path = "scala_{}".format(scala_major_version_suffix),
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

http_archive(
    name = "static_asset_d3plus",
    build_file_content = 'exports_files(["js/d3.min.js", "js/d3plus.min.js"])',
    sha256 = "7d31a500a4850364a966ac938eea7f2fa5ce1334966b52729079490636e7049a",
    strip_prefix = "d3plus.v1.9.8",
    type = "zip",
    urls = ["https://github.com/alexandersimoes/d3plus/releases/download/v1.9.8/d3plus.zip"],
)

load("//:bazel-haskell-deps.bzl", "daml_haskell_deps")

daml_haskell_deps()

load("@rules_haskell//tools:repositories.bzl", "rules_haskell_worker_dependencies")

# We don't use the worker mode, but this is required for bazel query to function.
# Call this after `daml_haskell_deps` to ensure that the right `stack` is used.
rules_haskell_worker_dependencies()

load("//bazel_tools:java.bzl", "dadew_java_configure", "nixpkgs_java_configure")

dadew_java_configure(
    name = "dadew_java_runtime",
    dadew_path = "ojdkbuild11",
) if is_windows else None

nixpkgs_java_configure(
    attribute_path = "jdk11.home",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
) if not is_windows else None

# rules_go used here to compile a wrapper around the protoc-gen-scala plugin
load("@io_tweag_rules_nixpkgs//nixpkgs:toolchains/go.bzl", "nixpkgs_go_configure")

nixpkgs_go_configure(
    nix_file = "//nix:bazel-go-toolchain.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
) if not is_windows else None

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains")

go_register_toolchains(version = "1.16.9") if is_windows else None

# gazelle:repo bazel_gazelle
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("//:go_deps.bzl", "go_deps")

# gazelle:repository_macro go_deps.bzl%go_deps
go_deps()

load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies")

go_rules_dependencies()

gazelle_dependencies()

load("@go_googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
    grpc = True,
    java = True,
)

load("//:bazel-java-deps.bzl", "install_java_deps")

install_java_deps()

load("@maven//:defs.bzl", "pinned_maven_install")

pinned_maven_install()

load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")

scala_config(scala_version)

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")

scala_repositories(
    fetch_sources = True,
    overriden_artifacts = scala_artifacts,
)

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

register_toolchains("//bazel_tools/scala:toolchain")

load("@io_bazel_rules_scala//testing:scalatest.bzl", "scalatest_repositories", "scalatest_toolchain")

scalatest_repositories()

scalatest_toolchain()

load("//bazel_tools:scalapb.bzl", "scalapb_version")

http_archive(
    name = "scalapb",
    build_file_content = """
proto_library(
    name = "scalapb_proto",
    srcs = ["protobuf/scalapb/scalapb.proto"],
    strip_import_prefix = "protobuf/",
    deps = [
        "@com_google_protobuf//:descriptor_proto",
    ],
    visibility = ["//visibility:public"],
)
""",
    sha256 = "2ddce4c5927fa8dd80069fba2fb60199f5b2b95e81e8da69b132665fae6c638c",
    strip_prefix = "ScalaPB-{}".format(scalapb_version),
    urls = ["https://github.com/scalapb/ScalaPB/archive/refs/tags/v{}.tar.gz".format(scalapb_version)],
)

load("@io_bazel_rules_scala//jmh:jmh.bzl", "jmh_repositories")

jmh_repositories()

# TODO (aherrmann) This wrapper is only used on Windows.
#   Replace by an appropriate Windows only `dadew_tool` call.
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
    win_tool = "nodejs",
)

# Setup the Node.js toolchain
load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "yarn_install")

node_repositories(
    # Using `dev_env_tool` introduces an additional layer of symlink
    # indirection. Bazel doesn't track dependencies through symbolic links.
    # Occasionally, this can cause build failures on CI if a build is not
    # invalidated despite a change of an original source. To avoid such issues
    # we use the `nixpkgs_package` directly.
    node_version = "16.13.0",
    package_json = ["//:package.json"],
    vendored_node = "@nodejs_dev_env" if is_windows else "@node_nix",
)

yarn_install(
    name = "npm",
    args = ["--frozen-lockfile"],
    package_json = "//:package.json",
    symlink_node_modules = False,
    yarn_lock = "//:yarn.lock",
)

# TODO use fine-grained managed dependency
yarn_install(
    name = "daml_extension_deps",
    args = ["--frozen-lockfile"],
    package_json = "//compiler/daml-extension:package.json",
    symlink_node_modules = False,
    yarn_lock = "//compiler/daml-extension:yarn.lock",
)

# TODO use fine-grained managed dependency
yarn_install(
    name = "navigator_frontend_deps",
    args = ["--frozen-lockfile"],
    package_json = "//navigator/frontend:package.json",
    symlink_node_modules = False,
    yarn_lock = "//navigator/frontend:yarn.lock",
)

# We’ve had a bunch of problems with typescript rules on Windows.
# Therefore we’ve disabled them completely for now.
# Since we need to @load stuff in @language_support_ts_deps
# and load statements can’t be conditional, we create a dummy
# workspace on Windows.
# See #4162 for more details.
yarn_install(
    name = "language_support_ts_deps",
    args = ["--frozen-lockfile"],
    package_json = "//language-support/ts/packages:package.json",
    symlink_node_modules = False,
    yarn_lock = "//language-support/ts/packages:yarn.lock",
) if not is_windows else create_workspace(
    name = "language_support_ts_deps",
    files = {
        "eslint/BUILD.bazel": 'exports_files(["index.bzl"])',
        "eslint/index.bzl": "def eslint_test(*args, **kwargs):\n    pass",
        "jest-cli/BUILD.bazel": 'exports_files(["index.bzl"])',
        "jest-cli/index.bzl": "def jest_test(*args, **kwargs):\n    pass",
        "@bazel/typescript/BUILD.bazel": 'exports_files(["index.bzl"])',
        "@bazel/typescript/index.bzl": "def ts_project(*args, **kwargs):\n    pass",
    },
)

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
    name = "postgresql_nix",
    attribute_path = "postgresql_10",
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
        "bin/createdb",
        "bin/dropdb",
        "bin/initdb",
        "bin/pg_ctl",
        "bin/postgres",
    ],
    required_tools = {
        "initdb": ["postgres"],
        "pg_ctl": ["postgres"],
    },
    tools = [
        "createdb",
        "dropdb",
        "initdb",
        "pg_ctl",
        "postgres",
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
        "bin/createdb.exe",
        "bin/dropdb.exe",
        "bin/initdb.exe",
        "bin/pg_ctl.exe",
        "bin/postgres.exe",
    ],
    win_tool = "msys2",
)

nixpkgs_package(
    name = "buf",
    attribute_path = "buf",
    fail_not_supported = False,
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

nixpkgs_package(
    name = "script_nix",
    attribute_path = "script",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
) if not is_windows else None
