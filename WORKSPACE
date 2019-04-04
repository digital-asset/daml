workspace(name = "com_github_digital_asset_daml")

load("//:deps.bzl", "daml_deps")
daml_deps()

load("@io_tweag_rules_haskell//haskell:repositories.bzl", "haskell_repositories")
haskell_repositories()
register_toolchains(
  "//:c2hs-toolchain"
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

load("//bazel_tools/dev_env_package:dev_env_package.bzl", "dev_env_package")
load("//bazel_tools/dev_env_package:dev_env_tool.bzl", "dev_env_tool")

load(
  "@io_tweag_rules_nixpkgs//nixpkgs:nixpkgs.bzl",
  "nixpkgs_local_repository", "nixpkgs_package", "nixpkgs_cc_configure",
)

load("//bazel_tools:os_info.bzl", "os_info")

os_info(name = "os_info")

load("@os_info//:os_info.bzl", "is_linux", "is_windows")


nixpkgs_local_repository(
    name = "nixpkgs",
    nix_file = "//nix:nixpkgs.nix",
    nix_file_deps = [
      "//nix:nixpkgs/nixos-18.09/default.nix",
      "//nix:nixpkgs/nixos-18.09/default.src.json",
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
    "//nix:nixpkgs/nixos-18.09/default.nix",
    "//nix:nixpkgs/nixos-18.09/default.src.json",
]

# Use Nix provisioned cc toolchain
nixpkgs_cc_configure(
    nix_file = "//nix:bazel-cc-toolchain.nix",
    repositories = dev_env_nix_repos,
    nix_file_deps = common_nix_file_deps + [
        "//nix:bazel-cc-toolchain.nix",
    ],
)

# Curl system dependency
nixpkgs_package(
    name = 'curl_nix',
    attribute_path = 'curl',
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# Patchelf system dependency
nixpkgs_package(
    name = 'patchelf_nix',
    attribute_path = 'patchelf',
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)

# Tar & gzip dependency
nixpkgs_package(
    name = 'tar_nix',
    attribute_path = 'gnutar',
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
    fail_not_supported = False,
)
dev_env_tool(
  name = "tar_dev_env",
  tool = "tar",
  win_tool = "msys2-20180531",
  win_include = ["usr/bin/tar.exe"],
  win_path = "usr/bin/tar.exe",
  nix_label = "@tar_nix",
  nix_include = ["bin/tar"],
  nix_path = "bin/tar",
)

nixpkgs_package(
    name = 'gzip_nix',
    attribute_path = 'gzip',
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
    fail_not_supported = False,
)
dev_env_tool(
  name = "gzip_dev_env",
  tool = "gzip",
  win_tool = "msys2-20180531",
  win_include = ["usr/bin/gzip.exe"],
  win_path = "usr/bin/gzip.exe",
  nix_label = "@gzip_nix",
  nix_include = ["bin/gzip"],
  nix_path = "bin/gzip",
)

nixpkgs_package(
    name = 'awk_nix',
    attribute_path = 'gawk',
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
)
nixpkgs_package(
    name = 'hlint_nix',
    attribute_path = 'hlint',
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps + [
        "//nix:overrides/hlint-2.1.15.nix",
        "//nix:overrides/haskell-src-exts-1.21.0.nix",
    ],
    repositories = dev_env_nix_repos,
)

nixpkgs_package(
    name = 'zip_nix',
    attribute_path = 'zip',
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
    fail_not_supported = False,
)

dev_env_tool(
  name = "zip_dev_env",
  tool = "zip",
  win_tool = "msys2-20180531",
  win_include = ["usr/bin/zip.exe"],
  win_path = "usr/bin/zip.exe",
  nix_label = "@zip_nix",
  nix_include = ["bin/zip"],
  nix_path = "bin/zip",
)

# c2hs
nixpkgs_package(
  name = "c2hs",
  repositories = dev_env_nix_repos,
  attribute_path = "ghcWithC2hs",
  nix_file = "//nix:bazel.nix",
  nix_file_deps = common_nix_file_deps + [
    "//nix:ghc.nix",
    "//nix:with-packages-wrapper.nix",
    "//nix:overrides/ghc-8.6.4.nix",
    "//nix:overrides/c2hs-0.28.6.nix",
    "//nix:overrides/ghc-8.6.3-binary.nix",
    "//nix:overrides/language-c-0.8.2.nix",
  ],
  build_file_content = '''

package(default_visibility = [ "//visibility:public" ])

filegroup(
    name = "bin",
    srcs = ["bin/c2hs"],
)
  '''
)

load(
    "@io_tweag_rules_haskell//haskell:haskell.bzl",
    "haskell_register_ghc_bindists",
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

# Used by Darwin and Linux
haskell_register_ghc_nixpkgs(
  version = "8.6.4",
  build_file = "@io_tweag_rules_haskell//haskell:ghc.BUILD",
  attribute_path = "ghcWithC2hs",
  nix_file = "//nix:bazel.nix",
  nix_file_deps = common_nix_file_deps + [
    "//nix:ghc.nix",
    "//nix:with-packages-wrapper.nix",
    "//nix:overrides/ghc-8.6.4.nix",
    "//nix:overrides/c2hs-0.28.6.nix",
    "//nix:overrides/ghc-8.6.3-binary.nix",
    "//nix:overrides/language-c-0.8.2.nix",
  ],
  locale_archive = "@glibc_locales//:locale-archive",
  repositories = dev_env_nix_repos,

  # -fexternal-dynamic-refs is required so that we produce position-independent
  # relocations against some functions (-fPIC alone isn’t sufficient).

  # -split-sections would allow us to produce significantly smaller binaries, e.g., for damlc,
  # the binary shrinks from 186MB to 83MB. -split-sections only works on Linux but
  # we get a similar behavior on Darwin by default.
  # However, we had to disable split-sections for now as it seems to interact very badly
  # with the GHCi linker to the point where :main takes several minutes rather than several seconds.
  compiler_flags = ["-O1", "-fexternal-dynamic-refs"],
  repl_ghci_args = ["-O0", "-fexternal-interpreter"],
)

# Used by Windows
haskell_register_ghc_bindists(
    version = "8.6.4",
) if is_windows else None

nixpkgs_package(
  name = "jq",
  attribute_path = "jq",
  nix_file = "//nix:bazel.nix",
  nix_file_deps = common_nix_file_deps,
  repositories = dev_env_nix_repos,
)

#node & npm
nixpkgs_package(
  name = "node_nix",
  attribute_path = "nodejs",
  nix_file = "//nix:bazel.nix",
  nix_file_deps = common_nix_file_deps,
  repositories = dev_env_nix_repos,
  fail_not_supported = False
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

#Pandoc
nixpkgs_package(
  name = "pandoc_nix",
  attribute_path = "pandoc",
  nix_file = "//nix:bazel.nix",
  nix_file_deps = common_nix_file_deps,
  repositories = dev_env_nix_repos,
)

#Javadoc
nixpkgs_package(
  name = "jdk_nix",
  attribute_path = "jdk8",
  nix_file = "//nix:bazel.nix",
  nix_file_deps = common_nix_file_deps,
  repositories = dev_env_nix_repos,
  fail_not_supported = False,
)
# This will not be needed after merge of the PR to bazel adding proper javadoc filegroups:
# https://github.com/bazelbuild/bazel/pull/7898
# `@javadoc_dev_env//:javadoc` could be then replaced with `@local_jdk//:javadoc` and the below removed
dev_env_tool(
  name = "javadoc_dev_env",
  tool = "javadoc",
  win_tool = "java-openjdk-8u201",
  win_include = ["bin", "include", "jre", "lib"],
  win_path = "bin/javadoc.exe",
  nix_label = "@jdk_nix",
  nix_include = ["bin/javadoc"],
  nix_path = "bin/javadoc",
)

# Dummy target //external:python_headers.
# To avoid query errors due to com_google_protobuf.
# See https://github.com/protocolbuffers/protobuf/blob/d9ccd0c0e6bbda9bf4476088eeb46b02d7dcd327/util/python/BUILD
bind(
    name = "python_headers",
    actual = "@com_google_protobuf//util/python:python_headers",
)

load("@ai_formation_hazel//:hazel.bzl", "hazel_repositories", "hazel_custom_package_github", "hazel_custom_package_hackage")
load("//hazel:packages.bzl", "core_packages", "packages")

load("//bazel_tools:haskell.bzl", "add_extra_packages")

# XXX: We do not have access to an integer-simple version of GHC on Windows.
# For the time being we build with GMP. See https://github.com/digital-asset/daml/issues/106
use_integer_simple = not is_windows

hazel_repositories(
  core_packages = core_packages + {
    "integer-simple": "0.1.1.1",

    # this is a core package, but not reflected in hazel/packages.bzl.
    "haskeline": "0.7.4.2",


    "Win32": "2.6.1.0",
  },
  packages = add_extra_packages(
      pkgs = packages,
      extra =
        [ # Read [Working on ghc-lib] for ghc-lib update instructions at
          # https://github.com/DACH-NY/daml/blob/master/ghc-lib/working-on-ghc-lib.md
          ("ghc-lib-parser", {"url": "https://digitalassetsdk.bintray.com/ghc-lib/ghc-lib-parser-0.20190409.tar.gz", "stripPrefix": "ghc-lib-parser-0.20190409", "sha256": "53d86b741a64c6ef41fe2635583bf3bdf288aee22006cb0e0395a955650f1d6e"})
        , ("ghc-lib", {"url": "https://digitalassetsdk.bintray.com/ghc-lib/ghc-lib-0.20190409.tar.gz", "stripPrefix": "ghc-lib-0.20190409", "sha256": "803ba87198191114ad6ef7b61e9ff5bd7cfc43feb9ee978207d281bf6f38d024"})
        , ("bytestring-nums", {"version": "0.3.6", "sha256": "bdca97600d91f00bb3c0f654784e3fbd2d62fcf4671820578105487cdf39e7cd"})
        , ("unix-time", {"version": "0.4.5", "sha256": "fe7805c62ad682589567afeee265e6e230170c3941cdce479a2318d1c5088faf"})
        , ("zip-archive", {"version": "0.3.3", "sha256": "988adee77c806e0b497929b24d5526ea68bd3297427da0d0b30b99c094efc84d"})
        , ("js-dgtable", {"version": "0.5.2", "sha256": "e28dd65bee8083b17210134e22e01c6349dc33c3b7bd17705973cd014e9f20ac"})
        , ("shake", {"version": "0.17.8", "sha256": "ade4162f7540f044f0446981120800076712d1f98d30c5b5344c0f7828ec49a2"})
        , ("filepattern", {"version": "0.1.1", "sha256": "f7fc5bdcfef0d43a793a3c64e7c0fd3b1d35eea97a37f0e69d6612ab255c9b4b"})
        , ("terminal-progress-bar", {"version": "0.4.0.1", "sha256": "c5a9720fcbcd9d83f9551e431ee3975c61d7da6432aa687aef0c0e04e59ae277"})
        ]
  ),
  exclude_packages = [
    "arx",
    "clock",
    "c2hs",
    "streaming-commons",
    "wai-app-static",
    "zlib",
  ] + (["network"] if is_windows else []),
  extra_flags = {
    "blaze-textual": { "integer-simple": use_integer_simple },
    "cryptonite": { "integer-gmp": not use_integer_simple },
    "hashable": { "integer-gmp": not use_integer_simple },
    "integer-logarithms": { "integer-gmp": not use_integer_simple},
    "text": { "integer-simple": use_integer_simple },
    "scientific": { "integer-simple": use_integer_simple },
  },
  extra_libs = {
    "z": "@com_github_madler_zlib//:z",
    "ffi": "@com_github_digital_asset_daml//3rdparty/haskell/ffi_windows:ffi" if is_windows else "@libffi_nix//:ffi",
  },
  ghc_workspaces = {
    "k8": "@io_tweag_rules_haskell_ghc-nixpkgs",
    "darwin": "@io_tweag_rules_haskell_ghc-nixpkgs",
    # although windows is not quite supported yet
    "x64_windows": "@io_tweag_rules_haskell_ghc_windows_amd64",
  },
)

c2hs_version = "0.28.3"
c2hs_hash = "80cc6db945ee7c0328043b4e69213b2a1cb0806fb35c8362f9dea4a2c312f1cc"
c2hs_package_id = "c2hs-{0}".format(c2hs_version)
c2hs_url = "https://hackage.haskell.org/package/{0}/{1}.tar.gz".format(
  c2hs_package_id,
  c2hs_package_id,
)
c2hs_build_file = "//3rdparty/haskell:BUILD.c2hs"
http_archive(
  name = "haskell_c2hs",
  build_file = c2hs_build_file,
  sha256 = c2hs_hash,
  strip_prefix = c2hs_package_id,
  urls = [c2hs_url],
  patches = ["@com_github_digital_asset_daml//bazel_tools:haskell-c2hs.patch"],
  patch_args = ["-p1"]
)

hazel_custom_package_hackage(
  package_name = "clock",
  version = "0.7.2",
  sha256 = "886601978898d3a91412fef895e864576a7125d661e1f8abc49a2a08840e691f",
  build_file = "//3rdparty/haskell:BUILD.clock",
)

# We only use a custom build on Windows
hazel_custom_package_hackage(
      package_name = "network",
      version = "2.8.0.0",
      sha256 = "c8905268b7e3b4cf624a40245bf11b35274a6dd836a5d4d531b5760075645303",
      build_file = "//3rdparty/haskell:BUILD.network",
    ) if is_windows else None

hazel_custom_package_hackage(
  package_name = "zlib",
  version = "0.6.2",
  sha256 = "0dcc7d925769bdbeb323f83b66884101084167501f11d74d21eb9bc515707fed",
  build_file = "//3rdparty/haskell:BUILD.zlib",
)

hazel_custom_package_hackage(
  package_name = "streaming-commons",
  version = "0.2.1.0",
  sha256 = "d8d1fe588924479ea7eefce8c6af77dfb373ee6bde7f4691bdfcbd782b36d68d",
  build_file = "//3rdparty/haskell:BUILD.streaming-commons",
)

hazel_custom_package_github(
  package_name = "wai-app-static",
  github_user = "nmattia-da",
  github_repo = "wai",
  strip_prefix = "wai-app-static",
  repo_sha = "05179164831432f207f3d43580c51161d519d191",
  build_file = "//3rdparty/haskell:BUILD.wai-app-static",
)

hazel_custom_package_github(
  package_name = "arx",
  github_user = "solidsnack",
  github_repo = "arx",
  repo_sha = "7561fed76bb613302d1ae104f0eb2ad13daa9fac",
  build_file = "//3rdparty/haskell:BUILD.arx",
  patches = ["@com_github_digital_asset_daml//bazel_tools:haskell-arx.patch"],
  patch_args = ["-p1"],
)

load("//bazel_tools:java.bzl", "java_home_runtime")
java_home_runtime(name = "java_home")

# rules_go used here to compile a wrapper around the protoc-gen-scala plugin
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

nixpkgs_package(
    name = "go_nix",
    attribute_path = "go",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
    build_file_content = """
    filegroup(
        name = "sdk",
        srcs = glob(["share/go/**"]),
        visibility = ["//visibility:public"],
    )
    """,
)

nixpkgs_package(
    name = "libffi_nix",
    attribute_path = "libffi.dev",
    nix_file = "//nix:bazel.nix",
    nix_file_deps = common_nix_file_deps,
    repositories = dev_env_nix_repos,
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
        root_file = "@go_nix//:share/go/README.md",
    ) if not is_windows else None

go_rules_dependencies()
go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")
gazelle_dependencies()

# protoc-gen-doc repo
go_repository(
    name = "com_github_pseudomuto_protoc_gen_doc",
    importpath = "github.com/pseudomuto/protoc-gen-doc",
    commit = "0c4d666cfe1175663cf067963396a0b9b34f543f",
)

# protokit repo
go_repository(
    name = "com_github_pseudomuto_protokit",
    importpath = "github.com/pseudomuto/protokit",
    commit = "7037620bf27b13fcdc10b1b17ddef82540db670b",
)

load(
  '@io_bazel_rules_scala//scala:scala.bzl',
  'scala_repositories'
)
scala_repositories(("2.12.6", {
    "scala_compiler": "3023b07cc02f2b0217b2c04f8e636b396130b3a8544a8dfad498a19c3e57a863",
    "scala_library": "f81d7144f0ce1b8123335b72ba39003c4be2870767aca15dd0888ba3dab65e98",
    "scala_reflect": "ffa70d522fc9f9deec14358aa674e6dd75c9dfa39d4668ef15bb52f002ce99fa"
}))

load('@io_bazel_rules_scala//scala:toolchains.bzl', 'scala_register_toolchains')
scala_register_toolchains()

load("@io_bazel_rules_scala//jmh:jmh.bzl", "jmh_repositories")
jmh_repositories()

dev_env_package(
  name = "nodejs_dev_env",
  nix_label = "@node_nix",
  win_tool = "nodejs-10.12.0",
  symlink_path = "nodejs_dev_env",
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
  name = "language_server_tests_deps",
  package_json = "//daml-foundations/daml-tools/language-server-tests:package.json",
  yarn_lock = "//daml-foundations/daml-tools/language-server-tests:yarn.lock",
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
