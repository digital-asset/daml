load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:ghc.version.bzl",
    "GHC_BINDISTS",
    "GHC_LLVM_BACKEND",
    "GHC_LLVM_BACKENDS",
    "GHC_VERSION",
)
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "GMP_SHA256",
    "GMP_VERSION",
    "NCURSES_LINUX_AARCH64_SHA256",
    "NCURSES_LINUX_AARCH64_VERSION",
    "NCURSES_LINUX_AMD64_SHA256",
    "NCURSES_LINUX_AMD64_VERSION",
    "NUMACTL_SHA256",
    "NUMACTL_VERSION",
)

_URL = "https://downloads.haskell.org/~ghc/{v}/ghc-{v}-{triple}.tar.xz"
_UNPACK_DIR = "bindist_unpacked"

def _platform_key(rctx):
    name = rctx.os.name.lower()
    arch = rctx.os.arch.lower()
    if "linux" in name:
        os = "linux"
    elif "mac" in name or "darwin" in name:
        os = "darwin"
    elif "windows" in name:
        os = "windows"
    else:
        os = name
    if arch in ["amd64", "x86_64"]:
        cpu = "amd64"
    elif arch in ["aarch64", "arm64"]:
        cpu = "aarch64"
    else:
        cpu = arch
    return (os, cpu)

_IMPORT_TEMPLATE = """\
haskell_import(
    name = {name},
    id = {id},
    version = {version},
    deps = {deps},
    linkopts = {linkopts},
    static_libraries = glob([{subdir} + "/*.a"], exclude = [{subdir} + "/*_p.a"], allow_empty = True),
    static_profiling_libraries = glob([{subdir} + "/*_p.a"], allow_empty = True),
    shared_libraries = glob([{subdir} + "/*.so", {subdir} + "/*.so.*"], allow_empty = True),
    hdrs = [],
    includes = [],
    visibility = ["//visibility:public"],
)
"""

def _haskell_import(pkg):
    subdir = "{}/{}".format(_UNPACK_DIR, pkg["build_subdir"])
    return _IMPORT_TEMPLATE.format(
        name = repr(pkg["name"]),
        id = repr(pkg["id"]),
        version = repr(pkg["version"]),
        deps = repr(pkg["deps"]),
        linkopts = repr(["-l" + lib for lib in pkg["extra_libraries"]]),
        subdir = repr(subdir),
    )

def _ghc_bindist_repo_impl(rctx):
    key = _platform_key(rctx)
    bindist = GHC_BINDISTS.get(key)
    if bindist == None:
        fail("no GHC {} bindist for platform {}; supported: {}".format(
            GHC_VERSION,
            key,
            GHC_BINDISTS.keys(),
        ))

    rctx.download_and_extract(
        url = _URL.format(v = GHC_VERSION, triple = bindist["triple"]),
        sha256 = bindist["sha256"],
        type = "tar.xz",
        stripPrefix = bindist["strip_prefix"],
        output = _UNPACK_DIR,
    )

    lockfiles = {
        ("linux", "amd64"): rctx.attr.lockfile_linux_amd64,
        ("linux", "aarch64"): rctx.attr.lockfile_linux_aarch64,
        ("darwin", "aarch64"): rctx.attr.lockfile_darwin_aarch64,
    }
    lockfile = lockfiles.get(key)
    if lockfile == None:
        fail("no GHC package pin for platform {}; supported: {}".format(key, lockfiles.keys()))
    lock = json.decode(rctx.read(rctx.path(lockfile)))
    packages = lock.get("packages", [])

    imports = "\n".join([_haskell_import(p) for p in packages])

    rctx.file(
        "libraries.bzl",
        content = "# Generated from the committed GHC package pin.\n" +
                  "TOOLCHAIN_LIBRARIES = {}\n".format(
                      repr(["@ghc_bindist//:" + p["name"] for p in packages]),
                  ),
        executable = False,
    )

    rctx.file(
        "BUILD.bazel",
        content = """\
load("@rules_haskell//haskell:defs.bzl", "haskell_import")

package(default_visibility = ["//visibility:public"])

{imports}

filegroup(
    name = "bindist_srcs",
    srcs = glob(["{unpack}/**"]),
)
""".format(
            imports = imports,
            unpack = _UNPACK_DIR,
        ),
        executable = False,
    )

_ghc_bindist_repo = repository_rule(
    implementation = _ghc_bindist_repo_impl,
    attrs = {
        "lockfile_linux_amd64": attr.label(
            default = "//bazel/haskell/ghc:pin/linux_amd64.lock.json",
            doc = "Committed per-platform package pin; regenerate with `bazel run //bazel/haskell/ghc:ghc_packages.pin`.",
        ),
        "lockfile_linux_aarch64": attr.label(
            default = "//bazel/haskell/ghc:pin/linux_aarch64.lock.json",
            doc = "Committed per-platform package pin; regenerate with `bazel run //bazel/haskell/ghc:ghc_packages.pin`.",
        ),
        "lockfile_darwin_aarch64": attr.label(
            default = "//bazel/haskell/ghc:pin/darwin_aarch64.lock.json",
            doc = "Committed per-platform package pin; regenerate with `bazel run //bazel/haskell/ghc:ghc_packages.pin`.",
        ),
    },
)

def _ghc_llvm_backend_impl(rctx):
    backend = GHC_LLVM_BACKENDS.get(_platform_key(rctx))
    if GHC_LLVM_BACKEND and backend:
        for artifact in backend["artifacts"]:
            rctx.download_and_extract(
                url = artifact["url"],
                sha256 = artifact["sha256"],
                type = "tar.bz2",
                output = artifact["output"],
            )
        srcs = '"tools/bin/opt", "tools/bin/llc", "lib/lib/{}"'.format(backend["shared_library"])
    else:
        srcs = ""
    rctx.file("BUILD.bazel", """\
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "backend",
    srcs = [{srcs}],
)
""".format(srcs = srcs))

_ghc_llvm_backend_repo = repository_rule(
    implementation = _ghc_llvm_backend_impl,
)

def _ghc_toolchain_impl(module_ctx):
    _ghc_bindist_repo(name = "ghc_bindist")
    _ghc_llvm_backend_repo(name = "ghc_llvm_backend")
    http_archive(
        name = "gmp",
        url = "https://gmplib.org/download/gmp/gmp-{}.tar.xz".format(GMP_VERSION),
        sha256 = GMP_SHA256,
        strip_prefix = "gmp-{}".format(GMP_VERSION),
        build_file = ":files/gmp.BUILD.bzl",
        patches = [":files/gmp_handauthored.patch"],
        patch_args = ["-p1"],
    )

    ncurses_version = NCURSES_LINUX_AMD64_VERSION
    ncurses_sha256 = NCURSES_LINUX_AMD64_SHA256
    if _platform_key(module_ctx) == ("linux", "aarch64"):
        ncurses_version = NCURSES_LINUX_AARCH64_VERSION
        ncurses_sha256 = NCURSES_LINUX_AARCH64_SHA256

    http_archive(
        name = "ncurses",
        url = "https://ftp.gnu.org/gnu/ncurses/ncurses-{}.tar.gz".format(ncurses_version),
        sha256 = ncurses_sha256,
        strip_prefix = "ncurses-{}".format(ncurses_version),
        build_file = ":files/ncurses.BUILD.bzl",
    )
    http_archive(
        name = "numactl",
        url = "https://github.com/numactl/numactl/releases/download/v{v}/numactl-{v}.tar.gz".format(v = NUMACTL_VERSION),
        sha256 = NUMACTL_SHA256,
        strip_prefix = "numactl-{}".format(NUMACTL_VERSION),
        build_file = ":files/numactl.BUILD.bzl",
    )

ghc_toolchain_extension = module_extension(
    implementation = _ghc_toolchain_impl,
)
