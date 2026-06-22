load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:ghc.version.bzl",
    "GHC_BINDISTS",
    "GHC_VERSION",
)
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "GMP_SHA256",
    "GMP_VERSION",
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
        stripPrefix = "ghc-{}".format(GHC_VERSION),
        output = _UNPACK_DIR,
    )

    lock = json.decode(rctx.read(rctx.path(rctx.attr.lockfile)))
    packages = lock.get("packages", [])

    imports = "\n".join([_haskell_import(p) for p in packages])

    rctx.file(
        "libraries.bzl",
        content = "# Generated from ghc_packages.lock.json (committed pin).\n" +
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
        "lockfile": attr.label(
            default = "//bazel/haskell/ghc:ghc_packages.lock.json",
            doc = "Committed package pin; regenerate with `bazel run //bazel/haskell/ghc:ghc_packages.pin`.",
        ),
    },
)

def _ghc_toolchain_impl(_module_ctx):
    _ghc_bindist_repo(name = "ghc_bindist")
    http_archive(
        name = "gmp",
        url = "https://gmplib.org/download/gmp/gmp-{}.tar.xz".format(GMP_VERSION),
        sha256 = GMP_SHA256,
        strip_prefix = "gmp-{}".format(GMP_VERSION),
        build_file = ":files/gmp.BUILD.bzl",
        patches = [":files/gmp_handauthored.patch"],
        patch_args = ["-p1"],
    )

ghc_toolchain_extension = module_extension(
    implementation = _ghc_toolchain_impl,
)
