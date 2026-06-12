load(
    "//bazel/versions:ghc.version.bzl",
    "GHC_LINUX_AMD64_SHA256",
    "GHC_VERSION",
)

_URL = "https://downloads.haskell.org/~ghc/{v}/ghc-{v}-x86_64-deb9-linux.tar.xz"
_UNPACK_DIR = "bindist_unpacked"

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

def _ghc_bindist_raw_repo_impl(rctx):
    rctx.download_and_extract(
        url = _URL.format(v = GHC_VERSION),
        sha256 = GHC_LINUX_AMD64_SHA256,
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
                      repr(["@ghc_bindist_raw//:" + p["name"] for p in packages]),
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

_ghc_bindist_raw_repo = repository_rule(
    implementation = _ghc_bindist_raw_repo_impl,
    attrs = {
        "lockfile": attr.label(
            default = "//bazel/haskell/ghc:ghc_packages.lock.json",
            doc = "Committed package pin; regenerate with `bazel run //bazel/haskell/ghc:ghc_packages.update`.",
        ),
    },
)

def _ghc_bindist_extension_impl(module_ctx):
    _ghc_bindist_raw_repo(name = "ghc_bindist_raw")

ghc_bindist_extension = module_extension(
    implementation = _ghc_bindist_extension_impl,
)
