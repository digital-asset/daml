load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "ZLIB_SHA256",
    "ZLIB_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "zlib_shared",
        url = "https://zlib.net/fossils/zlib-{}.tar.gz".format(ZLIB_VERSION),
        sha256 = ZLIB_SHA256,
        strip_prefix = "zlib-{}".format(ZLIB_VERSION),
        build_file = ":files/zlib.BUILD.bzl",
    )

zlib_shared = module_extension(implementation = _impl)
