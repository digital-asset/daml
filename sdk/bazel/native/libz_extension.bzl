load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "ZLIB_SHA256",
    "ZLIB_URLS",
    "ZLIB_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "libz",
        urls = ZLIB_URLS,
        sha256 = ZLIB_SHA256,
        strip_prefix = "zlib-{}".format(ZLIB_VERSION),
        build_file = ":files/libz.BUILD.bzl",
    )

libz = module_extension(implementation = _impl)
