load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "BZIP2_SHA256",
    "BZIP2_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "bzip2",
        url = "https://sourceware.org/pub/bzip2/bzip2-{}.tar.gz".format(BZIP2_VERSION),
        sha256 = BZIP2_SHA256,
        strip_prefix = "bzip2-{}".format(BZIP2_VERSION),
        build_file = ":files/bzip2.BUILD.bzl",
    )

bzip2 = module_extension(implementation = _impl)
