load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "GMP_SHA256",
    "GMP_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "gmp",
        url = "https://gmplib.org/download/gmp/gmp-{}.tar.xz".format(GMP_VERSION),
        sha256 = GMP_SHA256,
        strip_prefix = "gmp-{}".format(GMP_VERSION),
        build_file = ":files/gmp.BUILD.bzl",
    )

gmp = module_extension(implementation = _impl)
