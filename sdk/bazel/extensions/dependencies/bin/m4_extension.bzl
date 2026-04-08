load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "M4_SHA256",
    "M4_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "m4",
        urls = ["https://ftp.gnu.org/gnu/m4/m4-{}.tar.gz".format(M4_VERSION)],
        strip_prefix = "m4-{}".format(M4_VERSION),
        sha256 = M4_SHA256,
        build_file = ":files/m4.BUILD.bzl",
    )

m4 = module_extension(implementation = _impl)
