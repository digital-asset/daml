load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "MAKE_SHA256",
    "MAKE_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "make",
        urls = ["https://ftp.gnu.org/gnu/make/make-{}.tar.gz".format(MAKE_VERSION)],
        strip_prefix = "make-{}".format(MAKE_VERSION),
        sha256 = MAKE_SHA256,
        build_file = ":files/make.BUILD.bzl",
    )

make = module_extension(implementation = _impl)
