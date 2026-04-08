load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "AUTOMAKE_SHA256",
    "AUTOMAKE_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "automake",
        urls = ["https://ftp.gnu.org/gnu/automake/automake-{}.tar.gz".format(AUTOMAKE_VERSION)],
        strip_prefix = "automake-{}".format(AUTOMAKE_VERSION),
        sha256 = AUTOMAKE_SHA256,
        build_file = ":files/automake.BUILD.bzl",
    )

automake = module_extension(implementation = _impl)
