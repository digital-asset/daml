load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "AUTOCONF_SHA256",
    "AUTOCONF_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "autoconf",
        urls = ["https://ftp.gnu.org/gnu/autoconf/autoconf-{}.tar.gz".format(AUTOCONF_VERSION)],
        strip_prefix = "autoconf-{}".format(AUTOCONF_VERSION),
        sha256 = AUTOCONF_SHA256,
        build_file = ":files/autoconf.BUILD.bzl",
    )

autoconf = module_extension(implementation = _impl)
