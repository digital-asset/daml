load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "AUTOCONF_SHA256",
    "AUTOCONF_URLS",
    "AUTOCONF_VERSION",
)
load(
    "//bazel/versions:msys2.version.bzl",
    "MSYS_AUTOCONF_SHA256",
    "MSYS_AUTOCONF_URL",
)

_MSYS_BUILD = """filegroup(
    name = "files",
    srcs = glob(["usr/**"]),
    visibility = ["//visibility:public"],
)
"""

def _impl(module_ctx):
    http_archive(
        name = "autoconf",
        urls = AUTOCONF_URLS,
        strip_prefix = "autoconf-{}".format(AUTOCONF_VERSION),
        sha256 = AUTOCONF_SHA256,
        build_file = ":files/autoconf.BUILD.bzl",
    )
    http_archive(
        name = "msys_autoconf",
        urls = [MSYS_AUTOCONF_URL],
        sha256 = MSYS_AUTOCONF_SHA256,
        type = "tar.zst",
        build_file_content = _MSYS_BUILD,
    )

autoconf = module_extension(implementation = _impl)
