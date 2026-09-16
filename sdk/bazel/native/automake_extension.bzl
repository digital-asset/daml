load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "AUTOMAKE_SHA256",
    "AUTOMAKE_URLS",
    "AUTOMAKE_VERSION",
)
load(
    "//bazel/versions:msys2.version.bzl",
    "MSYS_AUTOMAKE_SHA256",
    "MSYS_AUTOMAKE_URL",
)

_MSYS_BUILD = """filegroup(
    name = "files",
    srcs = glob(["usr/**"]),
    visibility = ["//visibility:public"],
)
"""

def _impl(module_ctx):
    http_archive(
        name = "automake",
        urls = AUTOMAKE_URLS,
        strip_prefix = "automake-{}".format(AUTOMAKE_VERSION),
        sha256 = AUTOMAKE_SHA256,
        build_file = ":files/automake.BUILD.bzl",
    )
    http_archive(
        name = "msys_automake",
        urls = [MSYS_AUTOMAKE_URL],
        sha256 = MSYS_AUTOMAKE_SHA256,
        type = "tar.zst",
        build_file_content = _MSYS_BUILD,
    )

automake = module_extension(implementation = _impl)
