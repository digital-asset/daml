load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "MAKE_SHA256",
    "MAKE_URLS",
    "MAKE_VERSION",
)
load(
    "//bazel/versions:msys2.version.bzl",
    "MSYS_MAKE_SHA256",
    "MSYS_MAKE_URL",
)

def _impl(module_ctx):
    http_archive(
        name = "make",
        urls = MAKE_URLS,
        strip_prefix = "make-{}".format(MAKE_VERSION),
        sha256 = MAKE_SHA256,
        build_file = ":files/make.BUILD.bzl",
    )
    http_archive(
        name = "msys_make",
        urls = [MSYS_MAKE_URL],
        sha256 = MSYS_MAKE_SHA256,
        type = "tar.zst",
        build_file_content = """exports_files(["usr/bin/make.exe"], visibility = ["//visibility:public"])

alias(
    name = "make",
    actual = "usr/bin/make.exe",
    visibility = ["//visibility:public"],
)
""",
    )

make = module_extension(implementation = _impl)
