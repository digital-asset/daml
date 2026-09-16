load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "M4_SHA256",
    "M4_URLS",
    "M4_VERSION",
)
load(
    "//bazel/versions:msys2.version.bzl",
    "MSYS_M4_SHA256",
    "MSYS_M4_URL",
)

def _impl(module_ctx):
    http_archive(
        name = "m4",
        urls = M4_URLS,
        strip_prefix = "m4-{}".format(M4_VERSION),
        sha256 = M4_SHA256,
        build_file = ":files/m4.BUILD.bzl",
    )
    http_archive(
        name = "msys_m4",
        urls = [MSYS_M4_URL],
        sha256 = MSYS_M4_SHA256,
        type = "tar.zst",
        build_file_content = """exports_files(["usr/bin/m4.exe"], visibility = ["//visibility:public"])

alias(
    name = "m4",
    actual = "usr/bin/m4.exe",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "files",
    srcs = glob(["usr/**"]),
    visibility = ["//visibility:public"],
)
""",
    )

m4 = module_extension(implementation = _impl)
