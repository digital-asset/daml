load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:proto3_suite.version.bzl",
    "PROTO3SUITE_SHA256",
    "PROTO3SUITE_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "proto3-suite",
        build_file = ":files/proto3_suite.BUILD.bzl",
        sha256 = PROTO3SUITE_SHA256,
        strip_prefix = "proto3-suite-{}".format(PROTO3SUITE_VERSION),
        urls = ["https://github.com/awakesecurity/proto3-suite/archive/refs/tags/v{}.tar.gz".format(PROTO3SUITE_VERSION)],
        patches = [
            "//bazel/patches:proto3_suite/deriving_defaults.patch",
            "//bazel/patches:proto3_suite/support_reserved_enums.patch",
        ],
        patch_args = ["-p1"],
    )

proto3_suite_extension = module_extension(implementation = _impl)
