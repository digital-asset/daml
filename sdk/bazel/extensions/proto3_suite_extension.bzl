load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    PROTO3SUITE_REV = "0.5.1"
    PROTO3SUITE_SHA256 = "6e6d514867e8efe90fe6a7e5167b86260da00fdb7ef335af694295a1adde57f4"

    http_archive(
        name = "proto3-suite",
        build_file = ":files/proto3_suite.BUILD.bzl",
        sha256 = PROTO3SUITE_SHA256,
        strip_prefix = "proto3-suite-{}".format(PROTO3SUITE_REV),
        urls = ["https://github.com/awakesecurity/proto3-suite/archive/refs/tags/v{}.tar.gz".format(PROTO3SUITE_REV)],
        patches = [
            "//bazel/patches:proto3_suite/deriving_defaults.patch",
            "//bazel/patches:proto3_suite/support_reserved_enums.patch",
        ],
        patch_args = ["-p1"],
    )

proto3_suite_extension = module_extension(implementation = _impl)
