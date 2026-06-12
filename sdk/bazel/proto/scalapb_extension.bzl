load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel_tools:scalapb.bzl",
    "scalapb_sha256",
    "scalapb_version",
)

def _impl(module_ctx):
    http_archive(
        name = "scalapb",
        build_file_content = """\
proto_library(
    name = "scalapb_proto",
    srcs = ["protobuf/scalapb/scalapb.proto"],
    strip_import_prefix = "protobuf/",
    deps = [
        "@protobuf//:descriptor_proto",
    ],
    visibility = ["//visibility:public"],
)
""",
        sha256 = scalapb_sha256,
        strip_prefix = "ScalaPB-{}".format(scalapb_version),
        urls = ["https://github.com/scalapb/ScalaPB/archive/refs/tags/v{}.tar.gz".format(scalapb_version)],
    )

scalapb_extension = module_extension(implementation = _impl)
