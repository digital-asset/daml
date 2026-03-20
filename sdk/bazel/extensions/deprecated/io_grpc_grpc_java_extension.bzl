load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    http_archive(
        name = "io_grpc_grpc_java",
        strip_prefix = "grpc-java-1.60.2",
        urls = ["https://github.com/grpc/grpc-java/archive/v1.60.2.tar.gz"],
        sha256 = "4baf80f35488739d99515d71b8e72bd17a476c214a84106edc66b259fe8ac22c",
    )

io_grpc_grpc_java_extension = module_extension(implementation = _impl)
