load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:grpc_java.version.bzl",
    "GRPC_JAVA_SHA256",
    "GRPC_JAVA_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "io_grpc_grpc_java",
        strip_prefix = "grpc-java-{}".format(GRPC_JAVA_VERSION),
        urls = ["https://github.com/grpc/grpc-java/archive/v{}.tar.gz".format(GRPC_JAVA_VERSION)],
        sha256 = GRPC_JAVA_SHA256,
    )

io_grpc_grpc_java_extension = module_extension(implementation = _impl)
