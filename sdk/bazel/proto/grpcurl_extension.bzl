load("//bazel/proto:grpcurl.bzl", "grpcurl")
load(
    "//bazel/versions:grpcurl.version.bzl",
    "GRPCURL_SHA256",
    "GRPCURL_VERSION",
)

def _impl(module_ctx):
    grpcurl(
        name = "grpcurl",
        sha256 = GRPCURL_SHA256,
        version = GRPCURL_VERSION,
    )

grpcurl_extension = module_extension(implementation = _impl)
