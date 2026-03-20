load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# TODO: This is deprecated rules, check https://github.com/protocolbuffers/protobuf for newer version
#       Check https://github.com/bazelbuild/rules_proto for current version and annotation
def _impl(module_ctx):
    http_archive(
        name = "rules_proto",
        sha256 = "303e86e722a520f6f326a50b41cfc16b98fe6d1955ce46642a5b7a67c11c0f5d",
        strip_prefix = "rules_proto-6.0.0",
        url = "https://github.com/bazelbuild/rules_proto/releases/download/6.0.0/rules_proto-6.0.0.tar.gz",
    )

rules_proto_extension = module_extension(implementation = _impl)
