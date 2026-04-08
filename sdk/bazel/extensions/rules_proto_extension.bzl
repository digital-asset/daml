load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:rules_proto.version.bzl",
    "RULES_PROTO_SHA256",
    "RULES_PROTO_VERSION",
)

# TODO: Check https://github.com/bazelbuild/rules_proto for current version
#       Check https://github.com/protocolbuffers/protobuf for newer version
def _impl(module_ctx):
    http_archive(
        name = "rules_proto",
        sha256 = RULES_PROTO_SHA256,
        strip_prefix = "rules_proto-{}".format(RULES_PROTO_VERSION),
        url = "https://github.com/bazelbuild/rules_proto/releases/download/{v}/rules_proto-{v}.tar.gz".format(v = RULES_PROTO_VERSION),
    )

rules_proto_extension = module_extension(implementation = _impl)
