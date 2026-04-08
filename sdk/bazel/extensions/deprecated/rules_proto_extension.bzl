load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:deprecated.version.bzl",
    "RULES_PROTO_SHA256",
    "RULES_PROTO_VERSION",
)

# TODO: This is deprecated rules, check https://github.com/protocolbuffers/protobuf for newer version
#       Check https://github.com/bazelbuild/rules_proto for current version and annotation
def _impl(module_ctx):
    http_archive(
        name = "rules_proto",
        sha256 = RULES_PROTO_SHA256,
        strip_prefix = "rules_proto-{}".format(RULES_PROTO_VERSION),
        url = "https://github.com/bazelbuild/rules_proto/releases/download/{v}/rules_proto-{v}.tar.gz".format(v = RULES_PROTO_VERSION),
    )

rules_proto_extension = module_extension(implementation = _impl)
