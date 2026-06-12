load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:googleapis.version.bzl",
    "GOOGLEAPIS_SHA256",
    "GOOGLEAPIS_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "go_googleapis",
        # We must use the same version as rules_go
        # master, as of 2022-12-05
        urls = [
            "https://mirror.bazel.build/github.com/googleapis/googleapis/archive/{}.zip".format(GOOGLEAPIS_VERSION),
            "https://github.com/googleapis/googleapis/archive/{}.zip".format(GOOGLEAPIS_VERSION),
        ],
        sha256 = GOOGLEAPIS_SHA256,
        strip_prefix = "googleapis-{}".format(GOOGLEAPIS_VERSION),
        patches = [
            # The Haskell gRPC bindings require access to the status.proto source file.
            "//bazel/patches:go_googleapis/status-proto.patch",
        ],
        patch_args = ["-E", "-p1"],
    )

googleapis_extension = module_extension(implementation = _impl)
