load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    GOOGLEAPIS_VERSION = "83c3605afb5a39952bf0a0809875d41cf2a558ca"
    GOOGLEAPIS_SHA256 = "ba694861340e792fd31cb77274eacaf6e4ca8bda97707898f41d8bebfd8a4984"

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
