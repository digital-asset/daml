load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

_VERSION = "2.0.0"
_SHA256 = "79204ed1fa385c03b5235f65b25ced6ac51cf4b00e45e1157beca6a28bdb8043"

def _impl(module_ctx):
    http_archive(
        name = "com_github_bazelbuild_remote_apis",
        strip_prefix = "remote-apis-{}".format(_VERSION),
        urls = ["https://github.com/bazelbuild/remote-apis/archive/v{}.tar.gz".format(_VERSION)],
        sha256 = _SHA256,
        patches = [
            "//bazel/patches:remote_apis_no_services.patch",
            "//bazel/patches:remote_apis_build_files.patch",
        ],
        patch_args = ["-p1"],
    )

remote_apis_extension = module_extension(implementation = _impl)
