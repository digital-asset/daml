load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    http_archive(
        name = "freefont",
        build_file_content = """
filegroup(
  name = "fonts",
  srcs = glob(["**/*.otf"]),
  visibility = ["//visibility:public"],
)
""",
        sha256 = "3a6c51868c71b006c33c4bcde63d90927e6fcca8f51c965b8ad62d021614a860",
        strip_prefix = "freefont-20120503",
        urls = ["https://storage.googleapis.com/daml-binaries/build-inputs/freefont-otf-20120503.tar.gz"],
    )

freefont_extension = module_extension(implementation = _impl)