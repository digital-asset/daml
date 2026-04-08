load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:d3plus.version.bzl",
    "D3PLUS_SHA256",
    "D3PLUS_VERSION",
)

def _get_d3plus():
    http_archive(
        name = "static_asset_d3plus",
        build_file_content = 'exports_files(["js/d3.min.js", "js/d3plus.min.js"])',
        sha256 = D3PLUS_SHA256,
        strip_prefix = "d3plus.v{}".format(D3PLUS_VERSION),
        type = "zip",
        urls = ["https://github.com/alexandersimoes/d3plus/releases/download/v{}/d3plus.zip".format(D3PLUS_VERSION)],
    )

def _impl(module_ctx):
    _get_d3plus()

d3plus_extension = module_extension(implementation = _impl)
