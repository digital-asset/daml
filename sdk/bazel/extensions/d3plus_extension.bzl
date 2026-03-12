load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _get_d3plus():
    http_archive(
        name = "static_asset_d3plus",
        build_file_content = 'exports_files(["js/d3.min.js", "js/d3plus.min.js"])',
        sha256 = "7d31a500a4850364a966ac938eea7f2fa5ce1334966b52729079490636e7049a",
        strip_prefix = "d3plus.v1.9.8",
        type = "zip",
        urls = ["https://github.com/alexandersimoes/d3plus/releases/download/v1.9.8/d3plus.zip"],
    )

def _impl(module_ctx):
    _get_d3plus()

d3plus_extension = module_extension(implementation = _impl)
