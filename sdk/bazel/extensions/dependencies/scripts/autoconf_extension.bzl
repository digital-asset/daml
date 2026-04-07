load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    http_archive(
        name = "autoconf",
        urls = ["https://ftp.gnu.org/gnu/autoconf/autoconf-2.72.tar.gz"],
        strip_prefix = "autoconf-2.72",
        sha256 = "afb181a76e1ee72832f6581c0eddf8df032b83e2e0239ef79ebedc4467d92d6e",
        build_file = ":files/autoconf.BUILD.bzl",
    )

autoconf = module_extension(implementation = _impl)
