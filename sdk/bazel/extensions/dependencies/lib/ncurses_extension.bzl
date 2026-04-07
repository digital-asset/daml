load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _impl(module_ctx):
    http_archive(
        name = "ncurses",
        url = "https://ftp.gnu.org/gnu/ncurses/ncurses-6.4.tar.gz",
        sha256 = "6931283d9ac87c5073f30b6290c4c75f21632bb4fc3603ac8100812bed248159",
        strip_prefix = "ncurses-6.4",
        build_file = Label(":files/ncurses.BUILD.bzl"),
    )

ncurses = module_extension(implementation = _impl)
