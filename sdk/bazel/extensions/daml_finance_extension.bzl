load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:daml_finance.version.bzl",
    "DAML_FINANCE_SHA256",
    "DAML_FINANCE_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "daml-finance",
        strip_prefix = "daml-finance-{}".format(DAML_FINANCE_VERSION),
        urls = ["https://github.com/digital-asset/daml-finance/archive/{}.tar.gz".format(DAML_FINANCE_VERSION)],
        sha256 = DAML_FINANCE_SHA256,
        build_file = ":files/daml_finance.BUILD.bzl",
    )

daml_finance_extension = module_extension(implementation = _impl)
