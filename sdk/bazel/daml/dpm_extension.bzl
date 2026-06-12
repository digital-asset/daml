load("//bazel_tools:dpm.bzl", "dpm_binary")
load(
    "//bazel/versions:dpm.version.bzl",
    "DPM_SHA256",
    "DPM_VERSION",
)

def _get_dpm(module_ctx):
    dpm_binary(
        name = "dpm_binary",
        sha256 = DPM_SHA256,
        version = DPM_VERSION,
    )

def _impl(module_ctx):
    _get_dpm(module_ctx)

dpm_extension = module_extension(implementation = _impl)
