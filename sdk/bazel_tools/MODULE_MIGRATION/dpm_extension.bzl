load("//bazel_tools:dpm.bzl", "dpm_binary")

def _get_dpm(module_ctx):
    dpm_binary(
        name = "dpm_binary",
        # Hashes taken from the files themselves, not the OCI artifacts
        # See `layers[0].digest` in the manifest for each platform
        sha256 = {
            "linux_arm64": "0a108513d99d3f996282ed85c51dc6c10b5790737a299182671c450f87cdf851",
            "darwin_amd64": "edfeeec2ea7ac5eb03b56cc2a912ebc59d4ff9db9831cfa0b6033def218aa637",
            "darwin_arm64": "ed9c7c6724efc36ca39cf5193a87a73e146685ae2dd042580ee20ca685aa0e1e",
            "linux_amd64": "49dcc18c5dbea13bda52f6668b5831b53b46d209e1e539a1ef2a5553c3df6c09",
            "windows_amd64": "594ac706c4eff9f0779d0cf04c18cda5ea8f6e4cf82fbead3f03f6aa7de423f9",
        },
        version = "1.0.8",
    )

def _impl(module_ctx):
    _get_dpm(module_ctx)

dpm_extension = module_extension(implementation = _impl)
