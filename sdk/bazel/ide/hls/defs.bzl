_PLATFORMS = {
    "darwin_aarch64": ("osx", "aarch64"),
    "darwin_amd64": ("osx", "x86_64"),
    "linux_aarch64": ("linux", "aarch64"),
    "linux_amd64": ("linux", "x86_64"),
}

_PACKAGE = "//bazel/ide/hls:"

def _per_platform(values, default):
    choices = {_PACKAGE + platform: value for platform, value in values.items()}
    choices["//conditions:default"] = default
    return select(choices)

def supported_platforms_only():
    return _per_platform({platform: [] for platform in _PLATFORMS}, ["@platforms//:incompatible"])

def hls_platform_targets():
    for platform, (os, cpu) in _PLATFORMS.items():
        native.config_setting(
            name = platform,
            constraint_values = [
                "@platforms//os:" + os,
                "@platforms//cpu:" + cpu,
            ],
        )
    native.filegroup(
        name = "unsupported_platform",
        tags = ["manual"],
        target_compatible_with = ["@platforms//:incompatible"],
    )
    for name in ("binary", "libraries"):
        native.alias(
            name = name,
            actual = _per_platform(
                {platform: "@hls_{}//:{}".format(platform, name) for platform in _PLATFORMS},
                _PACKAGE + "unsupported_platform",
            ),
            tags = ["manual"],
        )

def _toolchain_ghc_impl(ctx):
    toolchain = ctx.toolchains["@rules_haskell//haskell:toolchain"]
    return [DefaultInfo(
        files = depset([toolchain.tools.ghc]),
        runfiles = ctx.runfiles(files = toolchain.bindir + toolchain.libdir),
    )]

toolchain_ghc = rule(
    implementation = _toolchain_ghc_impl,
    toolchains = ["@rules_haskell//haskell:toolchain"],
)
