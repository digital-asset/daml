load("//bazel/versions:cabal.version.bzl", "CABAL_PLATFORMS")

def _cabal_install_impl(repository_ctx):
    os_name = repository_ctx.os.name
    os_arch = repository_ctx.os.arch

    if "linux" in os_name:
        arch = "aarch64" if os_arch == "aarch64" else "x86_64"
        platform_key = "linux-" + arch
    elif "mac" in os_name:
        arch = "aarch64" if os_arch == "aarch64" else "x86_64"
        platform_key = "darwin-" + arch
    elif "windows" in os_name:
        platform_key = "windows-x86_64"
    else:
        fail("Unsupported OS for cabal-install: " + os_name)

    platform = CABAL_PLATFORMS[platform_key]
    repository_ctx.download_and_extract(
        url = platform["url"],
        sha256 = platform["sha256"],
    )

    cabal_bin = "cabal.exe" if "windows" in os_name else "cabal"
    repository_ctx.file(
        "BUILD.bazel",
        'exports_files(["{}"], visibility = ["//visibility:public"])\n'.format(cabal_bin),
    )

_cabal_install = repository_rule(
    implementation = _cabal_install_impl,
)

def _impl(module_ctx):
    _cabal_install(name = "cabal")

cabal = module_extension(implementation = _impl)
