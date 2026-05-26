"""Provides a hermetic patchelf binary for Linux x86_64 builds.

The upstream BCR `patchelf` module builds a host-executed `cc_binary`.
In this workspace the host C/C++ toolchain is overridden to a hermetic
Bootlin GCC toolchain whose glibc is newer than Ubuntu 22.04. That can
produce a `patchelf` binary that fails at execution time inside the
container (`GLIBC_2.38 not found`).

To keep action-time tools stable across local/container runs, this
extension pins the upstream patchelf static release artifact.
"""

_PATCHELF_VERSION = "0.18.0"
_PATCHELF_URL = "https://github.com/NixOS/patchelf/releases/download/{v}/patchelf-{v}-x86_64.tar.gz".format(v = _PATCHELF_VERSION)
_PATCHELF_SHA256 = "ce84f2447fb7a8679e58bc54a20dc2b01b37b5802e12c57eece772a6f14bf3f0"

def _patchelf_repo_impl(rctx):
    os_name = rctx.os.name.lower()
    arch = rctx.os.arch
    if "linux" not in os_name or arch not in ["x86_64", "amd64"]:
        fail("patchelf_extension only supports linux x86_64, got os={} arch={}".format(rctx.os.name, arch))

    rctx.download_and_extract(
        url = _PATCHELF_URL,
        sha256 = _PATCHELF_SHA256,
    )

    rctx.file("BUILD.bazel", """\
package(default_visibility = ["//visibility:public"])

exports_files(["bin/patchelf"])

alias(
    name = "patchelf",
    actual = "bin/patchelf",
)
""")

_patchelf_repo = repository_rule(
    implementation = _patchelf_repo_impl,
)

def _impl(module_ctx):
    _patchelf_repo(name = "patchelf")

patchelf = module_extension(implementation = _impl)
