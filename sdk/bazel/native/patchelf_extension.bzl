"""Provides a hermetic patchelf binary for Linux x86_64 builds.

The upstream BCR `patchelf` module builds a host-executed `cc_binary`.
In this workspace the host C/C++ toolchain is overridden to a hermetic
toolchain whose glibc is newer than Ubuntu 22.04. That can
produce a `patchelf` binary that fails at execution time inside the
container (`GLIBC_2.38 not found`).

To keep action-time tools stable across local/container runs, this
extension pins the upstream patchelf static release artifact.
"""

load("//bazel/versions:patchelf.version.bzl", "PATCHELF_SHA256", "PATCHELF_URL")

def _patchelf_repo_impl(rctx):
    os_name = rctx.os.name.lower()
    arch = rctx.os.arch
    if "linux" not in os_name or arch not in ["x86_64", "amd64"]:
        fail("patchelf_extension only supports linux x86_64, got os={} arch={}".format(rctx.os.name, arch))

    rctx.download_and_extract(
        url = PATCHELF_URL,
        sha256 = PATCHELF_SHA256,
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
