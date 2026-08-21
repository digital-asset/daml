"""Provides a hermetic patchelf binary for Linux builds.

The upstream BCR `patchelf` module builds a host-executed `cc_binary`.
In this workspace the host C/C++ toolchain is overridden to a hermetic
toolchain whose glibc is newer than Ubuntu 22.04. That can
produce a `patchelf` binary that fails at execution time inside the
container (`GLIBC_2.38 not found`).

To keep action-time tools stable across local/container runs, this
extension pins the upstream patchelf static release artifact.
"""

load("//bazel/versions:patchelf.version.bzl", "PATCHELF_BUILDS")

_ARCH_ALIASES = {
    "amd64": "x86_64",
    "x86_64": "x86_64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
}

def _patchelf_repo_impl(rctx):
    os_name = rctx.os.name.lower()
    build = PATCHELF_BUILDS.get(_ARCH_ALIASES.get(rctx.os.arch.lower()))
    if "linux" in os_name and build:
        rctx.download_and_extract(
            url = build["url"],
            sha256 = build["sha256"],
        )
    else:
        rctx.file(
            "bin/patchelf",
            "#!/bin/sh\necho 'patchelf is ELF-only and unavailable on this platform' >&2\nexit 1\n",
            executable = True,
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
