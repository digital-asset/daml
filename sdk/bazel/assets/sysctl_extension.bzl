_SYSCTL_FALLBACK_PATHS = [
    "/usr/sbin/sysctl",
    "/sbin/sysctl",
]

def _find_sysctl(repository_ctx):
    found = repository_ctx.which("sysctl")
    if found:
        return found
    for candidate in _SYSCTL_FALLBACK_PATHS:
        path = repository_ctx.path(candidate)
        if path.exists:
            return path
    return None

def _sysctl_impl(repository_ctx):
    sysctl = _find_sysctl(repository_ctx)
    if sysctl:
        repository_ctx.symlink(sysctl, "bin/sysctl")
    else:
        repository_ctx.file(
            "bin/sysctl",
            content = "#!/bin/sh\necho 'sysctl not available on this platform' >&2\nexit 1\n",
            executable = True,
        )
    repository_ctx.file("BUILD.bazel", content = """
package(default_visibility = ["//visibility:public"])

exports_files(["bin/sysctl"])
""")

_sysctl = repository_rule(
    implementation = _sysctl_impl,
    configure = True,
    local = True,
)

def _impl(module_ctx):
    _sysctl(name = "sysctl")

sysctl_extension = module_extension(implementation = _impl)
