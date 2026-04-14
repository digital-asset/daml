# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _sysctl_impl(repository_ctx):
    sysctl = repository_ctx.which("sysctl")
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
