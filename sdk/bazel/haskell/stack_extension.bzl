load("//bazel/versions:stack.version.bzl", "STACK_PLATFORMS", "STACK_VERSION")

def _stack_install_impl(repository_ctx):
    os_name = repository_ctx.os.name
    os_arch = repository_ctx.os.arch

    if "linux" in os_name:
        arch = "aarch64" if os_arch == "aarch64" else "x86_64"
        platform_key = "linux-" + arch
    elif "mac" in os_name:
        arch = "aarch64" if os_arch == "aarch64" else "x86_64"
        platform_key = "osx-" + arch
    elif "windows" in os_name:
        platform_key = "windows-x86_64"
    else:
        fail("Unsupported OS for stack: " + os_name)

    platform = STACK_PLATFORMS[platform_key]
    repository_ctx.download_and_extract(
        url = platform["url"],
        sha256 = platform["sha256"],
        stripPrefix = "stack-{}-{}".format(STACK_VERSION, platform_key),
    )

    if "windows" in os_name:
        repository_ctx.symlink("stack.exe", "stack")

    repository_ctx.file(
        "BUILD.bazel",
        """exports_files(["stack"], visibility = ["//visibility:public"])
""",
    )

_stack_install = repository_rule(
    implementation = _stack_install_impl,
)

def _impl(module_ctx):
    _stack_install(name = "stack")

stack = module_extension(implementation = _impl)
