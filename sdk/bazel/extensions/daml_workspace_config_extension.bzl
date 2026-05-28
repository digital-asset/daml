load("//bazel_tools:build_environment.bzl", "build_environment")
load("//bazel_tools:scala_version.bzl", "scala_version_configure")
load("//daml-lf:json_repo.bzl", "daml_versions_repo")

# TODO: Remove this temporary os_info replacement and migrate callers to ctx-based
# host detection per sdk/.bzlremaining/todo/remove-os-info.md.
_os_info_bzl_template = """
cpu_value = "{CPU_VALUE}"
is_darwin = cpu_value == "darwin_arm64" or cpu_value == "darwin_x86_64"
is_darwin_arm64 = cpu_value == "darwin_arm64"
is_darwin_amd64 = cpu_value == "darwin_x86_64"
is_linux_intel = cpu_value == "k8"
is_linux_arm = cpu_value == "aarch64"
is_linux = is_linux_intel or is_linux_arm
is_windows = cpu_value == "x64_windows"
os_name = "macos" if is_darwin else "linux" if is_linux_intel or is_linux_arm else "windows"
is_intel = is_windows or is_darwin or is_linux_intel
os_arch = "x86_64" if is_windows or is_linux_intel else "arm64" if is_darwin_arm64 else "aarch64"
"""

def _detect_cpu_value(ctx):
    os_name = ctx.os.name.lower()
    arch = ctx.os.arch

    if "windows" in os_name:
        if arch in ["aarch64", "arm64"]:
            return "arm64_windows"
        return "x64_windows"

    if "mac os x" in os_name or "darwin" in os_name:
        if arch in ["aarch64", "arm64"]:
            return "darwin_arm64"
        return "darwin_x86_64"

    if "linux" in os_name:
        return "aarch64" if arch in ["aarch64", "arm64"] else "k8"

    fail("Unsupported host platform: os={}, arch={}".format(ctx.os.name, arch))

def _os_info_for_bzlmod_impl(ctx):
    cpu = _detect_cpu_value(ctx)
    known_cpu_values = [
        "aarch64",  # linux arm64
        "darwin_x86_64",  # macOS amd64
        "darwin_arm64",  # macOS arm64
        "k8",  # linux amd64
        "x64_windows",
    ]
    if cpu not in known_cpu_values:
        fail("Unknown OS type {}, expected one of {}".format(cpu, ", ".join(known_cpu_values)))
    ctx.file(
        "os_info.bzl",
        _os_info_bzl_template.format(CPU_VALUE = cpu),
        False,
    )
    ctx.file("BUILD", "", False)

os_info_for_bzlmod = repository_rule(
    implementation = _os_info_for_bzlmod_impl,
    local = True,
)

def _daml_workspace_config_impl(module_ctx):
    scala_version_configure(name = "scala_version")
    os_info_for_bzlmod(name = "os_info")
    build_environment(name = "build_environment")
    daml_versions_repo(
        name = "daml_versions_data",
        json_file = "//daml-lf:daml-lf-versions.json",
    )

daml_workspace_config = module_extension(
    implementation = _daml_workspace_config_impl,
)
