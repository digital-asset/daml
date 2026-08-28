"""Canonical definitions of the platforms this repository supports."""

PLATFORMS = {
    "linux_x86_64": struct(
        name = "linux_x86_64",
        constraints = [
            "@platforms//os:linux",
            "@platforms//cpu:x86_64",
        ],
        cpu_value = "k8",
        ghc_key = ("linux", "amd64"),
        cabal = "linux-x86_64",
        protoc = "linux-x86_64",
        grpcurl = "linux_x86_64",
        dpm = "linux_amd64",
        damlc_legacy = "linux",
        nodejs_repo = "nodejs_linux_amd64",
        archive_ext = "tar.gz",
        exe = "",
    ),
    "linux_aarch64": struct(
        name = "linux_aarch64",
        constraints = [
            "@platforms//os:linux",
            "@platforms//cpu:aarch64",
        ],
        cpu_value = "aarch64",
        ghc_key = ("linux", "aarch64"),
        cabal = "linux-aarch64",
        protoc = "linux-aarch_64",
        grpcurl = "linux_arm64",
        dpm = "linux_arm64",
        damlc_legacy = None,
        nodejs_repo = "nodejs_linux_arm64",
        archive_ext = "tar.gz",
        exe = "",
    ),
    "macos_x86_64": struct(
        name = "macos_x86_64",
        constraints = [
            "@platforms//os:macos",
            "@platforms//cpu:x86_64",
        ],
        cpu_value = "darwin_x86_64",
        ghc_key = ("darwin", "amd64"),
        cabal = "darwin-x86_64",
        protoc = "osx-x86_64",
        grpcurl = "osx_x86_64",
        dpm = "darwin_amd64",
        damlc_legacy = "macos",
        nodejs_repo = "nodejs_darwin_amd64",
        archive_ext = "tar.gz",
        exe = "",
    ),
    "macos_aarch64": struct(
        name = "macos_aarch64",
        constraints = [
            "@platforms//os:macos",
            "@platforms//cpu:aarch64",
        ],
        cpu_value = "darwin_arm64",
        ghc_key = ("darwin", "aarch64"),
        cabal = "darwin-aarch64",
        protoc = "osx-aarch_64",
        grpcurl = "osx_arm64",
        dpm = "darwin_arm64",
        damlc_legacy = "macos",
        nodejs_repo = "nodejs_darwin_arm64",
        archive_ext = "tar.gz",
        exe = "",
    ),
    "windows_x86_64": struct(
        name = "windows_x86_64",
        constraints = [
            "@platforms//os:windows",
            "@platforms//cpu:x86_64",
        ],
        cpu_value = "x64_windows",
        ghc_key = ("windows", "amd64"),
        cabal = "windows-x86_64",
        protoc = "win64",
        grpcurl = "windows_x86_64",
        dpm = "windows_amd64",
        damlc_legacy = "windows",
        nodejs_repo = "nodejs_windows_amd64",
        archive_ext = "zip",
        exe = ".exe",
    ),
}

PLATFORM_NAMES = PLATFORMS.keys()

def platform_name(os_name, arch):
    """Canonical platform name for a raw `ctx.os.name` / `ctx.os.arch` pair.

    Args:
        os_name: value of `ctx.os.name`.
        arch: value of `ctx.os.arch`.

    Returns:
        A key of `PLATFORMS`, or None when the pair is not supported.
    """
    name = os_name.lower()
    if "linux" in name:
        os = "linux"
    elif "mac" in name or "darwin" in name:
        os = "macos"
    elif "windows" in name:
        os = "windows"
    else:
        return None

    machine = arch.lower()
    if machine in ["amd64", "x86_64"]:
        cpu = "x86_64"
    elif machine in ["aarch64", "arm64"]:
        cpu = "aarch64"
    else:
        return None

    key = os + "_" + cpu
    if key in PLATFORMS:
        return key
    return None

def host_platform(ctx):
    """The `PLATFORMS` entry describing the host `ctx` runs on.

    Args:
        ctx: a `repository_ctx` or `module_ctx`.

    Returns:
        The matching `PLATFORMS` struct. Fails when the host is unsupported.
    """
    key = platform_name(ctx.os.name, ctx.os.arch)
    if key == None:
        fail("unsupported host platform {} / {}; supported: {}".format(
            ctx.os.name,
            ctx.os.arch,
            ", ".join(PLATFORM_NAMES),
        ))
    return PLATFORMS[key]
