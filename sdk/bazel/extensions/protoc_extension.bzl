load(
    "//bazel/versions:protoc.version.bzl",
    "PROTOC_SHA256S",
    "PROTOC_VERSION",
)

_PROTOC_URL_TEMPLATE = "https://github.com/protocolbuffers/protobuf/releases/download/v{version}/protoc-{version}-{platform}.zip"

def _detect_platform(rctx):
    """Map host OS/arch to protoc release platform string.

    Returns:
        (platform_string, is_windows) tuple.
    """
    os = rctx.os.name.lower()
    arch = rctx.os.arch

    if "mac os x" in os:
        if arch == "aarch64":
            return "osx-aarch_64", False
        return "osx-x86_64", False

    if "windows" in os:
        return "win64", True

    if arch == "aarch64":
        return "linux-aarch_64", False
    return "linux-x86_64", False

def _protoc_bindist_impl(rctx):
    platform, is_windows = _detect_platform(rctx)
    sha256 = rctx.attr.sha256s.get(platform, "")
    url = _PROTOC_URL_TEMPLATE.format(
        version = rctx.attr.version,
        platform = platform,
    )

    rctx.download_and_extract(url = url, sha256 = sha256)

    actual_binary = "bin/protoc.exe" if is_windows else "bin/protoc"
    rctx.file("BUILD.bazel", """\
package(default_visibility = ["//visibility:public"])

exports_files(["{binary}"])

alias(
    name = "protoc",
    actual = "{binary}",
)
""".format(binary = actual_binary))

_protoc_bindist = repository_rule(
    implementation = _protoc_bindist_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "sha256s": attr.string_dict(),
    },
)

def _impl(module_ctx):
    _protoc_bindist(
        name = "protoc_bindist",
        version = PROTOC_VERSION,
        sha256s = PROTOC_SHA256S,
    )

protoc_extension = module_extension(implementation = _impl)
