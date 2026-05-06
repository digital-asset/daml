"""Module extension that downloads the Bootlin hermetic GCC 13.2.0 toolchain.

Only Linux x86_64 is wired today.  To add Linux arm64 or a lower-glibc variant,
insert a new entry into _PLATFORMS — no other code needs to change.
"""

_PLATFORMS = {
    "linux_amd64": struct(
        url = "https://toolchains.bootlin.com/downloads/releases/toolchains/x86-64-v2/tarballs/x86-64-v2--glibc--bleeding-edge-2024.02-1.tar.bz2",
        sha256 = "cefbe65c027b8a785088f7d690fc1eacef2cd39bd60c8a95cb09d666d6b98bb8",
        strip_prefix = "x86-64-v2--glibc--bleeding-edge-2024.02-1",
        repo_name = "hermetic_cc_linux_amd64",
        tool_prefix = "x86_64-buildroot-linux-gnu",
        gcc_version = "13.2.0",
        target_cpu = "k8",
        abi_version = "gcc-13.2.0",
        abi_libc_version = "glibc_2.39",
        target_system_name = "x86_64-linux-gnu",
        toolchain_identifier = "gcc-13.2.0-linux-x86_64",
    ),
}

# All literal Starlark braces are doubled ({{ / }}) to survive .format().
# Placeholders: {tool_prefix}, {target_cpu}, {toolchain_identifier},
#               {target_system_name}, {abi_version}, {abi_libc_version},
#               {compile_flags}, {cxx_flags}.
_BUILD_TPL = """\
load("@bazel_tools//tools/cpp:unix_cc_toolchain_config.bzl", "cc_toolchain_config")
load("@rules_cc//cc:defs.bzl", "cc_toolchain")

filegroup(
    name = "all_files",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cc_toolchain_config(
    name = "cc_toolchain_config",
    cpu = "{target_cpu}",
    compiler = "gcc",
    toolchain_identifier = "{toolchain_identifier}",
    host_system_name = "local",
    target_system_name = "{target_system_name}",
    target_libc = "glibc",
    abi_version = "{abi_version}",
    abi_libc_version = "{abi_libc_version}",
    # Empty: we use -nostdinc + explicit -isystem flags below instead of
    # cxx_builtin_include_directories, which would require knowing the
    # sandbox-local absolute path that changes per action (bazel#4605).
    cxx_builtin_include_directories = [],
    tool_paths = {{
        "gcc": "bin/{tool_prefix}-gcc",
        "ar": "bin/{tool_prefix}-ar",
        "ld": "bin/{tool_prefix}-ld",
        "cpp": "bin/{tool_prefix}-cpp",
        "gcov": "bin/{tool_prefix}-gcov",
        "nm": "bin/{tool_prefix}-nm",
        "objcopy": "bin/{tool_prefix}-objcopy",
        "objdump": "bin/{tool_prefix}-objdump",
        "strip": "bin/{tool_prefix}-strip",
        "dwp": "/bin/false",
        "llvm-cov": "/bin/false",
    }},
    # -nostdinc suppresses GCC's default include search so it reports
    # execroot-relative paths in .d files; -isystem re-adds the headers via
    # stable execroot-relative paths (external/<repo>/...) that Bazel's
    # strict-header check accepts even inside the sandbox.
    compile_flags = {compile_flags},
    cxx_flags = {cxx_flags},
    conly_flags = [],
    link_flags = [],
    archive_flags = [],
    # Conditional C++ runtime linkage: pure-C binaries get no spurious
    # DT_NEEDED entries for libstdc++ or libm (binutils --push-state support
    # is available since Bootlin's binutils 2.42).
    link_libs = [
        "-Wl,--push-state,--as-needed",
        "-lstdc++",
        "-lm",
        "-Wl,--pop-state",
    ],
    opt_compile_flags = ["-g0", "-O2", "-DNDEBUG", "-ffunction-sections", "-fdata-sections"],
    opt_link_flags = ["-Wl,--gc-sections"],
    dbg_compile_flags = ["-g"],
    unfiltered_compile_flags = ["-no-canonical-prefixes", "-fno-canonical-system-headers"],
    coverage_compile_flags = [],
    coverage_link_flags = [],
    supports_start_end_lib = True,
    # Bootlin GCC is configured --with-sysroot at build time; leave empty.
    builtin_sysroot = "",
)

cc_toolchain(
    name = "cc_toolchain",
    toolchain_config = ":cc_toolchain_config",
    all_files = ":all_files",
    ar_files = ":all_files",
    as_files = ":all_files",
    compiler_files = ":all_files",
    dwp_files = ":all_files",
    linker_files = ":all_files",
    objcopy_files = ":all_files",
    strip_files = ":all_files",
    supports_param_files = 1,
)

toolchain(
    name = "toolchain",
    exec_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    toolchain = ":cc_toolchain",
    toolchain_type = "@bazel_tools//tools/cpp:toolchain_type",
)
"""

def _isystem_flags(dirs):
    """Return a flat list of ['-isystem', dir, ...] pairs."""
    flags = []
    for d in dirs:
        flags.append("-isystem")
        flags.append(d)
    return flags

def _starlark_list(items):
    """Render a Python list of strings as a Starlark list literal."""
    return "[{}]".format(", ".join(['"{}"'.format(i) for i in items]))

def _gcc_toolchain_repo_impl(rctx):
    rctx.download_and_extract(
        url = rctx.attr.url,
        sha256 = rctx.attr.sha256,
        stripPrefix = rctx.attr.strip_prefix,
    )

    repo = rctx.name
    tool_prefix = rctx.attr.tool_prefix
    gcc_version = rctx.attr.gcc_version

    # Execroot-relative paths survive Bazel's sandboxed strict-header check.
    c_include_dirs = [
        "external/{}/lib/gcc/{}/{}/include".format(repo, tool_prefix, gcc_version),
        "external/{}/lib/gcc/{}/{}/include-fixed".format(repo, tool_prefix, gcc_version),
        "external/{}/{}/sysroot/usr/include".format(repo, tool_prefix),
    ]
    cxx_include_dirs = [
        "external/{}/{}/include/c++/{}".format(repo, tool_prefix, gcc_version),
        "external/{}/{}/include/c++/{}/{}".format(repo, tool_prefix, gcc_version, tool_prefix),
        "external/{}/{}/include/c++/{}/backward".format(repo, tool_prefix, gcc_version),
    ] + c_include_dirs

    compile_flags = _starlark_list(["-nostdinc"] + _isystem_flags(c_include_dirs))
    cxx_flags = _starlark_list(["-nostdinc", "-nostdinc++"] + _isystem_flags(cxx_include_dirs))

    rctx.file(
        "BUILD.bazel",
        _BUILD_TPL.format(
            tool_prefix = tool_prefix,
            target_cpu = rctx.attr.target_cpu,
            abi_version = rctx.attr.abi_version,
            abi_libc_version = rctx.attr.abi_libc_version,
            target_system_name = rctx.attr.target_system_name,
            toolchain_identifier = rctx.attr.toolchain_identifier,
            compile_flags = compile_flags,
            cxx_flags = cxx_flags,
        ),
    )

_gcc_toolchain_repo = repository_rule(
    implementation = _gcc_toolchain_repo_impl,
    attrs = {
        "url": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "strip_prefix": attr.string(mandatory = True),
        "tool_prefix": attr.string(mandatory = True),
        "gcc_version": attr.string(mandatory = True),
        "target_cpu": attr.string(mandatory = True),
        "abi_version": attr.string(mandatory = True),
        "abi_libc_version": attr.string(mandatory = True),
        "target_system_name": attr.string(mandatory = True),
        "toolchain_identifier": attr.string(mandatory = True),
    },
)

def _impl(module_ctx):
    for info in _PLATFORMS.values():
        _gcc_toolchain_repo(
            name = info.repo_name,
            url = info.url,
            sha256 = info.sha256,
            strip_prefix = info.strip_prefix,
            tool_prefix = info.tool_prefix,
            gcc_version = info.gcc_version,
            target_cpu = info.target_cpu,
            abi_version = info.abi_version,
            abi_libc_version = info.abi_libc_version,
            target_system_name = info.target_system_name,
            toolchain_identifier = info.toolchain_identifier,
        )

gcc_toolchain = module_extension(implementation = _impl)
