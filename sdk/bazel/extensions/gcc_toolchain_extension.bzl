"""Module extension that downloads the Bootlin hermetic GCC 13.2.0 toolchain.

Only Linux x86_64 is wired today.  To add Linux arm64 or a lower-glibc variant,
insert a new entry into _PLATFORMS — no other code needs to change.

Repository layout (after the rule runs):

  bin/x86_64-buildroot-linux-gnu-*    Bootlin tarball binaries (prefixed).
                                      Consumed by Bazel C/C++ actions via
                                      `tool_paths` in the generated BUILD.bazel.
  unprefixed/{ar,as,ld,nm,ranlib,     Symlinks to the prefixed binaries above,
              objcopy,objdump,strip}  exposed under their conventional autoconf
                                      names. Intended to be prepended to PATH
                                      by repository rules that drive
                                      autoconf-style host scripts (e.g.
                                      rules_haskell's `_ghc_bindist_impl`,
                                      which runs `./configure` on the GHC
                                      bindist and probes `ar`/`ld`/`ranlib`/
                                      `gcc`/`strip` via `AC_CHECK_TARGET_TOOL`).
                                      Each symlink is guarded by an existence
                                      check so a future Bootlin tarball that
                                      ships a smaller binutils set produces a
                                      smaller `unprefixed/` directory rather
                                      than crashing the rule.
  unprefixed/{gcc,cc}                 Tiny shell wrappers (not symlinks) that
                                      invoke `bin/<prefix>-gcc` by its full
                                      path. Bootlin's gcc is a Buildroot
                                      `toolchain-wrapper` that locates its
                                      real binary by appending `.br_real` to
                                      argv[0]'s basename; a symlink under a
                                      different basename therefore exec's a
                                      non-existent `gcc.br_real`. The wrapper
                                      keeps argv[0]'s basename as
                                      `<prefix>-gcc`, so the toolchain-wrapper
                                      finds its `.br_real` correctly. `cc`
                                      mirrors `gcc` for autoconf's `${CC-cc}`
                                      fallback.

The `unprefixed/` directory is consumed in two ways:

  1. Automatically by the generated `:all_files` filegroup
     (`glob(["**"])`), so every `cc_toolchain.*_files` attribute
     includes the symlinks without a BUILD-template edit.
  2. Explicitly by repository rules that resolve
     `ctx.path(Label("@hermetic_cc_linux_amd64//:unprefixed"))` and
     prepend that path to a child process's `PATH` so autoconf-style
     probes succeed without depending on host binutils.
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
#               {compile_flags}, {conly_flags}, {cxx_flags},
#               {cxx_builtin_include_directories}.
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
    # Execroot-relative directories (external/<repo>/...) that double as
    # (a) sandbox mount roots so Bazel materialises the libc + libstdc++
    # headers inside the sandbox, and (b) strict-headers acceptance roots
    # so .d-file dependencies under these prefixes are honoured. Paired
    # with -fno-canonical-system-headers (see unfiltered_compile_flags
    # below) so GCC emits relative paths in .d files; this is what lets us
    # use cxx_builtin_include_directories without re-triggering the
    # absolute-path strict-headers regression of bazel#4605.
    cxx_builtin_include_directories = {cxx_builtin_include_directories},
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
    # -nostdinc / -nostdinc++ suppress GCC's default include search so it
    # reports execroot-relative paths in .d files; -isystem re-adds the
    # headers via stable execroot-relative paths (external/<repo>/...) that
    # Bazel's strict-header check accepts even inside the sandbox.
    #
    # The C-only -isystem entries are emitted via conly_flags rather than
    # compile_flags so that for C++ compiles -- where Bazel concatenates
    # compile_flags then cxx_flags -- the C++ include roots come BEFORE the
    # C ones on the command line. That ordering is required for libstdc++
    # headers like <cstdlib> whose `#include_next <stdlib.h>` must resolve
    # to the libc header in the sysroot AFTER the C++ directory in which
    # cstdlib itself lives.
    compile_flags = {compile_flags},
    conly_flags = {conly_flags},
    cxx_flags = {cxx_flags},
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
    # Bootlin tarball ships only ld.bfd, which does not recognise --start-lib/--end-lib (gold/lld features).
    supports_start_end_lib = False,
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

    # Autoconf-compatible unprefixed shims. Bootlin ships only
    # target-prefixed names (bin/<tool_prefix>-ar, ...); autoconf-driven
    # `./configure` scripts probe `ar`/`ld`/`ranlib`/`gcc`/`strip` via
    # AC_CHECK_TARGET_TOOL, which ultimately falls back to the unprefixed
    # name. This symlink farm makes those probes succeed once
    # `unprefixed/` is on PATH.
    #
    # The binutils tools (ar, as, ld, nm, ranlib, objcopy, objdump, strip)
    # are real ELF binaries with no basename indirection, so plain
    # symlinks work. Each is guarded by an existence check so a Bootlin
    # tarball that drops an optional tool produces a smaller `unprefixed/`
    # rather than crashing the rule.
    unprefixed_binutils = [
        "ar",
        "as",
        "ld",
        "nm",
        "ranlib",
        "objcopy",
        "objdump",
        "strip",
    ]
    for tool in unprefixed_binutils:
        target = rctx.path("bin/{}-{}".format(tool_prefix, tool))
        if target.exists:
            rctx.symlink(target, "unprefixed/{}".format(tool))

    # Bootlin's `bin/<prefix>-gcc` is a symlink to a `toolchain-wrapper`
    # that derives the path to its real binary by appending `.br_real` to
    # the basename of argv[0]. A plain symlink under a different basename
    # (e.g. `unprefixed/gcc`) therefore exec's a non-existent `gcc.br_real`.
    # Use a tiny shell wrapper instead that invokes the prefixed binary by
    # its full path, so argv[0]'s basename remains `<prefix>-gcc` and the
    # toolchain-wrapper finds `<prefix>-gcc.br_real` as designed. The
    # wrapper resolves its own location with `readlink -f "$0"` so it
    # works regardless of how callers reach it.
    gcc_path = rctx.path("bin/{}-gcc".format(tool_prefix))
    if gcc_path.exists:
        gcc_wrapper = "\n".join([
            "#!/bin/sh",
            "exec \"$(dirname \"$(readlink -f \"$0\")\")/../bin/{}-gcc\" \"$@\"".format(tool_prefix),
            "",
        ])
        rctx.file("unprefixed/gcc", gcc_wrapper, executable = True)
        # `cc` is the conventional unprefixed synonym for the C compiler;
        # autoconf's ${CC-cc} fallback expects it under that exact name.
        rctx.file("unprefixed/cc", gcc_wrapper, executable = True)

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

    # compile_flags applies to both C and C++ compiles, but cxx_flags is
    # appended AFTER compile_flags for C++ only. To ensure libstdc++'s
    # `#include_next <stdlib.h>` resolves correctly, the C++ include roots
    # must precede the C ones on the C++ command line, so we keep the C-only
    # roots out of compile_flags and emit them via conly_flags / cxx_flags.
    compile_flags = _starlark_list(["-nostdinc"])
    conly_flags = _starlark_list(_isystem_flags(c_include_dirs))
    cxx_flags = _starlark_list(["-nostdinc", "-nostdinc++"] + _isystem_flags(cxx_include_dirs))

    # cxx_include_dirs already contains c_include_dirs (see "+ c_include_dirs"
    # above), so it is the union of both header sets.
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
            conly_flags = conly_flags,
            cxx_flags = cxx_flags,
            cxx_builtin_include_directories = _starlark_list(cxx_include_dirs),
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
