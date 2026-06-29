# Shared hermetic compile/link setup for from-source (autotools) builds.
#
# `./configure` and build scripts probe the host for a working compiler,
# headers, a preprocessor and binutils -- none of which are routed through the
# cc-toolchain's own flags. On a dev host those probes silently succeed against
# the system toolchain; in a minimal sandbox they fail. Passing the toolchain's
# full CFLAGS/CPPFLAGS/LDFLAGS plus a binutils tool dir keeps every probe on the
# hermetic toolchain.

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")

_PATH_PREFIXES = ["-L", "-B", "-I", "-F", "-iquote"]

def _rel(p):
    return p.startswith("external/") or p.startswith("bazel-out/")

# Toolchain flag paths are execroot-relative; configure builds in a temp dir, so
# rewrite them to absolute $EXECROOT/... (shell expands $EXECROOT at action time).
def _abs(tok):
    if _rel(tok):
        return "$EXECROOT/" + tok
    for p in _PATH_PREFIXES:
        if tok.startswith(p) and _rel(tok[len(p):]):
            return p + "$EXECROOT/" + tok[len(p):]
    return tok

# Per-compilation flags configure/build scripts add themselves.
_DROP = ["-c", "-S", "-E", "-o"]

def _flags(fc, action_name, variables):
    out = []
    for f in cc_common.get_memory_inefficient_command_line(
        feature_configuration = fc,
        action_name = action_name,
        variables = variables,
    ):
        if f not in _DROP:
            out.append(_abs(f))
    return out

def hermetic_cc_flags(ctx, cc_toolchain, link_dynamic = False):
    """Compiler/ar tool paths + CFLAGS/LDFLAGS for an autotools build.

    Returns struct(compiler, ar, cflags, ldflags). Paths are absolutized to
    $EXECROOT/... `link_dynamic` selects shared-library vs executable link flags.
    """
    fc = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    link_action = ACTION_NAMES.cpp_link_dynamic_library if link_dynamic else ACTION_NAMES.cpp_link_executable
    compile_vars = cc_common.create_compile_variables(
        feature_configuration = fc,
        cc_toolchain = cc_toolchain,
    )
    link_vars = cc_common.create_link_variables(
        feature_configuration = fc,
        cc_toolchain = cc_toolchain,
        is_linking_dynamic_library = link_dynamic,
    )
    return struct(
        compiler = cc_common.get_tool_for_action(
            feature_configuration = fc,
            action_name = ACTION_NAMES.c_compile,
        ),
        ar = cc_common.get_tool_for_action(
            feature_configuration = fc,
            action_name = ACTION_NAMES.cpp_link_static_library,
        ),
        cflags = " ".join(_flags(fc, ACTION_NAMES.c_compile, compile_vars)),
        ldflags = " ".join(_flags(fc, link_action, link_vars)),
    )

# Shell snippet (brace-free, safe to inject into a .format() command): exposes
# the toolchain's lld/llvm-* under plain binutils names on $PATH, since the
# sandbox has no host ld/ar/ranlib/nm. Requires $EXECROOT and $CLANG to be set.
TOOLBIN_SNIPPET = """
CLANGDIR="$(dirname "$CLANG")"
TOOLBIN="$(mktemp -d)"
ln -s "$CLANGDIR/ld.lld" "$TOOLBIN/ld"
ln -s "$CLANGDIR/llvm-ar" "$TOOLBIN/ar"
ln -s "$CLANGDIR/llvm-ranlib" "$TOOLBIN/ranlib"
ln -s "$CLANGDIR/llvm-nm" "$TOOLBIN/nm"
ln -s "$CLANGDIR/llvm-strip" "$TOOLBIN/strip"
export PATH="$TOOLBIN:$PATH"
"""
