load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain")
load("//bazel/rules:install_gnu_tool.bzl", "InstalledGnuToolInfo")

def _ghc_lib_sdist_impl(ctx):
    component = ctx.attr.component
    version = ctx.attr.version

    # -- Outputs (predeclared via attr.output) --
    cabal_file = ctx.outputs.cabal_out
    tarball = ctx.outputs.tarball_out

    # -- Haskell toolchain (GHC) --
    hs_toolchain = ctx.toolchains["@rules_haskell//haskell:toolchain"]
    ghc = hs_toolchain.tools.ghc
    ghc_bindir = hs_toolchain.bindir
    ghc_libdir = hs_toolchain.libdir

    # -- CC toolchain (CC / LD) --
    cc_toolchain = find_cc_toolchain(ctx)
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    cc = cc_common.get_tool_for_action(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.c_compile,
    )
    ld = cc_common.get_tool_for_action(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.cpp_link_executable,
    )

    # -- Autotools prefix (InstalledGnuToolInfo from install_gnu_tool) --
    autotools_info = ctx.attr.autotools[InstalledGnuToolInfo]

    # -- Perl binary --
    perl_files = ctx.files.perl
    perl_bin = [f for f in perl_files if f.basename == "perl"][0]

    # -- Extra tools on PATH (e.g. happy, alex) --
    extra_tool_path_entries = []
    for tool in ctx.files.extra_tools:
        extra_tool_path_entries.append('"$EXECROOT/{}"'.format(tool.dirname))

    # -- cpp_options --
    cpp_options = " ".join(["--cpp={}".format(cpp) for cpp in ctx.attr.cpp_options])

    # -- gmp library directory --
    # All hermetic `lib/libgmp.so*` files live in the same dirname, so
    # picking the first file's dirname yields the `-L` path that we
    # inject into every inner GHC invocation via the ghc wrapper below.
    gmp_lib_dir = ctx.files.gmp[0].dirname

    # -- Shell command --
    shell_cmd = """\
set -euo pipefail
EXECROOT="$PWD"

# Set up autotools from pre-built prefix tree.
# The prefix contains __EXECROOT__ placeholders that must be fixed up.
AUTOTOOLS_TMP=$(mktemp -d)
cp -rL "$EXECROOT/{autotools_prefix}/." "$AUTOTOOLS_TMP/"
find "$AUTOTOOLS_TMP" -type f | while IFS= read -r f; do
    if file --mime-type "$f" | grep -q text; then
        sed -i "s|__EXECROOT__/{autotools_prefix}|$AUTOTOOLS_TMP|g" "$f"
        sed -i "s|__EXECROOT__|$EXECROOT|g" "$f"
    fi
done
export PATH="$AUTOTOOLS_TMP/{autotools_bindir}:$PATH"

# Tool paths
export PATH="$(dirname "$EXECROOT/{ghc_path}"):$PATH"
export PATH="$(dirname "$EXECROOT/{perl_path}"):$PATH"
export PATH="$(dirname "$EXECROOT/{m4_path}"):$PATH"
export PATH="$(dirname "$EXECROOT/{hadrian_path}"):$PATH"
export PATH="$(dirname "$EXECROOT/{cabal_path}"):$PATH"
{extra_tool_path}

# Inject `-L<gmp_lib_dir>` into every link command driven by the inner
# GHC (hadrian, ./configure, etc.) so gcc/ld can resolve `-lgmp` under
# the hermetic Bootlin sysroot, which ships no libgmp. The hermetic
# `x86_64-buildroot-linux-gnu-gcc` is a *cross-compiler*; GCC's docs
# (Environment-Variables, "LIBRARY_PATH") say `LIBRARY_PATH` is only
# consulted "when configured as a native compiler", so the env-var
# route is silently ignored here. The minimal env-independent knob is
# `-L`, which gcc/ld always honor on the command line. We deliver it
# by shimming a `ghc` wrapper earlier on PATH; GHC's configure
# auto-detects `ghc` from PATH and records the wrapper's absolute path
# in mk/config.mk, so hadrian's `Run Ghc LinkHs Stage0` invocations go
# through the wrapper. The wrapper forwards every argument to the real
# rules_haskell GHC bindist plus an extra `-optl-L<gmp_lib_dir>`, which
# GHC translates into a `-L` on the gcc link command. Compile-only
# (`-c`) invocations silently ignore `-optl-L`, so the wrapper is safe
# to interpose on every ghc call.
GHC_WRAPPER_DIR=$(mktemp -d)
GHC_BIN_DIR="$(dirname "$EXECROOT/{ghc_path}")"
# Mirror every bindist binary into the wrapper dir via symlinks, then
# override `ghc` with our wrapper. GHC's autoconf locates `ghc-pkg` etc.
# by inspecting the directory of the detected `ghc` (not by PATH lookup),
# so the wrapper directory must contain a co-located `ghc-pkg`, `hsc2hs`,
# `runghc`, etc., or configure aborts with "Cannot find matching ghc-pkg".
for bin in "$GHC_BIN_DIR"/*; do
    name="$(basename "$bin")"
    if [ "$name" != "ghc" ]; then
        ln -s "$bin" "$GHC_WRAPPER_DIR/$name"
    fi
done
cat > "$GHC_WRAPPER_DIR/ghc" <<'WRAPPER_EOF'
#!/bin/sh
exec "__GHC_REAL__" -optl-L"__GMP_DIR__" "$@"
WRAPPER_EOF
sed -i \
    -e "s|__GHC_REAL__|$EXECROOT/{ghc_path}|" \
    -e "s|__GMP_DIR__|$EXECROOT/{gmp_lib_dir}|" \
    "$GHC_WRAPPER_DIR/ghc"
chmod +x "$GHC_WRAPPER_DIR/ghc"
export PATH="$GHC_WRAPPER_DIR:$PATH"

# Locale
if [ "$(uname)" = "Darwin" ]; then
    export LANG=en_US.UTF-8
else
    export LANG=C.UTF-8
fi

CC_PATH="{cc_path}"
LD_PATH="{ld_path}"
case "$CC_PATH" in /*) ;; *) CC_PATH="$EXECROOT/$CC_PATH" ;; esac
case "$LD_PATH" in /*) ;; *) LD_PATH="$EXECROOT/$LD_PATH" ;; esac
export CC="$CC_PATH"
export LD="$LD_PATH"

# Copy GHC source tree to a writable temp directory
GHC_DIR="$EXECROOT/{ghc_src_dir}"
TMP=$(mktemp -d)
trap "rm -rf $TMP $AUTOTOOLS_TMP" EXIT
cp -rLt $TMP $GHC_DIR/.
export HOME="$TMP"

# Generate ghc-lib{component} cabal project
$EXECROOT/{ghc_lib_gen_path} $TMP \
    --ghc-lib{component} \
    --ghc-flavor={ghc_flavor} \
    {cpp_options}

# Remove absolute paths to the execroot from GHC settings
sed -i.bak \
    -e "s#$EXECROOT/##" \
    $TMP/ghc-lib/stage0/lib/settings

# Patch the ghc-lib version
sed -i.bak \
    -e 's#version: 0.1.0#version: {version}#' \
    $TMP/ghc-lib{component}.cabal

# Copy cabal file to declared output
cp $TMP/ghc-lib{component}.cabal $EXECROOT/{cabal_output}

# Create source distribution tarball
(cd $TMP; $EXECROOT/{cabal_path} sdist -o $EXECROOT/{output_dir})
""".format(
        autotools_prefix = autotools_info.prefix.path,
        autotools_bindir = autotools_info.bindir,
        ghc_path = ghc.path,
        perl_path = perl_bin.path,
        m4_path = ctx.file.m4.path,
        hadrian_path = ctx.executable.hadrian.path,
        cabal_path = ctx.file.cabal.path,
        ghc_lib_gen_path = ctx.executable.ghc_lib_gen.path,
        extra_tool_path = "\n".join(
            ['export PATH={}:"$PATH"'.format(d) for d in extra_tool_path_entries]
        ),
        cc_path = cc,
        ld_path = ld,
        ghc_src_dir = ctx.file.readme.dirname,
        component = component,
        version = version,
        ghc_flavor = ctx.attr.ghc_flavor,
        cpp_options = cpp_options,
        cabal_output = cabal_file.path,
        output_dir = tarball.dirname,
        gmp_lib_dir = gmp_lib_dir,
    )

    # -- Action --
    ctx.actions.run_shell(
        outputs = [cabal_file, tarball],
        inputs = depset(
            direct = [
                ctx.file.readme,
                autotools_info.prefix,
                ctx.file.m4,
                ctx.file.cabal,
                perl_bin,
            ] + ctx.files.ghc_srcs + ctx.files.perl + ghc_bindir + ghc_libdir + ctx.files.extra_tools + ctx.files.gmp,
            transitive = [cc_toolchain.all_files],
        ),
        tools = [
            ctx.executable.ghc_lib_gen,
            ctx.executable.hadrian,
        ],
        command = shell_cmd,
        mnemonic = "GhcLibSdist",
        progress_message = "Generating ghc-lib%s sdist" % component,
        use_default_shell_env = False,
    )

    return [DefaultInfo(files = depset([cabal_file, tarball]))]

ghc_lib_sdist = rule(
    implementation = _ghc_lib_sdist_impl,
    attrs = {
        "ghc_srcs": attr.label(mandatory = True),
        "readme": attr.label(mandatory = True, allow_single_file = True),
        "ghc_lib_gen": attr.label(mandatory = True, executable = True, cfg = "exec"),
        "hadrian": attr.label(mandatory = True, executable = True, cfg = "exec"),
        "autotools": attr.label(mandatory = True, providers = [InstalledGnuToolInfo]),
        "m4": attr.label(mandatory = True, allow_single_file = True),
        "perl": attr.label(mandatory = True),
        "cabal": attr.label(mandatory = True, allow_single_file = True),
        "component": attr.string(default = "", values = ["", "-parser"]),
        "version": attr.string(mandatory = True),
        "ghc_flavor": attr.string(mandatory = True),
        "extra_tools": attr.label_list(allow_files = True, doc = "Additional tool binaries needed on PATH (e.g. happy, alex)."),
        "cpp_options": attr.string_list(default = []),
        "cabal_out": attr.output(mandatory = True, doc = "Declared output for the .cabal file."),
        "tarball_out": attr.output(mandatory = True, doc = "Declared output for the sdist tarball."),
        "gmp": attr.label(
            mandatory = True,
            doc = "Hermetic libgmp target (filegroup or cc_library). The " +
                  "file directory is injected as `-optl-L<dir>` into every " +
                  "inner GHC invocation (via a `ghc` wrapper shim on PATH) " +
                  "so hadrian's deriveConstants link can resolve `-lgmp` " +
                  "under the hermetic Bootlin sysroot.",
        ),
        "_cc_toolchain": attr.label(default = "@rules_cc//cc:current_cc_toolchain"),
    },
    toolchains = [
        "@rules_haskell//haskell:toolchain",
        "@rules_cc//cc:toolchain_type",
    ],
    fragments = ["cpp"],
)
