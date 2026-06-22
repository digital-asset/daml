load("@bazel_skylib//rules:expand_template.bzl", "expand_template")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_shared_library", "cc_test")
load(":limb.bzl", "limb_select")

package(default_visibility = ["//visibility:public"])

# This BUILD is injected as @gmp//:BUILD over the upstream gmp-6.3.0 tarball.
# The configure-replacing sources (config.h, gmp-mparam.h, limb.bzl, smoke.c)
# are added into the tree by handauthored.patch; op/*.c wrappers are generated
# below; everything else (the gmp .c sources, gmp-h.in, tests/) is upstream.
#
# Nails are unused on every target we build (always 0). Limb width is the only
# configure-probed value that varies per CPU; it comes from limb_select().

# =============================================================================
# gmp.h — substitute the @...@ placeholders in gmp-h.in. expand_template does
# this in-process (no shell, no sed). Only @GMP_LIMB_BITS@ depends on the target
# CPU; the rest are constant for the generic-C, non-DLL, unsigned-long-limb
# targets we support.
# =============================================================================
_GMP_H_SUBST_64 = {
    "@GMP_LIMB_BITS@": "64",
    "@GMP_NAIL_BITS@": "0",
    "@HAVE_HOST_CPU_FAMILY_power@": "0",
    "@HAVE_HOST_CPU_FAMILY_powerpc@": "0",
    "@LIBGMP_DLL@": "0",
    "@DEFN_LONG_LONG_LIMB@": "/* #undef _LONG_LONG_LIMB */",
    "@CC@": "cc",
    "@CFLAGS@": "-O2",
}

_GMP_H_SUBST_32 = {
    "@GMP_LIMB_BITS@": "32",
    "@GMP_NAIL_BITS@": "0",
    "@HAVE_HOST_CPU_FAMILY_power@": "0",
    "@HAVE_HOST_CPU_FAMILY_powerpc@": "0",
    "@LIBGMP_DLL@": "0",
    "@DEFN_LONG_LONG_LIMB@": "/* #undef _LONG_LONG_LIMB */",
    "@CC@": "cc",
    "@CFLAGS@": "-O2",
}

expand_template(
    name = "gmp_h",
    out = "gmp.h",
    substitutions = limb_select({
        "64": _GMP_H_SUBST_64,
        "32": _GMP_H_SUBST_32,
    }),
    template = "gmp-h.in",
)

# =============================================================================
# Table-generator host tools (compiled + run during the build).
# Most #include "bootstrap.c" -> "mini-gmp/mini-gmp.c"; expose those as a
# textual include library so they are staged but not compiled standalone.
# =============================================================================
cc_library(
    name = "bootstrap_includes",
    textual_hdrs = [
        "bootstrap.c",
        "mini-gmp/mini-gmp.c",
        "mini-gmp/mini-gmp.h",
    ],
    includes = ["."],
)

GEN_TOOLS = [
    "gen-fac",
    "gen-fib",
    "gen-bases",
    "gen-trialdivtab",
    "gen-sieve",
    "gen-jacobitab",
    "gen-psqr",
]

[cc_binary(
    name = t,
    srcs = [t + ".c"],
    deps = [":bootstrap_includes"],
) for t in GEN_TOOLS]

# =============================================================================
# Generated tables (headers + two compiled sources), produced by the tools.
# Invocation templates: {bits} is filled per CPU by limb_select(); nails are 0.
# =============================================================================
_TABLE_GENRULES = {
    "fac_table_h": ("fac_table.h", ":gen-fac", "$(location :gen-fac) {bits} 0"),
    "sieve_table_h": ("sieve_table.h", ":gen-sieve", "$(location :gen-sieve) {bits}"),
    "fib_table_h": ("fib_table.h", ":gen-fib", "$(location :gen-fib) header {bits} 0"),
    "mp_bases_h": ("mp_bases.h", ":gen-bases", "$(location :gen-bases) header {bits} 0"),
    "trialdivtab_h": ("trialdivtab.h", ":gen-trialdivtab", "$(location :gen-trialdivtab) {bits} 8000"),
    "fib_table_c": ("mpn/fib_table.c", ":gen-fib", "$(location :gen-fib) table {bits} 0"),
    "mp_bases_c": ("mpn/mp_bases.c", ":gen-bases", "$(location :gen-bases) table {bits} 0"),
    "jacobitab_h": ("mpn/jacobitab.h", ":gen-jacobitab", "$(location :gen-jacobitab)"),
    "perfsqr_h": ("mpn/perfsqr.h", ":gen-psqr", "$(location :gen-psqr) {bits} 0"),
}

[genrule(
    name = name,
    outs = [out],
    cmd = limb_select({
        "64": invocation.format(bits = "64") + " > $@",
        "32": invocation.format(bits = "32") + " > $@",
    }),
    tools = [tool],
) for name, (out, tool, invocation) in _TABLE_GENRULES.items()]

_GENERATED_HEADERS = [
    ":fac_table_h",
    ":sieve_table_h",
    ":fib_table_h",
    ":mp_bases_h",
    ":trialdivtab_h",
    ":jacobitab_h",
    ":perfsqr_h",
]

_GENERATED_SOURCES = [
    ":fib_table_c",
    ":mp_bases_c",
]

# Multi-function generic files: compiled once per function via op/ wrappers,
# never standalone, so they are textual includes rather than srcs.
_MULTIFUNC_GENERIC = [
    "mpn/generic/logops_n.c",
    "mpn/generic/popham.c",
    "mpn/generic/sec_aors_1.c",
    "mpn/generic/sec_pi1_div.c",
    "mpn/generic/sec_div.c",
]

# =============================================================================
# op/ wrappers — generated, one per (function, multi-function file) pair. Each
# selects a single function from a _MULTIFUNC_GENERIC source via GMP's
# -DOPERATION_* mechanism. Previously committed by hand; generated here so the
# overlay carries no per-function files.
# =============================================================================
_OP_WRAPPERS = {
    "and_n": "logops_n.c",
    "andn_n": "logops_n.c",
    "ior_n": "logops_n.c",
    "iorn_n": "logops_n.c",
    "nand_n": "logops_n.c",
    "nior_n": "logops_n.c",
    "xnor_n": "logops_n.c",
    "xor_n": "logops_n.c",
    "hamdist": "popham.c",
    "popcount": "popham.c",
    "sec_add_1": "sec_aors_1.c",
    "sec_sub_1": "sec_aors_1.c",
    "sec_div_qr": "sec_div.c",
    "sec_div_r": "sec_div.c",
    "sec_pi1_div_qr": "sec_pi1_div.c",
    "sec_pi1_div_r": "sec_pi1_div.c",
}

[write_file(
    name = "gen_op_" + op,
    out = "op/" + op + ".c",
    content = [
        "/* Generated wrapper: select %s from mpn/generic/%s (GMP -DOPERATION_ mechanism). */" % (op, src),
        "#define OPERATION_%s 1" % op,
        "#include \"mpn/generic/%s\"" % src,
        "",
    ],
) for op, src in _OP_WRAPPERS.items()]

_OP_SRCS = ["op/%s.c" % op for op in _OP_WRAPPERS]

# =============================================================================
# libgmp — the C library, generic-C mpn (no assembly).
# =============================================================================
cc_library(
    name = "gmp",
    srcs = glob(
        [
            "*.c",  # top-level: assert, errno, memory, mp_*, primesieve, ...
            "mpz/*.c",
            "mpq/*.c",
            "mpf/*.c",
            "mpn/generic/*.c",
            "printf/*.c",
            "scanf/*.c",
            "rand/*.c",
        ],
        exclude = [
            "bootstrap.c",  # host-tool helper, not part of libgmp
            "gen-*.c",  # host tools
            "smoke.c",  # overlay smoke test, not part of libgmp
            "tal-debug.c",  # temp-alloc variants we don't use
            "tal-notreent.c",
            "printf/repl-vsnprintf.c",  # have vsnprintf
            # CPU-specific generic files not used on x86_64 (no sdiv_qrnnd
            # macro in longlong.h for this target):
            "mpn/generic/udiv_w_sdiv.c",
        ] + _MULTIFUNC_GENERIC,
    ) + _GENERATED_SOURCES + _OP_SRCS,
    hdrs = [":gmp_h"],
    copts = [
        "-D__GMP_WITHIN_GMP",
        "-D_GNU_SOURCE",
    ],
    includes = [
        ".",
        "mpn",
    ],
    textual_hdrs = glob([
        "*.h",
        "mpn/generic/*.h",
        "mpf/*.h",
        "mpz/*.h",
        "rand/*.h",
    ]) + _GENERATED_HEADERS + _MULTIFUNC_GENERIC,
)

# Consumer-facing name: base's GMP_DEPS (and others) depend on @gmp//:gmp_cc_lib.
alias(
    name = "gmp_cc_lib",
    actual = ":gmp",
)

# Hermetic shared libgmp.so for consumers that need a `-L<dir>`/`-lgmp` lib
# directory (ghc_lib_sdist's inner GHC link, damlc's extra_lib_dirs). Built
# from the same generic-C objects, so it keeps the 2.28 floor.
cc_shared_library(
    name = "gmp_so",
    shared_lib_name = "libgmp.so",
    deps = [":gmp"],
)

filegroup(
    name = "libs",
    srcs = [":gmp_so"],
)

# =============================================================================
# Smoke test (smoke.c added by handauthored.patch).
# =============================================================================
cc_test(
    name = "smoke",
    srcs = ["smoke.c"],
    linkstatic = True,
    deps = [":gmp"],
)

# =============================================================================
# GMP's own test suite (upstream tests/).
# =============================================================================
cc_library(
    name = "tests_lib",
    srcs = [
        "tests/memory.c",
        "tests/misc.c",
        "tests/refmpf.c",
        "tests/refmpn.c",
        "tests/refmpq.c",
        "tests/refmpz.c",
        "tests/spinner.c",
        "tests/trace.c",
    ],
    copts = [
        "-D__GMP_WITHIN_GMP",
        "-D_GNU_SOURCE",
    ],
    includes = [
        ".",
        "tests",
    ],
    textual_hdrs = ["tests/tests.h"] + glob([
        "*.h",
        "mpn/generic/*.h",
    ]) + _GENERATED_HEADERS,
    deps = [":gmp"],
)

# Curated, representative subset of the upstream tests. Each links libtests +
# libgmp. Paths are relative to tests/ ; the target name is the leaf.
_TESTS = [
    "t-constants",
    "t-bswap",
    "t-sub",
    "t-popc",
    "mpz/t-addsub",
    "mpz/t-mul",
    "mpz/t-gcd",
    "mpz/t-powm",
    "mpz/t-tdiv",
    "mpz/t-cmp",
    "mpz/t-pow",
    "mpz/t-fib_ui",
    "mpq/t-aors",
    "mpq/t-cmp",
    "mpf/t-add",
    "mpf/t-conv",
    "mpf/t-sqrt",
    "mpn/t-div",
]

[cc_test(
    name = "test_" + t.replace("/", "_"),
    srcs = ["tests/%s.c" % t],
    copts = [
        "-D__GMP_WITHIN_GMP",
        "-D_GNU_SOURCE",
    ],
    linkstatic = True,
    deps = [":tests_lib"],
) for t in _TESTS]
