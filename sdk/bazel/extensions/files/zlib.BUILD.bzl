genrule(
    name = "zlib_build",
    srcs = glob(["**"]),
    outs = [
        "lib/libz.so",
        "lib/libz.so.1",
        "include/zlib.h",
        "include/zconf.h",
    ],
    # Resolve `make` from the hermetic GNU make repository instead of PATH.
    # This keeps the action reproducible across environments where host
    # `make` may not be installed.
    cmd = """
        SRC=$$(realpath $$(dirname $(location configure)))
        PREFIX=$$(realpath $(@D))
        CC_ABS=$$PWD/$(CC)
        MAKE=$$PWD/$(execpath @hermetic_make_linux_amd64//:bin/make)
        BUILD=$$(mktemp -d /tmp/zlib-XXXXXX)
        cp -rpL $$SRC/. $$BUILD
        chmod -R u+w $$BUILD
        cd $$BUILD && CC="$$CC_ABS" \
        ./configure \
            --prefix=$$PREFIX \
            --shared \
        && $$MAKE -j$$(nproc) CC="$$CC_ABS" \
        && $$MAKE install \
        && cd $$PREFIX/lib \
        && for f in *.so *.so.*; do \
            if [ -L "$$f" ]; then \
                target=$$(readlink -f "$$f"); \
                rm "$$f"; \
                cp "$$target" "$$f"; \
            fi; \
        done \
        && rm -rf $$BUILD
    """,
    tools = ["@hermetic_make_linux_amd64//:bin/make"],
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
)

filegroup(
    name = "libs",
    srcs = [":zlib_build"],
    visibility = ["//visibility:public"],
)

# Wraps the hermetic libz.so + zlib.h as a cc_library so it can be passed
# to rules_haskell's stack_snapshot `extra_deps` (which expects CcInfo
# providers). The `digest` and `zlib` Haskell packages fail Cabal's
# configure step ("Missing dependency on a foreign library: ... zlib.h /
# C library: z") without this wiring; the legacy WORKSPACE flow had
# `extra_deps = {"digest": ["@com_github_madler_zlib//:libz"], ...}`.
cc_library(
    name = "zlib_cc_lib",
    srcs = ["lib/libz.so"],
    hdrs = [
        "include/zlib.h",
        "include/zconf.h",
    ],
    includes = ["include"],
    visibility = ["//visibility:public"],
)
