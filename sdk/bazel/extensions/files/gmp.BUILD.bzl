genrule(
    name = "gmp_build",
    srcs = glob(["**"]),
    outs = [
        "lib/libgmp.so",
        "lib/libgmp.so.10",
    ],
    # Resolve `make` from the hermetic GNU make repository instead of PATH.
    # This keeps the action reproducible across environments where host
    # `make` may not be installed.
    cmd = """
        CC=$$PWD/$(CC)
        AR=$$PWD/$(AR)
        M4=$$PWD/$(execpath @m4//:m4_binary)
        MAKE=$$PWD/$(execpath @hermetic_make_linux_amd64//:bin/make)
        SRC=$$(dirname $(location configure))
        PREFIX=$$(realpath $(@D))
        cd $$SRC && CC="$$CC" AR="$$AR" M4=$$M4 \
        ./configure \
            --prefix=$$PREFIX \
            --with-shared \
            --disable-static \
        && $$MAKE -j$$(nproc) CC="$$CC" AR="$$AR" \
        && $$MAKE install \
        && cd $$PREFIX/lib \
        && for f in *.so *.so.*; do \
            if [ -L "$$f" ]; then \
                target=$$(readlink -f "$$f"); \
                rm "$$f"; \
                cp "$$target" "$$f"; \
            fi; \
        done
    """,
    tools = [
        "@hermetic_make_linux_amd64//:bin/make",
        "@m4//:m4_binary",
    ],
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
)

filegroup(
    name = "libs",
    srcs = [":gmp_build"],
    visibility = ["//visibility:public"],
)

# Wraps the hermetic libgmp.so as a cc_library so it can be passed to
# rules_haskell's stack_snapshot `extra_deps` (which expects CcInfo
# providers). GHC's integer-gmp emits `-lgmp` into every Haskell link
# command; without this, the hermetic sysroot's ld cannot find
# `-lgmp`.
cc_library(
    name = "gmp_cc_lib",
    srcs = ["lib/libgmp.so"],
    visibility = ["//visibility:public"],
)
