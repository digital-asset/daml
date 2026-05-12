genrule(
    name = "ncurses_build",
    srcs = glob(["**"]),
    outs = [
        "lib/libtinfow.so",
        "lib/libncursesw.so",
        "lib/libformw.so",
        "lib/libmenuw.so",
        "lib/libpanelw.so",
        "lib/libtinfo.so",
        "lib/libncurses.so",
        "lib/libform.so",
        "lib/libmenu.so",
        "lib/libpanel.so",
        "lib/libtinfo.so.5",
        "lib/libtinfo.so.6",
    ],
    cmd = """
        CC=$$PWD/$(CC)
        AR=$$PWD/$(AR)
        PATCHELF=$$PWD/$(execpath @patchelf//:patchelf)
        SRC=$$(dirname $(location configure))
        PREFIX=$$(realpath $(@D))
        cd $$SRC && CC="$$CC" AR="$$AR" \
        ./configure \
            --prefix=$$PREFIX \
            --with-shared \
            --with-termlib \
            --enable-widec \
            --without-debug \
            --without-ada \
            --without-manpages \
            --without-tests \
        && make -j$$(nproc) CC="$$CC" AR="$$AR" \
        && make install \
        && cd $$PREFIX/lib \
        && for f in *.so; do \
            if [ -L "$$f" ]; then \
                target=$$(readlink -f "$$f"); \
                rm "$$f"; \
                cp "$$target" "$$f"; \
            fi; \
        done \
        && for f in *.so.*; do \
            if [ -L "$$f" ]; then \
                target=$$(readlink -f "$$f"); \
                rm "$$f"; \
                cp "$$target" "$$f"; \
            fi; \
        done \
        && for lib in ncurses form menu panel; do \
            cp lib$${lib}w.so lib$${lib}.so; \
        done \
        && cp libtinfow.so libtinfo.so \
        && cp libtinfo.so libtinfo.so.5 \
        && cp libtinfo.so libtinfo.so.6 \
        && $$PATCHELF --set-soname libtinfo.so libtinfo.so
    """,
    tools = ["@patchelf//:patchelf"],
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
)

filegroup(
    name = "libs",
    srcs = [":ncurses_build"],
    visibility = ["//visibility:public"],
)

# Wraps the hermetic libtinfo.so as a cc_library so it can be passed to
# rules_haskell's stack_snapshot `extra_deps` (which expects CcInfo
# providers). GHC's `haskeline` package emits `-ltinfo` into every link
# command that pulls in `terminfo` / `haskeline`; without this, the
# hermetic Bootlin sysroot's ld cannot find `-ltinfo` (sdk/dump.txt:328).
# No header is required -- haskeline only needs the .so at link time.
cc_library(
    name = "tinfo_cc_lib",
    srcs = ["lib/libtinfo.so"],
    visibility = ["//visibility:public"],
)