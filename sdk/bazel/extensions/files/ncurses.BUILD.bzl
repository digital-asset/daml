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
    ],
    cmd = """
        SRC=$$(dirname $(location configure))
        PREFIX=$$(realpath $(@D))
        cd $$SRC && CC="$(CC)" AR="$(AR)" \
        ./configure \
            --prefix=$$PREFIX \
            --with-shared \
            --with-termlib \
            --enable-widec \
            --without-debug \
            --without-ada \
            --without-manpages \
            --without-tests \
        && make -j$$(nproc) CC="$(CC)" AR="$(AR)" \
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
        && cp libtinfo.so libtinfo.so.5
    """,
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
)

filegroup(
    name = "libs",
    srcs = [":ncurses_build"],
    visibility = ["//visibility:public"],
)