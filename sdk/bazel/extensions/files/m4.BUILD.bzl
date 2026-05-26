genrule(
    name = "m4_binary",
    srcs = glob(["**"]),
    outs = ["m4"],
    toolchains = ["@rules_cc//cc:current_cc_toolchain"],
    # Resolve `make` from the hermetic GNU make repository instead of PATH.
    # This keeps the action reproducible across environments where host
    # `make` may not be installed.
    cmd = """
        set -euo pipefail

        OUT=$$PWD/$@
        CC=$$PWD/$(CC)
        MAKE=$$PWD/$(execpath @hermetic_make_linux_amd64//:bin/make)
        SRC=$$(dirname $(location configure))

        cd $$SRC

        CC=$$CC ./configure
        $$MAKE -j

        cp src/m4 $$OUT
    """,
    tools = ["@hermetic_make_linux_amd64//:bin/make"],
    visibility = ["//visibility:public"],
)