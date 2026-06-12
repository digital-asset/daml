# zlib builds natively as a cc_library: a standard build needs no `configure`
# (the release ships a usable `zconf.h`), so Bazel's C++ actions compile it
# directly via the hermetic toolchain -- no genrule, no host make.
#
# Exposes `zlib_cc_lib` for rules_haskell's stack_snapshot `extra_deps` (which
# expects CcInfo). The `digest` and `zlib` Haskell packages fail Cabal's
# configure step ("Missing dependency on a foreign library: ... zlib.h /
# C library: z") without this wiring.
cc_library(
    name = "zlib_cc_lib",
    srcs = glob(["*.c"]),
    hdrs = glob(["*.h"]),
    # configure normally sets this; it gates <unistd.h> (write/read/close) in
    # the gz*.c sources via zconf.h.
    copts = ["-DHAVE_UNISTD_H"],
    includes = ["."],
    visibility = ["//visibility:public"],
)
