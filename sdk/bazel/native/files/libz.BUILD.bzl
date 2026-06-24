load("@rules_cc//cc:defs.bzl", "cc_library", "cc_shared_library")

cc_library(
    name = "libz",
    srcs = glob(["*.c"]),
    hdrs = glob(["*.h"]),
    copts = ["-DHAVE_UNISTD_H"],
    includes = ["."],
    visibility = ["//visibility:public"],
)

cc_shared_library(
    name = "libz_so",
    shared_lib_name = "libz.so",
    deps = [":libz"],
)

filegroup(
    name = "libs",
    srcs = [":libz_so"],
    visibility = ["//visibility:public"],
)
