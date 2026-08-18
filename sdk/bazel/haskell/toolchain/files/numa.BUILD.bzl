# cc_library, not configure_make: libtool drops -B/-resource-dir/-rtlib flags our hermetic clang needs when linking a .la shared library.

load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_cc//cc:defs.bzl", "cc_library", "cc_shared_library")

package(default_visibility = ["//visibility:public"])

write_file(
    name = "config_h",
    out = "config.h",
    content = [
        "#define HAVE_DLFCN_H 1",
        "#define HAVE_INTTYPES_H 1",
        "#define HAVE_STDINT_H 1",
        "#define HAVE_STDIO_H 1",
        "#define HAVE_STDLIB_H 1",
        "#define HAVE_STRINGS_H 1",
        "#define HAVE_STRING_H 1",
        "#define HAVE_SYS_STAT_H 1",
        "#define HAVE_SYS_TYPES_H 1",
        "#define HAVE_UNISTD_H 1",
        "#define PACKAGE \"numactl\"",
        "#define PACKAGE_NAME \"numactl\"",
        "#define PACKAGE_STRING \"numactl 2.0.19\"",
        "#define PACKAGE_TARNAME \"numactl\"",
        "#define PACKAGE_VERSION \"2.0.19\"",
        "#define STDC_HEADERS 1",
        "#define TLS __thread",
        "#define VERSION \"2.0.19\"",
        "",
    ],
)

# =============================================================================
# libnuma — the C library (libnuma.c + its 5 helper .c files).
# =============================================================================
cc_library(
    name = "numa_lib",
    srcs = [
        "libnuma.c",
        "syscall.c",
        "distance.c",
        "affinity.c",
        "sysfs.c",
        "rtnetlink.c",
    ],
    hdrs = [
        "numa.h",
        "numaif.h",
        "numacompat1.h",
    ],
    textual_hdrs = [
        "config.h",
        "affinity.h",
        "numaint.h",
        "rtnetlink.h",
        "sysfs.h",
        "util.h",
    ],
    copts = ["-DHAVE_CONFIG_H"],
    includes = ["."],
)

# Both need versions.ldscript: libnuma.c/syscall.c use `.symver` asm directives tied to its libnuma_1.x/2.x nodes.
cc_shared_library(
    name = "numa_so",
    shared_lib_name = "libnuma.so",
    additional_linker_inputs = ["versions.ldscript"],
    user_link_flags = ["-Wl,--version-script=$(location versions.ldscript)"],
    deps = [":numa_lib"],
)

cc_shared_library(
    name = "numa_so_1",
    shared_lib_name = "libnuma.so.1",
    additional_linker_inputs = ["versions.ldscript"],
    user_link_flags = ["-Wl,--version-script=$(location versions.ldscript)"],
    deps = [":numa_lib"],
)

filegroup(
    name = "libs",
    srcs = [
        ":numa_so",
        ":numa_so_1",
    ],
)
