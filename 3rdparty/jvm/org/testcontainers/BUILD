# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "jdbc",
    exports = [
        "//external:jar/org/testcontainers/jdbc",
        ":testcontainers"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "postgresql",
    exports = [
        "//external:jar/org/testcontainers/postgresql",
        ":jdbc"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "testcontainers",
    exports = [
        "//3rdparty/jvm/com/github/jnr:jffi",
        "//3rdparty/jvm/com/github/jnr:jffi_jar_native",
        "//3rdparty/jvm/com/github/jnr:jnr_constants",
        "//3rdparty/jvm/com/github/jnr:jnr_ffi",
        "//3rdparty/jvm/com/github/jnr:jnr_posix",
        "//3rdparty/jvm/com/github/jnr:jnr_x86asm",
        "//3rdparty/jvm/com/kohlschutter/junixsocket:junixsocket_common",
        "//3rdparty/jvm/com/kohlschutter/junixsocket:junixsocket_native_common",
        "//3rdparty/jvm/commons_codec:commons_codec",
        "//3rdparty/jvm/commons_io:commons_io",
        "//3rdparty/jvm/commons_lang:commons_lang",
        "//3rdparty/jvm/junit:junit",
        "//3rdparty/jvm/org/apache/commons:commons_compress",
        "//3rdparty/jvm/org/hamcrest:hamcrest_core",
        "//3rdparty/jvm/org/jetbrains:annotations",
        "//3rdparty/jvm/org/ow2/asm:asm",
        "//3rdparty/jvm/org/ow2/asm:asm_analysis",
        "//3rdparty/jvm/org/ow2/asm:asm_commons",
        "//3rdparty/jvm/org/ow2/asm:asm_tree",
        "//3rdparty/jvm/org/ow2/asm:asm_util",
        "//3rdparty/jvm/org/rnorth/duct_tape:duct_tape",
        "//3rdparty/jvm/org/rnorth/visible_assertions:visible_assertions",
        "//3rdparty/jvm/org/rnorth:tcp_unix_socket_proxy",
        "//3rdparty/jvm/org/scijava:native_lib_loader",
        "//3rdparty/jvm/org/slf4j:jcl_over_slf4j",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//3rdparty/jvm/org/slf4j:slf4j_ext",
        "//3rdparty/jvm/org/zeroturnaround:zt_exec",
        "//external:jar/org/testcontainers/testcontainers"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


