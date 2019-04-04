# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "junixsocket_common",
    exports = [
        "//3rdparty/jvm/log4j:log4j",
        "//external:jar/com/kohlschutter/junixsocket/junixsocket_common"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "junixsocket_native_common",
    exports = [
        "//3rdparty/jvm/log4j:log4j",
        "//3rdparty/jvm/org/scijava:native_lib_loader",
        "//external:jar/com/kohlschutter/junixsocket/junixsocket_native_common",
        ":junixsocket_common"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


