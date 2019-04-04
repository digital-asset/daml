# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "jna",
    exports = [
        "//external:jar/net/java/dev/jna/jna"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "jna_platform",
    exports = [
        "//external:jar/net/java/dev/jna/jna_platform",
        ":jna"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


