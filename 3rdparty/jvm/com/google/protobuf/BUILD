# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "protobuf_java",
    exports = [
        "//external:jar/com/google/protobuf/protobuf_java"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "protobuf_java_util",
    exports = [
        "//3rdparty/jvm/com/google/code/gson:gson",
        "//external:jar/com/google/protobuf/protobuf_java_util",
        ":protobuf_java"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


