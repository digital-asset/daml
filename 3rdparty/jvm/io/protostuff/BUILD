# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "protostuff_api",
    exports = [
        "//external:jar/io/protostuff/protostuff_api"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "protostuff_core",
    exports = [
        "//external:jar/io/protostuff/protostuff_core",
        ":protostuff_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)


