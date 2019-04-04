# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "hamcrest_all",
    exports = [
        "//external:jar/org/hamcrest/hamcrest_all"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "hamcrest_core",
    exports = [
        "//external:jar/org/hamcrest/hamcrest_core"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "hamcrest_library",
    exports = [
        "//external:jar/org/hamcrest/hamcrest_library",
        ":hamcrest_core"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


