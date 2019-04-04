# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "jline",
    exports = [
        "//external:jar/org/jline/jline"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jline_reader",
    exports = [
        "//external:jar/org/jline/jline_reader",
        ":jline_terminal"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jline_terminal",
    exports = [
        "//external:jar/org/jline/jline_terminal"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


