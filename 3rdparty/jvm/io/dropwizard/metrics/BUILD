# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "metrics_core",
    exports = [
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//external:jar/io/dropwizard/metrics/metrics_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "metrics_jmx",
    exports = [
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//external:jar/io/dropwizard/metrics/metrics_jmx",
        ":metrics_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)


