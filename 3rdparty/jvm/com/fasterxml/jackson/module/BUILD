# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "jackson_module_parameter_names",
    exports = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//external:jar/com/fasterxml/jackson/module/jackson_module_parameter_names"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jackson_module_paranamer",
    exports = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/thoughtworks/paranamer:paranamer",
        "//external:jar/com/fasterxml/jackson/module/jackson_module_paranamer"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "jackson_module_scala",
    exports = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_annotations",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":jackson_module_paranamer"
    ],
    jars = [
        "//external:jar/com/fasterxml/jackson/module/jackson_module_scala_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)


