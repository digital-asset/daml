# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "silencer_lib",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/github/ghik/silencer_lib_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "silencer_plugin",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_compiler",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":silencer_lib"
    ],
    jars = [
        "//external:jar/com/github/ghik/silencer_plugin_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)


