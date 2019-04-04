# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "scalameter",
    exports = [
        "//3rdparty/jvm/com/fasterxml/jackson/module:jackson_module_scala",
        "//3rdparty/jvm/commons_io:commons_io",
        "//3rdparty/jvm/io/spray:spray_json",
        "//3rdparty/jvm/org/apache/commons:commons_math3",
        "//3rdparty/jvm/org/mongodb/scala:mongo_scala_driver",
        "//3rdparty/jvm/org/scala_lang/modules:scala_parser_combinators",
        "//3rdparty/jvm/org/scala_lang/modules:scala_xml",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/scala_tools/testing:test_interface",
        ":scalameter_core"
    ],
    jars = [
        "//external:jar/com/storm_enroute/scalameter_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "scalameter_core",
    exports = [
        "//3rdparty/jvm/org/apache/commons:commons_lang3",
        "//3rdparty/jvm/org/apache/commons:commons_math3",
        "//3rdparty/jvm/org/ow2/asm:asm",
        "//3rdparty/jvm/org/scala_lang/modules:scala_parser_combinators",
        "//3rdparty/jvm/org/scala_lang/modules:scala_xml",
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/storm_enroute/scalameter_core_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)


