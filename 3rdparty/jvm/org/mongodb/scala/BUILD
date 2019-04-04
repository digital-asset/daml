# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "mongo_scala_bson",
    exports = [
        "//3rdparty/jvm/org/mongodb:mongodb_driver_async",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/scala_lang:scala_reflect"
    ],
    jars = [
        "//external:jar/org/mongodb/scala/mongo_scala_bson_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "mongo_scala_driver",
    exports = [
        "//3rdparty/jvm/org/mongodb:mongodb_driver_async",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/scala_lang:scala_reflect",
        ":mongo_scala_bson"
    ],
    jars = [
        "//external:jar/org/mongodb/scala/mongo_scala_driver_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


