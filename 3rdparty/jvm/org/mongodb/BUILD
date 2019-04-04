# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "bson",
    exports = [
        "//external:jar/org/mongodb/bson"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "casbah_commons_2_12_0_RC1",
    exports = [
        "//3rdparty/jvm/com/github/nscala_time:nscala_time_2_12_0_RC1",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        ":mongo_java_driver"
    ],
    jars = [
        "//external:jar/org/mongodb/casbah_commons_2_12_0_RC1"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "casbah_core_2_12_0_RC1",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":casbah_commons_2_12_0_RC1",
        ":casbah_query_2_12_0_RC1"
    ],
    jars = [
        "//external:jar/org/mongodb/casbah_core_2_12_0_RC1"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "casbah_gridfs_2_12_0_RC1",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":casbah_core_2_12_0_RC1"
    ],
    jars = [
        "//external:jar/org/mongodb/casbah_gridfs_2_12_0_RC1"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "casbah_query_2_12_0_RC1",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":casbah_commons_2_12_0_RC1"
    ],
    jars = [
        "//external:jar/org/mongodb/casbah_query_2_12_0_RC1"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "mongo_java_driver",
    exports = [
        "//external:jar/org/mongodb/mongo_java_driver"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "mongodb_driver_async",
    exports = [
        "//external:jar/org/mongodb/mongodb_driver_async",
        ":bson",
        ":mongodb_driver_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "mongodb_driver_core",
    exports = [
        "//external:jar/org/mongodb/mongodb_driver_core",
        ":bson"
    ],
    visibility = [
        "//visibility:public"
    ]
)


