# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "gigahorse_core",
    exports = [
        "//3rdparty/jvm/com/typesafe:ssl_config_core",
        "//3rdparty/jvm/org/reactivestreams:reactive_streams",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/slf4j:slf4j_api"
    ],
    jars = [
        "//external:jar/com/eed3si9n/gigahorse_core_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "gigahorse_okhttp",
    exports = [
        "//3rdparty/jvm/com/squareup/okhttp3:okhttp",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":gigahorse_core"
    ],
    jars = [
        "//external:jar/com/eed3si9n/gigahorse_okhttp_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "shaded_scalajson",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/eed3si9n/shaded_scalajson_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "sjson_new_core",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/eed3si9n/sjson_new_core_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "sjson_new_murmurhash",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":sjson_new_core"
    ],
    jars = [
        "//external:jar/com/eed3si9n/sjson_new_murmurhash_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



scala_import(
    name = "sjson_new_scalajson",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/spire_math:jawn_parser",
        ":shaded_scalajson",
        ":sjson_new_core"
    ],
    jars = [
        "//external:jar/com/eed3si9n/sjson_new_scalajson_2_12"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


