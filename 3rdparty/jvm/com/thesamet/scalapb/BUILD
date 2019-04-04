# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
scala_import(
    name = "compilerplugin",
    exports = [
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":protoc_bridge"
    ],
    jars = [
        "//external:jar/com/thesamet/scalapb/compilerplugin_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "lenses",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/thesamet/scalapb/lenses_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "protoc_bridge",
    exports = [
        "//3rdparty/jvm/org/scala_lang:scala_library"
    ],
    jars = [
        "//external:jar/com/thesamet/scalapb/protoc_bridge_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "scalapb_runtime",
    exports = [
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/lihaoyi:fastparse",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":lenses"
    ],
    jars = [
        "//external:jar/com/thesamet/scalapb/scalapb_runtime_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)



scala_import(
    name = "scalapb_runtime_grpc",
    exports = [
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        ":scalapb_runtime"
    ],
    jars = [
        "//external:jar/com/thesamet/scalapb/scalapb_runtime_grpc_2_12"
    ],
    visibility = [
        "//visibility:public"
    ]
)


