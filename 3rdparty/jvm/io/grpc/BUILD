# Do not edit. bazel-deps autogenerates this file from dependencies.yaml.
# See 3rdparty/README.md
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library")
load("@io_bazel_rules_scala//scala:scala_import.bzl", "scala_import")
java_library(
    name = "grpc_api",
    exports = [
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/errorprone:error_prone_annotations",
        "//3rdparty/jvm/com/google/guava:guava",
        "//3rdparty/jvm/org/codehaus/mojo:animal_sniffer_annotations",
        "//external:jar/io/grpc/grpc_api",
        ":grpc_context"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "grpc_context",
    exports = [
        "//external:jar/io/grpc/grpc_context"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "grpc_core",
    exports = [
        "//3rdparty/jvm/com/google/android:annotations",
        "//3rdparty/jvm/com/google/code/gson:gson",
        "//3rdparty/jvm/io/opencensus:opencensus_api",
        "//3rdparty/jvm/io/opencensus:opencensus_contrib_grpc_metrics",
        "//external:jar/io/grpc/grpc_core",
        ":grpc_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_netty",
    exports = [
        "//3rdparty/jvm/io/netty:netty_codec_http2",
        "//3rdparty/jvm/io/netty:netty_handler_proxy",
        "//external:jar/io/grpc/grpc_netty",
        ":grpc_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_protobuf",
    exports = [
        "//3rdparty/jvm/com/google/api/grpc:proto_google_common_protos",
        "//3rdparty/jvm/com/google/guava:guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//external:jar/io/grpc/grpc_protobuf",
        ":grpc_api",
        ":grpc_protobuf_lite"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_protobuf_lite",
    exports = [
        "//3rdparty/jvm/com/google/guava:guava",
        "//external:jar/io/grpc/grpc_protobuf_lite",
        ":grpc_api"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "grpc_services",
    exports = [
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        "//external:jar/io/grpc/grpc_services",
        ":grpc_core",
        ":grpc_protobuf",
        ":grpc_stub"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_stub",
    exports = [
        "//external:jar/io/grpc/grpc_stub",
        ":grpc_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)


