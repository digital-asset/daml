load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:grpc_java.version.bzl",
    "GRPC_JAVA_SHA256",
    "GRPC_JAVA_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "io_grpc_grpc_java",
        strip_prefix = "grpc-java-{}".format(GRPC_JAVA_VERSION),
        urls = ["https://github.com/grpc/grpc-java/archive/v{}.tar.gz".format(GRPC_JAVA_VERSION)],
        sha256 = GRPC_JAVA_SHA256,
        # grpc-java's BUILD files use the legacy WORKSPACE name @com_google_protobuf;
        # under Bzlmod it is exposed as @protobuf.
        # Protobuf 3.22+ (and 32.x via MVS) changed Descriptor::name() from
        # returning const std::string& to absl::string_view.  absl::string_view
        # does not have an implicit conversion to std::string, so all usages
        # where std::string is expected must be wrapped in std::string(...).
        # Wrap bare ->name() / ->file()->name() in std::string() so absl::string_view
        # (protobuf 3.22+ Descriptor::name()) converts to std::string. sed (in the base
        # image) keeps this fetch host-python-free; the three patterns are disjoint, so
        # the order is irrelevant and no double-wrap can occur.
        patch_cmds = [
            "sed -i 's|@com_google_protobuf|@protobuf|g' compiler/BUILD.bazel",
            "sed -i" +
            " -e 's/method->name()/std::string(method->name())/g'" +
            " -e 's/service->name()/std::string(service->name())/g'" +
            " -e 's/service->file()->name()/std::string(service->file()->name())/g'" +
            " compiler/src/java_plugin/cpp/java_generator.cpp",
        ],
    )

io_grpc_grpc_java_extension = module_extension(implementation = _impl)
