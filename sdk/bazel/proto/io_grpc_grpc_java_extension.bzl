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
        patch_cmds = [
            "sed -i 's|@com_google_protobuf|@protobuf|g' compiler/BUILD.bazel",
            """python3 -c "
import re, sys
path = 'compiler/src/java_plugin/cpp/java_generator.cpp'
text = open(path).read()
# Wrap all bare ->name() and ->file()->name() calls in std::string()
# so absl::string_view is explicitly converted to std::string everywhere.
for sv in ['method->name()', 'service->name()', 'service->file()->name()']:
    text = text.replace(sv, 'std::string(' + sv + ')')
# Undo double-wrapping introduced by the above loop.
for sv in ['method->name()', 'service->name()', 'service->file()->name()']:
    double = 'std::string(std::string(' + sv + '))'
    single = 'std::string(' + sv + ')'
    text = text.replace(double, single)
open(path, 'w').write(text)
" """,
        ],
    )

io_grpc_grpc_java_extension = module_extension(implementation = _impl)
