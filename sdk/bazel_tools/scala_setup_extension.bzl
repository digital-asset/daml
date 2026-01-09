load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")
load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

def _scala_setup_impl(module_ctx):
    scala_repositories()
    scala_register_toolchains()

scala_setup = module_extension(
    implementation = _scala_setup_impl,
)