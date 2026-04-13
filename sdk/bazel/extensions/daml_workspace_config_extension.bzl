load("//bazel_tools:build_environment.bzl", "build_environment")
load("//bazel_tools:os_info.bzl", "os_info")
load("//bazel_tools:scala_version.bzl", "scala_version_configure")
load("//daml-lf:json_repo.bzl", "daml_versions_repo")

def _daml_workspace_config_impl(module_ctx):
    scala_version_configure(name = "scala_version")
    os_info(name = "os_info")
    build_environment(name = "build_environment")
    daml_versions_repo(
        name = "daml_versions_data",
        json_file = "//daml-lf:daml-lf-versions.json",
    )

daml_workspace_config = module_extension(
    implementation = _daml_workspace_config_impl,
)
