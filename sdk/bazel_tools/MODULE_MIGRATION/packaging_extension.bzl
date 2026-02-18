load("//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dev_env_tool")

def _impl(module_ctx):
    """
    Note:
        This should be later migrated to use bazel defined toolset, not
        one from dev_env
    """
    dev_env_tool(
        name = "tar_dev_env",
        nix_include = ["bin/tar"],
        nix_label = "@tar_nix",
        nix_paths = ["bin/tar"],
        tools = ["tar"],
        win_include = ["usr/bin/tar.exe"],
        win_paths = ["usr/bin/tar.exe"],
        win_tool = "msys2",
    )
    dev_env_tool(
        name = "gzip_dev_env",
        nix_include = ["bin/gzip"],
        nix_label = "@gzip_nix",
        nix_paths = ["bin/gzip"],
        tools = ["gzip"],
        win_include = ["usr/bin/gzip.exe"],
        win_paths = ["usr/bin/gzip.exe"],
        win_tool = "msys2",
    )

packaging_extension = module_extension(implementation = _impl)