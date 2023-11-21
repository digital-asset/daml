load("@os_info//:os_info.bzl", "is_windows")

exports_files([
    "daml" if not is_windows else "daml.exe",
    "sdk-release-tarball-ce.tar.gz",
    "ledger-api-test-tool_deploy.jar",
])
