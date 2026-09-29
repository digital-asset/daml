load("@os_info//:os_info.bzl", "is_windows")

exports_files([
    "dpm" if not is_windows else "dpm.exe",
    "dpm-sdk-release-tarball.tar.gz",
    "daml-script-no-failure-status-oci.tar.gz",
])
