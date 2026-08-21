PATCHELF_VERSION = "0.19.1"

_URL = "https://github.com/NixOS/patchelf/releases/download/{v}/patchelf-{v}-{arch}.tar.gz"

PATCHELF_BUILDS = {
    "x86_64": {
        "url": _URL.format(v = PATCHELF_VERSION, arch = "x86_64"),
        "sha256": "a6818fef80128fb354423234ecacdcca3e993913d774e5d8346bc63f70fed4cf",
    },
    "aarch64": {
        "url": _URL.format(v = PATCHELF_VERSION, arch = "aarch64"),
        "sha256": "a2f8f5add5910a521d35062adf2c9f55d75b65ae5508d290758787004054e702",
    },
}
