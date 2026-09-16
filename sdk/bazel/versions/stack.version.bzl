# -- stack --
# https://docs.haskellstack.org/
STACK_VERSION = "2.15.7"

_BASE_URL = "https://github.com/commercialhaskell/stack/releases/download/v{v}/stack-{v}".format(
    v = STACK_VERSION,
)

STACK_PLATFORMS = {
    "linux-x86_64": {
        "url": _BASE_URL + "-linux-x86_64.tar.gz",
        "sha256": "4e635d6168f7578a5694a0d473c980c3c7ed35d971acae969de1fd48ef14e030",
    },
    "linux-aarch64": {
        "url": _BASE_URL + "-linux-aarch64.tar.gz",
        "sha256": "f0c4b038c7e895902e133a2f4c4c217e03c4be44aa5da48aec9f7947f4af090b",
    },
    "osx-x86_64": {
        "url": _BASE_URL + "-osx-x86_64.tar.gz",
        "sha256": "ef97f65759a922bc7f5399d9311afdc4a43cc454b70ea7426f991c067899cef1",
    },
    "osx-aarch64": {
        "url": _BASE_URL + "-osx-aarch64.tar.gz",
        "sha256": "fc963f041fbe3ddf3ff12271c74334846a583a0714ce808d8a2d2c91de4a3968",
    },
    "windows-x86_64": {
        "url": _BASE_URL + "-windows-x86_64.tar.gz",
        "sha256": "0e051f44aadcec138ffc1c1056bfec5c8216f0d256413c2e3a60ce55fe2db0ba",
    },
}
