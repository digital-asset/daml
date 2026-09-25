# -- cabal-install --
# https://www.haskell.org/cabal/
CABAL_INSTALL_VERSION = "3.10.3.0"

_BASE_URL = "https://downloads.haskell.org/~cabal/cabal-install-{v}/cabal-install-{v}".format(
    v = CABAL_INSTALL_VERSION,
)

CABAL_PLATFORMS = {
    "linux-x86_64": {
        "url": _BASE_URL + "-x86_64-linux-deb10.tar.xz",
        "sha256": "1d7a7131402295b01f25be5373fde095a404c45f9b5a5508fb7474bb0d3d057a",
    },
    "linux-aarch64": {
        "url": _BASE_URL + "-aarch64-linux-deb10.tar.xz",
        "sha256": "92d341620c60294535f03098bff796ef6de2701de0c4fcba249cde18a2923013",
    },
    "darwin-x86_64": {
        "url": _BASE_URL + "-x86_64-darwin.tar.xz",
        "sha256": "3aed78619b2164dd61eb61afb024073ae2c50f6655fa60fcc1080980693e3220",
    },
    "darwin-aarch64": {
        "url": _BASE_URL + "-aarch64-darwin.tar.xz",
        "sha256": "f4f606b1488a4b24c238f7e09619959eed89c550ed8f8478b350643f652dc08c",
    },
    "windows-x86_64": {
        "url": _BASE_URL + "-x86_64-windows.zip",
        "sha256": "b651ca732998eba5c0e54f4329c147664a7fb3fe3e74eac890c31647ce1e179a",
    },
}
