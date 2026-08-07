# -- ghc bindist --
# https://downloads.haskell.org/~ghc/
GHC_VERSION = "9.0.2"

GHC_BINDISTS = {
    ("linux", "amd64"): {
        "triple": "x86_64-deb9-linux",
        "sha256": "805f5628ce6cec678ba77ff48c924831ebdf75ec2c66368e8935a618913a150e",
        "strip_prefix": "ghc-{}".format(GHC_VERSION),
    },
    ("darwin", "aarch64"): {
        "triple": "aarch64-apple-darwin",
        "sha256": "b1fcab17fe48326d2ff302d70c12bc4cf4d570dfbbce68ab57c719cfec882b05",
        "strip_prefix": "ghc-{}-aarch64-apple-darwin".format(GHC_VERSION),
    },
}
