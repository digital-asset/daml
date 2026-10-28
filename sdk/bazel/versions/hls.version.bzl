HLS_URL = "https://downloads.haskell.org/~hls/haskell-language-server-{version}/haskell-language-server-{version}-{triple}.tar.xz"

HLS_BINDISTS = {
    ("linux", "amd64"): {
        "version": "2.4.0.0",
        "triple": "x86_64-linux-deb10",
        "sha256": "ac26c6d8648308247929102ed4a9fccbfb8d6245aa304b741b8317556b3110a9",
    },
    ("linux", "aarch64"): {
        "version": "2.4.0.0",
        "triple": "aarch64-linux-ubuntu20",
        "sha256": "2f7ef3e1ae1b40438e2d95218139f2e6985b3df15bc133aedab20c4686cfda20",
    },
    ("darwin", "amd64"): {
        "version": "2.4.0.0",
        "triple": "x86_64-apple-darwin",
        "sha256": "aecb93090ded96d76a11aad3cc4849c5c00faad9491856a020218aa6083409d8",
    },
    ("darwin", "aarch64"): {
        "version": "1.9.0.0",
        "triple": "aarch64-darwin",
        "sha256": "4524c9e700a0a91670976f089dc2d7ed0d970420e93faf9ca1ad1246ec348011",
    },
}
