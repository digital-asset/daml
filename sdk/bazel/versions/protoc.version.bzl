# -- protoc (prebuilt binary) --
# https://github.com/protocolbuffers/protobuf/releases/tag/v25.5
# Must stay aligned with Maven's com.google.protobuf:protobuf-java:3.25.5
PROTOC_VERSION = "25.5"

PROTOC_SHA256S = {
    "linux-x86_64": "e1ed237a17b2e851cf9662cb5ad02b46e70ff8e060e05984725bc4b4228c6b28",
    "linux-aarch_64": "dc715bb5aab2ebf9653d7d3efbe55e01a035e45c26f391ff6d9b7923e22914b7",
    "osx-x86_64": "c5447e4f0d5caffb18d9ff21eae7bc7faf2bb2000083d6f49e5b6000b30fceae",
    "osx-aarch_64": "781a6fc4c265034872cadc65e63dd3c0fc49245b70917821b60e2d457a6876ab",
    "win64": "d2861b0e3131660e5522d0f348dd95e656a5d35f37554c5c2722190ce4b0000b",
}
