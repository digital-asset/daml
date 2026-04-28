# -- grpcurl (gRPC command-line tool) --
# https://github.com/fullstorydev/grpcurl/releases
GRPCURL_VERSION = "1.9.3"

# keyed by Bazel cpu_value
GRPCURL_SHA256 = {
    "k8": "a926b62a85787ccf73ef8736b3ae554f1242e39d92bb8767a79d6dd23b11d1d5",
    "aarch64": "0b20a00c1cb82ab81ec32696766d4076e99b4cb5ca0823a71767ba64dbea0f263",
    "darwin_x86_64": "246a6669e58c282dcaf0e9dcb06dd1c8681833d59df24eb83d3123ec64c2d2e5",
    "darwin_arm64": "d8391485e99a728a3a4e82af3fd621f9fdea0c417a74e5122803ad20b207b623",
    "x64_windows": "895335dfa7be74803eeb5acf3ec5d3b06c1e9483fdda3c7622bdef9ad388f32a",
}
