PROTO3_SUITE_NO_TESTS=$(nix-build --no-out-link ../release.nix -A proto3-suite-no-tests)
"${PROTO3_SUITE_NO_TESTS}"/bin/compile-proto-file --out . --includeDir ../test-files --proto test_proto.proto
"${PROTO3_SUITE_NO_TESTS}"/bin/compile-proto-file --out . --includeDir ../test-files --proto test_proto_import.proto
"${PROTO3_SUITE_NO_TESTS}"/bin/compile-proto-file --out . --includeDir ../test-files --proto test_proto_oneof.proto
"${PROTO3_SUITE_NO_TESTS}"/bin/compile-proto-file --out . --includeDir ../test-files --proto test_proto_oneof_import.proto
