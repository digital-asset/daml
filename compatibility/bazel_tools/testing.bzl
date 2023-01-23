# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_linux", "is_windows")
load("//bazel_tools:versions.bzl", "version_to_name", "versions")
load("//:versions.bzl", "latest_stable_version")

# Each exclusion in the list below is defined in terms of:
#
# - A range of ledger API test tool versions described by `start` and `end` (both inclusive).
# - A list of platform (ledger) ranges, each described by `start` and `end` (both inclusive),
#   as well as the actual list of `--exclude` flags to be passed to the ledger API test tool.
#
# The "0.0.0" special version corresponds the current HEAD and is considered greater than
# all other versions. Also, HEAD corresponds to different commits in different CI runs:
#
# - In a PR, HEAD is the result of merging the latest PR commit with the tip of `main`
#   at the time the build starts.
# - In a nightly run, HEAD is the tip of `main` at the time the build starts.
#
# Either or both `start` and `end` can be omitted and, if present, are always inclusive.
# An interval extreme can be excluded by setting it to a non-existing version that is
# guaranteed to be between two existing ones, according to version ordering. This is
# especially useful to denote the upcoming (yet unknown) release; for example, if the
# current release is `1.17.0-snapshot.20210811.7565.0.f1a55aa4`, then
# `1.17.0-snapshot.20210811.7565.1` will be greater than the current release but
# smaller than HEAD and the upcoming release.
#
# Here are some change types that require adding exclusions:
#
# 1. A platform feature is added and new tests for it are provided that make the new
#    ledger API test tool incompatible with previous platforms, hence, the new tests
#    should be excluded for ledger API test tool versions greater than the current
#    release but less than HEAD and the upcoming release (i.e., start = last release
#    excluded) on all platforms up to and including the last release (i.e., end = last
#    release included).
# 2. An implementation-specific behavior is changed in a not backwards compatible
#    way, together with its accompanying implementation-specific API-level tests,
#    hence, the new ledger API test tool is incompatible with all released platforms
#    and the new platform is incompatible with all released ledger API tests.
#    This case requires, for the changed tests, both the exclusion above and its
#    dual (i.e., excluding such tests on ledger API test tool versions up to and
#    including the latest release, against platform versions greater than the
#    current release but less than HEAD and the next release).
#
# Finally, note that before 1.3 the granularity for disabling tests
# was sadly quite coarse. See
# https://discuss.daml.com/t/can-i-disable-individual-tests-in-the-ledger-api-test-tool/226
# for details.
#
# PRs that resulted in exclusions:
# - ContractKeysIT:
#   - https://github.com/digital-asset/daml/pull/5608
#   - https://github.com/digital-asset/daml/pull/7829
#   - https://github.com/digital-asset/daml/pull/9218
#   - https://github.com/digital-asset/daml/pull/15131
# - ContractKeysSubmitterIsMaintainerIT:
#   - https://github.com/digital-asset/daml/pull/5611
# - SemanticTests:
#   - https://github.com/digital-asset/daml/pull/9218
# - DeeplyNestedValueIT
#   - https://github.com/digital-asset/daml/pull/10393
# - KVCommandDeduplicationIT (only some test cases):
#   - https://github.com/digital-asset/daml/pull/11095
# - CommandDeduplicationIT:CDDeduplicateSubmitterBasic (fixed in https://github.com/digital-asset/daml/pull/11095):
#   - https://github.com/digital-asset/daml/pull/11141

last_nongranular_test_tool = "1.3.0-snapshot.20200617.4484.0.7e0a6848"
first_granular_test_tool = "1.3.0-snapshot.20200623.4546.0.4f68cfc4"

# Some of gRPC error codes changed from INVALID_ARGUMENT to ABORTED
# See https://github.com/digital-asset/daml/pull/9218
before_grpc_error_code_breaking_change = "1.12.0-snapshot.20210323.6567.0.90c5ce70"
after_grpc_error_code_breaking_change = "1.12.0-snapshot.20210323.6567.1.90c5ce70"
grpc_error_code_breaking_change_exclusions = [
    "SemanticTests:SemanticDoubleSpendShared",
    "SemanticTests:SemanticPrivacyProjections",
    "SemanticTests:SemanticDivulgence",
    "ContractKeysIT:CKFetchOrLookup",
    "ContractKeysIT:CKNoFetchUndisclosed",
    "ContractKeysIT:CKMaintainerScoped",
]

grpc_error_code_breaking_change_exclusions_suites = [
    "SemanticTests",
    "ContractKeysIT",
]

before_removing_legacy_error_codes = "2.0.0-snapshot.20220127.9042.0.4038d0a7"
after_removing_legacy_error_codes = "2.0.0-snapshot.20220127.9042.0.4038d0a7.1"

excluded_test_tool_tests = [
    {
        "start": "1.0.0",
        "end": "1.0.1",
        "platform_ranges": [
            {
                "exclusions": ["ContractKeysSubmitterIsMaintainerIT"],
            },
        ],
    },
    {
        "start": "1.0.0",
        "end": "1.0.0",
        "platform_ranges": [
            {
                "start": "1.0.1-snapshot.20200424.3917.0.16093690",
                "end": "1.0.1",
                "exclusions": ["ContractKeysIT"],
            },
            {
                "start": "1.1.0-snapshot.20200430.4057.0.681c862d",
                "exclusions": ["ContractKeysIT"],
            },
        ],
    },
    {
        "start": "1.0.1",
        "end": "1.0.1",
        "platform_ranges": [
            {
                "end": "1.0.1-snapshot.20200417.3908.1.722bac90",
                "exclusions": ["ContractKeysIT"],
            },
        ],
    },
    {
        "start": "1.1.1",
        "end": last_nongranular_test_tool,
        "platform_ranges": [
            {
                "end": "1.0.1-snapshot.20200417.3908.1.722bac90",
                "exclusions": ["ContractKeysIT"],
            },
        ],
    },
    {
        "start": first_granular_test_tool,
        "platform_ranges": [
            {
                "end": "1.0.1-snapshot.20200417.3908.1.722bac90",
                "exclusions": [
                    "ContractKeysIT:CKFetchOrLookup",
                    "ContractKeysIT:CKNoFetchUndisclosed",
                ],
            },
        ],
    },
    {
        "end": "1.3.0-snapshot.20200617.4484.0.7e0a6848",
        "platform_ranges": [
            {
                "start": "1.3.0-snapshot.20200701.4616.0.bdbefd11",
                "exclusions": [
                    "CommandServiceIT",
                ],
            },
        ],
    },
    {
        "start": "1.5.0-snapshot.20200902.5118.0.2b3cf1b3",
        "platform_ranges": [
            {
                "start": "1.0.0",
                "end": "1.3.0",
                "exclusions": [
                    # See https://github.com/digital-asset/daml/pull/7251
                    "CommandSubmissionCompletionIT:CSCAfterEnd",
                    "TransactionServiceIT:TXAfterEnd",
                    "TransactionServiceIT:TXTreesAfterEnd",
                ],
            },
        ],
    },
    {
        "end": last_nongranular_test_tool,
        "platform_ranges": [
            {
                "start": "1.6.0-snapshot.20200922.5258.0.cd4a06db",
                "exclusions": [
                    # See https://github.com/digital-asset/daml/pull/7400
                    "WronglyTypedContractIdIT",
                ],
            },
        ],
    },
    {
        "start": first_granular_test_tool,
        "end": "1.6.0-snapshot.20200915.5208.0.09014dc6",
        "platform_ranges": [
            {
                "start": "1.6.0-snapshot.20200922.5258.0.cd4a06db",
                "exclusions": [
                    # See https://github.com/digital-asset/daml/pull/7400
                    "WronglyTypedContractIdIT:WTFetchFails",
                ],
            },
        ],
    },
    {
        "end": last_nongranular_test_tool,
        "platform_ranges": [
            {
                "start": "1.7.0-snapshot.20201103.5565.0.e75d42dd",
                "exclusions": ["ContractKeysIT"],
            },
        ],
    },
    {
        "start": first_granular_test_tool,
        "end": "1.7.0-snapshot.20201027.5530.0.bdbf8977",
        "platform_ranges": [
            {
                "start": "1.7.0-snapshot.20201103.5565.0.e75d42dd",
                "exclusions": [
                    "ContractKeysIT:CKFetchOrLookup",
                    "ContractKeysIT:CKNoFetchUndisclosed",
                ],
            },
        ],
    },
    {
        "start": "1.10.0-snapshot.20210201.6207.0.7cf1914d",
        "platform_ranges": [
            {
                "end": "1.10.0-snapshot.20210125.6143.0.550aa48f",
                "exclusions": [
                    # See https://github.com/digital-asset/daml/pull/8642
                    "PartyManagementServiceIT:PMRejectLongPartyHints",
                    "PartyManagementServiceIT:PMRejectInvalidPartyHints",
                ],
            },
        ],
    },
    {
        "start": after_grpc_error_code_breaking_change,
        "platform_ranges": [
            {
                "end": before_grpc_error_code_breaking_change,
                "exclusions": grpc_error_code_breaking_change_exclusions + ["SemanticTests:SemanticDoubleSpendBasic"],
            },
        ],
    },
    {
        "end": last_nongranular_test_tool,
        "platform_ranges": [
            {
                "start": after_grpc_error_code_breaking_change,
                "exclusions": grpc_error_code_breaking_change_exclusions_suites,
            },
        ],
    },
    {
        "start": first_granular_test_tool,
        "end": "1.5.0",
        "platform_ranges": [
            {
                "start": after_grpc_error_code_breaking_change,
                "exclusions": grpc_error_code_breaking_change_exclusions + ["SemanticTests:SemanticDoubleSpend"],
            },
        ],
    },
    {
        "start": "1.6.0",
        "platform_ranges": [
            {
                "start": after_grpc_error_code_breaking_change,
                "exclusions": grpc_error_code_breaking_change_exclusions + ["SemanticTests:SemanticDoubleSpendBasic"],
            },
        ],
    },
    {
        # We drop visibily restrictions about local contract key in
        #  https://github.com/digital-asset/daml/pull/15131
        # Since this change does not raise any obvious security problems, it
        # seems superfluous to continue checking that old SDKs enforce the
        # restriction. So we disable completely the test.
        "start": "1.13.0-snapshot.20210419.6730.1.8c3a8c04",
        "end": "2.6.0-snapshot.20221226.11190.0.71548477",
        "platform_ranges": [
            {
                "start": "1.0.0",
                "exclusions": ["ContractKeysIT:CKLocalKeyVisibility"],
            },
        ],
    },
    {
        "start": "1.13.0-snapshot.20210419.6730.1.8c3a8c04",
        "platform_ranges": [
            {
                "end": "1.13.0-snapshot.20210504.6833.0.9ae787d0",
                "exclusions": ["ValueLimitsIT:VLLargeSubmittersNumberCreateContract"],
            },
        ],
    },
    {
        "end": last_nongranular_test_tool,
        "platform_ranges": [
            {
                "start": "1.14.0-snapshot.20210602.7086.1",
                "exclusions": [
                    "CommandServiceIT",
                    "CommandSubmissionCompletionIT",
                ],
            },
        ],
    },
    {
        "start": first_granular_test_tool,
        "end": "1.14.0-snapshot.20210602.7086.0.f36f556b",
        "platform_ranges": [
            {
                "start": "1.14.0-snapshot.20210602.7086.1",
                "exclusions": [
                    "CommandServiceIT:CSCreateAndBadExerciseChoice",
                    "CommandSubmissionCompletionIT:CSCRefuseBadChoice",
                ],
            },
        ],
    },
    {
        "start": "1.14.0-snapshot.20210602.7086.1",
        "platform_ranges": [
            {
                "end": "1.14.0-snapshot.20210602.7086.0.f36f556b",
                "exclusions": [
                    "CommandServiceIT:CSCreateAndBadExerciseChoice",
                    "CommandSubmissionCompletionIT:CSCRefuseBadChoice",
                ],
            },
        ],
    },
    {
        "start": "1.16.0-snapshot.20210713.7343.1.1f35db17",
        "end": "1.17.0-snapshot.20210907.7759.0.35a853fd",
        "platform_ranges": [
            {
                "end": "1.16.0-snapshot.20210713.7343.0.1f35db17",
                "exclusions": [
                    "ConfigManagementServiceIT:DuplicateSubmissionId",
                    "PackageManagementServiceIT:DuplicateSubmissionId",
                ],
            },
        ],
    },
    {
        "start": "1.17.0-snapshot.20210907.7759.1.35a853fd",
        "platform_ranges": [
            {
                "end": "1.16.0-snapshot.20210713.7343.0.1f35db17",
                "exclusions": [
                    "ConfigManagementServiceIT:CMDuplicateSubmissionId",
                    "PackageManagementServiceIT:PMDuplicateSubmissionId",
                ],
            },
        ],
    },
    {
        "start": "1.16.0-snapshot.20210727.7476.1",
        "platform_ranges": [
            {
                "end": "1.16.0-snapshot.20210727.7476.0.b5e9d861",
                "exclusions": [
                    "DeeplyNestedValueIT",
                ],
            },
        ],
    },
    {
        # Tests got renamed in
        # https://github.com/digital-asset/daml/commit/f2707cc54f5b7da339bc565bc322be1e57db5edb
        "end": last_nongranular_test_tool,
        "platform_ranges": [
            {
                "start": "1.17.0-snapshot.20210831.7702.1.f058c2f1",
                "exclusions": [
                    "CommandDeduplicationIT",
                ],
            },
        ],
    },
    {
        # Tests got renamed in
        # https://github.com/digital-asset/daml/commit/f2707cc54f5b7da339bc565bc322be1e57db5edb
        "start": first_granular_test_tool,
        "end": "1.5.0-snapshot.20200907.5151.0.eb68e680",
        "platform_ranges": [
            {
                "start": "1.17.0-snapshot.20210831.7702.1.f058c2f1",
                "exclusions": [
                    "CommandDeduplicationIT:CDSimpleDeduplication",
                    "CommandDeduplicationIT:CDSimpleDeduplicationCommandClient",
                ],
            },
        ],
    },
    {
        # Tests got renamed in
        # https://github.com/digital-asset/daml/commit/f2707cc54f5b7da339bc565bc322be1e57db5edb
        "start": "1.5.0-snapshot.20200907.5151.0.eb68e680",
        "end": "1.17.0-snapshot.20210831.7702.0.f058c2f1",
        "platform_ranges": [
            {
                "start": "1.17.0-snapshot.20210831.7702.1.f058c2f1",
                "exclusions": [
                    "CommandDeduplicationIT:CDSimpleDeduplicationBasic",
                    "CommandDeduplicationIT:CDSimpleDeduplicationCommandClient",
                ],
            },
        ],
    },
    {
        "end": last_nongranular_test_tool,
        "platform_ranges": [
            {
                "start": "1.17.0-snapshot.20210831.7702.1.f058c2f1",
                "exclusions": [
                    "CommandServiceIT",
                ],
            },
        ],
    },
    {
        "start": first_granular_test_tool,
        "end": "1.17.0-snapshot.20210831.7702.0.f058c2f1",
        "platform_ranges": [
            {
                "start": "1.17.0-snapshot.20210831.7702.1.f058c2f1",
                "exclusions": [
                    "CommandServiceIT:CSRefuseBadParameter",
                ],
            },
        ],
    },
    {
        # gRPC errors from transaction-related services have been enriched with definite answer details
        # and a new assertion has been added.
        # See: https://github.com/digital-asset/daml/pull/10832/files#diff-e0fa328a58650c48e8770804e35a1464c81cc80a51547860a01e9197a8fb9c71R49
        "start": "1.17.0-snapshot.20210910.7786.1",
        "platform_ranges": [
            {
                "end": "1.17.0-snapshot.20210910.7786.0.976ca400",
                "exclusions": [
                    "WronglyTypedContractIdIT:WTExerciseFails",
                    "WronglyTypedContractIdIT:WTFetchFails",
                    "WronglyTypedContractIdIT:WTMultipleExerciseFails",
                    "TransactionServiceExerciseIT:TXRejectOnFailingAssertion",
                    "ContractKeysIT:CKTransients",
                    "ContractKeysIT:CKExerciseByKey",
                    "ClosedWorldIT:ClosedWorldObserver",
                    "TransactionServiceAuthorizationIT:TXRejectMultiActorMissingAuth",
                    "TransactionServiceAuthorizationIT:TXRejectMultiActorExcessiveAuth",
                    "CommandServiceIT",
                    "ExceptionsIT",
                    "CommandSubmissionCompletionIT:CSCRefuseBadChoice",
                    "CommandSubmissionCompletionIT:CSCSubmitWithInvalidLedgerId",
                    "CommandSubmissionCompletionIT:CSCDisallowEmptyTransactionsSubmission",
                    "SemanticTests:SemanticDoubleSpendSameTx",
                    "SemanticTests:SemanticPartialSignatories",
                    "SemanticTests:SemanticAcceptOnBehalf",
                    "DeeplyNestedValueIT",
                ],
            },
        ],
    },
    {
        "start": "1.17.0-snapshot.20210910.7786.1",
        "platform_ranges": [
            {
                "start": "1.17.0-snapshot.20210811.7565.0.f1a55aa4",
                "end": "1.17.0-snapshot.20210910.7786.0.976ca400 ",
                "exclusions": [
                    "CommandDeduplicationIT",
                ],
            },
        ],
    },
    {
        "start": "1.18.0-snapshot.20210928.7948.1",
        "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
        "platform_ranges": [
            {
                "end": "1.18.0-snapshot.20210928.7948.0.b4d00317",
                "exclusions": [
                    "KVCommandDeduplicationIT:KVCommandDeduplicationDeduplicateSubmitterBasic",
                    "KVCommandDeduplicationIT:KVCommandDeduplicationSimpleDeduplicationBasic",
                ],
            },
        ],
    },
    {
        "start": "1.17.0-snapshot.20210910.7786.0.976ca400",  # The first version these tests appeared
        "end": "1.18.0-snapshot.20210928.7948.0.b4d00317",
        "platform_ranges": [
            {
                "start": "1.18.0-snapshot.20210928.7948.1",
                "exclusions": [
                    "KVCommandDeduplicationIT:KVCommandDeduplicationDeduplicateSubmitterBasic",
                    "KVCommandDeduplicationIT:KVCommandDeduplicationSimpleDeduplicationBasic",
                ],
            },
        ],
    },
    {
        "start": "1.17.0-snapshot.20210915.7841.0.b4328b3d",  # The first version this test appeared
        "end": "1.18.0-snapshot.20211026.8179.0.e474b2d1",  # The version when this test was removed
        "platform_ranges": [
            {
                "start": "1.18.0-snapshot.20210928.7948.1",
                "exclusions": [
                    "KVCommandDeduplicationIT:KVCommandDeduplicationCommitterDeduplication",
                ],
            },
        ],
    },
    {
        "start": "1.3.0",
        "end": "1.4.0",
        "platform_ranges": [
            {
                "start": "1.18.0-snapshot.20210928.7948.1",
                "exclusions": [
                    "CommandDeduplicationIT:CDDeduplicateSubmitter",  # Fixed in later ledger API test tools
                ],
            },
        ],
    },
    {
        "start": "1.5.0",
        "end": "1.16.0",
        "platform_ranges": [
            {
                "start": "1.18.0-snapshot.20210928.7948.1",
                "exclusions": [
                    "CommandDeduplicationIT:CDDeduplicateSubmitterBasic",  # Fixed in later ledger API test tools
                ],
            },
        ],
    },
    {
        "start": "1.18.0",
        "platform_ranges": [
            {
                "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
                "exclusions": [
                    "CommandDeduplicationIT",  # Latest version of the test is dependent on having the submission id populated
                ],
            },
        ],
    },
    {
        "start": "1.18.0",
        "end": "2.0.0-snapshot.20220105.8777.1",  # was removed in 2.0
        "platform_ranges": [
            {
                "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
                "exclusions": [
                    # Exclude dedup tests due to large number of changes (removed participant deduplication, switch to append-only schema, changes in deduplication duration)
                    "KVCommandDeduplicationIT",
                ],
            },
        ],
    },
    {
        # Self-service error code assertions adapted
        "end": "1.18.0-snapshot.20211102.8257.1",
        "platform_ranges": [
            {
                "start": "1.18.0-snapshot.20211102.8257.1",
                "exclusions": [
                    "PackageManagementServiceIT",
                ],
            },
        ],
    },
    {
        # Completion offset included in the CommandService responses
        "start": "1.18.0",
        "platform_ranges": [
            {
                "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
                "exclusions": [
                    "CommandServiceIT:CSsubmitAndWaitCompletionOffset",
                ],
            },
        ],
    },
    {
        "start": "2.0.0",
        "platform_ranges": [
            {
                "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
                "exclusions": [
                    # Unexpected failure (StatusRuntimeException) ALREADY_EXISTS: DUPLICATE_COMMAND(10,KVComman):
                    "CommandDeduplicationIT:DeduplicationMixedClients",
                    # Unexpected failure (StatusRuntimeException) ALREADY_EXISTS: DUPLICATE_COMMAND(10,KVComman):
                    "CommandDeduplicationIT:SimpleDeduplicationCommandClient",
                    # Offsets are not supported for versions < 2.0.0
                    "CommandDeduplicationIT:DeduplicateUsingOffsets",
                    # Actual error id (INCONSISTENT) does not match expected error id (DUPLICATE_CONTRACT_KEY}
                    "ExceptionsIT:ExRollbackDuplicateKeyCreated",
                    "ExceptionsIT:ExRollbackDuplicateKeyArchived",
                ],
            },
        ],
    },
    {
        "start": "1.18.0",
        "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
        "platform_ranges": [
            {
                "start": "2.0.0-snapshot.20211123.8463.0.bd2a6852",
                "exclusions": [
                    # Actual error id (INCONSISTENT) does not match expected error id (DUPLICATE_CONTRACT_KEY}
                    "ExceptionsIT:ExRollbackDuplicateKeyCreated",
                    "ExceptionsIT:ExRollbackDuplicateKeyArchived",
                ],
            },
        ],
    },
    {
        # max deduplication duration is no longer enforced in the ledger API
        "start": first_granular_test_tool,
        "end": "2.0.0-snapshot.20211210.8653.0.35beb44c ",
        "platform_ranges": [
            {
                "start": "2.0.0-snapshot.20211210.8653.0.35beb44c",
                "exclusions": [
                    "LedgerConfigurationServiceIT:CSLSuccessIfMaxDeduplicationTimeExceeded",
                ],
            },
        ],
    },
    {
        # max deduplication duration is no longer enforced in the ledger API
        "end": last_nongranular_test_tool,
        "platform_ranges": [
            {
                "start": "2.0.0-snapshot.20211210.8653.0.35beb44c",
                "exclusions": [
                    "LedgerConfigurationServiceIT",
                ],
            },
        ],
    },
    {
        "start": "1.3.0",
        "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
        "platform_ranges": [
            {
                "start": "2.0.0-snapshot.20220110.8812.0.3a08380b",
                "exclusions": [
                    "CommandServiceIT:CSReturnStackTrace",
                ],
            },
        ],
    },
    {
        "start": "1.16.0",
        "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
        "platform_ranges": [
            {
                "start": "2.0.0-snapshot.20220110.8812.0.3a08380b",
                "exclusions": [
                    "ExceptionsIT:ExUncaught",
                ],
            },
        ],
    },
    {
        "start": first_granular_test_tool,
        "end": "2.0.0-snapshot.20220110.8812.0.3a08380b",
        "platform_ranges": [
            {
                "start": "2.0.0-snapshot.20220110.8812.0.3a08380b",
                "exclusions": [
                    # Error message did not contain [\QParty not known on ledger\E], but was [Parties not known on ledger: [unallocated]].
                    "ClosedWorldIT:ClosedWorldObserver",
                ],
            },
        ],
    },
    {
        # Contract ID and participant pruning tests are no longer optional.
        "start": "2.0.0-snapshot.20220110.8812.1",
        "platform_ranges": [
            {
                "end": "2.0.0-snapshot.20220110.8812.1",
                "exclusions": [
                    "ContractIdIT",  # Contract ID tests are governed by feature descriptors.
                    "ParticipantPruningIT",  # Now enabled by default, but some ledgers may need to disable certain tests.
                ],
            },
        ],
    },
    {
        # The test requires the "definite_answer" gRPC error metadata entry, which is not present in old releases.
        "start": "2.0.0-snapshot.20220118.8919.1",
        "platform_ranges": [
            {
                "end": "1.16.0",
                "exclusions": [
                    "MultiPartySubmissionIT",
                ],
            },
        ],
    },
    {
        # Some command deduplication tests are no longer optional, but fail on older releases.
        "start": "2.0.0-snapshot.20220118.8919.1",
        "platform_ranges": [
            {
                "end": "2.0.0-snapshot.20220118.8919.1",
                "exclusions": [
                    "CommandDeduplicationParallelIT",
                    "CommandDeduplicationPeriodValidationIT",
                    "CompletionDeduplicationInfoITCommandService",
                    "CompletionDeduplicationInfoITCommandSubmissionService",
                ],
            },
        ],
    },
    {
        # Sandbox-on-X doesn't use participant-side command deduplication starting with next release,
        # hence older tests will fail to assert it.
        "start": "1.17.0",
        "end": "1.18.3",
        "platform_ranges": [
            {
                "start": "2.0.0-snapshot.20220126.9029.1",
                "exclusions": [
                    "CommandDeduplicationIT:ParticipantCommandDeduplication",
                ],
            },
        ],
    },
    {
        # Sandbox-on-X doesn't use participant-side command deduplication starting with next release,
        # hence older tests will fail to assert it.
        "start": "1.18.0",
        "end": "1.18.3",
        "platform_ranges": [
            {
                "start": "2.0.0-snapshot.20220126.9029.1",
                "exclusions": [
                    "CommandDeduplicationIT:ParticipantCommandDeduplicationSimpleDeduplicationMixedClients",
                    "CommandDeduplicationIT:ParticipantCommandDeduplicationDeduplicateSubmitterBasic",
                    "CommandDeduplicationIT:ParticipantCommandDeduplicationSimpleDeduplicationBasic",
                ],
            },
        ],
    },
    {
        # Self-service error codes becomes the only supported option. They are deprecated as an experimental feature.
        # Switching to the legacy codes is no longer available.
        "start": after_removing_legacy_error_codes,
        "platform_ranges": [
            {
                "end": "1.17.1",
                "exclusions": [
                    "ActiveContractsServiceIT",
                    "ClosedWorldIT",
                    "CommandServiceIT",
                    "CommandSubmissionCompletionIT",
                    "ConfigManagementServiceIT",
                    "ContractKeysIT",
                    "DeeplyNestedValueIT",
                    "ExceptionsIT",
                    "LedgerConfigurationServiceIT",
                    "MultiPartySubmissionIT",
                    "PackageManagementServiceIT",
                    "PackageServiceIT",
                    "PartyManagementServiceIT",
                    "SemanticTests",
                    "TransactionServiceAuthorizationIT",
                    "TransactionServiceExerciseIT",
                    "TransactionServiceQueryIT",
                    "TransactionServiceStreamsIT",
                    "TransactionServiceValidationIT",
                    "WronglyTypedContractIdIT",
                ],
            },
        ],
    },
    {
        "end": before_removing_legacy_error_codes,
        "platform_ranges": [
            {
                "start": after_removing_legacy_error_codes,
                "exclusions": [
                    "CommandSubmissionCompletionIT",
                    "ConfigManagementServiceIT",
                    "ContractKeysIT",
                    "SemanticTests",
                    "TransactionService",
                ],
            },
        ],
    },
    {
        "start": "1.16.0",
        "end": before_removing_legacy_error_codes,
        "platform_ranges": [
            {
                "start": after_removing_legacy_error_codes,
                "exclusions": [
                    "ExceptionsIT",
                ],
            },
        ],
    },
    {
        # Sandbox-on-X starts forwarding rejections on duplicate party allocation starting with next release
        "start": "2.0.0-snapshot.20220201.9108.1",
        "platform_ranges": [
            {
                "end": "2.0.0-snapshot.20220201.9108.0.aa2494f1",
                "exclusions": [
                    "PartyManagementServiceIT:PMRejectionDuplicateHint",
                ],
            },
        ],
    },
    {
        # Fixing childEventId ordering: this test is now checking conformance to ordering, so it needs to be excluded for conformance tests which come after, and for versions which are older
        "start": "2.2.0-snapshot.20220425.9780.1",
        "platform_ranges": [
            {
                "end": "2.2.0-snapshot.20220425.9780.0.f4d60375",
                "exclusions": [
                    "TransactionServiceVisibilityIT:TXTreeChildOrder",
                ],
            },
        ],
    },
    {
        "start": "2.3.0-snapshot.20220606.10031.1",
        "platform_ranges": [
            {
                "end": "2.3.0-snapshot.20220606.10031.0.ce98be86",
                "exclusions": [
                    "ExceptionsIT:ExCKRollbackGlobalArchivedCreate",
                    "ExceptionsIT:ExCKRollbackGlobalArchivedLookup",
                ],
            },
        ],
    },
    {
        "start": "2.3.0-snapshot.20220611.10066.1",
        "platform_ranges": [
            {
                "end": "2.3.0-snapshot.20220611.10066.0.458cfc43",
                "exclusions": [
                    "ExceptionsIT:ExRollbackCreate",
                    "ExceptionsIT:ExRollbackExerciseCreateLookup",
                ],
            },
        ],
    },
    {
        "start": "2.6.0-snapshot.20221226.11190.1",
        "platform_ranges": [
            {
                "end": "2.6.0-snapshot.20221226.11190.0.71548477",
                "exclusions": [
                    "ContractKeysIT:CKLocalFetchByKeyVisibility",
                    "ContractKeysIT:CKLocalLookupByKeyVisibility",
                ],
            },
        ],
    },
    {
        "end": "2.6.0-snapshot.20230119.11284.0.179b865a",
        "platform_ranges": [
            {
                "start": "2.6.0-snapshot.20230119.11284.1",
                "exclusions": [
                    "UserManagementServiceIT:TestGrantUserRights",
                    "UserManagementServiceIT:TestRevokeUserRights",
                    "UserManagementServiceIT:TestListUserRights",
                    "UserManagementServiceIT:RaceConditionGrantRights",
                    "UserManagementServiceIT:RaceConditionRevokeRights",
                ],
            },
        ],
    },
]

def in_range(version, range):
    start = range.get("start")
    end = range.get("end")
    if start and not versions.is_at_least(start, version):
        # Before start
        return False
    if end and not versions.is_at_most(end, version):
        # After end
        return False
    return True

def get_excluded_tests(test_tool_version, sandbox_version):
    exclusions = []
    for test_tool_range in excluded_test_tool_tests:
        if in_range(test_tool_version, test_tool_range):
            for platform_range in test_tool_range["platform_ranges"]:
                if in_range(sandbox_version, platform_range):
                    exclusions += platform_range["exclusions"]
    return exclusions

def extra_tags(sdk_version, platform_version):
    if sorted([sdk_version, platform_version]) == sorted(["0.0.0", latest_stable_version]):
        # These tests are the ones that we check on each PR since they
        # are the most useful ones and hopefully fast enough.
        return ["head-quick"]
    return []

def _concat(lists):
    return [v for l in lists for v in l]

def daml_ledger_test(
        name,
        sdk_version,
        daml,
        sandbox,
        sandbox_args = [],
        data = [],
        **kwargs):
    # We wrap it in an SH test to pass different arguments.
    native.sh_test(
        name = name,
        srcs = ["//bazel_tools:daml_ledger_test.sh"],
        args = [
            "$(rootpath //bazel_tools/daml_ledger:runner)",
            # "--daml",
            "$(rootpath %s)" % daml,
            # "--sandbox",
            "$(rootpath %s)" % sandbox,
            "--sdk-version",
            sdk_version,
        ] + _concat([["--sandbox-arg", arg] for arg in sandbox_args]),
        deps = ["@bazel_tools//tools/bash/runfiles"],
        data = data + depset(direct = [
            "//bazel_tools/daml_ledger:runner",
            # Deduplicate if daml and sandbox come from the same release.
            daml,
            sandbox,
        ]).to_list(),
        **kwargs
    )

def create_daml_app_dar(sdk_version):
    daml = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    messaging_patch = "@daml-sdk-{sdk_version}//:create_daml_app.patch".format(
        sdk_version = sdk_version,
    )
    native.genrule(
        name = "create-daml-app-dar-{sdk_version}".format(
            sdk_version = version_to_name(sdk_version),
        ),
        outs = ["create-daml-app-{sdk_version}.dar".format(
            sdk_version = version_to_name(sdk_version),
        )],
        srcs = [messaging_patch],
        tools = [daml, "@patch_dev_env//:patch"],
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
$(execpath {daml}) new $$TMP_DIR/create-daml-app create-daml-app
$(execpath @patch_dev_env//:patch) -s -d $$TMP_DIR/create-daml-app -p2 < $(execpath {messaging_patch})
$(execpath {daml}) build --project-root=$$TMP_DIR/create-daml-app -o $$PWD/$(OUTS)
""".format(
            daml = daml,
            sdk_version = sdk_version,
            messaging_patch = messaging_patch,
        ),
    )

def create_daml_app_codegen(sdk_version):
    daml = "@daml-sdk-{}//:daml".format(sdk_version)
    daml_types = "@daml-sdk-{}//:daml-types.tgz".format(sdk_version)
    daml_ledger = "@daml-sdk-{}//:daml-ledger.tgz".format(sdk_version)
    dar = "//:create-daml-app-{}.dar".format(version_to_name(sdk_version))
    native.genrule(
        name = "create-daml-app-codegen-{}".format(version_to_name(sdk_version)),
        outs = ["create-daml-app-codegen-{}.tar.gz".format(version_to_name(sdk_version))],
        srcs = [dar, daml_types, daml_ledger],
        tools = [
            daml,
            "//bazel_tools/create-daml-app:run-with-yarn",
            "@daml//bazel_tools/sh:mktgz",
        ],
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
mkdir -p $$TMP_DIR/daml-types $$TMP_DIR/daml-ledger
tar xf $(rootpath {daml_types}) --strip-components=1 -C $$TMP_DIR/daml-types
tar xf $(rootpath {daml_ledger}) --strip-components=1 -C $$TMP_DIR/daml-ledger
$(execpath //bazel_tools/create-daml-app:run-with-yarn) $$TMP_DIR $$PWD/$(execpath {daml}) codegen js -o $$TMP_DIR/daml.js $(execpath {dar})
rm -rf $$TMP_DIR/daml.js/node_modules
$(execpath @daml//bazel_tools/sh:mktgz) $@ -C $$TMP_DIR daml.js
""".format(
            daml = daml,
            daml_types = daml_types,
            daml_ledger = daml_ledger,
            dar = dar,
            sdk_version = sdk_version,
        ),
    )

def create_daml_app_test(
        name,
        daml,
        sandbox,
        sandbox_version,
        json_api,
        json_api_version,
        daml_types,
        daml_react,
        daml_ledger,
        messaging_patch,
        codegen_output,
        dar,
        data = [],
        **kwargs):
    native.sh_test(
        name = name,
        # See the comment on daml_ledger_test for why
        # we need the sh_test.
        srcs = ["//bazel_tools:create_daml_app_test.sh"],
        args = [
            "$(rootpath //bazel_tools/create-daml-app:runner)",
            #"--daml",
            "$(rootpath %s)" % daml,
            #"--sandbox",
            "$(rootpath %s)" % sandbox,
            sandbox_version,
            #"--json-api",
            "$(rootpath %s)" % json_api,
            json_api_version,
            "$(rootpath %s)" % daml_types,
            "$(rootpath %s)" % daml_ledger,
            "$(rootpath %s)" % daml_react,
            "$(rootpath %s)" % messaging_patch,
            "$(rootpath @nodejs//:npm_bin)",
            "$(rootpath @nodejs//:node)",
            "$(rootpath @patch_dev_env//:patch)",
            "$(rootpath //bazel_tools/create-daml-app:testDeps.json)",
            "$(rootpath //bazel_tools/create-daml-app:index.test.ts)",
            "$(rootpath %s)" % codegen_output,
            "$(rootpath %s)" % dar,
        ],
        data = data + depset(direct = [
            "//bazel_tools/create-daml-app:runner",
            "@nodejs//:npm_bin",
            "@nodejs//:node",
            "@patch_dev_env//:patch",
            "//bazel_tools/create-daml-app:testDeps.json",
            "//bazel_tools/create-daml-app:index.test.ts",
            # Deduplicate if daml and sandbox come from the same release.
            daml,
            sandbox,
            json_api,
            daml_types,
            daml_react,
            daml_ledger,
            messaging_patch,
            codegen_output,
            dar,
        ]).to_list(),
        deps = [
            "@bazel_tools//tools/bash/runfiles",
        ],
        **kwargs
    )

# FIXME
#
# SDK components may default to a LF version too recent for a given platform version.
#
# This predicate can be used to filter sdk_platform_test rules as a temporary
# measure to prevent spurious errors on CI.
#
# The proper fix is to use the appropriate version of Daml-LF for every SDK/platform pair.

def daml_lf_compatible(sdk_version, platform_version):
    return (
        # any platform supports any pre 1.11 SDK
        not in_range(sdk_version, {"start": "1.11.0-snapshot"})
    ) or (
        # any post 1.10.0 platform supports any pre 1.12 SDK
        in_range(platform_version, {"start": "1.10.0-snapshot"}) and not in_range(sdk_version, {"start": "1.12.0-snapshot"})
    ) or (
        # any post 1.10.0 platform supports any pre 1.14 SDK
        in_range(platform_version, {"start": "1.11.0-snapshot"}) and not in_range(sdk_version, {"start": "1.14.0-snapshot"})
    ) or (
        # any post 1.14.0 platform supports any pre 1.16 SDK
        in_range(platform_version, {"start": "1.14.0-snapshot"}) and not in_range(sdk_version, {"start": "1.16.0-snapshot"})
    ) or (
        # any post 1.15.0 platform supports any SDK
        in_range(platform_version, {"start": "1.15.0-snapshot"})
    )

def sdk_platform_test(sdk_version, platform_version):
    # SDK components
    daml_assistant = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    ledger_api_test_tool = "@daml-sdk-{sdk_version}//:ledger-api-test-tool".format(
        sdk_version = sdk_version,
    )
    dar_files = "@daml-sdk-{sdk_version}//:dar-files".format(
        sdk_version = sdk_version,
    )

    # Platform components
    sandbox = "@daml-sdk-{platform_version}//:daml".format(
        platform_version = platform_version,
    )

    json_api = "@daml-sdk-{platform_version}//:daml".format(
        platform_version = platform_version,
    )

    # We need to use weak seeding to avoid our tests timing out
    # if the CI machine does not have enough entropy.
    sandbox_args = [
        "sandbox",
        "--contract-id-seeding=testing-weak",
    ]

    sandbox_classic_args = ["sandbox-classic", "--contract-id-seeding=testing-weak"]

    sandbox_on_x = "@daml-sdk-{}//:sandbox-on-x".format(platform_version)
    sandbox_on_x_args = ["--contract-id-seeding=testing-weak", "--implicit-party-allocation=false", "--mutable-contract-state-cache", "--enable-user-management=true"]
    sandbox_on_x_cmd = ["run-legacy-cli-config"] if versions.is_at_least("2.4.0-snapshot.20220712.10212.0.0bf28176", platform_version) else []

    json_api_args = ["json-api"]

    # --implicit-party-allocation=false only exists in SDK >= 1.2.0 so
    # for older versions we still have to disable ClosedWorldIT
    (extra_sandbox_next_args, extra_sandbox_next_exclusions) = (["--implicit-party-allocation=false"], []) if versions.is_at_least("1.2.0", platform_version) else ([], ["--exclude=ClosedWorldIT"])
    extra_sandbox_classic_args = []
    extra_sandbox_on_x_args = []

    if versions.is_at_least("1.17.0", platform_version):
        extra_sandbox_next_args += ["--max-deduplication-duration=PT5S"]

    # https://github.com/digital-asset/daml/commit/60ffb79fb16b507d4143cfc991da342efea504a7
    # introduced a KV specific dedup test and we need to disable the non-kv test in return.
    kv_dedup_start_version = "1.17.0-snapshot.20210910.7786.0.976ca400"
    kv_dedup_end_version = "2.0.0-snapshot.20220110.8812.0.3a08380b"
    if versions.is_at_least(kv_dedup_start_version, sdk_version) and versions.is_at_most(kv_dedup_end_version, sdk_version) and \
       versions.is_at_least(kv_dedup_start_version, platform_version) and versions.is_at_most(kv_dedup_end_version, platform_version):
        extra_sandbox_next_exclusions += ["--exclude=CommandDeduplicationIT", "--additional=KVCommandDeduplicationIT"]

    # Error codes are enabled by default after 1.18.0-snapshot.20211117.8399.0.a05a40ae.
    # Before this SDK version, ledger-api-test-tool cannot correctly assert the self-service error codes.
    # Turning on the compatibility mode with `--use-pre-1.18-error-codes` is available up to 2.0.0-snapshot.20220127.9042.0.4038d0a7 (inclusive).
    # For this reason, all platforms newer than 1.18.0-snapshot.20211117.8399.0.a05a40ae up to 2.0.0-snapshot.20220127.9042.0.4038d0a7 (inclusive)
    # will run against old ledger-api-test-tools in compatibility mode (i.e. `--use-pre-1.18-error-codes`)
    error_codes_version_enabled_by_default = "1.18.0-snapshot.20211117.8399.0.a05a40ae"
    if versions.is_at_most(error_codes_version_enabled_by_default, platform_version) and versions.is_at_least(before_removing_legacy_error_codes, platform_version):
        extra_sandbox_next_args += ["--use-pre-1.18-error-codes"]
        extra_sandbox_classic_args += ["--use-pre-1.18-error-codes"]
        extra_sandbox_on_x_args += ["--use-pre-1.18-error-codes"]

    # ledger-api-test-tool test-cases
    name = "ledger-api-test-tool-{sdk_version}-platform-{platform_version}".format(
        sdk_version = version_to_name(sdk_version),
        platform_version = version_to_name(platform_version),
    )
    exclusions = ["--exclude=" + test for test in get_excluded_tests(test_tool_version = sdk_version, sandbox_version = platform_version)]

    if versions.is_stable(sdk_version) and versions.is_stable(platform_version):
        if versions.is_at_least("2.0.0", platform_version):
            # Ledger API test tool < 1.5 do not upload the DAR which doesn’t work on Sandbox on X
            if versions.is_at_least("1.5.0", sdk_version):
                client_server_test(
                    name = name + "-on-x",
                    client = ledger_api_test_tool,
                    client_args = [
                        "localhost:6865",
                    ] + exclusions,
                    data = [dar_files],
                    runner = "@//bazel_tools/client_server:runner",
                    runner_args = ["6865"],
                    server = sandbox_on_x,
                    server_args = sandbox_on_x_cmd + ["--participant participant-id=example,port=6865"] + sandbox_on_x_args + extra_sandbox_on_x_args,
                    tags = ["exclusive", sdk_version, platform_version] + extra_tags(sdk_version, platform_version),
                )
                client_server_test(
                    name = name + "-on-x-postgresql",
                    client = ledger_api_test_tool,
                    client_args = [
                        "localhost:6865",
                    ] + exclusions,
                    data = [dar_files],
                    runner = "@//bazel_tools/client_server:runner",
                    runner_args = ["6865"],
                    server = ":sandbox-with-postgres-{}".format(platform_version),
                    server_args = [platform_version, "sandbox-on-x"] + sandbox_on_x_cmd + ["--participant participant-id=example,port=6865,server-jdbc-url=__jdbcurl__"] + sandbox_on_x_args + extra_sandbox_on_x_args,
                    tags = ["exclusive", sdk_version, platform_version] + extra_tags(sdk_version, platform_version),
                ) if is_linux else None
        else:
            client_server_test(
                name = name,
                client = ledger_api_test_tool,
                client_args = [
                    "localhost:6865",
                ] + exclusions + extra_sandbox_next_exclusions,
                data = [dar_files],
                runner = "@//bazel_tools/client_server:runner",
                runner_args = ["6865"],
                server = sandbox,
                server_args = sandbox_args + extra_sandbox_next_args,
                server_files = ["$(rootpaths {dar_files})".format(
                    dar_files = dar_files,
                )],
                tags = ["exclusive", sdk_version, platform_version] + extra_tags(sdk_version, platform_version),
            )

            client_server_test(
                name = name + "-classic",
                client = ledger_api_test_tool,
                client_args = [
                    "localhost:6865",
                    "--exclude=ClosedWorldIT",
                ] + exclusions,
                data = [dar_files],
                runner = "@//bazel_tools/client_server:runner",
                runner_args = ["6865"],
                server = sandbox,
                server_args = sandbox_classic_args + extra_sandbox_classic_args,
                server_files = ["$(rootpaths {dar_files})".format(
                    dar_files = dar_files,
                )],
                tags = ["exclusive", sdk_version, platform_version] + extra_tags(sdk_version, platform_version),
            )

            client_server_test(
                name = name + "-postgresql",
                client = ledger_api_test_tool,
                client_args = [
                    "localhost:6865",
                ] + exclusions + extra_sandbox_next_exclusions,
                data = [dar_files],
                runner = "@//bazel_tools/client_server:runner",
                runner_args = ["6865"],
                server = ":sandbox-with-postgres-{}".format(platform_version),
                server_args = [platform_version, "daml"] + sandbox_args + extra_sandbox_next_args + ["--jdbcurl=__jdbcurl__"],
                server_files = ["$(rootpaths {dar_files})".format(
                    dar_files = dar_files,
                )],
                tags = ["exclusive"] + extra_tags(sdk_version, platform_version),
            ) if is_linux else None

            client_server_test(
                name = name + "-classic-postgresql",
                size = "large",
                client = ledger_api_test_tool,
                client_args = [
                    "localhost:6865",
                    "--exclude=ClosedWorldIT",
                ] + exclusions,
                data = [dar_files],
                runner = "@//bazel_tools/client_server:runner",
                runner_args = ["6865"],
                server = ":sandbox-with-postgres-{}".format(platform_version),
                server_args = [platform_version, "daml"] + sandbox_classic_args + extra_sandbox_classic_args + ["--jdbcurl=__jdbcurl__"],
                server_files = ["$(rootpaths {dar_files})".format(
                    dar_files = dar_files,
                )],
                tags = ["exclusive"] + extra_tags(sdk_version, platform_version),
            ) if is_linux else None

    # daml-ledger test-cases
    name = "daml-ledger-{sdk_version}-platform-{platform_version}".format(
        sdk_version = version_to_name(sdk_version),
        platform_version = version_to_name(platform_version),
    )
    daml_ledger_test(
        name = name,
        sdk_version = sdk_version,
        daml = daml_assistant,
        sandbox = sandbox_on_x if versions.is_at_least("2.0.0", platform_version) else sandbox,
        sandbox_args = sandbox_on_x_cmd + ["--contract-id-seeding=testing-weak", "--mutable-contract-state-cache", "--participant=participant-id=sandbox,port=0,port-file=__PORTFILE__"] if versions.is_at_least("2.0.0", platform_version) else ["sandbox", "--port=0", "--port-file=__PORTFILE__"],
        size = "large",
        # We see timeouts here fairly regularly so we
        # increase the number of CPUs.
        tags = ["cpu:2"] + extra_tags(sdk_version, platform_version),
    )

    # For now, we only cover the Daml Hub usecase where
    # sandbox and the JSON API come from the same SDK.
    # However, the test setup is flexible enough, that we
    # can control them individually.

    # We allocate parties via @daml/ledger which only supports this since SDK 1.8.0
    if versions.is_at_least("1.8.0", sdk_version):
        create_daml_app_test(
            name = "create-daml-app-{sdk_version}-platform-{platform_version}".format(sdk_version = version_to_name(sdk_version), platform_version = version_to_name(platform_version)),
            daml = daml_assistant,
            sandbox = sandbox_on_x if versions.is_at_least("2.0.0", platform_version) else sandbox,
            sandbox_version = platform_version,
            json_api = json_api,
            json_api_version = platform_version,
            daml_types = "@daml-sdk-{}//:daml-types.tgz".format(sdk_version),
            daml_react = "@daml-sdk-{}//:daml-react.tgz".format(sdk_version),
            daml_ledger = "@daml-sdk-{}//:daml-ledger.tgz".format(sdk_version),
            messaging_patch = "@daml-sdk-{}//:create_daml_app.patch".format(sdk_version),
            codegen_output = "//:create-daml-app-codegen-{}.tar.gz".format(version_to_name(sdk_version)),
            dar = "//:create-daml-app-{}.dar".format(version_to_name(sdk_version)),
            size = "large",
            # Yarn gets really unhappy if it is called in parallel
            # so we mark this exclusive for now.
            tags = extra_tags(sdk_version, platform_version) + ["exclusive", "dont-run-on-darwin"],
        )
