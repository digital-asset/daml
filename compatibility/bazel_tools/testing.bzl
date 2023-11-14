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
#   - https://github.com/digital-asset/daml/pull/17241
# - KVCommandDeduplicationIT (only some test cases):
#   - https://github.com/digital-asset/daml/pull/11095
# - CommandDeduplicationIT:CDDeduplicateSubmitterBasic (fixed in https://github.com/digital-asset/daml/pull/11095):
#   - https://github.com/digital-asset/daml/pull/11141

first_granular_test_tool = "1.3.0-snapshot.20200623.4546.0.4f68cfc4"

# Some of gRPC error codes changed from INVALID_ARGUMENT to ABORTED
# See https://github.com/digital-asset/daml/pull/9218
after_grpc_error_code_breaking_change = "1.12.0-snapshot.20210323.6567.1.90c5ce70"
grpc_error_code_breaking_change_exclusions = [
    "SemanticTests:SemanticDoubleSpendShared",
    "SemanticTests:SemanticPrivacyProjections",
    "SemanticTests:SemanticDivulgence",
    "ContractKeysIT:CKFetchOrLookup",
    "ContractKeysIT:CKNoFetchUndisclosed",
    "ContractKeysIT:CKMaintainerScoped",
]

before_removing_legacy_error_codes = "2.0.0-snapshot.20220127.9042.0.4038d0a7"
after_removing_legacy_error_codes = "2.0.0-snapshot.20220127.9042.0.4038d0a7.1"
first_canton_in_ledger_api_tests = "2.7.0-snapshot.20230504.11748.0.af51d660"

excluded_test_tool_tests = [
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
        "start": "2.0.0",
        "end": "2.1.0",
        "platform_ranges": [
            {
                "start": "2.6.0-snapshot.20230119.11284.1",
                "exclusions": [
                    "UserManagementServiceIT:TestGrantUserRights",
                    "UserManagementServiceIT:TestRevokeUserRights",
                    "UserManagementServiceIT:TestListUserRights",
                    "UserManagementServiceIT:UserManagementUserRightsLimit",
                    "UserManagementServiceIT:GrantRightsRaceCondition",
                ],
            },
        ],
    },
    {
        "start": "2.1.1",
        "end": "2.6.0-snapshot.20230119.11284.0.179b865a",
        "platform_ranges": [
            {
                "start": "2.6.0-snapshot.20230119.11284.1",
                "exclusions": [
                    "UserManagementServiceIT:TestGrantUserRights",
                    "UserManagementServiceIT:TestRevokeUserRights",
                    "UserManagementServiceIT:TestListUserRights",
                    "UserManagementServiceIT:UserManagementUserRightsLimit",
                    "UserManagementServiceIT:RaceConditionGrantRights",
                    "UserManagementServiceIT:RaceConditionRevokeRights",
                ],
            },
        ],
    },
    {
        "start": "2.6.0-snapshot.20230130.11335.0.a24439f0",
        "platform_ranges": [
            {
                "end": "2.6.0-snapshot.20230130.11335.1",
                "exclusions": [
                    "IdentityProviderConfigServiceIT",
                ],
            },
        ],
    },
    {
        "start": "2.6.0-snapshot.20230123.11292.1",
        "platform_ranges": [
            {
                "end": "2.6.0-snapshot.20230123.11292.0.b3f84bfc",
                "exclusions": [
                    # This test relies on a new Ledger API endpoint. Disable it for prior platforms
                    "ParticipantPruningIT:PRQueryLatestPrunedOffsets",
                ],
            },
        ],
    },
    {
        "start": "2.6.0-snapshot",
        "platform_ranges": [
            {
                "end": "2.6.0-snapshot",
                "exclusions": [
                    "ParticipantPruningIT:PREventsByContractIdPruned",
                    "ParticipantPruningIT:PREventsByContractKey",
                    "EventQueryServiceIT",
                ],
            },
        ],
    },
    {
        "start": "1.16.0",
        "platform_ranges": [
            {
                "start": first_canton_in_ledger_api_tests,
                "exclusions": [
                    "ClosedWorldIT",
                    "ConfigManagementServiceIT",
                    "LedgerConfigurationServiceIT",
                    "ParticipantPruningIT",
                ],
            },
        ],
    },
    {
        "start": "2.0.1",
        "platform_ranges": [
            {
                "start": first_canton_in_ledger_api_tests,
                "exclusions": [
                    "TLSOnePointThreeIT",
                    "TLSAtLeastOnePointTwoIT",
                    "CommandDeduplicationPeriodValidationIT:OffsetPruned",
                ],
            },
        ],
    },
    {
        "start": "2.6.3",
        "platform_ranges": [
            {
                "start": first_canton_in_ledger_api_tests,
                "exclusions": [
                    "ActiveContractsServiceIT:AcsBeforePruningOffsetIsDisallowed",
                    "ActiveContractsServiceIT:AcsAtPruningOffsetIsAllowed",
                ],
            },
        ],
    },
    {
        "start": "2.7.0-snapshot.20230529.11827.1",
        "platform_ranges": [
            {
                "end": "2.7.0-snapshot.20230529.11827.0.v3fbe7d01",
                "exclusions": [
                    "PartyManagementServiceIT:PMUpdatingPartyIdentityProviderNonDefaultIdps",
                    "PartyManagementServiceIT:PMUpdatingPartyIdentityProviderWithDefaultIdp",
                    "PartyManagementServiceIT:PMUpdatingPartyIdentityProviderNonExistentIdps",
                    "PartyManagementServiceIT:PMUpdatingPartyIdentityProviderMismatchedSourceIdp",
                    "PartyManagementServiceIT:PMUpdatingPartyIdentityProviderSourceAndTargetIdpTheSame",
                    "PartyManagementServiceIT:PMGetPartiesUsingDifferentIdps",
                    "UserManagementServiceIT:UserManagementUpdateUserIdpWithNonDefaultIdps",
                    "UserManagementServiceIT:UserManagementUpdateUserIdpWithDefaultIdp",
                    "UserManagementServiceIT:UserManagementUpdateUserIdpNonExistentIdps",
                    "UserManagementServiceIT:UserManagementUpdateUserIdpMismatchedSourceIdp",
                    "UserManagementServiceIT:UserManagementUpdateUserIdpSourceAndTargetIdpTheSame",
                ],
            },
        ],
    },
    {
        "start": "2.6.0",
        "end": "2.7.0-snapshot.20230619.11890.0.vedd1a5f6",
        "platform_ranges": [
            {
                "start": "2.7.0-snapshot.20230619.11890.0.vedd1a5f6.1",
                "exclusions": [
                    "InterfaceSubscriptionsIT:ISTransactionsEquivalentFilters",
                ],
            },
        ],
    },
    # Ledger api error structure change, all following tests make assertions on errors
    # New error api tool cannot be used on old api platform
    {
        "start": "2.7.0-snapshot.20230703.11931.1",
        "platform_ranges": [
            {
                "end": "2.7.0-snapshot.20230703.11931.0.vc04c7ac9",
                "exclusions": [
                    "TransactionServiceExerciseIT:TXRejectOnFailingAssertion",
                    "DeeplyNestedValueIT",
                    "MultiPartySubmissionIT:MPSLookupOtherByKeyInvisible",
                    "CommandServiceIT:CSReturnStackTrace",
                    "TransactionServiceAuthorizationIT:TXRejectMultiActorExcessiveAuth",
                    "ContractKeysIT:CKGlocalKeyVisibility",
                    "WronglyTypedContractIdIT",
                    "ExplicitDisclosureIT:EDMalformedDisclosedContracts",
                    "InterfaceSubscriptionsIT:ISTransactionsEquivalentFilters",
                    "TimeServiceIT:TSFailWhenTimeNotAdvanced",
                    "ExceptionsIT:ExUncaught",
                ],
            },
        ],
    },
    # Reverse of above, old api error tool cannot be used on new api platform
    # Split up to account for tests not existing in older tools
    {
        "end": "2.7.0-snapshot.20230703.11931.0.vc04c7ac9",
        "platform_ranges": [
            {
                "start": "2.7.0-snapshot.20230703.11931.1",
                "exclusions": [
                    "CommandServiceIT:CSReturnStackTrace",
                    "DeeplyNestedValueIT",
                    "ExceptionsIT:ExUncaught",
                    "MultiPartySubmissionIT:MPSLookupOtherByKeyInvisible",
                    "WronglyTypedContractIdIT",
                ],
            },
        ],
    },
    {
        "start": "1.17.1",
        "end": "2.7.0-snapshot.20230703.11931.0.vc04c7ac9",
        "platform_ranges": [
            {
                "start": "2.7.0-snapshot.20230703.11931.1",
                "exclusions": [
                    "TransactionServiceAuthorizationIT:TXRejectMultiActorExcessiveAuth",
                    "TransactionServiceExerciseIT:TXRejectOnFailingAssertion",
                ],
            },
        ],
    },
    {
        "start": "2.0.1",
        "end": "2.7.0-snapshot.20230703.11931.0.vc04c7ac9",
        "platform_ranges": [
            {
                "start": "2.7.0-snapshot.20230703.11931.1",
                "exclusions": [
                    "TimeServiceIT:TSFailWhenTimeNotAdvanced",
                ],
            },
        ],
    },
    {
        "start": "2.3.14",
        "end": "2.7.0-snapshot.20230703.11931.0.vc04c7ac9",
        "platform_ranges": [
            {
                "start": "2.7.0-snapshot.20230703.11931.1",
                "exclusions": [
                    "ContractKeysIT:CKGlocalKeyVisibility",
                ],
            },
        ],
    },
    {
        "start": "2.6.5",
        "end": "2.7.0-snapshot.20230703.11931.0.vc04c7ac9",
        "platform_ranges": [
            {
                "start": "2.7.0-snapshot.20230703.11931.1",
                "exclusions": [
                    "ExplicitDisclosureIT:EDMalformedDisclosedContracts",
                    "InterfaceSubscriptionsIT:ISTransactionsEquivalentFilters",
                ],
            },
        ],
    },
    # From 2.7.0snap - 2.7.x the DeeplyNestedValueIT test was misaligned with the real error
    # After this, it had changed on both tests and ledger to be different from <2.7.1
    # As such, Test tool <2.7.x cannot run with platform >= 2.7.1
    # and test tool >= 2.7.1 cannot run with platform <2.7.x
    {
        "end": "2.7.9",
        "platform_ranges": [
            {
                "start": "2.7.0-snapshot.20230703.11931.0.vc04c7ac9",
                "exclusions": [
                    "DeeplyNestedValueIT",
                ],
            },
        ],
    },
    {
        "start": "2.7.0-snapshot.20230703.11931.0.vc04c7ac9",
        "platform_ranges": [
            {
                "end": "2.7.9",
                "exclusions": [
                    "DeeplyNestedValueIT",
                ],
            },
        ],
    },
    # Changes to explicit disclosure Ledger API interface
    # where DisclosedContract.create_arguments(_blob), DisclosedContract.metadata,
    # InclusiveFilters.template_ids are deprecated and replaced by
    # DisclosedContract.created_event_blob and InclusiveFilters.template_filters respectively
    {
        "start": "2.6.0-snapshot.20230123.11292.0.b3f84bfc",
        "end": "2.8.0-snapshot.20231025.0",
        "platform_ranges": [
            {
                "start": "2.8.0-snapshot.20231025.1",
                "exclusions": [
                    "ExplicitDisclosureIT",
                    "InterfaceSubscriptionsIT:ISTransactionsCreateArgumentsBlob",
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
        # any post 1.15.0 platform supports any pre 2.6 SDK
        in_range(platform_version, {"start": "1.15.0-snapshot"}) and not in_range(sdk_version, {"start": "2.5.0-snapshot"})
    ) or (
        # any post 2.5.0 platform supports any SDK
        in_range(platform_version, {"start": "2.5.0-snapshot"})
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
    canton_sandbox = "@daml-sdk-{platform_version}//:daml".format(
        platform_version = platform_version,
    )
    canton_sandbox_args = ["sandbox", "--canton-port-file", "__PORTFILE__"]

    json_api = "@daml-sdk-{platform_version}//:daml".format(
        platform_version = platform_version,
    )

    use_canton = versions.is_at_least(first_canton_in_ledger_api_tests, platform_version)

    # ledger-api-test-tool test-cases
    name = "ledger-api-test-tool-{sdk_version}-platform-{platform_version}".format(
        sdk_version = version_to_name(sdk_version),
        platform_version = version_to_name(platform_version),
    )
    exclusions = ["--exclude=" + test for test in get_excluded_tests(test_tool_version = sdk_version, sandbox_version = platform_version)]

    if versions.is_at_least("1.16.0", sdk_version) and versions.is_stable(sdk_version) and versions.is_stable(platform_version):
        if use_canton:
            client_server_test(
                name = name + "-canton",
                client = ledger_api_test_tool,
                client_args = [
                    "localhost:6865",
                    "--concurrent-test-runs=2",  # lowered from default #procs to reduce flakes - details in https://github.com/digital-asset/daml/issues/7316
                    "--timeout-scale-factor=2",
                ] + exclusions,
                data = [dar_files],
                runner = "@//bazel_tools/client_server:runner",
                runner_args = ["6865", "7000"],
                server = canton_sandbox,
                server_args = [
                    "sandbox",
                    "--canton-port-file",
                    "_port_file",
                    "--",
                    "-C",
                    "canton.monitoring.health.server.port=7000",
                    "-C",
                    "canton.monitoring.health.check.type=ping",
                    "-C",
                    "canton.monitoring.health.check.participant=sandbox",
                    "-C",
                    "canton.monitoring.health.check.interval=5s",
                ],
                tags = ["exclusive", sdk_version, platform_version] + extra_tags(sdk_version, platform_version),
            )

    # daml-ledger test-cases
    name = "daml-ledger-{sdk_version}-platform-{platform_version}".format(
        sdk_version = version_to_name(sdk_version),
        platform_version = version_to_name(platform_version),
    )

    # Canton does not support LF<=1.8 found in earlier versions
    if versions.is_at_least("1.16.0", sdk_version):
        daml_ledger_test(
            name = name,
            sdk_version = sdk_version,
            daml = daml_assistant,
            sandbox = canton_sandbox,
            sandbox_args = canton_sandbox_args,
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
    if versions.is_at_least("2.0.1", sdk_version):
        create_daml_app_test(
            name = "create-daml-app-{sdk_version}-platform-{platform_version}".format(sdk_version = version_to_name(sdk_version), platform_version = version_to_name(platform_version)),
            daml = daml_assistant,
            sandbox = canton_sandbox,
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
