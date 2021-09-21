# This file was built as part of the Ledger API Test Tool Cleanup design work.
# See this design doc for context: https://docs.google.com/document/d/17vcSHNiucT8w5h5aQq5uGKeT2UTWcmUuW0dY50tS3yo/edit#heading=h.j1o9vy5fqmrz
#
# This file was manually built from grepping all BUILD files for conformance_test declarations.
# The purpose is to provide an overview of all of them AND generate a .csv file for further analysis.
# The resulting GSheet is here: https://docs.google.com/spreadsheets/d/1LpaFSVXoaDk77OaRReubsSsX6WeW73LXKDYSJltiTc4/edit#gid=1310969124
# 
# This future of this file is unclear. It depends on the outcomes of the desing work above. 


# Some helper functions
#######################

import csv

tests = []
field_names = ["file","name", "server", "server_args", "test_tool_args", "extra_data", "lf_versions", "ports", "runner", "tags", "flaky"]
current_file = ""
is_windows = False
oracle_testing = False

def conformance_test(name, server, server_args, test_tool_args, extra_data = [], lf_versions = [], ports = [], runner = "", tags = [], flaky = False):
    pp = lambda x: '\n'.join(x)
    args = [current_file, name, server, pp(server_args), pp(test_tool_args), pp(extra_data), lf_versions, ports, runner, pp(tags), flaky]

    tests.insert(0, dict(zip(field_names, args)))

def file(new_file):
    global current_file
    current_file = new_file

def server_conformance_test(name, servers, server_args = [], test_tool_args = [], flaky = False, lf_versions = ["default"]):
    for server_name, server in servers.items():
        test_name = "-".join([name, server_name])
        conformance_test(
            name = test_name,
            extra_data = server.get("extra_data", []),
            server = server["binary"],
            server_args = server.get("server_args", []) + server_args,
            test_tool_args = server.get("test_tool_args", []) + test_tool_args,
            lf_versions = lf_versions,
            flaky = flaky,
        )


# File and conformance test declarations pasted manually from a grep
####################################################################


file("//ledger/daml-on-sql")

conformance_test(
    name = "conformance-test",
    server = ":daml-on-sql-ephemeral-postgresql",
    server_args = [
        "--ledgerid=conformance-test",
        "--port=6865",
        "--eager-package-loading",
    ],
    test_tool_args = [
        "--verbose",
        "--additional=MultiPartySubmissionIT",
        "--additional=ContractIdIT:Accept",
    ],
)

# TODO append-only: cleanup
conformance_test(
    name = "conformance-test-append-only",
    server = ":daml-on-sql-ephemeral-postgresql",
    server_args = [
        "--ledgerid=conformance-test",
        "--port=6865",
        "--eager-package-loading",
        "--enable-append-only-schema",
    ],
    test_tool_args = [
        "--verbose",
        "--additional=MultiPartySubmissionIT",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
    ],
)

file("//ledger/ledger-api-test-tool")


conformance_test(
    name = "conformance-test",
    extra_data = [
        "//ledger/test-common/test-certificates:client.crt",
        "//ledger/test-common/test-certificates:client.pem",
        "//ledger/test-common/test-certificates:server.crt",
        "//ledger/test-common/test-certificates:server.pem",
        "//ledger/test-common/test-certificates:server.pem.enc",
        "//ledger/test-common/test-certificates:ca.crt",
    ],
    lf_versions = "lf_version_configuration_versions",
    ports = [6865],
    server = "//ledger/ledger-on-memory:app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant=participant-id=example,port=6865",
        "--crt $$(rlocation $$TEST_WORKSPACE/$(rootpath //ledger/test-common/test-certificates:server.crt))",
        "--cacrt $$(rlocation $$TEST_WORKSPACE/$(rootpath //ledger/test-common/test-certificates:ca.crt))",
        "--pem $$(rlocation $$TEST_WORKSPACE/$(rootpath //ledger/test-common/test-certificates:server.pem.enc))",
        "--tls-secrets-url https://raw.githubusercontent.com/digital-asset/daml/main/ledger/test-common/files/server-pem-decryption-parameters.json",
        "--min-tls-version 1.3",
    ],
    test_tool_args = [
        "--verbose",
        "--crt $$(rlocation $$TEST_WORKSPACE/$(rootpath //ledger/test-common/test-certificates:client.crt))",
        "--cacrt $$(rlocation $$TEST_WORKSPACE/$(rootpath //ledger/test-common/test-certificates:ca.crt))",
        "--pem $$(rlocation $$TEST_WORKSPACE/$(rootpath //ledger/test-common/test-certificates:client.pem))",
        "--additional=IdentityIT",
        "--additional=IdentityIT:IdNotEmpty",
        # Explicitly include retired tests here to make sure existing CI pipelines are not broken.
        # Retired tests will be eventually removed.
        "--additional=LotsOfPartiesIT",
        "--additional=TransactionScaleIT",
        "--additional=TLSOnePointThreeIT",
        "--exclude=CommandDeduplicationIT",
        # Makes sure that deprecated CLI options can still be used to make sure existing CI pipelines are not broken.
        # This test should fail if any deprecated CLI option has any effect whatsoever -- they are preserved
        # exclusively for backwards-compatibility.
        # Deprecated CLI options will be eventually removed.
        "--load-scale-factor=THIS_OPTION_IS_DEPRECATED_AND_HAS_NO_EFFECT",
        "--target-port=THIS_OPTION_IS_DEPRECATED_AND_HAS_NO_EFFECT",
        "--all-tests",
    ],
)


file("//ledger/ledger-api-test-tool-on-canton")

conformance_test(
    name = "conformance-test",
    extra_data = [
        ":bootstrap.canton",
        ":canton_deploy.jar",
        ":canton.conf",
        ":logback-debug.xml",
        "@coreutils_nix//:bin/base64",
        "@curl_nix//:bin/curl",
        "@grpcurl_nix//:bin/grpcurl",
        "@jq_dev_env//:jq",
        "@jdk11_nix//:bin/java",
    ],
    lf_versions = [
        "default",
        "latest",
    ],
    ports = [
        5011,
        5021,
        5031,
        5041,
    ],
    runner = "@//bazel_tools/client_server/runner_with_port_check",
    server = ":canton-test-runner-with-dependencies",
    server_args = [],
    test_tool_args = [
        "--verbose",
        "--concurrent-test-runs=1",  # lowered from default #procs to reduce flakes - details in https://github.com/digital-asset/daml/issues/7316
        # The following three contract key tests require uniqueness
        "--exclude=ContractKeysIT:CKFetchOrLookup,ContractKeysIT:CKNoFetchUndisclosed,ContractKeysIT:CKMaintainerScoped" +
        ",ParticipantPruningIT" +  # see "conformance-test-participant-pruning" below
        ",ConfigManagementServiceIT,LedgerConfigurationServiceIT" +  # dynamic config management not supported by Canton
        ",ClosedWorldIT" +  # Canton currently fails this test with a different error (missing namespace in "unallocated" party id)
        # Excluding tests that require contract key uniqueness and RWArchiveVsFailedLookupByKey (finding a lookup failure after contract creation)
        ",RaceConditionIT:WWDoubleNonTransientCreate,RaceConditionIT:WWArchiveVsNonTransientCreate,RaceConditionIT:RWTransientCreateVsNonTransientCreate,RaceConditionIT:RWArchiveVsFailedLookupByKey" +
        ",RaceConditionIT:RWArchiveVsLookupByKey,RaceConditionIT:RWArchiveVsNonConsumingChoice,RaceConditionIT:RWArchiveVsFetch,RaceConditionIT:WWDoubleArchive" +
        ",ExceptionsIT,ExceptionRaceConditionIT" +  # need UCK mode - added below
        ",DeeplyNestedValueIT" +  # FIXME: Too deeply nested values flake with a time out (half of the time)
        ",CommandServiceIT:CSReturnStackTrace" +  # FIXME: Ensure canton returns stack trace
        ",CommandServiceIT:CSsubmitAndWaitForTransactionIdInvalidLedgerId,CommandServiceIT:CSsubmitAndWaitForTransactionInvalidLedgerId,CommandServiceIT:CSsubmitAndWaitForTransactionTreeInvalidLedgerId" +  # FIXME: return definite_answer in gRPC errors
        ",CommandSubmissionCompletionIT:CSCRefuseBadChoice,CommandSubmissionCompletionIT:CSCSubmitWithInvalidLedgerId,CommandSubmissionCompletionIT:CSCDisallowEmptyTransactionsSubmission",  # FIXME: return definite_answer in gRPC errors
    ],
) if not is_windows else None



conformance_test(
    name = "conformance-test",
    ports = [6865],
    server = ":app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant=participant-id=example,port=6865",
        "--max-deduplication-duration=PT5S",
    ],
    test_tool_args = [
        "--additional=MultiPartySubmissionIT",
        "--additional=ContractIdIT:RejectV0,ContractIdIT:AcceptSuffixedV1,ContractIdIT:AcceptNonSuffixedV1",
        "--additional=ParticipantPruningIT",
        "--additional=KVCommandDeduplicationIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        # Disable tests targeting only append-only schema functionality
        "--exclude=ParticipantPruningIT:PRLocalAndNonLocalRetroactiveDivulgences,ParticipantPruningIT:PRRetroactiveDivulgences,ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "conformance-test-min-1.14",
    lf_versions = ["1.14"],
    ports = [6865],
    server = ":app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant=participant-id=example,port=6865",
        "--daml-lf-min-version-1.14-unsafe",
        "--max-deduplication-duration=PT5S",
    ],
    test_tool_args = [
        "--additional=MultiPartySubmissionIT",
        "--additional=KVCommandDeduplicationIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
    ],
)

conformance_test(
    name = "conformance-test-multi-participant",
    ports = [
        6865,
        6866,
    ],
    server = ":app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant=participant-id=example1,port=6865",
        "--participant=participant-id=example2,port=6866",
        "--max-deduplication-duration=PT5S",
    ],
    test_tool_args = [
        "--verbose",
        "--additional=ParticipantPruningIT",
        "--additional=KVCommandDeduplicationIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        "--exclude=ConfigManagementServiceIT",
        # Disable tests targeting only append-only schema functionality
        "--exclude=ParticipantPruningIT:PRLocalAndNonLocalRetroactiveDivulgences,ParticipantPruningIT:PRRetroactiveDivulgences,ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "conformance-test-append-only-multi-participant",
    ports = [
        6865,
        6866,
    ],
    server = ":app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--index-append-only-schema",
        "--mutable-contract-state-cache",
        "--participant=participant-id=example1,port=6865",
        "--participant=participant-id=example2,port=6866",
        "--max-deduplication-duration=PT5S",
        "--tracker-retention-period=PT5S",  # lower the command timeout duration, this is a workaround DPP-609
    ],
    test_tool_args = [
        "--verbose",
        "--additional=ParticipantPruningIT",
        "--additional=AppendOnlyKVCommandDeduplicationIT",
        # The following two tests don't actually care about multi-participant but they do need append-only.
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        "--exclude=ConfigManagementServiceIT",
    ],
)

conformance_test(
    name = "conformance-test-split-participant",
    ports = [
        6865,
    ],
    server = ":app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant=participant-id=split-example,port=6865,server-jdbc-url=jdbc:h2:mem:split-example;db_close_delay=-1;db_close_on_exit=false,shard-name=server1,run-mode=ledger-api-server",
        "--participant=participant-id=split-example,port=6865,server-jdbc-url=jdbc:h2:mem:split-example;db_close_delay=-1;db_close_on_exit=false,shard-name=indexer,run-mode=indexer",
        "--max-deduplication-duration=PT5S",
    ],
    test_tool_args = [
        "--verbose",
        "--additional=KVCommandDeduplicationIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        "--exclude=ConfigManagementServiceIT",
    ],
)

conformance_test(
    name = "benchmark-performance-envelope",
    ports = [6865],
    server = ":app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant=participant-id=example,port=6865",
    ],
    test_tool_args = [
        "--verbose",
        "--perf-tests=PerformanceEnvelope.Throughput.TwentyOPS",
        "--perf-tests=PerformanceEnvelope.Latency.1000ms",
        "--perf-tests=PerformanceEnvelope.TransactionSize.1000KB",
    ],
)

file("//ledger/ledger-on-sql")

supported_databases = [
    {
        "name": "h2-memory",
        "runtime_deps": [
            "@maven//:com_h2database_h2",
        ],
        "conformance_test_tags": [
            "manual",
        ],
        "conformance_test_server_args": [
            "--jdbc-url=jdbc:h2:mem:daml-on-sql-conformance-test",
        ],
    },
    {
        "name": "postgresql",
        "runtime_deps": [
            "@maven//:org_postgresql_postgresql",
        ],
        "conformance_test_server_main": "com.daml.ledger.on.sql.MainWithEphemeralPostgresql",
    },
    {
        "name": "sqlite-memory",
        "runtime_deps": [
            "@maven//:org_xerial_sqlite_jdbc",
        ],
        "conformance_test_server_args": [
            "--jdbc-url=jdbc:sqlite:file:daml-on-sql-conformance-test?mode=memory&cache=shared",
        ],
    },
]

for db in supported_databases:
    conformance_test(
        name = "conformance-test-{}".format(db["name"]),
        lf_versions = [
            "default",
            "latest",
            "preview",
        ],
        ports = [6865],
        server = ":conformance-test-{}-bin".format(db["name"]),
        server_args = [
            "--contract-id-seeding=testing-weak",
            "--participant participant-id=conformance-test,port=6865",
            "--max-deduplication-duration=PT5S",
        ] + db.get("conformance_test_server_args", []),
        tags = db.get("conformance_test_tags", []),
        test_tool_args = db.get("conformance_test_tool_args", []) + [
            "--verbose",
            "--additional=ParticipantPruningIT",
            "--additional=MultiPartySubmissionIT",
            "--additional=KVCommandDeduplicationIT",
            "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
            # Disable tests targeting only append-only schema functionality
            "--exclude=ParticipantPruningIT:PRLocalAndNonLocalRetroactiveDivulgences,ParticipantPruningIT:PRRetroactiveDivulgences,ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
        ],
    ),
    conformance_test(
        name = "benchmark-performance-envelope-{}".format(db["name"]),
        ports = [6865],
        server = ":conformance-test-{}-bin".format(db["name"]),
        server_args = [
            "--contract-id-seeding=testing-weak",
            "--participant participant-id=example,port=6865",
        ] + db.get("conformance_test_server_args", []),
        tags = db.get("benchmark_performance_envelope_tags", []),
        test_tool_args = db.get("benchmark_performance_envelope_tags", []) + [
            "--verbose",
            "--perf-tests=PerformanceEnvelope.Throughput.TwentyOPS",
            "--perf-tests=PerformanceEnvelope.Latency.1000ms",
            "--perf-tests=PerformanceEnvelope.TransactionSize.1000KB",
        ],
    )

conformance_test(
    name = "conformance-test-append-only-postgres",
    ports = [6865],
    server = ":conformance-test-postgresql-bin",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant participant-id=conformance-test,port=6865,contract-state-cache-max-size=1,contract-key-state-cache-max-size=1",
        "--index-append-only-schema",
        "--mutable-contract-state-cache",
        "--max-deduplication-duration=PT5S",
    ],
    tags = [],
    test_tool_args = [
        "--verbose",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--additional=AppendOnlyKVCommandDeduplicationIT",
        "--additional=ParticipantPruningIT",
        "--additional=MultiPartySubmissionIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        # Disable tests targeting only multi-participant setups
        "--exclude=ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "conformance-test-append-only-h2",
    ports = [6865],
    server = ":conformance-test-h2-memory-bin",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant participant-id=conformance-test,port=6865,contract-state-cache-max-size=1,contract-key-state-cache-max-size=1",
        "--index-append-only-schema",
        "--mutable-contract-state-cache",
        "--jdbc-url=jdbc:h2:mem:daml-on-sql-conformance-test",
        "--max-deduplication-duration=PT5S",
        "--tracker-retention-period=PT5S",  # lower the command timeout duration, this is a workaround DPP-609
    ],
    tags = [],
    test_tool_args = [
        "--verbose",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--additional=AppendOnlyKVCommandDeduplicationIT",
        "--additional=ParticipantPruningIT",
        "--additional=MultiPartySubmissionIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        # Disable tests targeting only multi-participant setups
        "--exclude=ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "conformance-test-append-only-oracle",
    ports = [6865],
    server = ":conformance-test-oracle-bin",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant participant-id=conformance-test,port=6865,contract-state-cache-max-size=1,contract-key-state-cache-max-size=1",
        "--index-append-only-schema",
        "--mutable-contract-state-cache",
        "--max-deduplication-duration=PT5S",
        "--tracker-retention-period=PT5S",  # lower the command timeout duration, this is a workaround DPP-609
    ],
    tags = [] if oracle_testing else ["manual"],
    test_tool_args = [
        "--verbose",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--additional=AppendOnlyKVCommandDeduplicationIT",
        "--additional=ParticipantPruningIT",
        "--additional=MultiPartySubmissionIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        # Disable tests targeting only multi-participant setups
        "--exclude=ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "conformance-test-oracle",
    ports = [6865],
    server = ":conformance-test-oracle-bin",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant participant-id=conformance-test,port=6865,contract-state-cache-max-size=1,contract-key-state-cache-max-size=1",
        "--max-deduplication-duration=PT5S",
    ],
    tags = [] if oracle_testing else ["manual"],
    test_tool_args = [
        "--verbose",
        "--additional=ParticipantPruningIT",
        "--additional=MultiPartySubmissionIT",
        "--additional=KVCommandDeduplicationIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        # Disable tests targeting only append-only schema functionality
        "--exclude=ParticipantPruningIT:PRLocalAndNonLocalRetroactiveDivulgences,ParticipantPruningIT:PRRetroactiveDivulgences,ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "conformance-test-append-only-in-memory-fan-out-postgres",
    ports = [6865],
    server = ":conformance-test-postgresql-bin",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant participant-id=conformance-test,port=6865,contract-state-cache-max-size=1,contract-key-state-cache-max-size=1,ledger-api-transactions-buffer-max-size=10",
        "--index-append-only-schema",
        "--mutable-contract-state-cache",
        "--buffered-ledger-api-streams-unsafe",
        "--max-deduplication-duration=PT5S",
    ],
    tags = [],
    test_tool_args = [
        "--verbose",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--additional=AppendOnlyKVCommandDeduplicationIT",
        "--additional=ParticipantPruningIT",
        "--additional=MultiPartySubmissionIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        # Disable tests targeting only multi-participant setups
        "--exclude=ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "conformance-test-append-only-in-memory-fan-out-oracle",
    ports = [6865],
    server = ":conformance-test-oracle-bin",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant participant-id=conformance-test,port=6865,contract-state-cache-max-size=1,contract-key-state-cache-max-size=1,ledger-api-transactions-buffer-max-size=10",
        "--index-append-only-schema",
        "--mutable-contract-state-cache",
        "--buffered-ledger-api-streams-unsafe",
        "--max-deduplication-duration=PT5S",
        "--tracker-retention-period=PT5S",  # lower the command timeout duration, this is a workaround DPP-609
    ],
    tags = [] if oracle_testing else ["manual"],
    test_tool_args = [
        "--verbose",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--additional=AppendOnlyKVCommandDeduplicationIT",
        "--additional=ParticipantPruningIT",
        "--additional=MultiPartySubmissionIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        # Disable tests targeting only multi-participant setups
        "--exclude=ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "conformance-test-contract-ids",
    ports = [6865],
    server = ":conformance-test-h2-memory-bin",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant participant-id=conformance-test,port=6865,contract-state-cache-max-size=1,contract-key-state-cache-max-size=1",
        "--jdbc-url=jdbc:h2:mem:daml-on-sql-conformance-test",
    ],
    test_tool_args = [
        "--verbose",
        "--include=ContractIdIT:RejectV0,ContractIdIT:AcceptSuffixedV1,ContractIdIT:AcceptNonSuffixedV1",
    ],
)

file("//ledger/sandbox")

NEXT_SERVERS = {
    "memory": {
        "binary": ":sandbox-binary",
        "server_args": [
            "--contract-id-seeding=testing-weak",
            "--port=6865",
            "--eager-package-loading",
        ],
    },
    "postgresql": {
        "binary": ":sandbox-ephemeral-postgresql",
        "server_args": [
            "--contract-id-seeding=testing-weak",
            "--port=6865",
            "--eager-package-loading",
        ],
    },
}

server_conformance_test(
    name = "next-conformance-test-static-time",
    lf_versions = [
        "default",
        "latest",
        "preview",
    ],
    server_args = [
        "--static-time",
    ],
    servers = NEXT_SERVERS,
    test_tool_args = [
        "--open-world",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        "--exclude=ClosedWorldIT",
    ],
)

server_conformance_test(
    name = "next-conformance-test-wall-clock",
    server_args = [
        "--wall-clock-time",
        "--max-deduplication-duration=PT5S",
    ],
    servers = NEXT_SERVERS,
    test_tool_args = [
        "--open-world",
        "--additional=KVCommandDeduplicationIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        "--exclude=ClosedWorldIT",
    ],
)

server_conformance_test(
    name = "next-conformance-test-closed-world",
    server_args = [
        "--wall-clock-time",
        "--implicit-party-allocation=false",
    ],
    servers = NEXT_SERVERS,
    test_tool_args = [
        "--verbose",
        "--include=ClosedWorldIT",
    ],
)

server_conformance_test(
    name = "next-conformance-test-contract-ids",
    servers = {"memory": NEXT_SERVERS["memory"]},
    test_tool_args = [
        "--verbose",
        "--include=ContractIdIT:RejectV0,ContractIdIT:AcceptSuffixedV1,ContractIdIT:AcceptNonSuffixedV1",
    ],
)

# TODO append-only: cleanup
server_conformance_test(
    name = "next-conformance-test-append-only",
    server_args = [
        "--enable-append-only-schema",
        "--max-deduplication-duration=PT5S",
    ],
    servers = {"postgresql": NEXT_SERVERS["postgresql"]},
    test_tool_args = [
        "--open-world",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--additional=AppendOnlyKVCommandDeduplicationIT",
        "--exclude=CommandDeduplicationIT",  # It's a KV ledger so it needs the KV variant
        "--exclude=ClosedWorldIT",
    ],
)

file("//ledger/sandbox-classic")

SERVERS = {
    "memory": {
        "binary": ":sandbox-classic-binary",
        "server_args": [
            "--port=6865",
            "--eager-package-loading",
        ],
    },
    "h2database": {
        "binary": ":sandbox-classic-binary",
        "server_args": [
            "--port=6865",
            "--eager-package-loading",
            # "db_close_delay=-1" is needed so that the in-memory database is not closed
            # (and therefore lost) after the flyway migration
            "--sql-backend-jdbcurl=jdbc:h2:mem:conformance-test;db_close_delay=-1",
        ],
    },
    "postgresql": {
        "binary": ":sandbox-classic-ephemeral-postgresql",
        "server_args": [
            "--port=6865",
            "--eager-package-loading",
        ],
    },
}

# TODO append-only: only for temporary testing
ONLY_POSTGRES_SERVER = {
    "postgresql": {
        "binary": ":sandbox-classic-ephemeral-postgresql",
        "server_args": [
            "--port=6865",
            "--eager-package-loading",
        ],
    },
}

server_conformance_test(
    name = "conformance-test-static-time",
    lf_versions = [
        "default",
        "preview",
    ],
    server_args = [
        "--static-time",
        "--contract-id-seeding=testing-weak",
    ],
    servers = SERVERS,
    test_tool_args = [
        "--open-world",
        "--exclude=ClosedWorldIT",
    ],
)

# TODO append-only: only for temporary testing
server_conformance_test(
    name = "conformance-test-static-time-append-only",
    lf_versions = [
        "default",
    ],
    server_args = [
        "--static-time",
        "--contract-id-seeding=testing-weak",
        "--enable-append-only-schema",
    ],
    servers = ONLY_POSTGRES_SERVER,
    test_tool_args = [
        "--open-world",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--exclude=ClosedWorldIT",
    ],
)

server_conformance_test(
    name = "conformance-test-wall-clock",
    server_args = [
        "--wall-clock-time",
        "--contract-id-seeding=testing-weak",
    ],
    servers = SERVERS,
    test_tool_args = [
        "--open-world",
        "--exclude=ClosedWorldIT",
    ],
)

# TODO append-only: only for temporary testing
server_conformance_test(
    name = "conformance-test-wall-clock-append-only",
    server_args = [
        "--wall-clock-time",
        "--contract-id-seeding=testing-weak",
        "--enable-append-only-schema",
    ],
    servers = ONLY_POSTGRES_SERVER,
    test_tool_args = [
        "--open-world",
        "--additional=AppendOnlyCommandDeduplicationParallelIT",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandService",
        "--additional=AppendOnlyCompletionDeduplicationInfoITCommandSubmissionService",
        "--additional=ParticipantPruningIT",
        "--exclude=ClosedWorldIT",
        # Excluding tests that require using pruneAllDivulgedContracts option that is not supported by sandbox-classic
        "--exclude=ParticipantPruningIT:PRLocalAndNonLocalRetroactiveDivulgences,ParticipantPruningIT:PRRetroactiveDivulgences,ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

server_conformance_test(
    name = "conformance-test-legacy-cid",
    lf_versions = ["legacy"],
    server_args = [
        "--wall-clock-time",
        "--contract-id-seeding=no",
    ],
    servers = SERVERS,
    test_tool_args = [
        "--open-world",
        "--exclude=ClosedWorldIT",
    ],
)

server_conformance_test(
    name = "conformance-test-contract-ids",
    server_args = ["--contract-id-seeding=testing-weak"],
    servers = {"memory": SERVERS["memory"]},
    test_tool_args = [
        "--verbose",
        "--include=ContractIdIT:Accept",
    ],
)

file("//ledger/sandbox-on-x")

conformance_test(
    name = "conformance-test",
    ports = [6865],
    server = ":app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant participant-id=example,port=6865",
        "--max-deduplication-duration=PT5S",
        "--index-append-only-schema",
        "--mutable-contract-state-cache",
    ],
    test_tool_args = [
        "--verbose",
        "--additional=ParticipantPruningIT",
        "--additional=MultiPartySubmissionIT",
        "--exclude=CommandDeduplicationIT",
        "--exclude=ClosedWorldIT",
        "--exclude=ContractKeysIT",
        "--exclude=SemanticTests",
        "--exclude=RaceConditionIT",
        "--exclude=ExceptionRaceConditionIT",
        "--exclude=ExceptionsIT:ExRollbackDuplicateKeyCreated",
        "--exclude=ExceptionsIT:ExRollbackDuplicateKeyArchived",
        "--exclude=ConfigManagementServiceIT:CMConcurrentSetConflicting",
        # Disable tests targeting only append-only schema functionality
        "--exclude=ParticipantPruningIT:PRLocalAndNonLocalRetroactiveDivulgences,ParticipantPruningIT:PRRetroactiveDivulgences,ParticipantPruningIT:PRImmediateAndRetroactiveDivulgence",
    ],
)

conformance_test(
    name = "benchmark-performance-envelope",
    ports = [6865],
    server = ":app",
    server_args = [
        "--contract-id-seeding=testing-weak",
        "--participant=participant-id=example,port=6865",
        "--index-append-only-schema",
        "--mutable-contract-state-cache",
    ],
    test_tool_args = [
        "--verbose",
        "--perf-tests=PerformanceEnvelope.Throughput.TwentyOPS",
        "--perf-tests=PerformanceEnvelope.Latency.1000ms",
        "--perf-tests=PerformanceEnvelope.TransactionSize.1000KB",
    ],
)


# Output generation
############################################################################

# Quick validation
print(tests)

# CSV file for further processing
with open('conformance_tests.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(tests)