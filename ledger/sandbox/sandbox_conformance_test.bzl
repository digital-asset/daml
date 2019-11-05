load(
    "//ledger/ledger-api-test-tool:conformance.bzl",
    "conformance_test",
)

def sandbox_conformance_test(name, server_args, test_tool_args):
    default_server_args = [
        "--port 6865",
        "--eager-package-loading",
    ]
    default_test_tool_args = [
        "--timeout-scale-factor=10",
    ]

    conformance_test(
        name = "conformance-test-" + name,
        server = ":sandbox-binary",
        server_args = default_server_args + server_args,
        test_tool_args = default_test_tool_args + test_tool_args,
    )

    conformance_test(
        name = "conformance-test-" + name + "-postgres",
        extra_data = [
            "@postgresql_dev_env//:all",
            "@postgresql_dev_env//:createdb",
            "@postgresql_dev_env//:initdb",
            "@postgresql_dev_env//:pg_ctl",
        ],
        server = ":sandbox-ephemeral-postgres",
        server_args = default_server_args + server_args,
        test_tool_args = default_test_tool_args + test_tool_args,
    )

    conformance_test(
        name = "conformance-test-" + name + "-h2database",
        server = ":sandbox-binary",
        server_args = default_server_args + server_args + [
            # "db_close_delay=-1" is needed so that the in-memory database is not closed
            # (and therefore lost) after the flyway migration
            "--sql-backend-jdbcurl jdbc:h2:mem:static_time;db_close_delay=-1",
        ],
        test_tool_args = default_test_tool_args + test_tool_args,
    )

