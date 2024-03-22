# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _client_server_build_impl(ctx):
    posix = ctx.toolchains["@rules_sh//sh/posix:toolchain_type"]
    ctx.actions.run_shell(
        outputs = ctx.outputs.outs,
        inputs = ctx.files.data,
        tools = depset([
            ctx.executable.runner,
            ctx.executable.client,
            ctx.executable.server,
        ]),
        command = """
        export {output_env}="{output_path}"
        {runner} "{client}" "{client_args} {client_files}" "{server}" "{server_args} {server_files}" "{runner_args}" &> runner.log
        if [ "$?" -ne 0 ]; then
          {cat} runner.log
          exit 1
        fi
      """.format(
            cat = posix.commands["cat"],
            output_env = ctx.attr.output_env,
            output_path = " ".join([o.path for o in ctx.outputs.outs]),
            runner = ctx.executable.runner.path,
            client = ctx.executable.client.path,
            server = ctx.executable.server.path,
            runner_args = " ".join(ctx.attr.runner_args),
            server_args = " ".join(ctx.attr.server_args),
            server_files = " ".join([f.path for f in ctx.files.server_files]),
            client_args = " ".join(ctx.attr.client_args),
            client_files = " ".join([f.path for f in ctx.files.client_files]),
        ),
    )
    return [
        DefaultInfo(
            files = depset(ctx.outputs.outs),
        ),
    ]

client_server_build = rule(
    implementation = _client_server_build_impl,
    attrs = {
        "runner": attr.label(
            cfg = "host",
            allow_single_file = True,
            executable = True,
            default = Label("@//bazel_tools/client_server/runner_with_port_file"),
        ),
        "runner_args": attr.string_list(),
        "client": attr.label(
            cfg = "target",
            executable = True,
        ),
        "client_args": attr.string_list(),
        "client_files": attr.label_list(allow_files = True),
        "server": attr.label(
            cfg = "target",
            executable = True,
        ),
        "server_args": attr.string_list(),
        "server_files": attr.label_list(allow_files = True),
        "outs": attr.output_list(mandatory = True),
        "output_env": attr.string(),
        "data": attr.label_list(allow_files = True),
    },
    toolchains = ["@rules_sh//sh/posix:toolchain_type"],
)
"""Creates a build target for a client-server run.

This rule is similar to the client_server_test rule, but
instead of producing a test target it produces a build target
that creates some result files from the run. Useful for producing
test data from integration tests that can then be used for
e.g. testing backwards compatibility.

The rule takes mostly the same arguments as the client_server_test rule, with
the exception that {client,server}_files is a label_list. Additionally it takes
the required arguments "output" and "output_env", which specify the list of
output files and an environment variable in which to store the paths to the
output files in a space separated list. This variable can be used either by the
client or the server to write the output.

Note that additional data files are not provided as runfiles (as they are
with the client_server_test rule), but rather placed to a relative to working
directory (e.g. "//some:file.dat" would go into "$PWD/some/file.dat").

Example:
  ```bzl
    client_server_build(
      name = "my_client_server_build",
      data = [":additional-data"],
      client = ":my_client",
      client_args = ["--extra-argument"],
      client_files = [":file-target-for-client"]
      server = ":my_server",
      server_args = ["--fast"],
      server_files = [":file-target-for-server"],
      outs = ["my_client_server_build.out"],
      output_env = "MY_TEST_OUT",
    )
  ```
"""
