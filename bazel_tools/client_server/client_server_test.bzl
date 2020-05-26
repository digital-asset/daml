# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _expand_args(ctx, args):
    return " ".join([ctx.expand_location(a, ctx.attr.data).replace("'", "'\\''") for a in args])

def _client_server_test_impl(ctx):
    # Construct wrapper to execute the runner, which in turn
    # will start the client and server.
    wrapper = ctx.actions.declare_file(ctx.label.name + "_wrapper.sh")
    ctx.actions.write(
        output = wrapper,
        content = """#!/usr/bin/env bash
set -eou pipefail
canonicalize_rlocation() {{
  # Note (MK): This is a fun one: Let’s say $TEST_WORKSPACE is "compatibility"
  # and the argument points to a target from an external workspace, e.g.,
  # @daml-sdk-0.0.0//:daml. Then the short path will point to
  # ../daml-sdk-0.0.0/daml. Putting things together we end up with
  # compatibility/../daml-sdk-0.0.0/daml. On Linux and MacOS this works
  # just fine. However, on windows we need to normalize the path
  # or rlocation will fail to find the path in the manifest file.
  rlocation $(realpath -L -s -m --relative-to=$PWD $TEST_WORKSPACE/$1)
}}

runner=$(canonicalize_rlocation "{runner}")
runner_args="{runner_args}"
client=$(canonicalize_rlocation "{client}")
server=$(canonicalize_rlocation "{server}")
server_args="{server_args}"
for file in {server_files}; do
    server_args+=" $(canonicalize_rlocation $file)"
done

client_args="$@"
if [ -z "$client_args" ]; then
    client_args="{client_args}"
    for file in {client_files}; do
        client_args+=" $(canonicalize_rlocation $file)"
    done
fi

$runner $client "$client_args" $server "$server_args" "$runner_args"
""".format(
            runner = ctx.executable.runner.short_path,
            runner_args = _expand_args(ctx, ctx.attr.runner_args),
            client = ctx.executable.client.short_path,
            client_args = _expand_args(ctx, ctx.attr.client_args),
            client_files = _expand_args(ctx, ctx.attr.client_files),
            server = ctx.executable.server.short_path,
            server_args = _expand_args(ctx, ctx.attr.server_args),
            server_files = _expand_args(ctx, ctx.attr.server_files),
        ),
        is_executable = True,
    )

    runfiles = ctx.runfiles(files = [wrapper], collect_data = True)
    runfiles = runfiles.merge(ctx.attr.runner[DefaultInfo].default_runfiles)
    runfiles = runfiles.merge(ctx.attr.client[DefaultInfo].default_runfiles)
    runfiles = runfiles.merge(ctx.attr.server[DefaultInfo].default_runfiles)

    return DefaultInfo(
        executable = wrapper,
        files = depset([wrapper]),
        runfiles = runfiles,
    )

client_server_test = rule(
    implementation = _client_server_test_impl,
    test = True,
    executable = True,
    attrs = {
        "runner": attr.label(
            cfg = "host",
            allow_single_file = True,
            executable = True,
            default = Label("@//bazel_tools/client_server/runner:runner"),
        ),
        "runner_args": attr.string_list(),
        "client": attr.label(
            cfg = "target",
            executable = True,
        ),
        "client_args": attr.string_list(),
        "client_files": attr.string_list(),
        "server": attr.label(
            cfg = "target",
            executable = True,
        ),
        "server_args": attr.string_list(),
        "server_files": attr.string_list(),
        "data": attr.label_list(allow_files = True),
    },
)
"""Create a client-server test.

The rule takes a client and server executables and their
arguments as parameters. The server port is passed via a
temporary file, which is passed to the server executable via the
"--port-file" parameter. This file is parsed and the port number
is passed to the client application via the "--target-port" argument.

The server process is killed after the client process exits.

The client and server executables can be any Bazel target that
is executable, e.g. scala_binary, sh_binary, etc.

The client and server files must be valid arguments to rlocation, as
can be obtained using $(rootpath ...) or $(rootpaths ...). (See
https://docs.bazel.build/versions/master/be/make-variables.html#predefined_label_variables.)
Once expended using rlocation, those are simply appended to client
and server arguments, respectively.

Example:
  ```bzl
  client_server_test(
    name = "my_test",
    runner_args = [],
    client = ":my_client",
    client_args = ["--extra-argument"],
    client_files = ["$(rootpath :target-for-client)"]
    server = ":my_server",
    server_args = ["--fast"],
    server_files = ["$(rootpath :target-for-client)"]
  )
  ```
"""
