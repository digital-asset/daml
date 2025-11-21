# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools/sh:sh.bzl", "sh_inline_test")

def _escape_args(args):
    return " ".join([
        a.replace("'", "'\\''")
        for a in args
    ])

def client_server_test(
        name,
        runner = "//bazel_tools/client_server/runner_with_port_file",
        runner_args = [],
        runner_files = [],
        runner_files_prefix = "",
        client = None,
        client_args = [],
        client_files = [],
        server = None,
        server_args = [],
        server_files = [],
        server_files_prefix = "",
        data = [],
        **kwargs):
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
    Once expanded using rlocation, those are simply appended to client
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
    sh_inline_test(
        name = name,
        # Deduplicate in case any of runner, client, server are identical.
        data = depset([runner, client, server]).to_list() + data,
        cmd = """\
runner=$$(canonicalize_rlocation $$(get_exe $(rootpaths {runner})))
runner_args="{runner_args}"
for file in {runner_files}; do
    runner_args+=" {runner_files_prefix}$$(canonicalize_rlocation $$file)"
done
client=$$(canonicalize_rlocation $$(get_exe $(rootpaths {client})))
server=$$(canonicalize_rlocation $$(get_exe $(rootpaths {server})))
server_args="{server_args}"
for file in {server_files}; do
    server_args+=" {server_files_prefix}$$(canonicalize_rlocation $$file)"
done

client_args="$$@"
if [ -z "$$client_args" ]; then
    client_args="{client_args}"
    for file in {client_files}; do
        client_args+=" $$(canonicalize_rlocation $$file)"
    done
fi

$$runner $$client "$$client_args" $$server "$$server_args" "$$runner_args"
""".format(
            runner = runner,
            runner_args = _escape_args(runner_args),
            runner_files = _escape_args(runner_files),
            runner_files_prefix = runner_files_prefix,
            client = client,
            client_args = _escape_args(client_args),
            client_files = _escape_args(client_files),
            server = server,
            server_args = _escape_args(server_args),
            server_files = _escape_args(server_files),
            server_files_prefix = server_files_prefix,
        ),
        **kwargs
    )

def client_server_test_canton_sh(
    name, data, src,
    additional_canton_args = [],
    server_files = [],
    tags = [],
):
    native.genrule(
        name = name + "-client-sh",
        outs = [name + "-client.sh"],
        tools = data,
        cmd = """\
cat >$(OUTS) <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
canonicalize_rlocation() {{
  # Note (MK): This is a fun one: Let's say $$TEST_WORKSPACE is "compatibility"
  # and the argument points to a target from an external workspace, e.g.,
  # @daml-sdk-0.0.0//:daml. Then the short path will point to
  # ../daml-sdk-0.0.0/daml. Putting things together we end up with
  # compatibility/../daml-sdk-0.0.0/daml. On Linux and MacOS this works
  # just fine. However, on windows we need to normalize the path
  # or rlocation will fail to find the path in the manifest file.
  rlocation $$(realpath -L -s -m --relative-to=$$PWD $$TEST_WORKSPACE/$$1)
}}

get_exe() {{
  if [[ %os% = windows ]]; then
    for arg in "$$@"; do
      if [[ $$arg = *.exe ]]; then
        echo "$$arg"
        return
      fi
    done
    echo "$$1"
  else
    echo "$$1"
  fi
}}

trap 'status=$$?; kill -TERM $$PID; wait $$PID; exit $$status' INT TERM

timeout=60
while [ ! -e _port_file ]; do
    if [ "$$timeout" = 0 ]; then
        echo "Timed out waiting for Canton startup" >&2
        exit 1
    fi
    sleep 1
    timeout=$$((timeout - 1))
done

{src}
EOF
chmod +x $(OUTS)
""".format(src = src),
    )

    native.sh_binary(
        name = name + "-client",
        srcs = [name + "-client.sh"],
        data = data,
    )

    server = "@daml-sdk-0.0.0//:daml"
    server_args = ["sandbox", "--canton-port-file", "_port_file"] + additional_canton_args
    server_files_prefix = "--dar="

    client_server_test(
        name = name,
        client = "{}-client".format(name),
        client_args = [],
        client_files = [],
        data = [],
        runner = "//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = server,
        server_args = server_args,
        server_files = server_files,
        server_files_prefix = server_files_prefix,
        tags = tags + ["exclusive"],
    )
