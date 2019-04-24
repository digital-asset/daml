.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Ledger API Test Tool
####################

The Ledger API Test Tool is a command line tool for testing the correctness of
implementations of the :doc:`Ledger API
</app-dev/ledger-api-introduction/index>`, i.e. DAML ledgers. For example, it
will show you if there is consistency or conformance problem with your
implementation.

Its intended audience are developers of DAML ledgers, who are using the
:doc:`DAML Ledger Implementation Kit </ledger-integration-kit/index>` to develop
a DAML ledger on top of their distributed-ledger or database of choice.

Use this tool to verify if your Ledger API endpoint conforms to the :doc:`DA
Ledger Model </concepts/ledger-model/index>`.

Please note that currently the tool is in ALPHA status:

- ALPHA: contains a subset of the tests we use internally to test our ledger
  implementations.
- BETA: will be reached once we extended the coverage to include the full set of
  our test as per <https://github.com/digital-asset/daml/issues/146>.

Downloading the tool
====================

TODO: To be defined in <https://github.com/digital-asset/daml/issues/142>.

Extracting ``.dar`` file required to run the tests
======================================================

Before you can run the Ledger API test tool on your ledger, you need to load a
specific set of DAML templates onto your ledger.

#. To obtain the corresponding ``.dar`` file, run:

   ``ledger-api-test-tool -x``

   This creates a file ``SemanticTests.dar`` in the current directory.

#. Load ``SemanticTests.dar`` into your Ledger.

Running the tool against a custom Ledger API endpoint
=====================================================

Run this command to test your Ledger API endpoint exposed at host ``<host>`` and
at a port ``<port>``:

    ``ledger-api-test-tool -h <host> -p <port>``

For example

    ``ledger-api-test-tool -h localhost -p 6865``

Exploring options the tool provides
===================================

Run the tool with ``--help`` flag to obtain the list of options the tool provides:

   .. code-block:: console

     $ ledger-api-test-tool --help

Try out the Ledger API Test Tool against DAML Sandbox
=====================================================

If you wanted to test out the tool, you can run it against :doc:`DAML Sandbox
</tools/sandbox>`. To do this:

   .. code-block:: console

     $ ledger-api-test-tool -x
     $ da sandbox -- SemanticTests.dar
     $ ledger-api-test-tool

This should always succeed, as the Sandbox is tested to correctly implement the
Ledger API. This is useful if you do not have yet a custom Ledger API endpoint.

You don't need to supply the hosts and ports arguments, because the Ledger API
Test Tool defaults to using ``localhost:6865``, which the Sandbox uses by
default.

Testing your tool from continuous integration pipelines
=======================================================

To test your ledger in a CI pipeline, run it as part of your pipeline:

   .. code-block:: console

     $ ledger-api-test-tool 2>&1 /dev/null
     $ echo $?
     0

The tool is tailored to be used in CI pipelines: as customary, when the tests
succeed, it will produce minimal output and return the success exit code.

Using the tool with a known-to-be-faulty Ledger API implementation
==================================================================

Use flag ``--must-fail`` if you expect one or more or the scenario tests to
fail. If enabled, the tool will succeed when at least one test fails, and it
will fail when all tests succeed:

    ``ledger-api-test-tool --must-fail -h localhost -p 6865``

This is useful during development of a DAML ledger implementation, when tool
needs to be used against a known-to-be-faulty implementation (e.g. in CI).

We used this flag during tool development to ensure that the tool does not
always return success.
