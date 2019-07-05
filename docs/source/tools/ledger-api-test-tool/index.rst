.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Ledger API Test Tool
####################

The Ledger API Test Tool is a command line tool for testing the correctness of
implementations of the :doc:`Ledger API
</app-dev/index>`, i.e. DAML ledgers. For example, it
will show you if there are consistency or conformance problem with your
implementation.

Its intended audience are developers of DAML ledgers, who are using the
:doc:`DAML Ledger Implementation Kit </daml-integration-kit/index>` to develop
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

Run the following command to fetch the tool:

.. code-block:: shell

     curl -L 'https://bintray.com/api/v1/content/digitalassetsdk/DigitalAssetSDK/com/daml/ledger/testtool/ledger-api-test-tool_2.12/$latest/ledger-api-test-tool_2.12-$latest.jar?bt_package=sdk-components' -o ledger-api-test-tool.jar

This will create a file ``ledger-api-test-tool.jar`` in your current directory.

Extracting ``.dar`` files required to run the tests
======================================================

Before you can run the Ledger API test tool on your ledger, you need to load a
specific set of DAML templates onto your ledger.

#. To obtain the corresponding ``.dar`` files, run:

   .. code-block:: console

     $ java -jar ledger-api-test-tool.jar --extract

   This writes all ``.dar`` files required for the tests into the current directory.

#. Load all ``.dar`` files into your Ledger.

Running the tool against a custom Ledger API endpoint
=====================================================

Run this command to test your Ledger API endpoint exposed at host ``<host>`` and
at a port ``<port>``:

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar  -h <host> -p <port>

For example

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar  -h localhost -p 6865

If any test embedded in the tool fails, it will print out details of the failure
for further debugging.

Exploring options the tool provides
===================================

Run the tool with ``--help`` flag to obtain the list of options the tool provides:

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar  --help

Selecting tests to run
~~~~~~~~~~~~~~~~~~~~~~

Running the tool without any arguments runs the *default tests*. Use the following command line flags to select which tests to run:

- ``--list``: print all available tests to the console
- ``--include``: only run the tests provided as argument
- ``--exclude``: do not run the tests provided as argument
- ``--all-tests``: run all default and optional tests. This flag can be combined with the ``--exclude`` flag.

Examples:

.. code-block:: console
   :caption: Only run ``TestA``

   $ java -jar ledger-api-test-tool.jar --include TestA

.. code-block:: console
   :caption: Run all default tests, but not ``TestB``

   $ java -jar ledger-api-test-tool.jar --exclude TestB

.. code-block:: console
   :caption: Run all tests

   $ java -jar ledger-api-test-tool.jar --all-tests

.. code-block:: console
   :caption: Run all tests, but not ``TestC``

   $ java -jar ledger-api-test-tool.jar --all-tests --exclude TestC


Try out the Ledger API Test Tool against DAML Sandbox
=====================================================

If you wanted to test out the tool, you can run it against :doc:`DAML Sandbox
</tools/sandbox>`. To do this:

   .. code-block:: console

     $ java -jar ledger-api-test-tool.jar --extract
     $ da sandbox -- *.dar
     $ java -jar ledger-api-test-tool.jar

This should always succeed, as the Sandbox is tested to correctly implement the
Ledger API. This is useful if you do not have yet a custom Ledger API endpoint.

You don't need to supply the hosts and ports arguments, because the Ledger API
Test Tool defaults to using ``localhost:6865``, which the Sandbox uses by
default.

Testing a DAML Ledger servced by multiple Ledger API endpoints
==============================================================

Note: Only a subset of tests supports this mode of operation. Refer to
``--help`` for details.

To test your ledger, which handles different parties at different Ledger API endpoints:

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar --mapping:Alice=serverA:6865 --mapping:Bob=serverB:6865 -h localhost -p 6865

This will route commands related to party ``Alice`` to endpoint ``serverA:6865``
and commands related to party ``Bob`` to endpoint ``serverB:6865``. For other
parties and for tests which do not make use of this capability the endpoint
defined by ``-h`` and ``-p`` arguments will be used, which is ``localhost:6865``
in this example.

Testing your tool from continuous integration pipelines
=======================================================

To test your ledger in a CI pipeline, run it as part of your pipeline:

   .. code-block:: console

     $ java -jar ledger-api-test-tool 2>&1 /dev/null
     $ echo $?
     0

The tool is tailored to be used in CI pipelines: as customary, when the tests
succeed, it will produce minimal output and return the success exit code.

Using the tool with a known-to-be-faulty Ledger API implementation
==================================================================

Use flag ``--must-fail`` if you expect one or more or the scenario tests to
fail. If enabled, the tool will return the success exit code when at least one
test fails, and it will return a failure exit code when all tests succeed:

    ``java -jar ledger-api-test-tool.jar --must-fail -h localhost -p 6865``

This is useful during development of a DAML ledger implementation, when tool
needs to be used against a known-to-be-faulty implementation (e.g. in CI). It
will still print information about failed tests.

We used this flag during tool development to ensure that the tool does not
always return success.

Tuning the testing behaviour of the tool
========================================

Use the command line options ``--timeout-scale-factor`` and
``--command-submission-ttl-scale-factor`` to tune timeouts applied by the tool.

- Set ``--timeout-scale-factor`` to a floating point value higher than 1.0 to make
  the tool wait longer for expected events coming from the DAML ledger
  implementation under test. Conversely use values smaller than 1.0 to make it
  wait shorter.
- Set ``--command-submission-ttl-scale-factor`` to a value higher than 1.0 to
  make the test tool generate Ledger API Commands with higher than default
  maximum record time, which might be necessary for DAML ledger implementations
  which take a long time to commit a proposed transaction. Conversely use values
  smaller than 1.0 to make it give less time for a DAML ledger implementation to
  commit a proposed transaction.
