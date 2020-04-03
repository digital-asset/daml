.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Ledger API Test Tool
####################

The Ledger API Test Tool is a command line tool for testing the correctness of
implementations of the :doc:`Ledger API
</app-dev/ledger-api>`, i.e. DAML ledgers. For example, it
will show you if there are consistency or conformance problem with your
implementation.

Its intended audience are developers of DAML ledgers, who are using the
DAML Ledger Implementation Kit to develop
a DAML ledger on top of their distributed-ledger or database of choice.

Use this tool to verify if your Ledger API endpoint conforms to the :doc:`DA
Ledger Model </concepts/ledger-model/index>`.

Downloading the tool
====================

Download the Ledger API Test Tool from :ledger-api-test-tool-maven:`Maven <ledger-api-test-tool>`
and save it as ``ledger-api-test-tool.jar`` in your current directory.

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

   $ java -jar ledger-api-test-tool.jar <host>:<port>

For example

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar localhost:6865

If any test embedded in the tool fails, it will print out details of the failure
for further debugging.

Exploring options the tool provides
===================================

Run the tool with ``--help`` flag to obtain the list of options the tool provides:

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar --help

Selecting tests to run
~~~~~~~~~~~~~~~~~~~~~~

Running the tool without any argument runs only the *default tests*.

Those include all tests that are known to be safe to be run concurrently as part of a single run.

Tests that either change the global state of the ledger (e.g. configuration management) or are designed to stress the implementation need to be explicitly included using the available command line options.

Use the following command line flags to select which tests to run:

- ``--list``: print all available tests to the console, shows if they are run by default
- ``--include``: only run the tests provided as argument
- ``--exclude``: do not run the tests provided as argument
- ``--all-tests``: run all default and optional tests. This flag can be combined with the ``--exclude`` flag.

Examples (hitting a single participant at ``localhost:6865``):

.. code-block:: console
   :caption: Only run ``TestA``

   $ java -jar ledger-api-test-tool.jar --include TestA localhost:6865

.. code-block:: console
   :caption: Run all default tests, but not ``TestB``

   $ java -jar ledger-api-test-tool.jar --exclude TestB localhost:6865

.. code-block:: console
   :caption: Run all tests

   $ java -jar ledger-api-test-tool.jar --all-tests localhost:6865

.. code-block:: console
   :caption: Run all tests, but not ``TestC``

   $ java -jar ledger-api-test-tool.jar --all-tests --exclude TestC


Try out the Ledger API Test Tool against DAML Sandbox
=====================================================

If you wanted to test out the tool, you can run it against :doc:`DAML Sandbox
</tools/sandbox>`. To do this:

   .. code-block:: console

     $ java -jar ledger-api-test-tool.jar --extract
     $ daml sandbox *.dar
     $ java -jar ledger-api-test-tool.jar localhost:6865

This should always succeed, as the Sandbox is tested to correctly implement the
Ledger API. This is useful if you do not have yet a custom Ledger API endpoint.

Using the tool with a known-to-be-faulty Ledger API implementation
==================================================================

Use flag ``--must-fail`` if you expect one or more or the scenario tests to
fail. If enabled, the tool will return the success exit code when at least one
test fails, and it will return a failure exit code when all tests succeed:

    ``java -jar ledger-api-test-tool.jar --must-fail localhost:6865``

This is useful during development of a DAML ledger implementation, when tool
needs to be used against a known-to-be-faulty implementation (e.g. in CI). It
will still print information about failed tests.

Tuning the testing behaviour of the tool
========================================

Use the command line option ``--timeout-scale-factor`` to tune timeouts applied
  by the tool.

- Set ``--timeout-scale-factor`` to a floating point value higher than 1.0 to make
  the tool wait longer for expected events coming from the DAML ledger
  implementation under test. Conversely use values smaller than 1.0 to make it
  wait shorter.

Verbose output
==============

Use the command line option ``--verbose`` to print full stack traces on failures.

Concurrent test runs
====================

To minimize parallelized runs of tests, ``--concurrent-test-runs`` can be set to 1 or 2.
The default value is the number of processors available.

