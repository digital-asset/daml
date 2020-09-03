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

Download the Ledger API Test Tool from `Maven <api-test-tool_>`_
and save it as ``ledger-api-test-tool.jar`` in your current directory.

Running the tool against a custom Ledger API endpoint
=====================================================

Run this command to test your Ledger API endpoint exposed at host ``<host>`` and
at a port ``<port>``:

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar <host>:<port>

For example:

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar localhost:6865

The tool will upload the required DARs to the ledger, and then run all tests.

If any test embedded in the tool fails, it will print out details of the failure
for further debugging.

Exploring options the tool provides
===================================

Run the tool with ``--help`` flag to obtain the list of options the tool provides:

.. code-block:: console

   $ java -jar ledger-api-test-tool.jar --help

Selecting tests to run
^^^^^^^^^^^^^^^^^^^^^^

Running the tool without any argument runs only the *default tests*.

Those include all tests that are known to be safe to be run concurrently as part of a single run.

Tests that either change the global state of the ledger (e.g. configuration management) or are designed to stress the implementation need to be explicitly included using the available command line options.

Use the following command line flags to select which tests to run:

- ``--list``: print all available test suites to the console, shows if they are run by default
- ``--list-all``: print all available tests to the console, shows if they are run by default
- ``--include``: only run the tests that match the argument
- ``--exclude``: do not run the tests that match the argument
- ``--perf-tests``: list performance tests to run; cannot be combined with normal tests

Include and exclude are matched as prefixes, e.g. ``--exclude=SemanticTests``
will exclude all tests whose name starts with ``SemanticTests``. Test names
always start with their suite name followed by a colon, so the test suite
names shown by ``--list`` can be useful for coarse-grained inclusion/exclusion.

Both ``--include`` and ``--exclude`` (and ``--perf-tests``) can be specified
multiple times and/or provide comma-separated lists, i.e. all of these are
equivalent:

- ``--include=a,b,c``
- ``--include=a --include=b --include=c``
- ``--include=a,b --include=c``

The logic is always to first select included tests, then remove from that the
excluded ones, i.e. include directives never override a corresponding exclude
directive.

If no ``--include`` flag is given, all of the tests are included. You
cannot run performance and non-performance tests in the same invocation.
``--exclude`` is ignored when running performance tests, and the program will
stop if it detects that both ``--perf-tests`` and ``--include`` have been specified.

Examples (hitting a single participant at ``localhost:6865``):

.. code-block:: console
   :caption: Only run ``TestA``

   $ java -jar ledger-api-test-tool.jar --include TestA localhost:6865

.. code-block:: console
   :caption: Run all tests, but not ``TestB``

   $ java -jar ledger-api-test-tool.jar --exclude TestB localhost:6865

.. code-block:: console
   :caption: Run all tests

   $ java -jar ledger-api-test-tool.jar localhost:6865

.. code-block:: console
   :caption: Run all tests, but not ``TestC``

   $ java -jar ledger-api-test-tool.jar --exclude TestC

Performance tests
^^^^^^^^^^^^^^^^^

The available performance tests allow to establish the "performance envelope"
of the ledger under test (a term `borrowed from aeronautics <https://en.wikipedia.org/wiki/Flight_envelope>`__),
which offers an indication of the amount of the parameters under which a
ledger implementation is supposed to perform.

Those tests include tail latency, throughput and maximum size of a single
transaction. You can run the tool with the ``--list`` option to see a list
of available test suites that includes individual performance envelope test
cases. You can mix and match those tests to produce a test suite tailored
to match the expected performance envelope of a given ledger implementation
using a specific hardware setup.

For example, the following will verify that the ledger under test can
have a tail latency of one second when processing twenty pings, perform
twenty pings per seconds and being able to process a transaction one
megabyte in size:

.. code-block:: console

    $ java -jar ledger-api-test-tool.jar \
      --perf-tests=PerformanceEnvelope.Latency.1000ms \
      --perf-tests=PerformanceEnvelope.Throughput.TwentyOPS \
      --perf-tests=PerformanceEnvelope.TransactionSize.1000KB \
      localhost:6865

.. note::

  A "ping" is a collective name for two templates used to evaluate
  the performance envelope. Each of the two templates, "Ping" and
  "Pong", have a single choice allowing the controller to create
  an instance of the complementary template, directed to the
  original sender.

The test run will also produce a short summary of statistics which is
printed to standard output by default but that can be written to a
specific file path using the ``--perf-tests-report`` command line option.

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

Accomodating different ledger clock intervals
=============================================

Use the command line option ``--ledger-clock-granularity`` to indicate the maximum
 interval at which the ledger's clock will increment.

- If running on a ledger where ledger time increments in a time period greater than 10s,
  set ``--ledger-clock-granularity`` to a value higher than 10000 (10,000ms).  Tests
  that are sensitive to the ledger clock will then wait for a corresponding longer period
  of time to ensure completion of operations, avoiding timeouts and premature failures.
  The command deduplication test suite is particularly sensitive to this value.

Verbose output
==============

Use the command line option ``--verbose`` to print full stack traces on failures.

Concurrent test runs
====================

To minimize concurrent runs of tests, ``--concurrent-test-runs`` can be set to 1 or 2.
The default value is the number of processors available.

Note that certain tests, known to be possibly interfering with others (e.g.
configuration management), are always run sequentially and as the last tests in a run.

Retired tests
=============

A few tests can be retired over time as they could be deemed not providing the
necessary signal to a developer or operator that an integration correctly
implements the DAML Ledger API. Those test will nominally be kept in the
test suite for a time to prevent unwanted breakages of existing CI pipelines.
They will however not be run and they will eventually be removed. You are
advised to remove any explicit reference to those tests while they are in
their deprecation period.

Retired tests are not listed when using ``--list`` or ``--list-all`` but can
be included in a run using ``--include``. In this case, nothing will be run
and the test report will mention that the test has been retired and skipped.
