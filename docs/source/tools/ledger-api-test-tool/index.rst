.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _ledger-api-test-tool:

Ledger API Test Tool
####################

The Ledger API Test Tool is a command line tool for testing the correctness of
ledger implementations based on DAML and the :doc:`Ledger API
</app-dev/ledger-api-introduction/index>`. It’s useful for ledger implementation
developers, who are using the DAML Ledger Implementation Kit to develop a DAML
Ledger on top of their distributed-ledger or database of choice.

Use this tool to verify if the Ledger API endpoint conforms to the :doc:`DA
Ledger Model </concepts/ledger-model/index>`.

.. contents:: On this page:
  :local:

Downloading the tool
====================

TODO

Extracting :code:`.dar` file required to run the tests
======================================================

Before you can run the Ledger API test tool on your ledger, you need to load a
specific set of DAML templates onto your ledger.

#. Obtain the corresponding :code:`.dar` file with:

   ``ledger-api-test-tool -x``

#. Load the file :code:`SemanticTests.dar` created in the current directory into your
   Ledger.

Running the tool against a custom Ledger API endpoint
=====================================================

Run this command to test your Ledger API endpoint exposed at host :code:`<host>` and
at a port :code:`<port>`:

    ``ledger-api-test-tool -h <host> -p <port>``

For example

    ``ledger-api-test-tool -h localhost -p 6865``

Exploring options the tool provides
===================================

Run the tool with :code:`--help` flag to obtain the list of options the tool provides:

   .. code-block:: console

     $ ledger-api-test-tool --help

     The Ledger API Test Tool is a command line tool for testing the correctness of
     ledger implementations based on DAML and Ledger API.
     Usage: ledger-api-test-tool [options]

       --help                   prints this usage text
       -p, --target-port <value>
                                Ledger API server port. Defaults to 6865.
       -h, --host <value>       Ledger API server host. Defaults to localhost.
       --must-fail              One or more of the scenario tests must fail. Defaults to false.
       -r, --reset              Perform a ledger reset before running the tests. Defaults to false.
       -x, --extract            Extract the testing archive files and exit.

Try out the Ledger API Test Tool against DAML Sandbox
=====================================================

To run the tool against [DAML
Sandbox](https://docs.daml.com/tools/sandbox.html), run:

   .. code-block:: console

     ledger-api-test-tool -x
     da sandbox -- SemanticTests.dar
     ledger-api-test-tool

This should always succeed! This is useful if you do not have yet a custom
Ledger API endpoint.

The DAML Sandbox starts at by default at :code:`localhost:6865`
and the Ledger API Test Tool uses this as the default endpoint to test, hence
hosts and ports command line arguments can be omitted.

Testing your tool from continuous integration pipelines
=======================================================

To test your ledger in your CI pipelines, execute it as part of your pipeline:


   .. code-block:: console

     $ ledger-api-test-tool
     Running 10 scenarios against localhost:6865...
     Testing scenario: Test:timeTravel
     Testing scenario: Test:authorization_rule1_failure
     Testing scenario: Test:authorization_success2_delegation
     Testing scenario: Test:authorization_rule2_failure
     Testing scenario: Test:startsAtEpoch
     Testing scenario: Test:authorization_success1
     Testing scenario: Test:privacy_projections1
     Testing scenario: Test:consistency_doublespend1
     Testing scenario: Test:consistency_doublespend3
     Testing scenario: Test:consistency_doublespend2
     All scenarios completed.
     $ echo $?
     0

The tool is tailored to be used in CI pipelines: as customary, when the tests
succeed, it will produce minimal output and return the success exit code.

Using the tool with a known-to-be-faulty Ledger API implementation
==================================================================

To force the tool to always return success exit code, use :code:`--must-fail` flag:

    ``ledger-api-test-tool --must-fail -h localhost -p 6865``

This is useful during development of a DAML Ledger implementation, when tool
needs to be used against a known-to-be-faulty implementation (e.g. in CI).

