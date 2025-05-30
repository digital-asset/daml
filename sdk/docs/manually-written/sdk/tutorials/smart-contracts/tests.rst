.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Test Daml contracts
===================

This chapter is about testing and debugging the Daml contracts you've built using the tools from earlier chapters.
You've already met Daml Script as a way of testing your code inside the IDE.
In this chapter you'll learn about more ways to run Daml Scripts, as well as other tools you can use for testing and debugging.
Specifically we will cover:

- Running Daml Scripts to test and debug templates
- The ``trace`` and ``debug`` functions
- The choice coverage

Note that this section only covers testing your Daml contracts.

.. hint::

  Remember that you can load all the code for this section into a folder called ``intro12`` by running ``daml new intro12 --template daml-intro-12``

Multi-package project structure
-------------------------------

In :ref:`project-structures` you learned that it is important to keep the scripts and templates in separate packages.
The project for this section is a multi-package build, containing all the code from :ref:`compose` and :ref:`dependencies`.
The folder structure looks like this:

.. code-block:: none

  .
  ├── asset
  ├── asset-tests
  ├── multi-trade
  ├── multi-trade-tests
  └── multi-package.yaml

``asset``, ``asset-tests``, ``multi-trade`` and ``multi-trade-tests`` are packages.
They each contain there own ``daml.yaml`` file.
``asset`` contains the code for the ``Asset`` and ``Trade`` templates.
The scripts for testing ``Asset`` and ``Trade`` are in the ``asset-tests`` package, which depends on ``asset``.
Contrary to ``asset-tests``, ``asset`` is meant to be uploaded to a Canton ledger.

Similarly ``multi-trade`` contains the ``MultiTrade`` template, and ``multi-trade-tests`` contains the corresponding scripts.

``multi-package.yaml`` is the multi-package configuration.
It allows the Daml Assistant to orchestrate the build of each sub-package.
It contains the list of sub-packages:

.. code-block:: yaml

  packages:
  - ./asset
  - ./asset-tests
  - ./multi-trade
  - ./multi-trade-tests

Run ``daml build --all`` at the root of the multi-package project, to build all sub-packages.
Or you can run ``daml build`` in any of the sub-package. It detects the dependencies, and build them in the correct order.
For instance, running ``daml build`` in ``multi-trade-tests`` triggers the build of ``asset``, ``multi-trade`` and ``multi-trade-tests`` in that order.

Test templates with Daml Scripts
--------------------------------

:ref:`Daml Script <daml-script>` is the main tool to test Daml contracts.
In a script, you can submit commands and queries from multiple parties on a fresh, initially empty, ledger.

.. TODO https://github.com/DACH-NY/docs-website/issues/398 to fix the broken ref
   `daml-script-ref` provides you with a detailed overview of all the commands and queries available in the Daml Script library.

There are two main ways to run a Daml Script:
- Click the ``Script results`` lens in VS Code: it provides the table and transaction views, which are useful for debugging.
- Run the ``daml test`` command of the Daml assistant, which is useful for regression checks and coverage.

Run a script in VS Code
~~~~~~~~~~~~~~~~~~~~~~~

In :ref:`test-using-scripts`, you learned how to write and run a Daml Script in VS Code.

As a refresher, find a script in the ``asset-tests`` or ``multi-trade-tests`` and click the ``Script results`` lens, that appears on top of the script name.
VS Code should open the table view in a side pane.
The table view describes the final state of the ledger at the end of the script.
It shows the list of active contracts, their data, and for each party, if it can see the contract or not.
Click the ``Show archived`` toggle to see the list of archived contracts.

In the side pane, click the ``Show transaction view`` button to switch to the transaction view.
It shows you all the transactions, and sub-transactions, executed by the script.
It also contain the failure message whenever a script fail.
Try to change the submitting party of an `exerciseCmd` and see if the script is still succeeding.

You can use the table and transaction views in VS Code to better understand the visibility of each contract, and authority of each party.

Run all scripts with the Daml Assistant
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Daml Assistant can run all the scripts inside a package.
This is useful for quick regression check, and their automation in the CI.

Open a terminal in the ``multi-trade-tests`` folder and run ``daml test``.
It should succeed and print the following test summary:

.. code-block:: none

  Test Summary

  daml/Intro/Asset/MultiTradeTests.daml:testMultiTrade: ok, 12 active contracts, 28 transactions.
  daml/Intro/Asset/TradeSetup.daml:setupRoles: ok, 2 active contracts, 4 transactions.
  daml/Intro/Asset/TradeSetup.daml:test_issuance: ok, 3 active contracts, 5 transactions.
  daml/Intro/Asset/TradeSetup.daml:tradeSetup: ok, 6 active contracts, 10 transactions.
  Modules internal to this package:
  - Internal templates
    0 defined
    0 (100.0%) created
  - Internal template choices
    0 defined
    0 (100.0%) exercised
  - Internal interface implementations
    0 defined
      0 internal interfaces
      0 external interfaces
  - Internal interface choices
    0 defined
    0 (100.0%) exercised
  Modules external to this package:
  - External templates
    7 defined
    5 ( 71.4%) created in any tests
    5 ( 71.4%) created in internal tests
    0 (  0.0%) created in external tests
  - External template choices
    27 defined
    7 ( 25.9%) exercised in any tests
    7 ( 25.9%) exercised in internal tests
    0 (  0.0%) exercised in external tests
  - External interface implementations
    0 defined
  - External interface choices
    0 defined
    0 (100.0%) exercised in any tests
    0 (100.0%) exercised in internal tests
    0 (100.0%) exercised in external tests

The first part of the summary is a list of each executed script.
For each script, you can see the total number of transactions, and active contracts at the end of the script.
For instance the ``testMultiTrade`` executed 28 transactions, to create 12 active contracts.

The second part of the summary is the coverage report.
It shows you how many templates and choices are tested by the complete set of scripts in the package, in proportion of the total number of templates and choices.

.. TODO: https://github.com/DACH-NY/docs-website/issues/406 to fix the brokenref

To learn more about Daml test coverage, read the :brokenref:`how-to-daml-test-coverage`.


Debug, trace, and stacktraces
-----------------------------

The above demonstrates nicely how to test the happy path, but what if a function doesn't behave as you expected? Daml has two functions that allow you to do fine-grained printf debugging: ``debug`` and ``trace``. Both allow you to print something to StdOut if the code is reached. The difference between ``debug`` and ``trace`` is similar to the relationship between ``abort`` and ``error``:

- ``debug : Text -> m ()`` maps a text to an Action that has the side-effect of printing to StdOut.
- ``trace : Text -> a -> a`` prints to StdOut when the expression is evaluated.

.. code-block:: none

  daml> let a : Script () = debug "foo"
  daml> let b : Script () = trace "bar" (debug "baz")
  [Daml.Script:378]: "bar"
  daml> a
  [DA.Internal.Prelude:532]: "foo"
  daml> b
  [DA.Internal.Prelude:532]: "baz"
  daml>

If in doubt, use ``debug``. It's the easier of the two to interpret the results of.

The thing in the square brackets is the last location. It'll tell you the Daml file and line number that triggered the printing, but often no more than that because full stacktraces could violate subtransaction privacy quite easily. If you want to enable stacktraces for some purely functional code in your modules, you can use the machinery in :ref:`module-da-stack-24914` to do so, but we won't cover that any further here.
