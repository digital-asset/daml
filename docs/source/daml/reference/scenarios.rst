.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: scenarios
####################

This page gives reference information on scenario syntax, used for testing templates:

.. contents:: :local:

For an introduction to scenarios, see :doc:`../testing-scenarios`.

.. _daml-ref-scenario:

Scenario keyword
****************

- ``scenario`` function. Introduces a series of transactions to be submitted to the ledger.

.. _daml-ref-commits:

Submit
******

- ``submit`` keyword.
- Submits an action (a create or an exercise) to the ledger.
- Takes two arguments, the party submitting followed by the expression, for example: ``submit bankOfEngland do create ...``

.. Changes in DAML 1.2.

.. _daml-ref-fails:

submitMustFail
**************

- ``submitMustFail`` keyword.
- Like ``submit``, but you're asserting it should fail.
- Takes two arguments, the party submitting followed by the expression by a party, for example: ``submitMustFail bankOfEngland do create ...``

Scenario body
*************

Updates
=======

- Usually :ref:`create <daml-ref-create>` and :ref:`exercise <daml-ref-exercise>`. But you can also use other updates, like :ref:`assert <daml-ref-assert>` and :ref:`fetch <daml-ref-fetch>`.
- Parties can only be named explicitly in scenarios.

Passing time
============

In a scenario, you may want time to pass so you can test something properly. You can do this with ``pass``.

Here's an example of passing time:

.. literalinclude:: ../code-snippets/Snippets.daml
   :language: daml
   :lines: 61-72

Binding variables
=================

As in choices, you can :ref:`bind to variables <daml-ref-binding-variables>`. Usually, you'd bind commits to variables in order to get the returned value (usually the contract).
