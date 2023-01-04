.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _intro-to-daml:

An Introduction to Daml
=======================

Daml is a smart contract language designed to build composable applications on an abstract :ref:`da-ledgers`.

In this introduction, you will learn about the structure of a Daml Ledger, and how to write Daml applications that run on any Daml Ledger implementation, by building an asset-holding and -trading application. You will gain an overview over most important language features, how they relate to the :ref:`da-ledgers` and how to use Daml's developer tools to write, test, compile, package and ship your application.

This introduction is structured such that each section presents a new self-contained application with more functionality than that from the previous section. You can find the Daml code for each section `here <https://github.com/digital-asset/daml/tree/main/docs/source/daml/intro/daml>`_ or download them using the Daml assistant. For example, to load the sources for section 1 into a folder called ``intro1``, run ``daml new intro1 --template daml-intro-1``.

Prerequisites:

- You have installed the :doc:`Daml SDK </getting-started/installation>`

Next: :doc:`1_Token`.
