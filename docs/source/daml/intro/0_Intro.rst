.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

An introduction to DAML
=======================

DAML is a smart contract language designed to build composable applications on an abstract :ref:`da-ledgers`.

In this introduction, you will learn about the structure of a DAML Ledger, and how to write DAML applications that run on any DAML Ledger implementation, by building an asset-holding and -trading application. You will gain an overview over most important language features, how they relate to the :ref:`da-ledgers` and how to use DAML Connect's developer tools to write, test, compile, package and ship your application.

This introduction is structured such that each section presents a new self-contained application with more functionality than that from the previous section. You can find the DAML code for each section `here <https://github.com/digital-asset/daml/tree/master/docs/source/daml/intro/daml>`_ or download them using the DAML assistant. For example, to load the sources for section 1 into a folder called ``1_Token``, run ``daml new 1_Token --template daml-intro-1``.

Prerequisites:

- You have installed the :doc:`DAML Connect SDK </getting-started/installation>`

Next: :doc:`1_Token`.

.. toctree::
  :maxdepth: 2
  :hidden:

  1_Token
  2_DamlScript
  3_Data
  4_Transformations
  5_Restrictions
  6_Parties
  7_Composing
  8_Dependencies
  9_Functional101
  10_StdLib
  11_Testing
  