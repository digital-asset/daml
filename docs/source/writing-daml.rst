.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Writing Daml
=============

Daml is a smart contract language designed to build composable applications on the `Daml Ledger Model <https://docs.daml.com/concepts/ledger-model/index.html#da-ledgers>`_.

The Writing Daml section will teach you how to write Daml applications that run on any Daml Ledger implementation, including key language features, how they relate to the Daml Ledger Model and how to use Daml’s developer tools. It also covers the structure of a Daml Ledger as it pertains to designing your application.

You can find the Daml code for the example application and features in each section `here <https://github.com/digital-asset/daml/tree/main/docs/source/daml/intro/daml>`_ or download it using the Daml assistant. For example, to load the sources for section 1 into a folder called ``intro1``, run ``daml new intro1 --template daml-intro-1``.

To run the examples, you will first need to `install the Daml SDK <https://docs.daml.com/getting-started/installation.html>`_.

.. toctree::
  :maxdepth: 2

  daml/intro/0_Intro
  daml/reference/index
  daml/stdlib/index
  daml/patterns
