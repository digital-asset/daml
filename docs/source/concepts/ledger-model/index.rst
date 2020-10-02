.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _da-ledgers:

DAML Ledger Model
=================

DAML Ledgers enable multi-party workflows by providing
parties with a virtual *shared ledger*, which encodes the current
state of their shared contracts, written in DAML. At a high level, the interactions are visualized as
follows:

.. https://www.lucidchart.com/documents/edit/505709a9-e972-4272-b1fd-c01674c323b8
.. image:: ./images/da-ledger-model.svg

The DAML ledger model defines:

  #. what the ledger looks like - the structure of DAML ledgers
  #. who can request which changes - the integrity model for DAML ledgers
  #. who sees which changes and data - the privacy model for DAML ledgers

The below sections review these concepts of the ledger model in turn.
They also briefly describe the link between DAML and the model.

.. toctree::
   :maxdepth: 3

   ledger-structure
   ledger-integrity
   ledger-privacy
   ledger-daml
