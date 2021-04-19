.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Ledger Export
#############

Export is currently an :doc:`Early Access Feature in Labs status </support/status-definitions>`.

Introduction
************

Daml ledger exports read the transaction history or active contract set (ACS)
from the ledger and write it to disk encoded as a :doc:`Daml Script
</daml-script/index>` that will reproduce the ledger state when executed. This
can be useful to migrate the history or state of a ledger from one network to
another, or to replicate the ledger state locally for testing or debugging
purposes.

Usage
*****

The command to generate a Daml ledger export has the following form. ::

  daml ledger export <format> <options>

Right now Daml script, specified as ``script``, is the only supported export
format. You can get an overview of the available command-line options using the
``--help`` flag as follows. ::

  daml ledger export script --help

A full example invocation looks like this: ::

  daml ledger export script --host localhost --port 6865 --party Alice --party Bob --output ../out --sdk-version 0.0.0

The flags ``--host`` and ``--port`` define the Daml ledger to connect to. You
can omit these flags if you are invoking the command from within a Daml project
with a running ledger, e.g. with a running ``daml start``.

The ``--party`` flags define which contracts will be included in the export. In
the above example only contracts visible to the parties Alice and Bob will be
included in the export. Lack of visibility of certain events may cause exports to fail, see :ref:`caveats <export-caveats>`.

The ``--output`` flag defines the directory prefix under which to generate the
Daml project that contains the Daml script that represents the ledger export.
The flag ``--sdk-version`` defines which Daml Connect version to configure in
the generated ``daml.yaml`` configuration file.

By default an export will reproduce all transactions in the ledger history. The
:ref:`ledger offsets <export-ledger-offsets>` section describes how to change
this behavior.

Output
******

The generated Daml code contains the following top-level definitions:

``data Parties``
  A record that holds the relevant Daml parties.
``allocateParties : Script Parties``
  A Daml script that allocates fresh parties on the ledger and returns them in
  the ``Parties`` record.
``export : Parties -> Script ()``
  The Daml ledger export encoded as a Daml script. Given the relevant parties
  this script will reproduce the ledger state when executed. You can read this
  script to understand the exported ledger state or history, and you can modify
  this script for debugging or testing purposes.
``testExport : Script ()``
  A Daml script that will first invoke ``allocateParties`` and then ``export``.

In most simple cases the generated Daml script will use the functions
``submit`` or ``submitMulti`` to issue ledger commands that reproduce a
transaction or ACS. In some cases the generated Daml script
will fall back to the more general functions ``submitTree`` or
``submitTreeMulti``.

For example, the following generated code issues a create-and-exercise command
that creates an instance of ``ContractA`` and exercises the choice ``ChoiceA``.
The function ``submitTree`` returns a ``TransactionTree`` object that captures
all contracts that are created in the transaction. The ``fromTree`` function is
then used to extract the contract ids of the ``ContractB`` contracts that were
created by ``ChoiceA``.

.. code-block:: daml

  tree <- submitTree alice_0 do
    createAndExerciseCmd
      Main.ContractA with
        owner = alice_0
      Main.ChoiceA
  let contractB_1_1 = fromTree tree $
        exercised @Main.ContractA "ChoiceA" $
        created @Main.ContractB
  let contractB_1_2 = fromTree tree $
        exercised @Main.ContractA "ChoiceA" $
        createdN @Main.ContractB 1

.. TODO[AH] Add a full example project and example export.

.. _export-ledger-offsets:

Ledger Offsets
**************

By default ``daml ledger export`` will reproduce all transactions, as seen by
the selected parties, from the beginning of the ledger history to the current
end. The command-line flags ``--start`` and ``-end`` can be used to change this
behavior. Both flags accept ledger offsets, either the special offsets
``start`` and ``end``, or an arbitrary ledger offset.

``--start``
  Transactions up to and including the start offset will be reproduced as a
  sequence of create commands that reproduce the ACS as of the start offset.
  Later transactions will be reproduced as seen by the configured parties. In
  particular, ``--start end`` will reproduce the current ACS but no transaction
  history, ``--start start`` (the default) will reproduce the history of all
  transactions as seen by the configured parties.
``--end``
  Export transactions up to and including this end offset.

.. TODO[AH] Provide a reference or hints how to obtain arbitrary ledger offsets.

.. _export-caveats:

Caveats
*******

Unknown Contract Ids
====================

Daml ledger export does currently not handle references to unknown contract
ids, but will just fail if any such references are encountered. This may occur
if a contract was divulged to one of the configured parties, but the event that
initially created that contract is not visible to any of the configured
parties. This may also occur if a contract was archived before the configured
start offset, such that it is neither part of the recreated ACS nor created in
any of the exported transactions.

Contracts Created and Referenced in Same Transaction
====================================================

Daml ledger export may fail in certain cases when it attempts to reproduce a
transaction that creates a contract and then references that contract within
the same transaction.

The Daml ledger API allows only a few ways in which a contract that was created
in a set of commands can be referenced within the same set of commands. Namely,
create-and-exercise and exercise-by-key. Choice implementations, on the other
hand, are not restricted in this way.

If the configured parties only see part of a given transaction tree, then
events that were originally emitted by a choice may be lifted to the root of
the transaction tree. This could produce a transaction tree that cannot be
replicated using the ledger API. In such cases Daml ledger export will fail.

Transaction Time
================

Daml ledger exports may fail to reproduce the ledger state or transaction
history if contracts are sensitive to ledger time. The current implementation
does not attempt to reproduce the history faithfully with regard to ledger
time. In future this will be implemented by optionally issuing ``setTime``
commands in the generated Daml script. However, this is not supported by all
ledgers.
