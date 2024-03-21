.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Ledger Export
#############

Export is currently an :doc:`Early Access Feature in Alpha status </support/status-definitions>`.

Introduction
************

Daml ledger exports read the transaction history or active contract set (ACS)
from the ledger and write it to disk encoded as a
:doc:`Daml Script </daml-script/index>` that will reproduce the ledger state
when executed. This can be useful to migrate the history or state of a ledger
from one network to another, or to replicate the ledger state locally for
testing or debugging purposes.

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
included in the export. Alternatively, you can set ``--all-parties`` to export
contracts seen by all known parties. Lack of visibility of certain events may
cause references to :ref:`unknown contract ids <export-unknown-cids>`.

The ``--output`` flag defines the directory prefix under which to generate the
Daml project that contains the Daml script that represents the ledger export.
The flag ``--sdk-version`` defines which Daml SDK version to configure in
the generated ``daml.yaml`` configuration file.

By default an export will reproduce all transactions in the ledger history. The
:ref:`ledger offsets <export-ledger-offsets>` section describes how to change
this behavior.

Output
******

Daml Script
===========

The generated Daml code in ``Export.daml`` contains the following top-level definitions:

``type Parties``
  A mapping from parties in the original ledger state to parties to be used in
  the new reconstructed ledger state.
``lookupParty : Text -> Parties -> Party``
  A helper function to look up parties in the ``Parties`` mapping.
``allocateParties : Script Parties``
  A Daml script that allocates fresh parties on the ledger and returns them in
  a ``Parties`` mapping.
``type Contracts``
  A mapping from unknown contract ids to replacement contract ids,
  see :ref:`unknown contract ids <export-unknown-cids>`.
``lookupContract : Text -> Contracts -> ContractId a``
  A helper function to look up unknown contract ids in the ``Contracts`` mapping.
``data Args``
  A record that holds all arguments to the export script.
``export : Args -> Script ()``
  The Daml ledger export encoded as a Daml script. Given the relevant arguments
  this script will reproduce the ledger state when executed. You can read this
  script to understand the exported ledger state or history, and you can modify
  this script for debugging or testing purposes.
``testExport : Script ()``
  A Daml script that will first invoke ``allocateParties`` and then ``export``.
  It will use an empty ``Contracts`` mapping. This can be useful to test the
  export in Daml studio. If your export references unknown contract ids then
  you may need to manually extend the ``Contracts`` mapping.

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

Arguments
=========

Daml export will generate a default arguments file in ``args.json``, which
configures the export to use the same party names as in the original ledger
state and to map unknown contract ids to themselves. For example:

.. code-block:: json

  {
    "contracts": {
      "001335..": "001335..."
    },
    "parties": {
      "Alice": "Alice",
      "Bob": "Bob"
    }
  }

.. TODO[AH] Add a full example project and example export.

Execute the Export
******************

The generated Daml project is configured such that ``daml start`` will execute
the Daml export with the default arguments defined in ``args.json``.
Alternatively you can build and execute the generated Daml script manually
using commands of the following form: ::

  daml build
  daml script --ledger-host localhost --ledger-port 6865 --dar .daml/dist/export-1.0.0.dar --script-name Export:export --input-file args.json

The arguments ``--ledger-host`` and ``--ledger-port`` configure the address of
the ledger and the argument ``--input-file`` points to a JSON file that defines
the export script's arguments.

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

.. _export-unknown-cids:

Unknown Contract Ids
********************

Daml ledger export may encounter references to unknown contracts. This may occur
if a contract was divulged to one of the configured parties, but the event that
initially created that contract is not visible to any of the configured
parties. This may also occur if a contract was archived before the configured
start offset, such that it is neither part of the recreated ACS nor created in
any of the exported transactions, and another live contract retains a reference
to this archived contract.

In such cases Daml export will not generate commands to recreate these unknown
contracts. Instead, it will generate a lookup in the ``Contracts`` mapping
defined in the scripts arguments. You can define a mapping from unknown
contract ids to replacement contract ids in the JSON input file. The default
``args.json`` generated by Daml ledger export will map unknown contract ids to
themselves.

Note that you may submit references to non-existing contract ids to the ledger
using this feature. A ``fetch`` on such a dangling contract id will fail.

Transaction Time
****************

Daml ledger exports may fail to reproduce the ledger state or transaction
history if contracts are sensitive to ledger time. You can enable the
``--set-time`` option to issue ``setTime`` commands in the generated Daml
script. However, this is not supported by all ledgers.

.. _export-caveats:

Caveats
*******

Contracts Created and Referenced In the Same Transaction
========================================================

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
