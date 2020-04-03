.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML REPL
###########

**WARNING:** DAML REPL is an early access feature that is actively
being designed and is *subject to breaking changes*.
We welcome feedback about the DAML REPL on
`our issue tracker <https://github.com/digital-asset/daml/issues/new>`_
or `on Slack <https://hub.daml.com/slack/>`_.

The DAML REPL allows you to use the :doc:`/daml-script/index` API
interactively. This is useful for debugging and for interactively
inspecting and manipulating a ledger.

Usage
=====

First create a new project based on the ``script-example``
template. Take a look at the documentation for
:doc:`/daml-script/index` for details on this template.

.. code-block:: sh

   daml new script-example script-example # create a project called script-example based on the template
   cd script-example # switch to the new project

Now, build the project and start :doc:`/tools/sandbox`, the in-memory
ledger included in the DAML SDK. Note that we are starting Sandbox in
wallclock mode. Static time is not supported in ``daml repl``.

.. code-block:: sh

   daml build
   daml sandbox --wall-clock-time --port=6865 .daml/dist/script-example-0.0.1.dar

Now that the ledger has been started, you can launch the REPL in a
separate terminal using the following command.

.. code-block:: sh

   daml repl --ledger-host=localhost --ledger-port=6865 .daml/dist/script-example-0.0.1.dar

The ``--ledger-host`` and ``--ledger-port`` parameters point to the
host and port your ledger is running on. In addition to that, you also
need to pass in the name of a DAR containing the templates and other
definitions that will be accessible in the REPL.

You should now see a prompt looking like

.. code-block:: none

   daml>

You can think of this prompt like a line in a ``do``-block of the
``Script`` action. Each line of input has to have one of the following
two forms:

1. An expression ``expr`` of type ``Script a`` for some type ``a``. This
   will execute the script ignoring the result.

2. A binding of the form ``pat <- expr`` where ``pat`` is pattern, e.g.,
   a variable name ``x`` to bind the result to
   and ``expr`` is an expression of type ``Script a``.
   This will execute the script and match the result against
   the pattern ``pat`` bindings the matches to the variables in the pattern.
   You can then use those variables on subsequent lines.

First create two parties: A party with the display name ``"Alice"``
and the party id ``"alice"`` and a party with the display name
``"Bob"`` and the party id ``"bob"``.

.. code-block:: none

   daml> alice <- allocatePartyWithHint "Alice" (PartyIdHint "alice")
   daml> bob <- allocatePartyWithHint "Bob" (PartyIdHint "bob")

Next, create a ``CoinProposal`` from ``Alice`` to ``Bob``

.. code-block:: none

   daml> submit alice (createCmd (CoinProposal (Coin alice bob)))

As Bob, you can now get the list of active ``CoinProposal`` contracts
using the ``query`` function. The ``debug : Show a => a -> Script ()``
function can be used to print values.

.. code-block:: none

   daml> proposals <- query @CoinProposal bob
   daml> debug proposals
   [Daml.Script:39]: [(<contract-id>,CoinProposal {coin = Coin {issuer = 'alice', owner = 'bob'}})]

Finally, accept all proposals using the ``forA`` function to iterate
over them.

.. code-block:: none

   daml> forA proposals $ \(contractId, _) -> submit bob (exerciseCmd contractId Accept)

Using the ``query`` function we can now verify that there is one
``Coin`` and no ``CoinProposal``:

.. code-block:: none

   daml> coins <- query @Coin bob
   daml> debug coins
   [Daml.Script:39]: [(<contract-id>,Coin {issuer = 'alice', owner = 'bob'})]
   daml> proposals <- query @CoinProposal bob
   [Daml.Script:39]: []

To exit ``daml repl`` press ``Control-D``.


What is in scope at the prompt?
===============================

In the prompt, all modules from the main dalf of the DAR passed to
``daml repl`` are imported. In addition to that the ``Daml.Script``
module is imported and gives you access to the DAML Script API.

You can use import declarations at the prompt to import additional modules.

.. code-block:: none

   daml> import DA.Time
   daml> debug (days 1)

Connecting via TLS
==================

You can connect to a ledger that requires TLS by passing ``--tls``.  A
custom root certificate used for validating the server certificate can
be set via ``--cacrt``. Finally, you can also enable client
authentication by passing ``--pem client.key --crt client.crt``. If
``--cacrt`` or ``--pem`` and ``--crt`` are passed TLS is automatically
enabled so ``--tls`` is redundant.

Connection to a Ledger with Authentication
==========================================

If your ledger requires an authentication token you can pass it via
``--access-token-file``.
