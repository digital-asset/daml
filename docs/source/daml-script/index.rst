.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Script
###########

**WARNING:** DAML Script is an experimental feature that is actively
being designed and is *subject to breaking changes*.
We welcome feedback about DAML script on
`our issue tracker <https://github.com/digital-asset/daml/issues/new?milestone=DAML+Script>`_
or `on Slack <https://hub.daml.com/slack/>`_.

DAML scenarios provide a simple API for experimenting with DAML models
and getting quick feedback in DAML studio. However, scenarios are run
in a special process and do not interact with an actual ledger. This
means that you cannot use scenarios to test other ledger clients,
e.g., your UI or :doc:`DAML triggers </triggers/index>`.

DAML script addresses this problem by providing you with an API with
the simplicity of DAML scenarios and all the benefits such as being
able to reuse your DAML types and logic while running against an
actual ledger. This means that you can use it to test automation
logic, your UI but also for ledger initialization where scenarios
cannot be used (with the exception of :doc:`/tools/sandbox`).

Usage
=====

Our example for this tutorial consists of 2 templates.

First, we have a template called ``Coin``:

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- COIN_TEMPLATE_BEGIN
   :end-before: -- COIN_TEMPLATE_END

This template represents a coin issued to ``owner`` by ``issuer``.
``Coin`` has both the ``owner`` and the ``issuer`` as signatories.

Second, we have a template called ``CoinProposal``:

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- COIN_PROPOSAL_TEMPLATE_BEGIN
   :end-before: -- COIN_PROPOSAL_TEMPLATE_END

``CoinProposal`` is only signed by the ``issuer`` and it provides a
single ``Accept`` choice which, when exercised by the controller will
create the corresponding ``Coin``.

Having defined the templates, we can now move on to write DAML scripts
that operate on these templates. To get accees to the API used to implement DAML scripts, you need to add the ``daml-script``
library to the ``dependencies`` field in ``daml.yaml``.

.. literalinclude:: ./template-root/daml.yaml.template
   :start-after: # script-dependencies-begin
   :end-before: # script-dependencies-end

In addition to that you also need to import the ``Daml.Script`` module
and since DAML script provides ``submit`` and ``submitMustFail``
functions that collide with the ones used in scenarios, we need to
hide those. We also enable the ``ApplicativeDo`` extension. We will
see below why this is useful.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- DAML_SCRIPT_HEADER_BEGIN
   :end-before: -- DAML_SCRIPT_HEADER_END

Since on an actual ledger parties cannot be arbitrary strings, we
define a record containing all the parties that we will use in our
script so that we can easily swap them out.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- LEDGER_PARTIES_BEGIN
   :end-before: -- LEDGER_PARTIES_END

Let us now write a function to initialize the ledger with 3
``CoinProposal``s and accept 2 of them. This function takes the
``LedgerParties`` as an argument and return something of type ``Script
()`` which is DAML script’s equivalent of ``Scenario ()``.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- INITIALIZE_SIGNATURE_BEGIN
   :end-before: -- INITIALIZE_SIGNATURE_END

First we create the proposals. To do so, we use the ``submit``
function to submit a transaction. The first argument is the party
submitting the transaction. In our case, we want all proposals to be
created by the bank so we use ``parties.bank``. The second argument
must be of type ``Commands a`` so in our case ``Commands (ContractId
CoinProposal, ContractId CoinProposal, ContractId CoinProposal)``
corresponding to the 3 proposals that we create. ``Commands`` is
similar to ``Update`` which is used in the ``submit`` function in
scenarios. However, ``Commands`` requires that the individual commands
do not depend on each other. This matches the restriction on the
Ledger API where a transaction consists of a list of commands.  Using
``ApplicativeDo`` we can still use ``do``-notation as long as we
respect this. In ``Commands`` we use ``createCmd`` instead of
``create`` and ``exerciseCmd`` instead of ``exercise``.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- INITIALIZE_PROPOSAL_BEGIN
   :end-before: -- INITIALIZE_PROPOSAL_END

Now that we have created the ``CoinProposal``s, we want ``Alice`` and
``Bob`` to accept the proposal while the ``Bank`` will ignore the
proposal that it has created for itself. To do so we use separate
``submit`` statements for ``Alice`` and ``Bob`` and call
``exerciseCmd``.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- INITIALIZE_ACCEPT_BEGIN
   :end-before: -- INITIALIZE_ACCEPT_END

Finally, we call ``pure ()`` on the last line of our script to match
the type ``Script ()``.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- INITIALIZE_PURE_BEGIN
   :end-before: -- INITIALIZE_PURE_END

We have now defined a way to initialize the ledger so we can write a
test that checks that the contracts that we expect exist afterwards.

First, we define the signature of our test. We will create the parties
used here in the test, so it does not take any arguments.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- TEST_SIGNATURE_BEGIN
   :end-before: -- TEST_SIGNATURE_END

Now, we create the parties using the ``allocateParty`` function. This
uses the party management service to create new parties with the given
display name. Note that the display name does not identify a party
uniquely. If you call ``allocateParty`` twice with the same display
name, it will create 2 different parties. This is very convenient for
testing since a new party cannot see any old contracts on the ledger
so using new parties for each test removes the need to reset the
ledger.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- TEST_ALLOCATE_BEGIN
   :end-before: -- TEST_ALLOCATE_END

We now call the ``initialize`` function that we defined before on the
parties that we have just allocated.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- TEST_INITIALIZE_BEGIN
   :end-before: -- TEST_INITIALIZE_END

To verify the contracts on the ledger, we use the ``query``
function. We pass it the type of the template and a party. It will
then give us all active contracts of the given type visible to the
party. In our example, we expect to see one active ``CoinProposal``
for ``bank`` and one ``Coin`` contract for each of ``Alice`` and
``Bob``.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- TEST_QUERIES_BEGIN
   :end-before: -- TEST_QUERIES_END

To run our script, we first build it with ``daml build`` and then run
it by pointing to the DAR, the name of our script and the host and
port our ledger is running on.

``daml script --dar .daml/dist/script-example-0.0.1.dar --script-name ScriptExample:test --ledger-host localhost --ledger-port 6865``

Up to now, we have worked with parties that we have allocated in the
test. We can also pass in the path to a file containing
the input in the :doc:`/json-api/lf-value-specification`.

.. literalinclude:: ./template-root/ledger-parties.json
   :language: daml

We can then initialize our ledger passing in the json file via ``--input-file``.

``daml script daml script --dar .daml/dist/script-example-0.0.1.dar --script-name ScriptExample:initialize --ledger-host localhost --ledger-port 6865 --input-file ledger-parties.json``

If you open Navigator, you can now see the contracts that have been created.
