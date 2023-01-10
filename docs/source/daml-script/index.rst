.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _daml-script:

Daml Script
###########

.. toctree::
   :hidden:

   api/index

Daml Script provides a simple way of testing Daml models and getting
quick feedback in Daml studio. In addition to running it in a virtual
ledger in :ref:`Daml Studio <script-results>`, you can also
point it against an actual ledger. This means that you can use it for
application scripting, to test automation logic and also for
:ref:`ledger initialization <script-ledger-initialization>`.

You can also use Daml Script interactively using :doc:`/daml-repl/index`.

.. hint::

  Remember that you can access all the example code by running ``daml new script-example --template script-example``

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

Having defined the templates, we can now move on to write Daml scripts
that operate on these templates. To get access to the API used to implement Daml scripts, you need to add the ``daml-script``
library to the ``dependencies`` field in ``daml.yaml``.

.. literalinclude:: ./template-root/daml.yaml.template
   :start-after: # script-dependencies-begin
   :end-before: # script-dependencies-end

We also enable the ``ApplicativeDo`` extension. We will
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
``CoinProposal`` contracts and accept 2 of them. This function takes the
``LedgerParties`` as an argument and returns a value of type ``Script
()`` which is Daml script’s equivalent of ``Scenario ()``.

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
corresponding to the 3 proposals that we create. However, ``Commands`` requires that the individual commands
do not depend on each other. This matches the restriction on the
Ledger API where a transaction consists of a list of commands.  Using
``ApplicativeDo`` we can still use ``do``-notation as long as we
respect this and the last statement in the ``do``-block is of the form
``return expr`` or ``pure expr``.
In ``Commands`` we use ``createCmd`` instead of
``create`` and ``exerciseCmd`` instead of ``exercise``.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- INITIALIZE_PROPOSAL_BEGIN
   :end-before: -- INITIALIZE_PROPOSAL_END

Now that we have created the ``CoinProposal``\ s, we want ``Alice`` and
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

Party Management
----------------

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
ledger. We factor out party allocation into a function so we can reuse it in later sections.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- ALLOCATE_PARTIES_BEGIN
   :end-before: -- ALLOCATE_PARTIES_END

We now call the ``initialize`` function that we defined before on the
parties that we have just allocated.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- TEST_INITIALIZE_BEGIN
   :end-before: -- TEST_INITIALIZE_END

Queries
-------

To verify the contracts on the ledger, we use the ``query``
function. We pass it the type of the template and a party. It will
then give us all active contracts of the given type visible to the
party. In our example, we expect to see one active ``CoinProposal``
for ``bank`` and one ``Coin`` contract for each of ``Alice`` and
``Bob``. We get back list of ``(ContractId t, t)`` pairs from
``query``. In our tests, we do not need the contract ids, so we throw
them away using ``map snd``.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- TEST_QUERIES_BEGIN
   :end-before: -- TEST_QUERIES_END

Interfaces
----------

To use interfaces within Daml code, the target language version must be at least ``1.15``.

.. literalinclude:: ./template-root/daml.yaml.template
   :start-after: # script-build-options-begin
   :end-before: # script-build-options-end

Now we can define an ``Asset`` interface which can be implemented by the ``Coin`` template. We also define ``AssetInfo`` for use as the viewtype.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- ASSET_INTERFACE_BEGIN
   :end-before: -- ASSET_INTERFACE_END

Now we use the ``queryInterface`` function. We pass it the type of the interface and a party. It will return a list of active contract views for the given interface type. As before we throw away the contract ids using ``map snd``.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- TEST_INTERFACE_BEGIN
   :end-before: -- TEST_INTERFACE_END

Run a Script
------------

To run our script, we first build it with ``daml build`` and then run
it by pointing to the DAR, the name of our script, and the host and
port our ledger is running on.

``daml script --dar .daml/dist/script-example-0.0.1.dar --script-name ScriptExample:test --ledger-host localhost --ledger-port 6865``

Up to now, we have worked with a script (``test``) that is entirely
self-contained. This is fine for running unit-test type script in the IDE,
but for more complex use-cases you may want to vary the inputs of a script and
inspect its outputs, ideally without having to recompile it. To that end, the
``daml script`` command supports the flags ``--input-file`` and
``--output-file``. Both flags take a filename, and said file will be
read/written as JSON, following the :doc:`/json-api/lf-value-specification`.

The ``--output-file`` option instructs ``daml script`` to write the
result of the given ``--script-name`` to the given filename (creating
the file if it does not exist; overwriting it otherwise). This is most
useful if the given program has a type ``Script b``, where ``b`` is a
meaningful value. In our example, we can use this to write out the
party ids that have been allocated by ``allocateParties``:

``daml script --dar .daml/dist/script-example-0.0.1.dar --script-name ScriptExample:allocateParties --ledger-host localhost --ledger-port 6865 --output-file ledger-parties.json``

The resulting file will look similar to the following but the actual
party IDs will be different each time you run it:

.. literalinclude:: ./template-root/ledger-parties.json
   :language: daml

Next, we want to call the ``initialize`` function with those parties
using the ``--input-file`` flag.  If the ``--input-file`` flag is
specified, the ``--script-name`` flag must point to a function of one
argument returning a ``Script``, and the function will be called with
the result of parsing the input file as its argument. For example, we
can initialize our ledger using the ``initialize`` function defined
above.

Using the previosuly created ``-ledger-parties.json`` file, we can
initialize our ledger as follows:

``daml script --dar .daml/dist/script-example-0.0.1.dar --script-name ScriptExample:initialize --ledger-host localhost --ledger-port 6865 --input-file ledger-parties.json``

.. _script-ledger-initialization:

Use Daml Script for Ledger Initialization
=========================================

You can use Daml script to initialize a ledger on startup. To do so,
specify an ``init-script: ScriptExample:initializeUser`` field in your
``daml.yaml``. This will automatically be picked up by ``daml start``
and used to initialize sandbox. During development not being able to
control party ids can often be inconvenient. Here, we rely on
:ref:`users <user-management-service>` which do put us in control of
their id. User ids can be used in Navigator, triggers & other tools
instead of party ids.

.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- INITIALIZE_USER_BEGIN
   :end-before: -- INITIALIZE_USER_END

Migrate From Scenarios
----------------------

Existing scenarios that you used for ledger initialization can be
translated to Daml script but there are a few things to keep in mind:

#. You need to add ``daml-script`` to the list of dependencies in your
   ``daml.yaml``.
#. You need to import the ``Daml.Script`` module.
#. Calls to ``create``, ``exercise``, ``exerciseByKey`` and
   ``createAndExercise`` need to be suffixed with ``Cmd``, e.g.,
   ``createCmd``.
#. Instead of specifying a ``scenario`` field in your ``daml.yaml``,
   you need to specify an ``init-script`` field. The initialization
   script is specified via ``Module:identifier`` for both fields.
#. In Daml script, ``submit`` and ``submitMustFail`` are limited to
   the functionality provided by the ledger API: A list of independent
   commands consisting of ``createCmd``, ``exerciseCmd``,
   ``createAndExerciseCmd`` and ``exerciseByKeyCmd``. There are two
   issues you might run into when migrating an existing scenario:

   #. Your commands depend on each other, e.g., you use the result of
      a ``create`` within a following command in the same
      ``submit``. In this case, you have two options: If it is not
      important that they are part of a single transaction, split
      them into multiple calls to ``submit``. If you do need them to
      be within the same transaction, you can move the logic to a
      choice and call that using ``createAndExerciseCmd``.

   #. You use something that is not part of the 4 ledger API command
      types, e.g., ``fetch``. For ``fetch`` and ``fetchByKey``, you
      can instead use ``queryContractId`` and ``queryContractKey``
      with the caveat that they do not run within the same
      transaction. Other types of ``Update`` statements can be moved
      to a choice that you call via ``createAndExerciseCmd``.
#. Instead of Scenario’s ``getParty``, Daml Script provides you with
   ``allocateParty`` and ``allocatePartyWithHint``. There are a few
   important differences:

   #. Allocating a party always gives you back a new party (or
      fails). If you have multiple calls to ``getParty`` with the same
      string and expect to get back the same party, you should instead
      allocate the party once at the beginning and pass it along to
      the rest of the code.

   #. If you want to allocate a party with a specific party id, you
      can use ``allocatePartyWithHint x (PartyIdHint x)`` as a replacement for `getParty x`. Note that
      while this is supported in Daml Studio, some
      ledgers can behave differently and ignore the party id hint or
      interpret it another way. Try to not rely on any specific
      party id.
#. Instead of ``pass`` and ``passToDate``, Daml Script provides
   ``passTime`` and ``setTime``.

.. _daml-script-distributed:

Use Daml Script in Canton
=========================

So far, we have run Daml script against a single participant node. It
is also possible to run it in a setting where different parties
are hosted on different participant nodes. To do so, pass the
``--participant-config participant-config.json`` file to ``daml script``
instead of ``--ledger-host`` and ``ledger-port``.
You can generate this file by calling
:ref:`utils.generate_daml_script_participants_conf(defaultParticipant = Some(one)) <utils.generate_daml_script_participants_conf>`
in the canton console or in :ref:`the bootstrap scripts <automation-using-bootstrap-scripts>`.

The generated file will look similar to the one shown below:

.. literalinclude:: ./participants-config.json
   :language: json

This will define a participant called ``one``, declare ``one`` as the default participant and it defines that the party ``alice`` is hosted on participant
``one``. Whenever you submit something as party, we will use the
participant for that party or if none is specified
``default_participant``.

If you use ``utils.generate_daml_script_participants_conf()``
without a default participant, the `default_participant` won't be defined and therefore
using a party with an unspecified participant is an error.

``allocateParty`` will also use the ``default_participant``. If you
want to allocate a party on a specific participant, you can use
``allocatePartyOn`` which accepts the participant name as an extra
argument.


Hints for synchronizing contracts on multiple-participant Canton
----------------------------------------------------------------
When you create a contract on ``participant1`` and try to use it on
``participant2``, you can run into synchronization issues where
``participant2`` doesn't see the contract yet. One option to workaround
`this limitation <https://github.com/digital-asset/daml/issues/10618>`_
is to poll until the contract is visible. In the example below, the ``bank``
and ``alice`` parties are allocated on two different participants and to avoid
synchronization issues, we wait until the contract is visible on ``alice`` participant.


.. literalinclude:: ./template-root/src/ScriptExample.daml
   :language: daml
   :start-after: -- SYNC_CONTRACT_IDS_BEGIN
   :end-before: -- SYNC_CONTRACT_IDS_END

.. _daml-script-auth:

Run Daml Script Against Ledgers with Authorization
==================================================

To run Daml Script against a ledger that verifies authorization,
you need to specify an access token. There are two ways of doing that:

1. Specify a single access token via ``--access-token-file
   path/to/jwt``. This token will then be used for all requests so it
   must provide claims for all parties that you use in your script.
2. If you need multiple tokens, e.g., because you only have
   single-party tokens you can define the ``access_token`` field in the
   participant config specified via ``--participant-config``.
   Note that you can specify the same participant
   twice if you want different auth tokens.
   The file should be of the format


.. literalinclude:: ./participants-example.json
   :language: json

If you specify both ``--access-token-file`` and
``--participant-config``, the participant config takes precedence and
the token from the file will be used for any participant that does not
have a token specified in the config.

.. _daml-script-json-api:

Run Daml Script Against the HTTP JSON API
=========================================

In some cases, you only have access to the
:doc:`HTTP JSON API </json-api/index>` but not to the gRPC of a ledger, e.g., on
`Daml Hub <https://hub.daml.com>`_. For this usecase, Daml
script can be run against the JSON API. Note that if you do have
access to the gRPC Ledger API, running Daml script against the JSON API does
not have any advantages.

To run Daml script against the JSON API you have to pass the ``--json-api`` parameter to ``daml script``. There are a few differences and limitations compared to running Daml Script against the gRPC Ledger API:

#. When running against the JSON API, the ``--host`` argument has to
   contain an ``http://`` or ``https://`` prefix, e.g., ``daml
   script --host http://localhost --port 7575 --json-api``.
#. The JSON API only supports single-command submissions. This means
   that within a single call to ``submit`` you can only execute one
   ledger API command, e.g., one ``createCmd`` or one ``exerciseCmd``.
#. The JSON API requires authorization tokens even when it is run
   against a ledger that doesn't verify authorization. The section on
   :ref:`authorization <daml-script-auth>` describes how to specify
   the tokens.
#. The parties used for command submissions and queries must match the
   parties specified in the token exactly.  For command submissions
   that means ``actAs`` and ``readAs`` must match exactly what you
   specified whereas for queries the union of ``actAs`` and ``readAs``
   must match the parties specified in the query.
#. If you use multiple parties within your Daml Script, you need to
   specify one token per party or every submission and query must
   specify all parties of the multi-party token.
#. ``getTime`` will always return the Unix epoch in static time mode
   since the time service is not exposed via the JSON API.
#. ``setTime`` is not supported and will throw a runtime error.
