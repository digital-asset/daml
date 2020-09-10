.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Triggers - Off-Ledger Automation in DAML
#############################################

.. toctree::
   :hidden:

   api/index

**WARNING:** DAML Triggers are an early access feature that is actively
being designed and is *subject to breaking changes*.
We welcome feedback about DAML triggers on
`our issue tracker <https://github.com/digital-asset/daml/issues/new?milestone=DAML+Triggers>`_,
`our forum <https://discuss.daml.com>`_, or `on Slack <https://slack.daml.com>`_.

In addition to the actual DAML logic which is uploaded to the Ledger
and the UI, DAML applications often need to automate certain
interactions with the ledger. This is commonly done in the form of a
ledger client that listens to the transaction stream of the ledger and
when certain conditions are met, e.g., when a template of a given type
has been created, the client sends commands to the ledger, e.g., it
creates a template of another type.

It is possible to write these clients in a language of your choice,
e.g., JavaScript, using the HTTP JSON API. However, that introduces an
additional layer of friction: You now need to translate between the
template and choice types in DAML and a representation of those DAML
types in the language you are using for your client. DAML triggers
address this problem by allowing you to write certain kinds of
automation directly in DAML reusing all the DAML types and logic that
you have already defined. Note that while the logic for DAML triggers
is written in DAML, they act like any other ledger client: They are
executed separately from the ledger, they do not need to be uploaded
to the ledger and they do not allow you to do anything that any other
ledger client could not do.

Usage
=====

Our example for this tutorial consists of 3 templates.

First, we have a template called ``Original``:

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- ORIGINAL_TEMPLATE_BEGIN
   :end-before: -- ORIGINAL_TEMPLATE_END

This template has an ``owner``, a ``name`` that identifies it and some
``textdata`` that we just represent as ``Text`` to keep things simple.  We
have also added a contract key to ensure that each owner can only have
one ``Original`` with a given ``name``.

Second, we have a template called ``Subscriber``:

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- SUBSCRIBER_TEMPLATE_BEGIN
   :end-before: -- SUBSCRIBER_TEMPLATE_END

This template allows the ``subscriber`` to subscribe to ``Original`` s where ``subscribedTo`` is the ``owner``.
For each of these ``Original`` s, our DAML trigger should then automatically create an instance of
third template called ``Copy``:

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- COPY_TEMPLATE_BEGIN
   :end-before: -- COPY_TEMPLATE_END

Our trigger should also ensure that the ``Copy`` contracts stay in sync with changes on the ledger. That means
that we need to archive ``Copy`` contracts if there is more than one for the same ``Original``, we need to archive
``Copy`` contracts if the corresponding ``Original`` has been archived and we need to archive
all ``Copy`` s for a given subscriber if the corresponding ``Subscriber`` contract has been archived.

Implementing a DAML Trigger
---------------------------

Having defined what our DAML trigger is supposed to do, we can now
move on to its implementation. A DAML trigger is a regular DAML
project that you can build using ``daml build``. To get access to the
API used to build a trigger, you need to add the ``daml-triggers``
library to the ``dependencies`` field in ``daml.yaml``.

.. literalinclude:: ./template-root/daml.yaml.template
   :start-after: # trigger-dependencies-begin
   :end-before: # trigger-dependencies-end

In addition to that you also need to import the ``Daml.Trigger``
module.

DAML triggers automatically track the active contract set and the
commands in flight for you. In addition to that, they allow you to
have user-defined state that is updated based on new transactions and
command completions. For our copy trigger, the ACS is sufficient, so
we will simply use ``()`` as the type of the user defined state.

To create a trigger you need to define a value of type ``Trigger s`` where ``s`` is the type of your user-defined state:

.. code-block:: daml

    data Trigger s = Trigger
      { initialize : ACS -> s
      , updateState : ACS -> Message -> s -> s
      , rule : Party -> ACS -> Time -> Map CommandId [Command] -> s -> TriggerA ()
      , registeredTemplates : RegisteredTemplates
      , heartbeat : Optional RelTime
      }

The ``initialize`` function is called on startup and allows you to
initialize your user-defined state based on the active contract set.

The ``updateState`` function is called on new transactions and command
completions and can be used to update your user-defined state based on
the ACS and the transaction or completion. Since our DAML trigger does
not have any interesting user-defined state, we will not go into
details here.

The ``rule`` function is the core of a DAML trigger. It
defines which commands need to be sent to the ledger based on the
party the trigger is executed at, the current state of the ACS, the
current time, the commands in flight and the user defined state.
The type ``TriggerA`` allows you to emit commands that are then sent
to the ledger. Like ``Scenario`` or ``Update``, you can use ``do``
notation with ``TriggerA``.

We can specify the templates that our trigger will operate
on. In our case, we will simply specify ``AllInDar`` which means that
the trigger will receive events for all template types defined in the
DAR. It is also possible to specify an explicit list of templates,
e.g., ``RegisteredTemplates [registeredTemplate @Original, registeredTemplate @Subscriber, registeredTemplate @Copy]``.
This is mainly useful for performance reasons if your DAR contains many templates that are not relevant for your trigger.

Finally, you can specify an optional heartbeat interval at which the trigger
will be sent a ``MHeartbeat`` message. This is useful if you want to ensure
that the trigger is executed at a certain rate to issue timed commands.

For our DAML trigger, the definition looks as follows:

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- TRIGGER_BEGIN
   :end-before: -- TRIGGER_END

Now we can move on to the most complex part of our DAML trigger, the implementation of ``copyRule``.
First let’s take a look at the signature:

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- RULE_SIGNATURE_BEGIN
   :end-before: -- RULE_SIGNATURE_END

We will need the party and the ACS to get the ``Original`` contracts
where we are the owner, the ``Subscriber`` contracts where we are in
the ``subscribedTo`` field and the ``Copy`` contracts where we are the
``owner`` of the corresponding ``Original``.

The commands in flight will be useful to avoid sending the same
command multiple times if ``copyRule`` is run multiple times before we
get the corresponding transaction. Note that DAML triggers are
expected to be designed such that they can cope with this, e.g., after
a restart or a crash where the commands in flight do not contain
commands in flight from before the restart, so this is an optimization
rather than something required for them to function correctly.

First, we get all ``Subscriber``, ``Original`` and ``Copy`` contracts
from the ACS. For that, the DAML trigger API provides a
``getContracts`` function that given the ACS will return a list of all
contracts of a given template.

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- ACS_QUERY_BEGIN
   :end-before: -- ACS_QUERY_END

Now, we can filter those contracts to the ones where we are the
``owner`` as described before.

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- ACS_FILTER_BEGIN
   :end-before: -- ACS_FILTER_END

We also need a list of all parties that have subscribed to us.

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- SUBSCRIBING_PARTIES_BEGIN
   :end-before: -- SUBSCRIBING_PARTIES_END

As we have mentioned before, we only want to keep one ``Copy`` per
``Original`` and ``Subscriber`` and archive all others. Therefore, we
group identical ``Copy`` contracts and keep the first of each group
while archiving the others.

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- GROUP_COPIES_BEGIN
   :end-before: -- GROUP_COPIES_END

In addition to duplicate copies, we also need to archive copies where
the corresponding ``Original`` or ``Subscriber`` no longer exists.

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- ARCHIVE_COPIES_BEGIN
   :end-before: -- ARCHIVE_COPIES_END

To send the corresponding archive commands to the ledger, we iterate
over ``archiveCopies`` using ``forA`` and call the ``emitCommands``
function. Each call to ``emitCommands`` takes a list of commands which
will be submitted as a single transaction. The actual commands can be
created using ``exerciseCmd`` and ``createCmd``. In addition to that,
we also pass in a list of contract ids. Those contracts will be marked
pending and not be included in the result of ``getContracts`` until
the commands have either been comitted to the ledger or the command
submission failed.

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- ARCHIVE_COMMAND_BEGIN
   :end-before: -- ARCHIVE_COMMAND_END

Finally, we also need to create copies that do not already exists. We
want to avoid creating copies for which there is already a command in
flight. The DAML Trigger API provides a ``dedupCreate`` helper for this
which only sends the commands if it is not already in flight.

.. literalinclude:: ./template-root/src/CopyTrigger.daml
   :language: daml
   :start-after: -- CREATE_COPIES_BEGIN
   :end-before: -- CREATE_COPIES_END

Running a DAML Trigger
----------------------

To try this example out, you can replicate it using
``daml new copy-trigger --template copy-trigger``. You first have to build the trigger like
you would build a regular DAML project using ``daml build``.
Then start the sandbox and navigator using ``daml start``.

Now we are ready to run the trigger using ``daml trigger``:

.. code-block:: sh

    daml trigger --dar .daml/dist/copy-trigger-0.0.1.dar --trigger-name CopyTrigger:copyTrigger --ledger-host localhost --ledger-port 6865 --ledger-party Alice

The first argument specifies the ``.dar`` file that we have just
built. The second argument specifies the identifier of the trigger
using the syntax ``ModuleName:identifier``. Finally, we need to
specify the ledger host, port, the party that our trigger is executed
as, and the time mode of the ledger which is the sandbox default, i.e,
static time.

Now open Navigator at http://localhost:7500/.

First, login as ``Alice`` and create an ``Original`` contract with
``party`` set to ``Alice``.  Now, logout and login as ``Bob`` and
create a ``Subscriber`` contract with ``subscriber`` set to ``Bob``
and ``subscribedTo`` set to ``Alice``. After a short delay you should
now see a ``Copy`` contract corresponding to the ``Original`` that you
have created as ``Alice``. Once you archive the ``Subscriber``
contract, you can see that the ``Copy`` contract will also be
archived.

When using DAML triggers against a Ledger with authentication, you can
pass ``--access-token-file token.jwt`` to ``daml trigger`` which will
read the token from the file ``token.jwt``.

When not to use DAML triggers
=============================

DAML triggers deliberately only allow you to express automation that
listens for ledger events and reacts to them by sending commands to
the ledger. If your automation needs to interact with data outside of
the ledger then DAML triggers are not the right tool. For this case,
you can use the HTTP JSON API.
