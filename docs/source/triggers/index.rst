.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml Triggers - Off-Ledger Automation in Daml
#############################################

.. toctree::
   :hidden:

   api/index

In addition to the actual Daml logic which is uploaded to the Ledger
and the UI, Daml applications often need to automate certain
interactions with the ledger. This is commonly done in the form of a
ledger client that listens to the transaction stream of the ledger and
when certain conditions are met, e.g., when a template of a given type
has been created, the client sends commands to the ledger to
create a template of another type.

It is possible to write these clients in a language of your choice,
such as JavaScript, using the HTTP JSON API. However, that introduces an
additional layer of friction: you now need to translate between the
template and choice types in Daml and a representation of those Daml
types in the language you are using for your client. Daml triggers
address this problem by allowing you to write certain kinds of
automation directly in Daml, reusing all the Daml types and logic that
you have already defined. Note that, while the logic for Daml triggers
is written in Daml, they act like any other ledger client: they are
executed separately from the ledger, they do not need to be uploaded
to the ledger and they do not allow you to do anything that any other
ledger client could not do.


If you don't want to follow along, but still want to get the final code
for this section to play with, you can get it by running::

      daml new --template=gsg-trigger gsg-trigger

How To Think About Triggers
===========================

It is tempting to think of Daml Triggers as snippets of code that "react to
ledger events". However, this is not the best way to think about them; while it
will work in some cases, in many corner cases that line of thought will lead to
subtle errors.

Instead, you should think of, and write, your triggers from the perspective of
"correcting the current ACS" to match some predefined expectations. Trigger
rules should be a combination of checking those expectations on the current ACS
and applying corrective actions to bring back the ACS in line with its expected
state.

The "trigger" part is best thought of as an optimization: rather than check the
ACS constantly, we only apply our rules when something happens that we believe
*may* lead to the state of the ledger diverging from our expectations.

Sample Trigger
==============

Our example for this tutorial builds upon the Getting Started Guide,
specifically picking up right after the :doc:`/getting-started/first-feature`
section.

We assume that our requirements are to build a chatbot that reponds to every
message with:

  "Please, tell me more about that."

That should fool anyone and pass the Turing test, easily.

As explained above, while the layman description may be "responds to every
message", our technical description is better phrased as "ensure that, at all
times, the last message we can see has been sent by us; if that is not the
case, the corrective action is to send a response to the last message we can
see".

Daml Trigger Basics
===================

A Daml trigger is a regular Daml project that you can build using ``daml
build``. To get access to the API used to build a trigger, you need to add the
``daml-trigger`` library to the ``dependencies`` field in ``daml.yaml``:

.. literalinclude:: /_templates/gsg-trigger/daml.yaml.template
  :language: yaml
  :start-after: # trigger-dependencies-begin
  :end-before: # trigger-dependencies-end

**Note**: In the specific case of the Getting Started Guide, this is already
included as part of the ``create-daml-app`` template.

In addition to that you also need to import the ``Daml.Trigger``
module in your own code.

Daml triggers automatically track the active contract set (ACS), i.e., the set of contracts
that have been created and have not been archived, and the
commands in flight for you. In addition to that, they allow you to
have user-defined state that is updated based on new transactions and
command completions. For our chatbot trigger, the ACS is sufficient, so
we will simply use ``()`` as the type of the user defined state.

To create a trigger you need to define a value of type ``Trigger s`` where ``s`` is the type of your user-defined state:

.. code-block:: daml

    data Trigger s = Trigger
      { initialize : TriggerInitializeA s
      , updateState : Message -> TriggerUpdateA s ()
      , rule : Party -> TriggerA s ()
      , registeredTemplates : RegisteredTemplates
      , heartbeat : Optional RelTime
      }

To clarify, this is the definition in the ``Daml.Trigger`` library, reproduced
here for illustration purposes. This is not something you need to add to your
own code.

The ``initialize`` function is called on startup and allows you to
initialize your user-defined state based on querying the active contract
set.

The ``updateState`` function is called on new transactions and command
completions and can be used to update your user-defined state based on
the ACS and the transaction or completion. Since our Daml trigger does
not have any interesting user-defined state, we will not go into
details here.

The ``rule`` function is the core of a Daml trigger. It defines which
commands need to be sent to the ledger based on the party the trigger is
executed at, the current state of the ACS, and the user defined state.
The type ``TriggerA`` allows you to emit commands that are then sent to
the ledger, query the ACS with ``query``, update the user-defined state,
as well as retrieve the commands in flight with ``getCommandsInFlight``.
Like ``Scenario`` or ``Update``, you can use ``do`` notation and
``getTime`` with ``TriggerA``.

We can specify the templates and interfaces that our trigger will operate
on. In our case, we will simply specify ``AllInDar`` which means that
the trigger will receive events for all template and interface types defined in the
DAR.

It is also possible to specify an explicit list of templates and interfaces. For example,
to only receive events for the ``Message`` template, one would write:

.. code-block:: daml

   ...
   registeredTemplates = RegisteredTemplates [registeredTemplate @Message],
   ...

This is mainly useful for performance reasons if your DAR contains many templates and interfaces that are not relevant
for your trigger.
Note that providing an explicit list of templates and interfaces also filters the result of querying the ACS using
the Trigger API: contracts of the excluded templates and interfaces cannot be queried.

.. note::
    In these examples we used templates. Note that interfaces can be passed as well
    wherever a template is passed, using the same `RegisteredTemplates` type. You are
    free to pass multiple templates and interfaces and possibly mix the two freely in
    a single request.

Finally, you can specify an optional heartbeat interval at which the trigger
will be sent a ``MHeartbeat`` message. This is useful if you want to ensure
that the trigger is executed at a certain rate to issue timed commands. We will
not be using heartbeats in this example.

.. _running-a-no-op-trigger:

Run a No-Op Trigger
===================

To implement a no-op trigger, one could write the following in a separate
``daml/ChatBot.daml`` file:

.. literalinclude:: /_templates/gsg-trigger/daml/NoOp.daml
  :language: daml

In the context of the Getting Started app, if you write the above file, then
run ``daml start`` and ``npm start`` as usual, and then set up the trigger
with:

.. code-block:: bash

    daml trigger --dar .daml/dist/gsg-trigger-0.1.0.dar \
                 --trigger-name NoOp:noOp \
                 --ledger-host localhost \
                 --ledger-port 6865 \
                 --ledger-user "bob"

and then play with the app as ``alice`` and ``bob`` just like you did for
:doc:`/getting-started/first-feature`, you should see the trigger command
printing a line for each interaction, containing the message ``triggered`` as
well as other debug information.

Diversion: Updating ``Message``
===============================

Before we can make our Trigger more useful, we need to think a bit more about
what it is supposed to do. For example, we don't want to respond to ``bob``'s
own messages. We also do not want to send messages when we have not received
any.

In order to start with something reasonably simple, we're going to set the rule as

  *if* the last message we can see was *not* sent by ``bob``, *then* we'll send
  ``"Please, tell me more about that."`` to whoever sent the last message we
  can see.

This raises the question of how we can determine which message is the last one,
given the current structure of a message. In order to solve that, we need to
add a ``Time`` field to ``Message``, which can be done by editing the
``Message`` template in ``daml/User.daml`` to look like:

.. literalinclude:: /_templates/gsg-trigger/daml/User.daml
  :language: daml
  :start-after: -- MESSAGE_BEGIN
  :end-before: -- MESSAGE_END

This should result in Daml Studio reporting an error in the ``SendMessage``
choice, as it now needs to set the ``receivedAt`` field. Here is the updated
code for ``SendMessage``:

.. literalinclude:: /_templates/gsg-trigger/daml/User.daml
  :language: daml
  :start-after: -- SEND_BEGIN
  :end-before: -- SEND_END

The ``getTime`` action (`doc </daml/stdlib/Prelude.html#function-da-internal-lf-gettime-99334>`__)
returns the time at which the command was received by the sandbox. In more
sensitive applications, this may not be sufficiently reliable, as transactions
may be processed in parallel (so "received at" timestamp order may not match
actual transaction order), and in distributed cases dishonest participants may
fudge this value. It's good enough for this example, though.

Now that we have a field to sort on, and thus a way to identify the *latest*
message, we can turn our attention back to our trigger code.

AutoReply
=========

Open up the trigger code again (``daml/ChatBot.daml``), and change it to:

.. literalinclude:: /_templates/gsg-trigger/daml/ChatBot.daml
  :language: daml

Refresh ``daml start`` by pressing ``r`` (followed by ``Enter`` on Windows) in
its terminal, then start the trigger with:

.. code-block:: bash

    daml trigger --dar .daml/dist/gsg-trigger-0.1.0.dar \
                 --trigger-name ChatBot:autoReply \
                 --ledger-host localhost \
                 --ledger-port 6865 \
                 --ledger-user "bob"

Play a bit with ``alice`` and ``bob`` in your browser, to get a feel for how
the trigger works. Watch both the messages in-browser and the debug statements
printed by the trigger runner.

Let's walk through the ``rule`` code line-by-line:

- We use the ``query`` function to get all of the ``Message`` templates visible
  to the current party (``p``; in our case this will be ``bob``). Per the
  `documentation </triggers/api/Daml-Trigger.html#function-daml-trigger-query-2759>`_,
  this returns a list of tuples (contract id, payload), which we store as
  ``message_contracts``.
- We then `map </daml/stdlib/Prelude.html#function-ghc-base-map-40302>`_ the
  `snd </daml/stdlib/Prelude.html#function-da-internal-prelude-snd-86578>`_
  function on the result to get only the payloads, i.e. the actual data of the
  messages we can see.
- We print, as a ``debug`` message, the number of messages we can see.
- On the next line, get the message with the highest ``receivedAt`` field
  (`maximumOn </daml/stdlib/DA-List-Total.html#function-da-list-total-maximumon-67732>`_).
- We then print another debug message, this time printing the message our code
  has identified as "the last message visible to the current party". If you run
  this, you'll see that ``lastMessage`` is actually a ``Optional Message``. This
  is because the `maximumOn </daml/stdlib/DA-List-Total.html#function-da-list-total-maximumon-67732>`_
  function will return the element from a list for which the given functions
  produces the highest value *if* the list has at least one element, but it
  needs to still do something sensible if the list is empty; in this case, it
  would return ``None``.
- When ``lastMessage`` is ``Some m``
  (`whenSome </daml/stdlib/DA-Optional.html#function-da-optional-whensome-23804>`_),
  we execute the given function.  Otherwise, ``lastMessage`` is ``None`` and we
  implicitly do nothing.
- Next, we need to check whether the message has been sent *to* or *by* the
  party running the trigger (with the current Daml model, it has to be one or
  the other, as messages are only visible to the sender and receiver).
  `when </daml/stdlib/DA-Action.html#function-da-action-when-53144>`_ the
  expression ``m.receiver == p`` is ``True``, our expectations of the
  ledger state are wrong and we need to correct it. Otherwise, the state
  matches our rule and we don't need to do anything.
- At this point we know the state is "wrong", per our expectations, and start
  engaging in correcting actions. For this trigger, this means sending a message
  to the sender of the last message. In order to do that, we need to find the
  ``User`` contract for the sender. We start by getting the list of all
  ``User`` contracts we know about, which will be all users who
  follow the party running the trigger (and that party's own ``User``
  contract). As for ``Message`` contracts earlier, the result of ``query
  @User`` is going to be a list of tuples with (contract id, payload). The big
  difference is that this time we actually want to keep the contract ids, as
  that is what we'll use to send a message back.
- We print the list of users we just fetched, as a debug message.
- We create a function ``isSender`` to identify the user we are looking for.
- We get the user contract by applying our ``isSender`` function as a
  `filter <https://docs.daml.com/daml/stdlib/Prelude.html#function-da-internal-prelude-filter-41317>`_
  on the list of users, and then taking the
  `head <https://docs.daml.com/daml/stdlib/DA-List-Total.html#function-da-list-total-head-26095>`_
  of that list, i.e. its first element.
- Just like  ``maximumOn``, ``head`` will return an ``Optional a``, so the next
  step is to check whether we have actually found the relevant ``User``
  contract. In most cases we should find it, but remember that users can send
  us a message if *we* follow *them*, whereas we can only answer if *they*
  follow *us*.
- If we did find some ``User`` contract to reply to, we extract the
  corresponding contract id (first element of the tuple, ``sender``) and
  discard the payload (second element, ``_``), and we
  `exercise <https://docs.daml.com/triggers/api/Daml-Trigger.html#function-daml-trigger-dedupexercise-2238>`_
  the ``SendMessage`` choice, passing in the current party ``p`` as the sender.
  See below for additional information on what that ``dedup`` in the name of the
  command means.

Command Deduplication
=====================

Daml Triggers react to many things, and it's usually important to make sure
that the same command is not sent mutiple times.

For example, in our ``autoReply`` chatbot above, the rule will be triggered not
only when we receive a message, but also when we send one, as well as when we
follow a user or get followed by a user, and when we stop following a user or a
user stops following us.

It's easy to imagine a sequence of events that would make a naive trigger
implementation send too many messages. For example:

- ``alice`` sends ``"hi"``, so the trigger runs and sends an ``exercise`` command.
- _Before_ the ``exercise`` command is fully processed, ``carol`` follows
  ``bob``, which triggers the rule again. The state of all the ``Message``
  contracts ``bob`` can see has not changed, so the rule might send the
  response to ``alice`` again.

We obviously don't want that to happen, as it would likely prevent us from
passing that Turing test we were after.

Triggers offer a few features to help users manage that. Possibly the simplest
one is the ``dedup*`` family of ledger operations. When using those, the
trigger runner will keep track of the commands currently sent and prevent
sending the exact same command again. In the above example, the trigger would
see that, when ``carol`` follows ``bob`` and the rule runs ``dedupExercise``,
there is already an Exercise command in flight with the exact same value, in
this case same message, same sender and same receiver.

Note that, if instead the in-between event is ``alice`` following ``carol``,
this simple deduplication mechanism might not work as expected: because the
``User`` contract ID for ``alice`` would have changed, the new command is not
the same as the in-flight one and thus a second ``SendMessage`` exercise would
be sent to the ledger.

Similarly, if ``alice`` sends a second message quickly after the first one,
this deduplication would prevent it, because the "response" does not have any
reference to which message it's responding to. This may or may not be what we
want.

If this simple deduplication is not suited to your use-case, you have two other
tools at your disposal. The first one is the second argument to the
``emitCommands`` action
(`doc <https://docs.daml.com/triggers/api/Daml-Trigger.html#function-daml-trigger-emitcommands-10563>`__),
which is a list of contract IDs. These IDs will be filtered out of any ACS
``query`` made by this trigger until the commands submitted as part of the same
``emitCommands`` call have completed. If your trigger is based on seeing
certain contracts, this can be a simple, effective way to prevent triggering it
multiple times.

The last tool you have at your disposal is the ``getCommandsInflight`` action
(`doc <https://docs.daml.com/triggers/api/Daml-Trigger.html#function-daml-trigger-getcommandsinflight-32524>`__),
which returns all of the commands this instance of the trigger runner has sent
and that have not yet been resolved (i.e. either committed or failed). You can
then build your own logic based on this list, the ACS, and possibly your own
trigger state.

Finally, do keep in mind that all of these mechanisms rely on internal state
from the trigger runner, which keeps track of which commands it has sent and
for which it's not seen a completion. They will all fail to deduplicate if that
internal state is lost, e.g. if the trigger runner is shut down and a new one
is started. As such, these deduplication mechanisms should be seen as an
optimization rather than a requirement for correctness. The Daml model should
be designed such that duplicated commands are either rejected (e.g. using keys
or relying on changing contract IDs) or benign.

Authorization
=============

When using Daml triggers against a Ledger with :doc:`request authorization </app-dev/authorization>`, you can
pass ``--access-token-file token.jwt`` to ``daml trigger`` which will
read the token from the file ``token.jwt``.

If you plan to run more than one trigger at a time, or triggers for more than
one party at a time, you may be interested in the
:doc:`/tools/trigger-service/index`.

When Not to Use Daml Triggers
=============================

Daml triggers deliberately only allow you to express automation that
listens for ledger events and reacts to them by sending commands to
the ledger.

Daml Triggers are not suited for automation that needs to interact
with services or data outside of the ledger. For those cases, you can
write a ledger client using the
:doc:`JavaScript bindings </app-dev/bindings-ts/index>`
running against the HTTP JSON API or the
:doc:`Java bindings</app-dev/bindings-java/index>` running against the
gRPC Ledger API.
