.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

A Simple Chatbot
################

So far, the application is entirely driven by users interacting with the
browser. Sometimes, your application design will require that some actions are
taken automatically, or some users will want to automate their own behaviour on
the ledger. This can be done through various APIs and using various languages,
but in this section we'll give you a quick tour of the simplest route to
automation: :doc:`Daml Triggers </triggers/index>`.

Objective
*********

We're going to build a chatbot that answers evry message with

  "Please, tell me more about that."

That should fool anyone and pass the Turing test, easily.

A No-Op Trigger
***************

In order to get a feel for how triggers work, we'll first build one that does
nothing. Fire up `daml studio` if you don't have it open already, and edit a
new file under `daml/ChatBot.daml`. Fill it with (we'll walk through that in a
minute)::

    module ChatBot where

    import qualified Daml.Trigger as T

    autoReply : T.Trigger ()
    autoReply = T.Trigger
      { initialize = pure ()
      , updateState = \_ -> pure ()
      , rule = \_ -> do
          debug "triggered"
          pure ()
      , registeredTemplates = T.AllInDar
      , heartbeat = None
      }

If you still have ``daml start`` running, you can just press ``r`` (followed by
``Enter`` on Windows) in the terminal running it. Otherwise, start it by
opening a new terminal and typing ``daml start``.

This will rebuild all of your Daml code, including the trigger, but will not
run it. To start the trigger, run::

    daml trigger --dar .daml/dist/create-daml-app-0.1.0.dar \
                 --trigger-name ChatBot:autoReply \
                 --ledger-host localhost \
                 --ledger-port 6865 \
                 --ledger-party "bob"

from the root of the project.

Once the trigger has started, you can log in as ``alice`` and ``bob`` in
separate browser windows, just like you did
:doc:`while developing the messaging feature <first-feature>`,
and you should see the trigger console printing ``triggered`` each time you
send a message between ``alice`` and ``bob``.

Let's take a closer look at that trigger code. For this tutorial, we will
ignore the three options ``initialize``, ``updateState`` and ``heartbeat``;
refer to the :doc:`trigger documentation </triggers/index>` for more
information. The ``registeredTemplate`` field, being set to ``T.AllInDar``
here, instructs the Daml trigger mechanism to fire up for every single template
we know about. This means that the trigger will execute every time the given
party (in our case ``bob``) sees a contract being created or archived. For that
reason, you may have noticed that the trigger also printed ``triggered`` when
``bob`` logged in for the first time (creation of his ``User`` contract) and
when ``alice`` followed ``bob`` (creation of ``alice``'s ``User`` contract with
``bob`` as an observer). It would also trigger when ``bob``, or anyone ``bob``
is following, follows someone, or when anyone follows ``bob``. And of course
whenever a new ``Message`` that ``bob`` can see is created, i.e. when ``bob``
either sends or receives a message.

Finally, the ``rule`` field specifies what to do when the trigger executes. In
this case all we're doing is printing a debug statement, but we'll soon change
that to do a bit more.

Updating ``Message``
********************

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
``Message`` template in ``daml/User.daml`` to look like::

    template Message with
      sender: Party
      receiver: Party
      content: Text
      receivedAt: Time
    where
      signatory sender, receiver

This should result in Daml Studio reporting an error in the ``SendMessage``
choice, as it now needs to set the ``receivedAt`` field. Here is the updated
code for ``SendMessage``::

        nonconsuming choice SendMessage: ContractId Message with
            sender: Party
            content: Text
          controller sender
          do
            assertMsg "Designated user must follow you back to send a message" (elem sender following)
            now <- getTime
            create Message with sender, receiver = username, content, receivedAt = now

The ``getTime`` action (`doc </daml/stdlib/Prelude.html#function-da-internal-lf-gettime-99334>`_)
returns the time at which the command was received by the sandbox, which should
be good enough for our purposes, though bear in mind that it may not be
suitable for some use-cases.

Now that we have a field to sort on, and thus a way to identify the *latest*
message, we can turn our attention back to our trigger code.

AutoReply
*********

Open up the trigger code again (``daml/ChatBot.daml``), and change it to::

    module ChatBot where

    import qualified Daml.Trigger as T
    import qualified User
    import qualified DA.List.Total as List

    autoReply : T.Trigger ()
    autoReply = T.Trigger
      { initialize = pure ()
      , updateState = \_ -> pure ()
      , rule = \p -> do
          message_contracts <- T.query @User.Message
          let messages = map snd message_contracts
          debug $ "Messages so far: " <> show (length messages)
          let lastMessage = List.last $ List.sortOn (.receivedAt) messages
          debug $ "Last message: " <> show lastMessage
          case lastMessage of
            Some m ->
              if m.receiver == p
              then do
                users <- T.query @User.User
                debug users
                let isSender = (\user -> user.username == m.sender)
                let replyTo = List.head $ filter (\(_, user) -> isSender user) users
                case replyTo of
                  None -> pure ()
                  Some (sender, _) -> do
                    T.dedupExercise sender (User.SendMessage p "Please, tell me more about that.")
              else pure ()
            None -> pure ()
      , registeredTemplates = T.AllInDar
      , heartbeat = None
      }

Refresh ``daml start`` by pressing ``r`` (followed by ``Enter`` on Windows) in
its terminal, then kill (CTRL-C) the trigger and start it again. Play a bit
with ``alice`` and ``bob`` in your browser again, to get a feel for how the
trigger works. Watch both the messages in-browser and the debug statements
printed by the trigger runner.

Walking through the rule code line by line gives:

- We use the ``query`` function to get all of the ``Message`` templates visible
  to the current party (``p``; in our case this will be ``bob``). Per the
  `documentation </triggers/api/Daml-Trigger.html#function-daml-trigger-query-2759>`_,
  this returns a list of tuples (contract id, payload).
- We then `map </daml/stdlib/Prelude.html#function-ghc-base-map-40302>`_ the
  `snd </daml/stdlib/Prelude.html#function-da-internal-prelude-snd-86578>`_
  function on the result to get only the payloads, i.e. the actual data of the
  messages we can see.
- We print, as a ``debug`` message, the number of messages we can see.
- On the next line, we sort the messages on the ``receivedAt`` field
  (`sortOn </daml/stdlib/DA-List.html#function-da-list-sorton-1185>`_), then take the
  `last </daml/stdlib/DA-List-Total.html#function-da-list-total-last-89790>`_
  one (sorting order is ascending).
- We then print another debug message, this time printing the message our code
  has identified as "the last message visible to the current party". If you run
  this, you'll see that ``lastMessage`` is actually a ``Optional Message``. This
  is because the `last </daml/stdlib/DA-List-Total.html#function-da-list-total-last-89790>`_
  function will return the last element from a list *if* the list has at least
  one element, but it needs to still do something sensible if the list is
  empty, like returning ``None``.
- We then check what we actually have in ``lastMessage``.
- If there is some message, then we need to check whether the message has been
  sent *to* or *by* the party running the trigger (with the current Daml model,
  it has to be one or the other, as messages are only visible to the sender and
  receiver).
- If the message was sent *to* the current party, then we want to reply to it.
  In order to do that, we need to find the ``User`` contract for the sender. We
  start by getting the list of all ``User`` contracts we know about, which will
  be all users who follow the party running the trigger. As for ``Message``
  contracts earlier, the result of ``query @User`` is going to be a list of
  tuples with (contract id, payload). The big difference is that this time we
  actually want to keep the contract ids, as that is what we'll use to send a
  message bback.
- We print the list of users we just fetched, as a debug message.
- We create a function to identify the user we are looking for.
- We get the user contract by applying our ``isSender`` function as a
  `filter </daml/stdlib/Prelude.html#function-da-internal-prelude-filter-27394>`_
  on the list of users, and then taking the
  `head <daml/stdlib/DA-List-Total.html#function-da-list-total-head-74336>`_
  of that list, i.e. its first element.
- Just like  ``last``, ``head`` will return an ``Optional a``, so the next step
  is to check whether we have actually found the relevant ``User`` contract. In
  most cases we should find it, but remember that users can send us a message if
  *we* follow *them*, whereas we can only answer if *they* follow *us*.
- If ``replyTo`` is ``None``, then the sender is not, or no longer, following
  us, and we can't respond. In that case we just do nothing, which we indicate by
  returning ``pure ()``.
- If we did find some ``User`` contrac to reply to, we extract the
  corresponding contract id (first element of the tuple) and discard the
  payload (second element), and we
  `exercise <triggers/api/Daml-Trigger.html#function-daml-trigger-dedupexercise-37617>`_
  the ``SendMessage`` choice, passing in the current party ``p`` as the sender.
- If the receiver of the last message was not the current party, then the last
  message was sent by it and we don't need to reply to ourselves, so we do
  nothing.
- Finally, if we had not found any message at all, and thus ``lastMessage`` was
  ``None``, we have nothing to do.

Next Steps
**********

If you haven't already, you should go and read our
:doc:`Introduction to the Daml language </daml/intro/0_Intro>` to learn the
Daml language itself. Afterwards, you could try to extend this trigger with the
following features:

- Make it fully automated by automatically following anyone who follows the
  current party.
- Respond with more varied messages, perhaps including a few words from the
  message we are responding to.
- Be able to follow multiple conversations. Right now the trigger only reacts
  to the latest message it received, but it really should react to the latest
  message *per Party we're having a conversation with*.
- For a more challenging mini-project, you could add the ability to unfollow
  users to the UI and the Daml model, and then use the trigger state (see
  :doc:`trigger documentation </triggers/index>` for details) to keep track of
  users that have unfollowed you in the past, and send them ``"Welcome back!"``
  as a message when they follow you again.

Of course, if you have other ideas of cool things to implement with Daml, those
are fine too. Either way, do not hesitate to ask for help on
`the Daml forum <https://discuss.daml.com>`_ if you get stuck at any point.

In the next part of this tutorial, we show you a way to setup complete
end-to-end tests for a full-stack Daml application.
