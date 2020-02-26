.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Your First Feature
******************

Let's dive into implementing a new feature for our social network app.
This will give us a better idea how to develop DAML applications using our template.

At the moment, our app lets us add friends to our network, but we have no way to communicate with them!
Let's fix that by adding a *private messaging* feature.
This should let a user send messages to any chosen friend, and see all messages that have been sent to them.

This feature should also respect *authorization* and *privacy*.
This means:
    1. You cannot send a message to someone unless they have added you as a friend.
    2. You cannot see a message unless it was sent specifically to you.
We will see that DAML lets us implement these guarantees in a direct and intuitive way.

There are two parts to building the messaging feature: the DAML model and the UI.
As usual, we must start with the DAML model and base our UI changes on top of that.

DAML Changes
============

As mentioned in the :doc:`architecture <app-architecture>` section, the DAML code defines the data and *workflow* of the application.
The workflow aspect refers to the interactions between parties that are permitted by the system.
In the context of a messaging feature, these are essentially the authorization and privacy concerns listed above.

For the authorization part, we take the following approach: a user Bob can message another user Alice exactly when Alice has added Bob as a friend.
When Alice adds Bob as a friend, she gives permission or *authority* to Bob to send her a message.
It is important to remember that friendships can go in a single direction in our app.
This means its possible for Bob to message Alice without Alice being able to message him back!

To implement this workflow, we add a new choice to the ``User`` template.
Copy and paste the following code at the end of the ``User.daml`` file.
(Take care to match the indentation of the ``AddFriend`` choice, as the DAML parser pays attention to it.)

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- SENDMESSAGE_BEGIN
  :end-before: -- SENDMESSAGE_END

As with the ``AddFriend`` choice, there are a few aspects to note here.

    - The choice is ``nonconsuming`` because sending a message should not consume the ``User`` contract.
    - By convention, the choice returns the ``ContractId`` of the resulting ``Message`` contract (which we'll show next).
    - The parameters to the choice are the ``sender`` and ``content`` of this message.
    - The ``controller`` clause suggests that it is the ``sender`` who can exercise the choice.
    - The body of the choice first ensures that the sender is a friend of the user and then creates the ``Message`` contract with the ``receiver`` being the signatory of the ``User`` contract.

If you've added the ``SendMessage`` choice correctly, you should an error in the bottom *Problems* pane of Visual Studio Code that ``Message`` is undefined.
Let's add the ``Message`` template at the bottom of the same file (copy it from below).

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- MESSAGE_BEGIN
  :end-before: -- MESSAGE_END

This template is very simple: it contains data but no choices.
The ``signatory`` clause is the interesting part: both the sender and receiver are signatories on the template.
This enforces the fact that creation and archival of ``Message`` contracts must be authorized by both parties.

This completes the workflow for messaging in our app.
Now let's integrate this functionality into the UI.

TypeScript Code Generation
==========================

Remember that we interface with the DAML model from the UI components using generated TypeScript.
Since we have changed our DAML code, we also need to rerun the TypeScript code generator.
Let's do this by running::

  daml build
  daml codegen ts .daml/dist/create-daml-app-0.1.0.dar -o daml-ts/src

As the TypeScript code is generated into the separate ``daml-ts`` workspace which the UI depends on, we need to rebuild the workspaces from the root ``create-daml-app`` folder using::

  yarn workspaces run build

We should now have an up-to-date TypeScript interface to our DAML model, in particular to the ``Message`` template and ``SendMessage`` choice.

We can now implement our messaging feature in the UI!

Messaging UI
============

The UI for messaging will consist of a new *Messages* panel in addition to the *Friends* and *Network* panel.
This panel will have two parts:

    1. A list of messages you've received with their senders.
    2. A form with a dropdown menu for friend selection and a text field for composing the message.

We will implement each part as a React component, which we'll name ``MessageList`` and ``MessageEdit`` respectively.
Let's start with the simpler ``MessageList``.

MessageList Component
---------------------

The goal of the ``MessageList`` component is to query all ``Message`` contracts where the ``receiver`` is the current user, and display their contents and senders in a list.
The entire component is shown below.
You should copy this into a new ``MessageList.tsx`` file in ``ui/src/components``.

.. TODO Include file in template with placeholder for component logic.

.. literalinclude:: code/ui-after/MessageList.tsx

First we get the ``username`` of the current user with the ``useParty`` hook.
Then ``messagesResult`` gets the stream of all ``Message`` contracts where the ``receiver`` is our ``username``.
The streaming aspect means that we don't need to reload the page when new messages come in.
We extract the *payload* of every ``Message`` contract (the data as opposed to metadata like the contract ID) in ``messages``.
The rest of the component simply constructs a React ``List`` element with an item for each message.

There is one important point about privacy here.
No matter how we write our ``Message`` query in the UI code, it is impossible to break the privacy rules given by the DAML model.
That is, it is impossible to see a ``Message`` contract of which you are not the ``sender`` or the ``receiver`` (the only parties that can observe the contract).
This is a major benefit of writing apps on DAML: the burden of ensuring privacy and authorization is confined to the DAML model.

MessageEdit Component
---------------------

Next we need the ``MessageEdit`` component to compose and send messages to selected friends.
Again we show the entire component here; you should copy this into a new file in ``ui/src/components``.

.. TODO Include file in template with placeholder for component logic.

.. literalinclude:: code/ui-after/MessageEdit.tsx

You will first notice a ``Props`` type near the top of the file with a single ``friends`` field.
A *prop* in React is an input to a component; in this case a list of users from which to select the message receiver.
The prop will be passed down from the ``MainView`` component, reusing the work required to query users from the ledger.
You can see this ``friends`` field bound at the start of the ``MessageEdit`` component.

We use the React ``useState`` hook to get and set the current choices of message ``receiver`` and ``content``.
The DAML-specific ``useExerciseByKey`` hook gives us a function to both look up a ``User`` contract and exercise the ``SendMessage`` choice on it.
The call to ``exerciseSendMessage`` in ``sendMessage`` looks up the ``User`` contract with the receiver's username and exercises ``SendMessage`` with the appropriate arguments.
The ``sendMessage`` wrapper reports potential errors to the user, and ``submitMessage`` additionally uses the ``isSubmitting`` state to ensure message requests are processed one at a time.
The result of a successful call to ``submitMessage`` is a new ``Message`` contract created on the ledger.

The return value of this component is the React ``Form`` element.
This contains a dropdown menu to select a receiver from the ``friends``, a text field for the message content, and a *Send* button which triggers ``submitMessage``.

There is again an important point here, in this case about how *authorization* is enforced.
Due to the logic of the ``SendMessage`` choice, it is impossible to send a message to a user who has not added you as a friend (even if you could somehow access their ``User`` contract).
The assertion that ``sender `elem` friends`` in ``SendMessage`` ensures this: no mistake or malice by the UI programmer could breach this.

MainView Component
------------------

The ``MainView`` component composes the different subcomponents (for our friends, the network and the messaging components above) to build the full app view.

We first import our two new components.

.. literalinclude:: code/ui-after/MainView.tsx
  :language: typescript
  :start-after: // IMPORTS_BEGIN
  :end-before: // IMPORTS_END

Then we can integrate our new messaging components into the main screen view.
In another segment we add the panel including our two new components: the ``MessageEdit`` and the ``MessageList``.

.. literalinclude:: code/ui-after/MainView.tsx
  :start-after: // MESSAGES_SEGMENT_BEGIN
  :end-before: // MESSAGES_SEGMENT_END

This wraps up the implementation of your first end-to-end DAML feature!
Let's give the new functionality a spin.
Follow the instructions in "Running the app" to start up the new app.
