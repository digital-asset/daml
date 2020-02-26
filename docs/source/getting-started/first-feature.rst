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

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- SENDMESSAGE_BEGIN
  :end-before: -- SENDMESSAGE_END

Let's break this down.
The choice is ``nonconsuming`` because sending a message should not affect the existence of the ``User`` contract.
By convention, the choice returns the ``ContractId`` of the resulting ``Message`` contract (which we'll show next).
The parameters to the choice are the sender (the party wishing to talk to the signatory of this ``User`` contract) and the message text.
The ``controller`` clause suggests that it is the ``sender`` who can exercise the choice.
Finally, the body of the choice makes sure that the sender is a friend of the user and then creates the ``Message`` with the sender, receiver and content.

Now let's see the ``Message`` contract template.
This is very simple - data and no choices - as well as the ``signatory`` declaration.

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- MESSAGE_BEGIN
  :end-before: -- MESSAGE_END

Note that we have two signatories on the ``Message`` contract: both the sender and receiver.
This enforces the fact that the contract creation (and archival) must be authorized by both parties.

Now we've specified the workflow of sending messages, let's integrate the functionality into our app.

TypeScript Code Generation
==========================

Remember that we interface with our DAML code from the UI components using the generated TypeScript.
Since we have changed our DAML code, we also need to rerun the TypeScript code generator.
Let's do this now by running::

  daml build
  daml codegen ts .daml/dist/create-daml-app-0.1.0.dar -o daml-ts/src

As the TypeScript code is generated into the separate ``daml-ts`` workspace on which the UI depends, we need to rebuild the workspaces from the root directory using::

  yarn workspaces run build

We should now have the updated TypeScript code with equivalents of the ``Message`` template and ``SendMessage`` choice.

Now let's implement our messaging feature in the UI!

Messaging UI
============

Our messaging feature has two parts: a form with inputs for selecting friends and composing the message text, and a "feed" of messages that have been sent to you.
Both parts will be implemented as React components that render on the main screen.

MessageList Component
---------------------

The MessageList component is fairly straight-forward: it queries all ``Message`` contracts and displays their contents as a list.
Here is the code for the entire component.

.. literalinclude:: code/ui-after/MessageList.tsx

The ``messagesResult`` tracks the state of ``Message`` contracts on the ledger, where we specify no restrictions on the query.
We extract the actual message data in ``messages``.
Note that for any particular user, the ``Message`` query yields exactly the messages that have been either written by or sent to that user.
This is due to how we modelled the signatories and observers in the ``Message`` template, and means we do not risk a privacy breach coming from the application code.

Message Edit Component
----------------------

In addition to the feed component, we need a component for composing messages and sending them using the appropriate choice on the ``User`` contract.

.. literalinclude:: code/ui-after/MessageEdit.tsx

In this component we use React hooks to track the current choice of message receiver and content.
The ``exerciseSendMessage`` hook gives us a function to exercise the appropriate choice on our ``User``.
We wrap this in the ``sendMessage`` function to report potential errors to the user, and then the ``submitMessage`` function, called when the "Send" button is clicked.
The ``isSubmitting`` state is used to ensure that message requests are processed one at a time.
The result of each send is a new ``Message`` contract created on the ledger.

View Component
--------------------

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
