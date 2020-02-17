.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Your First Feature: Messaging Friends
*************************************

Let's dive into implementing a feature for our social network app.
From that we'll get a better idea of how to build DAML applications using our template.

Right now our app allows us to add and remove friends, but we can't communicate with them!
Let's fix that by adding a private messaging feature.
We will allow a user to send messages to a number of friends at once, and see all the messages that have been sent to them.
Of course we must make sure that no one can see messages that were not sent to them.
We will see that DAML lets us implement this in a direct and intuitive way.

There are two parts to building the messaging feature: the DAML code and the UI.
Let's start with adding to the DAML code, on which we will base our UI changes.

DAML Changes
============

The DAML code defines the *workflow* of the application.
This means: what interactions between users (or *parties*) are permitted by the system?
In the context of our feature, the question is: when is a user allowed to message another user?

The approach we'll take is: a user Bob can message another user Alice if Alice has added Bob as a friend.
Remember that friendships are single-directional in our app.
So Alice adding Bob as a friend means that she gives permission (or *authority*) for Bob to send her a message.

In DAML this workflow is represented as a new choice on the ``User`` contract.

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- SENDMESSAGE_BEGIN
  :end-before: -- SENDMESSAGE_END

Let's break this down.
The choice is ``nonconsuming`` because sending a message should not affect the existence of the ``User`` contract.
By convention, the choice returns the ``ContractId`` of the resulting ``Message`` contract (which we'll show next).
Next, the parameters to the choice are the sender (the party wishing to talk to the signatory of this ``User`` contract) and the message text.
The ``controller`` clause suggests that it is the ``sender`` who can exercise the choice.
Finally, the body of the choice simply creates the new ``Message`` with the sender, receiver and content.

Note that there is no explicit check in the choice that the ``sender`` is a friend of the user.
This is because the ``User`` contract is only ever visible to friends (the observers of the contract).

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

Feed Component
--------------

The feed component is fairly straight-forward: it queries all ``Message`` contracts and displays their contents as a list.
Here is the code for the entire component.

.. literalinclude:: code/ui-after/Feed.tsx
  :language: ts

The key point here is that for any particular user, the ``Message`` query yields exactly the messages that have been either written by or sent to that user.
This is due to how we modelled the signatories and observers in the ``Message`` template, and means we do not risk a privacy breach coming from the application code.

Message Edit Component
----------------------

In addition to the feed component, we need a component for composing messages and sending them using the appropriate choice on the ``User`` contract.

.. literalinclude:: code/ui-after/MessageEdit.tsx
  :language: ts

In this component we use React hooks for the message content and receiver.
You can see these used in the ``submitMessage`` function, called when the "Send" button is clicked.
The ``isSubmitting`` state is used to ensure that message requests are processed one at a time.
The result of each send is a new ``Message`` contract created, after which the form is cleared.

View Component
--------------------

The ``MainView`` component is the workhorse of this application which queries the ledger for data and houses the different subcomponents (e.g. friends, the network and our messaging components above).
To support the messaging components, we will need DAML React hooks for querying ``Message`` contracts and exercising the ``SendMessage`` choice on our ``User`` contract.

First import the generated Typescript code for the ``Message`` contract template, as well as our two new components.

.. literalinclude:: code/ui-after/MainView.tsx
  :language: ts
  :start-after: -- IMPORTS_BEGIN
  :end-before: -- IMPORTS_END

Then we declare the hooks themselves at the start of the component.

.. literalinclude:: code/ui-after/MainView.tsx
  :language: ts
  :start-after: -- HOOKS_BEGIN
  :end-before: -- HOOKS_END

The ``messagesResult`` tracks the state of ``Message`` contracts on the ledger, where we specify no restrictions on the query.
We extract the actual message data in ``messages``.
The ``exerciseSendMessage`` hook gives us a function to exercise the appropriate choice on our ``User``.
We wrap this in another ``sendMessage`` function which splits an input string into a list of parties and then exercises the choice, reporting to the user in the case of an error.

.. literalinclude:: code/ui-after/MainView.tsx
  :language: ts
  :start-after: -- SENDMESSAGE_BEGIN
  :end-before: -- SENDMESSAGE_END

Finally we can integrate our new messaging components into the main screen view.
In another segment we add the panel including our two new components: the ``MessageEdit`` and the ``Feed``.

.. literalinclude:: code/ui-after/MainView.tsx
  :language: html
  :start-after: -- MESSAGES_SEGMENT_BEGIN
  :end-before: -- MESSAGES_SEGMENT_END

You have now finished implementing your first end-to-end DAML feature!
Let's give the new functionality a spin.
We follow the instructions in "Running the app" to start up the new app.
