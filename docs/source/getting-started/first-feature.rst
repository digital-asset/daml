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

The first addition we make is a template for a message contracts.
This is very simple, containing only the message content as well as the sending and receiving parties.

.. literalinclude:: quickstart/code/daml/Message.daml
  :language: daml
  :start-after: -- MESSAGE_BEGIN
  :end-before: -- MESSAGE_END

The sender is the signatory, the one who can create and archive the post, and the receivers are listed as observers of the contract.
This simple setup gives the same desirable behaviour as the ``User`` contracts discussed earlier: querying the ledger for messages will yield exactly those which have been sent to the current user (or which that user has written), and it is impossible to see any other messages.

Now we have defined what messages look like, we need a way to create them.
We implement this with a choice in the ``User`` template.
We didn't talk much about choices earlier, but these are essentially operations on contracts which can perform updates to the ledger.
In our case, we simply want to add an operation for a user to create a ``Message`` contract on the ledger, without performing any other updates.

.. literalinclude:: quickstart/code/daml/User.daml
  :language: daml
  :start-after: -- SENDMESSAGE_BEGIN
  :end-before: -- SENDMESSAGE_END

There are a few things to note in these few lines of code.
Firstly the ``nonconsuming`` keyword means that the ``SendMessage`` choice can be performed any number of times without affecting the ``User`` contract it is exercised on.
Second, we can see that the choice takes the content and receivers as arguments and returns the ``ContractId`` of the message that is created (see section on contract IDs).
Here the ``user`` party (defined in the ``User`` template data) is the *controller* of the choice, meaning that no one can send a message on behalf of a user.
The last line of the choice is the actual action that creates the ``Message`` contract.

.. TODO Refer to relevant sections to explain the concepts above.

Now let's see how to integrate this new functionality, which we've written in DAML, into the rest of our application code.

TypeScript Code Generation
==========================

Remember that we interface with our DAML code from the UI components using generated TypeScript classes.
Since we have changed our DAML code, we also need to rerun the TypeScript code generator.
Let's do this now by running::

  daml build
  daml daml2ts

in the ``create-daml-app/daml`` directory.

.. TODO Fiddle with daml2ts arguments later.

We should now have the updated TypeScript classes with equivalents of the ``SendMessage`` choice and ``Message`` template.
Let's implement our messaging feature in the UI!

Messaging UI
============

Our messaging feature has two parts: a form with inputs for selecting friends and composing the message text, and a "feed" of messages that have been sent to you.
Both parts will be implemented as React components that render on the main screen.

Feed Component
--------------

The feed component is fairly straight-forward: it queries all ``Message`` contracts and displays their contents as a list.
Here is the code for the entire component.

.. literalinclude:: quickstart/code/ui-after/Feed.tsx
  :language: ts

The key point here is that for any particular user, the ``Message`` query yields exactly the messages that have been either written by or sent to that user.
This is due to how we modelled the signatories and observers in the ``Message`` template, and means we do not risk a privacy breach coming from the application code.

Message Edit Component
----------------------

In addition to the feed component, we need a component for composing messages and sending them using the appropriate choice on the ``User`` contract.

.. literalinclude:: quickstart/code/ui-after/MessageEdit.tsx
  :language: ts

In this component we use React hooks for the message content and receivers.
You can see these used in the ``submitMessage`` function, called when the "Send" button is clicked.
The ``isSubmitting`` state is used to ensure that message requests are processed one at a time.
The result of each send is a new ``Message`` contract created, after which the form is cleared.

View Component
--------------------

The ``MainView`` component is the workhorse of this application which queries the ledger for data and houses the different subcomponents (e.g. friends, the network and our messaging components above).
To support the messaging components, we will need DAML React hooks for querying ``Message`` contracts and exercising the ``SendMessage`` choice on our ``User`` contract.

First import the generated Typescript code for the ``Message`` contract template, as well as our two new components.

.. literalinclude:: quickstart/code/ui-after/MainView.tsx
  :language: ts
  :start-after: -- IMPORTS_BEGIN
  :end-before: -- IMPORTS_END

Then we declare the hooks themselves at the start of the component.

.. literalinclude:: quickstart/code/ui-after/MainView.tsx
  :language: ts
  :start-after: -- HOOKS_BEGIN
  :end-before: -- HOOKS_END

The ``messageHook`` tracks the state of ``Message`` contracts on the ledger, where we specify no restrictions on the query.
We extract the actual message data in ``messages``.
The ``exerciseSendMessage`` hook gives us a function to exercise the appropriate choice on our ``User``.
We wrap this in another ``sendMessage``function which splits an input string into a list of parties and then exercises the choice, reporting to the user in the case of an error.

.. literalinclude:: quickstart/code/ui-after/MainView.tsx
  :language: ts
  :start-after: -- SENDMESSAGE_BEGIN
  :end-before: -- SENDMESSAGE_END

Finally we can integrate our new messaging components into the main screen view.
The first change is just reformatting the main screen to have a new messages panel in the right column.

.. literalinclude:: quickstart/code/ui-after/MainView.tsx
  :language: html
  :start-after: -- FORMATTING_BEGIN
  :end-before: -- FORMATTING_END

In the new column we add the panel including our two new components: the ``MessageEdit`` above and ``Feed`` below.

.. literalinclude:: quickstart/code/ui-after/MainView.tsx
  :language: html
  :start-after: -- MESSAGEPANEL_BEGIN
  :end-before: -- MESSAGEPANEL_END

You have now finished implementing your first end-to-end DAML feature!
Let's give the new functionality a spin.
We follow the instructions in "Running the app" to start up the new app.
