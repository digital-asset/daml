.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _new-quickstart:

New Quickstart guide
####################

The goal of this tutorial is to get you up and running with full-stack, DAML-driven app development. We will provide you with the template for a miniature social networking app, get you writing your first end-to-end feature and finally deploy your new app to a persistent ledger.

By the end of the tutorial, you should have an idea of what DAML contracts and ledgers are, how the UI interacts with them, and how you might solve a potential use case with a DAML-driven solution. We do not aim to give a comprehensive guide to all DAML concepts and tools; for that, see the later sections of the documentation. With that, let's get started!

.. TODO: reference specific sections of docs instead of saying "later sections".

Prerequisites
*************

If you haven't already, see :doc:`installation` for the DAML SDK and VSCode development environment.

You will also need some common software tools to build and interact with the template project.

- `Git <https://git-scm.com/>`_ version control system
- `Yarn <https://yarnpkg.com/>`_ package manager for Javascript
- A terminal application for command line interaction


Running the app
***************

First off, open a terminal and clone the template project using
``git clone https://github.com/digital-asset/create-daml-app.git``

We'll start by getting the app up and running, and then explain the different components which we will later extend.
To build the app, move to the project folder
``cd create-daml-app``
and use Yarn to install the dependencies and build the project::

    yarn install
    yarn workspaces run build

If you see ``Compiled successfully.`` in the output then everything is working as expected.

.. TODO: Give instructions for possible failures.

Now we can run the app in two steps.
You'll need two terminal windows running for this.

In one terminal, at the root of the ``create-daml-app`` directory, run the script::

    ./daml-start.sh

This compiles the DAML component of the project and starts a DAML *Sandbox* ledger for the app.
The ledger in this case is stored in the Sandbox application memory; it is not a persistent storage but is useful for testing and development.
We will let the Sandbox continue to run in order to serve requests from the UI, which result in changes to the in-memory ledger.

In a second terminal, navigate to the ``create-daml-app/ui`` folder and run::

    yarn start

This runs the UI application connected to the already running Sandbox.
The command should automatically open a window in your browser at http://localhost:3000.
If it doesn't, just open that link in any web browser.

At this point you should see the login page for the social network.

.. TODO: Screenshot

Enter a user name of your choice and click the calculator icon next to the password field to generate a password token.
(We do not have proper authentication in this app for simplicity.)
Once you click "Sign up", you can see a screen with panels for your friends and the entire social network.
Initially these are both empty as you don't have any friends yet!
Go ahead and add some using the form (and remove them using the cross icons if you change your mind).

Now let's grow the network. Log out and sign up using the name of one of your friends.
Let's say your name is Alice and your friend's name is Bob.
Bob should now see Alice in the network (since she added him as a friend) and he is able to add her back.
Note that in this app, friendships can be added in one direction at a time, similar to how "followers" work in Twitter.

Add a few more friends as Bob, and play around a bit more by logging in as different users and adding/removing friends from the network.
This should give you a idea of the app's functionality.


Dissecting the app
******************

If you've ever used a social media app before, you're probably thinking that ours is pretty lame!
It's definitely lacking some features, which we'll work on shortly.
However there is already a lot going on under the hood of this basic app.
So let's take a look at the components we have and what they do.

The DAML Model
==============

Perhaps the best place to start looking is in the ``daml`` subdirectory.
Let's look at a snippet of ``User.daml``.

.. literalinclude:: quickstart/code/daml/User.daml
  :language: daml
  :start-after: -- MAIN_TEMPLATE_BEGIN
  :end-before: -- MAIN_TEMPLATE_END

.. TODO Relax or omit ensure clause.

This is a DAML contract *template* which describes what users of our app.
Since we are developing for a distributed ledger, all data are represented as immutable contracts.
There are two main parts here:

1. The data definition, or schema, which prescribes the data that is stored with each user contract.
In this case it is simply the user's party identifier, and the list of the user's friends.

2. The signatories and observers of the user contract.
The signatories - the single user in this case - are the parties authorized to make changes to the contract (which we'll see next).
The observers - in this case the user's friends - are the parties who can see the contract on the ledger.

The ``observer`` clause explains something about the behaviour of our app.
A user, say Alice, can only see another user Bob in the network if Alice is listed as Bob's friend.
Otherwise Bob's user contract is invisible to Alice.

These 6 lines of code are saying a lot!
A key point is that privacy and authorization are central to the way we write code in DAML and the resulting app behaviour.
This is a radical change to how apps are written usually - with privacy and security as an afterthought - and is key to writing secure distributed applications.

The only other thing we'll say about the User template for now is that it has two operations - called *choices* - to add or remove a friend.
As DAML contracts are immutable, exercising one of these choices in fact *archives* the existing user contract and creates a new one with the modified data.
So let's see how user contracts are controlled through the UI.

The UI
======

Our UI is written using `React <https://reactjs.org/>`_ and `TypeScript <https://www.typescriptlang.org/>`_.
React helps us write modular UI components and TypeScript is a variant of Javascript that gives us typechecking support during development.

The interesting thing is how we interact with the DAML ledger from the UI, specifically through React.
One can think of the ledger as a global state that we read and write from different components of the UI.
Since React usually promotes all data being passed as arguments (called *props*) to different components, we use a state management feature called `Hooks <https://reactjs.org/docs/hooks-intro.html>`_.
You can see the capabilities of the DAML React hooks in ``create-daml-app/ui/src/daml-react-hooks/hooks.ts``.
For example, we can query the ledger for all visible contracts (relative to a particular user), create contracts and exercise choices on contracts.

Let's see some examples of DAML React hooks.

.. literalinclude:: quickstart/code/ui-before/MainView.tsx
  :language: ts
  :start-after: -- HOOKS_BEGIN
  :end-before: -- HOOKS_END

This is the start of the component which provides data from the current state of the ledger to the main screen of our app.
The three declarations within ``MainView`` all use DAML hooks to get information from the ledger.
For instance, ``allUsers`` uses a catch-all query to get the ``User`` contracts on the ledger.
However, the query respects the privacy guarantees of a DAML ledger: the contracts returned are only those visible to the currently logged in party.
This explains why you cannot see *all* users in the network on the main screen, only those who have added you as a friend (making you an observer of their ``User`` contract).

.. TODO You also see friends of friends; either explain or prevent this.

There is one more technicality to explain before we can dive into building a new feature for our app.
In the above example we refer to the ``User`` template in our Typescript code (the React component).
However we wrote our template in DAML, not Typescript.
Therefore we need a way to bridge the gap and allow us access to DAML templates (and data types they depend on) from the UI code.

.. TODO How do we run daml2ts? Hopefully it will run automatically in a DAML watch command, or be shipped with the SDK and called through the assistant.

Our solution to this is a simple code generation tool called ``daml2ts``.
This is shipped with the SDK and can be run with ``daml daml2ts``.
The tool reads a compiled DAML project and generates a Typescript file corresponding to each DAML source file.
We won't show the generated code here as it simply contains Typescript equivalents of the data structures used in the DAML templates (it does not include equivalents for signatories, observers or other DAML-specific constructs).
With this tool to help bridge the gap between our DAML code and the UI, we can get started on our first full-stack DAML feature!


Your First Feature: Private Messaging
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
  :start-after: -- IMPORT_BEGIN
  :end-before: -- IMPORT_END

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
  :language: ts
  :start-after: -- FORMATTING_BEGIN
  :end-before: -- FORMATTING_END

In the new column we add the panel including our two new components: the ``MessageEdit`` above and ``Feed`` below.

.. literalinclude:: quickstart/code/ui-after/MainView.tsx
  :language: ts
  :start-after: -- MESSAGEPANEL_BEGIN
  :end-before: -- MESSAGEPANEL_END

You have now finished implementing your first end-to-end DAML feature!
Let's give the new functionality a spin.
We follow the instructions in "Running the app" to start up the new app.
