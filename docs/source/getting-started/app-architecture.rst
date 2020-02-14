.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

App Architecture
****************

In this section we'll look at the different components of our social network app.
The goal is to familiarise you enough to feel comfortable extending the code with a new feature in the next section.

There are two main components in the code - the DAML model and the React/TypeScript frontend - with generated TypeScript code to bridge the two.
Let's start by looking at the DAML model, as this sets the core logic of the application.

The DAML Model
==============

Using VSCode (or a code editor of your choice), navigate to the ``daml`` subdirectory.
There is a single DAML file called ``User.daml`` with the model for users of the app.

The core data is in the ``User`` contract template.

.. literalinclude:: quickstart/code/daml/User.daml
  :language: daml
  :start-after: -- MAIN_TEMPLATE_BEGIN
  :end-before: -- MAIN_TEMPLATE_END

This is a DAML contract *template* describing the data for users of our app.
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

.. TODO Update above depending on consuming/nonconsuming nature.

Now let's see how our DAML code gets reflected and then used on the UI side.

TypeScript Code Generation
==========================

The user interface for our app is written in a variant of Javascript called TypeScript.
The main feature of TypeScript is a rich type system which gives us more support through type checking during development.

Of course, we need a way to refer to our DAML model (template and choices) in our TypeScript code.
This is where we have a code generation tool come in to play.

The command ``daml codegen ts`` takes as argument a DAR file (a compiled form of our DAML model) and produces a number of corresponding TypeScript types and objects.
This is a crucial bridge to programming the UI around our DAML.

The commands to run code generation (or "codegen") is::

    daml build
    daml codegen ts .daml/dist/create-daml-app-0.1.0.dar -o daml-ts/src

The first command builds the ``create-daml-app-0.1.0.dar`` DAR file.
The ``codegen`` command translates the DAML into a series of TypeScript files in the ``daml-ts/src`` directory.
These definitions can then be used in the UI code, as we'll see next.

The UI
======

Our UI is written using `React <https://reactjs.org/>`_ and `TypeScript <https://www.typescriptlang.org/>`_.
React helps us write modular UI components through the judicious use of both state and "props" (arguments passed to components).

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
