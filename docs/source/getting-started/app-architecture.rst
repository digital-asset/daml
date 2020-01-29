.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

App Architecture
****************

As you saw from playing around with our social network app, we have a basic interface and backend (the Sandbox) for adding and removing friends.
In this section we'll walk through the components of this system, positioning us to extend these components with added functionality in the next section.

The DAML Model
==============

The first thing to look at is the DAML code, located in the ``daml`` subdirectory.
There is only one short DAML module for this whole project, the user model in
``User.daml``.
Here is the first part of it.

.. literalinclude:: quickstart/code/daml/User.daml
  :language: daml
  :start-after: -- MAIN_TEMPLATE_BEGIN
  :end-before: -- MAIN_TEMPLATE_END

.. TODO Relax or omit ensure clause.

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
