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

The core data is at the start of the ``User`` contract template.

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- MAIN_TEMPLATE_BEGIN
  :end-before: -- MAIN_TEMPLATE_END

There are two important aspects here:

1. The data definition (a *schema* in database terms), describing the data stored with each user contract.
In this case it is an identifier for the user and their current list of friends.
Both fields use the built-in ``Party`` type which lets us use them in the following clauses.

2. The signatories and observers of the contract.
The signatories are the parties authorized to create new versions of the contract or archive the contract.
In this case only the user has those rights.
The observers are the parties who are able to view the contract on the ledger.
In this case all friends of a user are able to see the user contract.

Let's say what the ``signatory`` and ``observer`` clauses mean in our app more concretely.
A user Alice can see another user Bob in the network only when Alice is a friend in Bob's user contract.
For this to be true, Bob must have previously added Alice as a friend (i.e. updated his user contract), as he is the sole signatory on his user contract.
If not, Bob will be invisible to Alice.

We can see some concepts here that are central to DAML, namely *privacy* and *authorization*.
Privacy is about who can *see* what, and authorization is about who can *do* what.
In DAML we must answer these questions upfront, as they fundamentally change the design of an application.

The last thing we'll point out about the DAML model for now is the operation to add friends, called a *choice* in DAML.

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- ADDFRIEND_BEGIN
  :end-before: -- ADDFRIEND_END

DAML contracts are *immutable* (can not be changed in place), so they must be updated by archiving the current instance and creating a new one.
That is what the ``AddFriend`` choice does: after checking some preconditions, it creates a new user contract with the new friend added to the list.
The ``choice`` syntax automatically includes the archival of the current instance.

.. TODO Update depending on consuming/nonconsuming choice.

Next we'll see how our DAML code is reflected and used on the UI side.

TypeScript Code Generation
==========================

The user interface for our app is written in `TypeScript <https://www.typescriptlang.org/>`_.
TypeScript is a variant of Javascript that provides more support in development through its type system.

In order to build an application on top of DAML, we need a way to refer to the DAML template and choices in TypeScript.
We do this using a DAML to TypeScript code generation tool in the DAML SDK.

To run code generation, we first need to compile the DAML model to an archive format (with a ``.dar`` extension).
Then the command ``daml codegen ts`` takes this file as argument to produce a number of TypeScript files in the specified location.

    daml build
    daml codegen ts .daml/dist/create-daml-app-0.1.0.dar -o daml-ts/src

We now have TypeScript types and companion objects in the ``daml-ts`` workspace which we can use from our UI code.
We'll see that next.

The UI
======

On top of TypeScript, we use the UI framework `React <https://reactjs.org/>`_.
React helps us write modular UI components using a functional style - a component is rerendered whenever one of its inputs changes - with careful use of global state.

We can see the latter in the way we handle ledger state throughout the application code.
For this we use a state management feature in React called `Hooks <https://reactjs.org/docs/hooks-intro.html>`_.
You can see the capabilities of the DAML React hooks in ``create-daml-app/ui/src/daml-react-hooks/hooks.ts``.
For example, we can query the ledger for all visible contracts (relative to a particular user), create contracts and exercise choices on contracts.

.. TODO Update location to view DAML react hooks API

Let's see some examples of DAML React hooks.

.. literalinclude:: code/ui-before/MainView.tsx
  :start-after: -- HOOKS_BEGIN
  :end-before: -- HOOKS_END

This is the start of the component which provides data from the current state of the ledger to the main screen of our app.
The three declarations within ``MainView`` all use DAML hooks to get information from the ledger.
For instance, ``allUsers`` uses a catch-all query to get the ``User`` contracts on the ledger.
However, the query respects the privacy guarantees of a DAML ledger: the contracts returned are only those visible to the currently logged in party.
This explains why you cannot see *all* users in the network on the main screen, only those who have added you as a friend (making you an observer of their ``User`` contract).

.. TODO You also see friends of friends; either explain or prevent this.
