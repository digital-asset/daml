.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

App Architecture
****************

In this section we'll look at the different components of our social network app.
The goal is to familiarise you enough to feel comfortable extending the code with a new feature in the next section.

There are two main components: the DAML model and the React/TypeScript frontend.
We generate TypeScript code to bridge the two.
Let's start by looking at the DAML model, which defines the core logic of the application.

The DAML Model
==============

In your terminal, navigate to the root ``create-daml-app`` directory and run::

  daml studio

This should open the Visual Studio Code editor at the root of the project.
(You may get a new tab pop up with release notes for the latest SDK - just close this.)
Using the file *Explorer* on the left sidebar, navigate to the ``daml`` folder and double-click on the ``User.daml`` file.
This models the data and workflow for users of the app.
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
The signatories are the parties whose authorization is required to create or archive instances of the contract template, in this case the user herself.
The observers are the parties who are able to view the contract on the ledger.
In this case all friends of a user are able to see the user contract.

Let's say what the ``signatory`` and ``observer`` clauses mean in our app more concretely.
A user Alice can see another user Bob in the network only when Alice is a friend in Bob's user contract.
For this to be true, Bob must have previously added Alice as a friend, as he is the sole signatory on his user contract.
If not, Bob will be invisible to Alice.

Here we see two concepts that are central to DAML: *authorization* and *privacy*.
Authorization is about who can *do* what, and privacy is about who can *see* what.
In DAML we must answer these questions upfront, as they fundamentally change the design of an application.

The last thing we'll point out about the DAML model for now is the operation to add friends, called a *choice* in DAML.

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- ADDFRIEND_BEGIN
  :end-before: -- ADDFRIEND_END

DAML contracts are *immutable* (can not be changed in place), so the only way to "update" one is to archive it and create a new instance.
That is what the ``AddFriend`` choice does: after checking some preconditions, it archives the current user contract and creates a new one with the extra friend added to the list.

There is some boilerplate to set up the choice (full details in the :doc:`DAML reference </daml/reference/choices>`):

    - We make contract archival explicit by marking the choice as ``nonconsuming`` and then calling ``archive self`` in the body (choices which aren't ``nonconsuming`` archive or *consume* the contract implicitly).
    - The return type is ``ContractId User``, a reference to the new contract for the calling code.
    - The new ``friend: Party`` is passed as an argument to the choice.
    - The ``controller``, the party able to exercise the choice, is the one named on the ``User`` contract.

Let's move on to how our DAML model is reflected and used on the UI side.

TypeScript Code Generation
==========================

The user interface for our app is written in `TypeScript <https://www.typescriptlang.org/>`_.
TypeScript is a variant of Javascript that provides more support during development through its type system.

In order to build an application on top of DAML, we need a way to refer to our DAML templates and choices in TypeScript.
We do this using a DAML to TypeScript code generation tool in the DAML SDK.

To run code generation, we first need to compile the DAML model to an archive format (with a ``.dar`` extension).
Then the command ``daml codegen ts`` takes this file as argument to produce a number of TypeScript files in the specified location.
::

    daml build
    daml codegen ts .daml/dist/create-daml-app-0.1.0.dar -o daml-ts/src

We now have TypeScript types and companion objects in the ``daml-ts`` workspace which we can use from our UI code.
We'll see that next.

The UI
======

On top of TypeScript, we use the UI framework `React <https://reactjs.org/>`_.
React helps us write modular UI components using a functional style - a component is rerendered whenever one of its inputs changes - with careful use of global state.

The latter is especially interesting as it's how we handle ledger state in our application.
We use a state management feature of React called `Hooks <https://reactjs.org/docs/hooks-intro.html>`_.
We use custom DAML React hooks to query the ledger for contracts, create new contracts, and exercise choices.

.. TODO Link to DAML react hooks API

We can see examples of this in the ``MainView`` component.
This is the React component that enables the main functionality of the app.

.. literalinclude:: code/ui-before/MainView.tsx
  :language: typescript
  :start-after: // USERS_BEGIN
  :end-before: // USERS_END

For instance, ``allUsers`` uses the query hook to get the ``User`` contracts on the ledger.
Note however that the query preserves privacy: only users that have added the party currently logged in are shown.
This is because the observers of a ``User`` contract are exactly the user's friends.

.. TODO Explain why you see friends of friends.

Another example is how we exercise the ``AddFriend`` choice of the ``User`` template.

.. literalinclude:: code/ui-before/MainView.tsx
  :language: typescript
  :start-after: // ADDFRIEND_BEGIN
  :end-before: // ADDFRIEND_END

We use the ``useExerciseByKey`` hook to gain access to the ``exerciseAddFriend`` function.
The *key* in this case is the username of the current user, used to look up the corresponding ``User`` contract.
The wrapper function ``addFriend`` is then passed to the subcomponents of ``MainView``.
For example, ``addFriend`` is passed to the ``UserList`` component as an argument (called a *prop* in React terms).
This gets triggered when you click the button next to a user's name in the "Network" panel.

.. literalinclude:: code/ui-before/MainView.tsx
  :language: typescript
  :start-after: // USERLIST_BEGIN
  :end-before: // USERLIST_END

This gives you a taste of how the UI works alongside a DAML ledger.
You'll see this more as we develop :doc:`your first feature <first-feature>` for our social network.
