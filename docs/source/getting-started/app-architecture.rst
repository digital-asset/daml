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

The DAML code defines the *data* and *workflow* of the application.
Both are described in the ``User`` contract *template*.
Let's look at the data portion first.

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

The last part of the DAML model is the operation to add friends, called a *choice* in DAML.

.. literalinclude:: code/daml/User.daml
  :language: daml
  :start-after: -- ADDFRIEND_BEGIN
  :end-before: -- ADDFRIEND_END

DAML contracts are *immutable* (can not be changed in place), so the only way to "update" one is to archive it and create a new instance.
That is what the ``AddFriend`` choice does: after checking some preconditions, it archives the current user contract and creates a new one with the extra friend added to the list. Here is a quick explanation of the code: 

    - The choice starts with the ``nonconsuming choice`` keyword followed by the choice name ``AddFriend``.
    - The return type of a choice is defined next. In this case it is ``ContractId User``.
    - After that we pass arguments for the choice with ``with`` keyword. Here this is the friend we are trying to add.
    - The keyword ``controller`` defines the ``Party`` that is allowed to execute the choice. In this case it would be the logged in user with the defined ``username``.  
    - The ``do`` keyword marks the start of the choice's body where its functionality will be written.
    - After passing some checks current contract is archived with ``archive self`` 
    - New contract containig the new friend added to the list is creted with ``create this with friends = friend :: friends``

This information should be enough for understanding how choices work in this guide. More detailed information on choices can be found in :doc:`our docs </daml/reference/choices>`).

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

Let's see an example of a React component.
All components are in the ``ui/src/components`` folder.
You can navigate there within Visual Studio Code using the file explorer on the left sidebar.
We'll first look at ``App.tsx``, which is the entry point to our application.

.. literalinclude:: code/ui-before/App.tsx
  :start-after: // APP_BEGIN
  :end-before: // APP_END

An important tool in the design of our components is a React feature called `Hooks <https://reactjs.org/docs/hooks-intro.html>`_.
Hooks allow you to share and update state across components, avoiding having to thread it through manually.
We take advantage of hooks in particular to share ledger state across components.
We use custom *DAML React hooks* to query the ledger for contracts, create new contracts, and exercise choices.
This library uses the :doc:`HTTP JSON API </json-api/index>` behind the scenes.

.. TODO Link to DAML react hooks API

The ``useState`` hook (not specific to DAML) here keeps track of the user's credentials.
If they are not set, we render the ``LoginScreen`` with a callback to ``setCredentials``.
If they are set, then we render the ``MainScreen`` of the app.
This is wrapped in the ``DamlLedger`` component, a `React context <https://reactjs.org/docs/context.html>`_ with a handle to the ledger.

Let's move on to more advanced uses of our DAML React library.
The ``MainScreen`` is a simple frame around the ``MainView`` component, which houses the main functionality of our app.
It uses DAML React hooks to query and update ledger state.

.. literalinclude:: code/ui-before/MainView.tsx
  :language: typescript
  :start-after: // USERS_BEGIN
  :end-before: // USERS_END

The ``useParty`` hook simply returns the current user as stored in the ``DamlLedger`` context.
A more interesting example is the ``allUsers`` line.
This uses the ``useStreamQuery`` hook to get all ``User`` contracts on the ledger.
(``User`` here is an object generated by ``daml codegen ts`` - it stores metadata of the ``User`` template defined in ``User.daml``.)
Note however that this query preserves privacy: only users that have added the current user have their contracts revealed.
This behaviour is due to the observers on the ``User`` contract being exactly the user's friends.

A final point on this is the *streaming* aspect of the query.
This means that results are updated as they come in - there is no need for periodic or manual reloading to see updates.

.. TODO Explain why you see friends of friends.

Another example, showing how to *update* ledger state, is how we exercise the ``AddFriend`` choice of the ``User`` template.

.. literalinclude:: code/ui-before/MainView.tsx
  :language: typescript
  :start-after: // ADDFRIEND_BEGIN
  :end-before: // ADDFRIEND_END

The ``useExerciseByKey`` hook returns the ``exerciseAddFriend`` function.
The *key* in this case is the username of the current user, used to look up the corresponding ``User`` contract.
The wrapper function ``addFriend`` is then passed to the subcomponents of ``MainView``.
For example, ``addFriend`` is passed to the ``UserList`` component as an argument (a `prop <https://reactjs.org/docs/components-and-props.html>`_ in React terms).
This gets triggered when you click the icon next to a user's name in the *Network* panel.

.. literalinclude:: code/ui-before/MainView.tsx
  :language: typescript
  :start-after: // USERLIST_BEGIN
  :end-before: // USERLIST_END

This should give you a taste of how the UI works alongside a DAML ledger.
You'll see this more as you develop :doc:`your first feature <first-feature>` for our social network.
