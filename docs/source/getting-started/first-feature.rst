.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Your First Feature
******************

Let's dive into implementing a new feature for our social network app.
This will give us a better idea how to develop DAML applications using our template.

At the moment, our app lets us follow users in the network, but we have no way to communicate with them!
Let's fix that by adding a *direct messaging* feature.
This should let users that follow each other send messages, respecting *authorization* and *privacy*.
This means:

    1. You cannot send a message to someone unless they have given you the authority by following you back.
    2. You cannot see a message unless you sent it or it was sent to you.

We will see that DAML lets us implement these guarantees in a direct and intuitive way.

There are three parts to building and running the messaging feature:

    1. Adding the necessary changes to the DAML model
    2. Making the corresponding changes in the UI
    3. Running the app with the new feature.

As usual, we must start with the DAML model and base our UI changes on top of that.

DAML Changes
============

As mentioned in the :doc:`architecture <app-architecture>` section, the DAML code defines the *data* and *workflow* of the application.
The workflow aspect refers to the interactions between parties that are permitted by the system.
In the context of a messaging feature, these are essentially the authorization and privacy concerns listed above.

For the authorization part, we take the following approach: a user Bob can message another user Alice when Alice starts following Bob back.
When Alice starts following Bob back, she gives permission or *authority* to Bob to send her a message.

To implement this workflow, let's start by adding the new *data* for messages.
Navigate to the ``daml/User.daml`` file and copy the following ``Message`` template to the bottom.
Indentation is important: it should be at the top level like the original ``User`` template.

.. literalinclude:: code/templates-tarball/create-daml-app/daml/User.daml
  :language: daml
  :start-after: -- MESSAGE_BEGIN
  :end-before: -- MESSAGE_END

This template is very simple: it contains the data for a message and no choices.
The interesting part is the ``signatory`` clause: both the ``sender`` and ``receiver`` are signatories on the template.
This enforces the fact that creation and archival of ``Message`` contracts must be authorized by both parties.

Now we can add messaging into the workflow by adding a new choice to the ``User`` template.
Copy the following choice to the ``User`` template after the ``Follow`` choice. The indentation for the ``SendMessage`` choice must match the one of ``Follow`` . *Make sure you save the file after copying the code*.

.. literalinclude:: code/templates-tarball/create-daml-app/daml/User.daml
  :language: daml
  :start-after: -- SENDMESSAGE_BEGIN
  :end-before: -- SENDMESSAGE_END

As with the ``Follow`` choice, there are a few aspects to note here.

    - By convention, the choice returns the ``ContractId`` of the resulting ``Message`` contract.
    - The parameters to the choice are the ``sender`` and ``content`` of this message; the receiver is the party named on this ``User`` contract.
    - The ``controller`` clause states that it is the ``sender`` who can exercise the choice.
    - The body of the choice first ensures that the sender is a user that the receiver is following and then creates the ``Message`` contract with the ``receiver`` being the signatory of the ``User`` contract.

This completes the workflow for messaging in our app.
Now let's integrate this functionality into the UI.

TypeScript Code Generation
==========================

Remember that we interface with the DAML model from the UI components using generated TypeScript.
Since we have changed our DAML code, we also need to rerun the TypeScript code generator.
Open a new terminal and run the following commands::

  daml build
  daml codegen js .daml/dist/create-daml-app-0.1.0.dar -o daml.js

The result is an up-to-date TypeScript interface to our DAML model, in particular to the new ``Message`` template and ``SendMessage`` choice.

To make sure that Yarn picks up the newly generated JavaScript code,
we have to run the following command in the ``ui`` directory::

  yarn install --force --frozen-lockfile

Once that command finishes, you have to close Visual Studio Code
and restart it by running ``daml studio`` from the root directory of
your project.

We can now implement our messaging feature in the UI!

Messaging UI
============

The UI for messaging will consist of a new *Messages* panel in addition to the *Follow* and *Network* panel.
This panel will have two parts:

    1. A list of messages you've received with their senders.
    2. A form with a dropdown menu for follower selection and a text field for composing the message.

We will implement each part as a React component, which we'll name ``MessageList`` and ``MessageEdit`` respectively.
Let's start with the simpler ``MessageList``.

MessageList Component
---------------------

The goal of the ``MessageList`` component is to query all ``Message`` contracts where the ``receiver`` is the current user, and display their contents and senders in a list.
The entire component is shown below.
You should copy this into a new ``MessageList.tsx`` file in ``ui/src/components`` and save it.

.. TODO Include file in template with placeholder for component logic.

.. literalinclude:: code/templates-tarball/create-daml-app/ui/src/components/MessageList.tsx
  :language: tsx
  :start-after: // MESSAGELIST_BEGIN
  :end-before: // MESSAGELIST_END

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

Next we need the ``MessageEdit`` component to compose and send messages to our followers.
Again we show the entire component here; you should copy this into a new ``MessageEdit.tsx`` file in ``ui/src/components`` and save it.

.. TODO Include file in template with placeholder for component logic.

.. literalinclude:: code/templates-tarball/create-daml-app/ui/src/components/MessageEdit.tsx
  :language: tsx
  :start-after: // MESSAGEEDIT_BEGIN
  :end-before: // MESSAGEEDIT_END

You will first notice a ``Props`` type near the top of the file with a single ``following`` field.
A *prop* in React is an input to a component; in this case a list of users from which to select the message receiver.
The prop will be passed down from the ``MainView`` component, reusing the work required to query users from the ledger.
You can see this ``following`` field bound at the start of the ``MessageEdit`` component.

We use the React ``useState`` hook to get and set the current choices of message ``receiver`` and ``content``.
The DAML-specific ``useLedger`` hook gives us an object we can use to perform ledger operations.
The call to ``ledger.exerciseByKey`` in ``sendMessage`` looks up the ``User`` contract with the receiver's username and exercises ``SendMessage`` with the appropriate arguments.
The ``sendMessage`` wrapper reports potential errors to the user, and ``submitMessage`` additionally uses the ``isSubmitting`` state to ensure message requests are processed one at a time.
The result of a successful call to ``submitMessage`` is a new ``Message`` contract created on the ledger.

The return value of this component is the React ``Form`` element.
This contains a dropdown menu to select a receiver from the ``following``, a text field for the message content, and a *Send* button which triggers ``submitMessage``.

There is again an important point here, in this case about how *authorization* is enforced.
Due to the logic of the ``SendMessage`` choice, it is impossible to send a message to a user who is not following us (even if you could somehow access their ``User`` contract).
The assertion that ``elem sender following`` in ``SendMessage`` ensures this: no mistake or malice by the UI programmer could breach this.

MainView Component
------------------

Finally we can see these components come together in the ``MainView`` component.
We want to add a new panel to house our messaging UI.
Open the ``ui/src/components/MainView.tsx`` file and start by adding imports for the two new components.

.. literalinclude:: code/templates-tarball/create-daml-app/ui/src/components/MainView.tsx
  :language: tsx
  :start-after: // IMPORTS_BEGIN
  :end-before: // IMPORTS_END

Next, find where the *Network* ``Segment`` closes, towards the end of the component.
This is where we'll add a new ``Segment`` for *Messages*. Make sure you've saved the file after copying the code.

.. literalinclude:: code/templates-tarball/create-daml-app/ui/src/components/MainView.tsx
  :language: tsx
  :start-after: {/* MESSAGES_SEGMENT_BEGIN */}
  :end-before: {/* MESSAGES_SEGMENT_END */}

You can see we simply follow the formatting of the previous panels and include the new messaging components: ``MessageEdit`` supplied with the usernames of all visible parties as props, and ``MessageList`` to display all messages.

That is all for the implementation!
Let's give the new functionality a spin.

Running the New Feature
=======================

We need to terminate the previous ``daml start`` process and run it again, as we need to have a Sandbox instance with a DAR file containing the new feature. As a reminder, by running ``daml start`` again we will

  - Compile our DAML code into a *DAR file containing the new feature*
  - Run a fresh instance of the *Sandbox with the new DAR file*
  - Start the HTTP JSON API

First, navigate to the terminal window where the ``daml start`` process is running and terminate the active process by hitting ``Ctrl-C``.
This shuts down the previous instances of the sandbox.
Then in the root ``create-daml-app`` folder run ``daml start``.

As mentioned at the beginning of this *Getting Started with DAML* guide, DAML Sandbox uses an in-memory store, which means it loses its state when stopped or restarted. That means that all user data and follower relationships are lost.

If you have the frontend UI up and running you're all set. In case you don't have the UI running open a new terminal window and navigate to the ``create-daml-app/ui`` folder and run the ``yarn start`` command, which will start the UI.
Once you've done all these changes you should see the same login page as before at http://localhost:3000.

   .. figure:: images/create-daml-app-login-screen.png
      :scale: 50 %
      :alt: Login screen for the create-daml-app
      :class: no-scaled-link

Once you've logged in, you'll see a familiar UI but with our new *Messages* panel at the bottom!

   .. figure:: images/create-daml-app-messaging-feature.png
      :alt: You can now see the messaging feature in the create-daml-app

Go ahead and add follow more users, and log in as some of those users in separate browser windows to follow yourself back.
Then, if you click on the dropdown menu in the *Messages* panel, you'll be able to see some followers to message!

   .. figure:: images/create-daml-app-messaging-select-user.png
      :scale: 50 %
      :alt: Select a follower from a dropdown list in the create-daml-app

Send some messages between users and make sure you can see each one from the other side.
You'll notice that new messages appear in the UI as soon as they are sent (due to the *streaming* React hooks).

   .. figure:: images/create-daml-app-message-received.png
      :scale: 50 %
      :alt: In the create-daml-app as soon as you send the message it will appear in the "Messsaging" panel

Next Steps
==========

We've gone through the process of setting up a full-stack DAML app and implementing a useful feature end to end.
Have a think about how you might further improve or extend this app.
For example, you might have noticed that your list of messages can get out of order.
You could add a timestamp to the ``Message`` template and sort messages in the ``MessageList`` component so your most recent are at the top.
Of course there are many more features you could imagine (just think of your favourite social media app).

Hopefully this exercise gives you a sense of the power and ease of building DAML apps.
Explore the documentation to learn more, and keep shipping DAML apps.
Have fun!
