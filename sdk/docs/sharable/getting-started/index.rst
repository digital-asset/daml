.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _new-quickstart:

Getting Started with Daml
#########################

The goal of this tutorial is to get you up and running with full-stack Daml development.
Through the example of a simple social networking application,
you will learn:

    1. How to build and run the application
    2. The design of its different components (:doc:`app-architecture`)
    3. How to write a new feature for the app (:doc:`first-feature`)

The goal is that by the end of this tutorial,
you'll have a good idea of the following:

     - What Daml contracts and ledgers are
     - How a user interface (UI) interacts with a Daml ledger
     - How Daml helps you build a real-life application fast.

This is not a comprehensive guide to all Daml concepts and tools or all deployment options; these are covered in-depth in the User Guide.
**For a quick overview of the most important Daml concepts used in this tutorial you can refer to** `the Daml cheat-sheet <https://docs.daml.com/cheat-sheet/>`_ .

With that, let's get started!

Prerequisites
*************

Make sure that you have the Daml SDK, Java 11 or higher, and Visual Studio Code (the only supported IDE) installed as per the instructions in :doc:`installation`.

You will also need some common software tools to build and interact with the template project:

- `Node <https://nodejs.org/en/>`_ and the associated package manager ``npm``. Use the `Active LTS <https://nodejs.org/en/about/releases/>`_ Node version, currently ``v18`` (check with ``node --version``).
- A terminal application for command line interaction.


Run the App
***********

To get the app up and running:

1. Open a terminal, select a folder in which to create your first application, and instantiate the template project.
::

    daml new create-daml-app --template create-daml-app

This creates a new folder with contents from our template. To see
a list of all available templates run ``daml new --list``.

2. Change to the new folder::

    cd create-daml-app

.. TODO: Give instructions for possible failures.

3. Open two terminal windows.
4. In one terminal, at the root of the ``create-daml-app`` directory, run the command::

    daml start

Any commands starting with ``daml`` are using the :doc:`Daml Assistant </tools/assistant>`, a
command line tool in the SDK for building and running Daml apps.

The command has started successfully when you see the ``INFO  com.daml.http.Main$ - Started server: ServerBinding(/127.0.0.1:7575)`` message in the terminal. The command does a few things:

    1. Compiles the Daml code to a DAR (Daml Archive) file
    2. Generates a JavaScript library in ``ui/daml.js`` to connect the UI with your Daml code
    3. Starts an instance of the :doc:`Sandbox </tools/sandbox>`, an in-memory ledger useful for development, loaded with our DAR
    4. Starts a server for the :doc:`HTTP JSON API </json-api/index>`, a simple way to run commands against a Daml ledger (in this case the running Sandbox)

We'll leave these processes running to serve requests from our UI.

5. In the second terminal, navigate to the ``create-daml-app/ui`` folder and use ``npm`` to install the project dependencies::

    cd create-daml-app/ui
    npm install

This step may take a couple of moments.
You should see ``success Saved lockfile.`` in the output if everything worked as expected.

6. Start the UI with::

    npm start

This starts the web UI connected to the running Sandbox and JSON API server.
The command should automatically open a window in your default browser at http://localhost:3000.

Once the web UI has been compiled and started, you should see ``Compiled successfully!`` in your terminal.
If you don't, open http://localhost:3000 in a web browser.
Depending on your firewall settings, you may be asked whether to allow the app to receive network connections. It is safe to accept.

You should now see the login page for the social network. For simplicity, in this app there is no password or sign-up required.

1. Enter a user name. Valid user names are bob, alice, or charlie (note that these are all lower-case, although they are displayed in the social network UI by their alias instead of their user id, with the usual capitalization).
2. Click *Log in*.

   .. figure:: images/create-daml-app-login-screen.png
      :scale: 50 %
      :alt: Login screen for the app.
      :class: no-scaled-link

You should see the main screen with two panels. The top panel displays the social network users you are following; the bottom displays the aliases of the users who follow you. Initially these are both empty as you are not following anyone and you don't have any followers.
To start following a user, select their name in the drop-down list and click the *Follow* button in the top panel. At the moment, you will notice that the drop-down shows only your own user because no other user has registered yet.

   .. figure:: images/create-daml-app-main-screen-initial-view.png
      :alt: Main view of the app.

Next, open a new browser window/tab at http://localhost:3000 and log in as a different user.
(Having separate windows/tabs allows you to see both your own screen and the screen of the user you are following at the same time.)

Now that the other user (Alice in this example) has logged in, go back to the previous window/tab, select them drop-down list and click the *Follow* button in the top panel.

The user you just started following appears in the *Following* panel.
However, they do not yet appear in the *Network* panel.
This is because they have not yet started following you.
This social network is similar to Twitter and Instagram, where by following someone, say Alice, you make yourself visible to her but not vice versa.
We will see how we encode this in Daml in the next section.

   .. figure:: images/create-daml-app-bob-follows-alice.png
      :alt: The app now shows Alice in Bob's Users I Follow section.

To make this relationship reciprocal, go back to the other window/tab where you logged in as the second user (Alice in this example).
You should now see your name in her network.
In fact, Alice can see the entire list of users you are following in the *Network* panel.
This is because this list is part of the user data that became visible when you started following her.

   .. figure:: images/create-daml-app-alice-sees-bob.png
      :alt: The app from Alice's point of view, with the list of users Bob is following in the The Network section.

When Alice starts following you, you can see her in your network as well.
Switch to the window where you are logged in as yourself - the network should update automatically.

   .. figure:: images/create-daml-app-bob-sees-alice-in-the-network.png
      :alt: The app now shows Bob the list of users Alice is following in the The Network section.

Play around more with the app at your leisure: create new users and start following more users.
Observe when a user becomes visible to others - this will be important to understanding Daml's privacy model later.
When you're ready, let's move on to the :doc:`architecture of our app <app-architecture>`.

.. tip:: Congratulations on completing the first part of the Getting Started Guide! `Join our forum <https://discuss.daml.com>`_ and share a screenshot of your accomplishment to `get your first of 3 getting started badges <https://discuss.daml.com/badges/125/it-works>`_! You can get the next one by :doc:`implementing your first feature </getting-started/first-feature>`.
