.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _new-quickstart:

Getting Started with Daml
#########################

The goal of this tutorial is to get you up and running with full-stack Daml development.
We do this through the example of a simple social networking application,
showing you three things:

    1. How to build and run the application
    2. The design of its different components (:doc:`app-architecture`)
    3. How to write a new feature for the app (:doc:`first-feature`)

We do not aim to be comprehensive in all Daml concepts and tools (covered in :doc:`Writing Daml </daml/intro/0_Intro>`) or in all deployment options (see :doc:`Deploying </deploy/index>`).
**For a quick overview of the most important Daml concepts used in this tutorial open** `the Daml cheat-sheet <https://docs.daml.com/cheat-sheet/>`_ **in a separate tab**. The goal is that by the end of this tutorial,
you'll have a good idea of the following:

    1. What Daml contracts and ledgers are
    2. How a user interface (UI) interacts with a Daml ledger
    3. How Daml helps you build a real-life application fast.

With that, let's get started!

Prerequisites
*************

Please make sure that you have the Daml Connect SDK, Java 8 or higher, and Visual Studio Code (the only supported IDE) installed as per instructions from our :doc:`installation` page.

You will also need some common software tools to build and interact with the template project.

- `Node <https://nodejs.org/en/>`_ and the associated package manager ``npm``. You need ``node --version`` to report at least ``12.22``; if you have an older version, see `this link <https://docs.npmjs.com/downloading-and-installing-node-js-and-npm>`_ for additional installation options.
- A terminal application for command line interaction.


Running the app
***************

We'll start by getting the app up and running, and then explain the different components which we will later extend.

First off, open a terminal, change to a folder in which to create your first application, and instantiate the template project.
::

    daml new create-daml-app --template create-daml-app

This creates a new folder with contents from our template. To see
a list of all available templates run ``daml new --list``.

Change to the new folder::

    cd create-daml-app

.. TODO: Give instructions for possible failures.

We can now run the app in two steps.
You'll need two terminal windows running for this.
In one terminal, at the root of the ``create-daml-app`` directory, run the command::

    daml start

Any commands starting with ``daml`` are using the :doc:`Daml Assistant </tools/assistant>`, a
command line tool in the SDK for building and running Daml apps.

You will know that the command has started successfully when you see the ``INFO  com.daml.http.Main$ - Started server: ServerBinding(/127.0.0.1:7575)`` message in the terminal. The command does a few things:

    1. Compiles the Daml code to a DAR (Daml Archive) file.
    2. Generates a JavaScript library in ``ui/daml.js`` to connect the UI with your Daml code.
    3. Starts an instance of the :doc:`Sandbox </tools/sandbox>`, an in-memory ledger useful for development, loaded with our DAR.
    4. Starts a server for the :doc:`HTTP JSON API </json-api/index>`, a simple way to run commands against a Daml ledger (in this case the running Sandbox).

We'll leave these processes running to serve requests from our UI.

In a second terminal, navigate to the ``create-daml-app/ui`` folder and use ``npm`` to install the project dependencies::

    cd create-daml-app/ui
    npm install

This step may take a couple of moments (it's worth it!).
You should see ``success Saved lockfile.`` in the output if everything worked as expected.

Now you can start the UI with::

    npm start

This starts the web UI connected to the running Sandbox and JSON API server.
The command should automatically open a window in your default browser at http://localhost:3000.
Once the web UI has been compiled and started, you should see ``Compiled successfully!`` in your terminal.
If it doesn't, just open that link in a web browser.
(Depending on your firewall settings, you may be asked whether to allow the app to receive network connections. It is safe to accept.)
You should now see the login page for the social network. For simplicity of this app, there is no password or sign-up required.
First enter your name and click *Log in*.

   .. figure:: images/create-daml-app-login-screen.png
      :scale: 50 %
      :alt: Login screen for the create-daml-app
      :class: no-scaled-link

You should see the main screen with two panels. One for the users you are following and one for your followers.
Initially these are both empty as you are not following anyone and you don't have any followers!
Go ahead and start following users by typing their usernames in the text box and clicking on the *Follow* button in the top panel.

   .. figure:: images/create-daml-app-main-screen-initial-view.png
      :alt: Main view of the create-daml-app

You'll notice that the users you just started following appear in the *Following* panel.
However they do *not* yet appear in the *Network* panel.
This is either because they have not signed up and are not parties on the ledger or they have not yet started following you.
This social network is similar to Twitter and Instagram, where by following someone, say Alice, you make yourself visible to her but not vice versa.
We will see how we encode this in Daml in the next section.

   .. figure:: images/create-daml-app-bob-follows-alice.png
      :alt: In the create-daml-app users can follow each other in a similar fashion as in Twitter or Instagram

To make this relationship reciprocal, open a new browser window/tab at http://localhost:3000.
(Having separate windows/tabs allows you to see both you and the screen of the user you are following at the same time.)
Once you log in as the user you are following - Alice, you'll notice your name in her network.
In fact, Alice can see the entire list of users you are following in the *Network* panel.
This is because this list is part of the user data that became visible when you started following her.

   .. figure:: images/create-daml-app-alice-sees-bob.png
      :alt: In the create-daml-app when you start following someone you reveal the list of people you are following

When Alice starts following you, you can see her in your network as well.
Just switch to the window where you are logged in as yourself - the network should update automatically.

   .. figure:: images/create-daml-app-bob-sees-alice-in-the-network.png
      :alt: In the create-daml-app when the user you are following follows you back s/he reveals the list of people they are following

Play around more with the app at your leisure: create new users and start following more users.
Observe when a user becomes visible to others - this will be important to understanding Daml's privacy model later.
When you're ready, let's move on to the :doc:`architecture of our app <app-architecture>`.

.. tip:: Congratulations on completing the first part of the Getting Started Guide! `Join our forum <https://discuss.daml.com>`_ and share a screenshot of your accomplishment to `get your first of 3 getting started badges <https://discuss.daml.com/badges/125/it-works>`_! You can get the next one by :doc:`implementing your first feature </getting-started/first-feature>`.
