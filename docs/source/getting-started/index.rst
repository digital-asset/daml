.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _new-quickstart:

Getting Started with DAML
#########################

The goal of this tutorial is to get you up and running with full-stack DAML development.
We do this through the example of a simple social networking application,
showing you three things:

    1. How to build and run the application
    2. The design of its different components (:doc:`app-architecture`)
    3. How to write a new feature for the app (:doc:`first-feature`)

We do not aim to be comprehensive in all DAML concepts and tools (covered in :doc:`Writing DAML </daml/intro/0_Intro>`) or in all deployment options (see :doc:`Deploying </deploy/index>`).
**For a quick overview of the most important DAML concepts used in this tutorial open** `the DAML cheat-sheet <https://docs.daml.com/cheat-sheet/>`_ **in a separate tab**. The goal is that by the end of this tutorial,
you'll have a good idea of the following:

    1. What DAML contracts and ledgers are
    2. How a user interface (UI) interacts with a DAML ledger
    3. How DAML helps you build a real-life application fast.

With that, let's get started!

Prerequisites
*************

Please make sure that you have the DAML SDK, Java 8 or higher, and Visual Studio Code (the only supported IDE) installed as per instructions from our :doc:`installation` page.

You will also need some common software tools to build and interact with the template project.

- `Git <https://git-scm.com/downloads>`_ version control system
- `Node <https://docs.npmjs.com/downloading-and-installing-node-js-and-npm>`_ package manager for JavaScript.
- A terminal application for command line interaction


Running the app
***************

We'll start by getting the app up and running, and then explain the different components which we will later extend.

First off, open a terminal and instantiate the template project.
::

    daml new create-daml-app --template create-daml-app

This creates a new folder with contents from our template. To see
a list of all available templates run ``daml new --list``.

Change to the new folder::

    cd create-daml-app

Next we need to compile the DAML code to a DAR file::

    daml build

Once the DAR file is created you will see this message in terminal ``Created .daml/dist/create-daml-app-0.1.0.dar``.

Any commands starting with ``daml`` are using the :doc:`DAML Assistant </tools/assistant>`, a command line tool in the DAML SDK for building and running DAML apps.
In order to connect the UI code to this DAML, we need to run a code generation step::

    daml codegen js .daml/dist/create-daml-app-0.1.0.dar -o daml.js

Now, changing to the ``ui`` folder, use ``npm`` to install the project dependencies::

    cd ui
    npm install --force --frozen-lockfile

This step may take a couple of moments (it's worth it!).
You should see ``success Saved lockfile.`` in the output if everything worked as expected.

.. TODO: Give instructions for possible failures.

We can now run the app in two steps.
You'll need two terminal windows running for this.
In one terminal, at the root of the ``create-daml-app`` directory, run the command::

    daml start

You will know that the command has started successfully when you see the ``INFO  com.daml.http.Main$ - Started server: ServerBinding(/127.0.0.1:7575)`` message in the terminal. The command does a few things:

    1. Compiles the DAML code to a DAR file as in the previous ``daml build`` step.
    2. Starts an instance of the :doc:`Sandbox </tools/sandbox>`, an in-memory ledger useful for development, loaded with our DAR.
    3. Starts a server for the :doc:`HTTP JSON API </json-api/index>`, a simple way to run commands against a DAML ledger (in this case the running Sandbox).

We'll leave these processes running to serve requests from our UI.

In a second terminal, navigate to the ``create-daml-app/ui`` folder and run the application::

    cd ui
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
This is either because they have not signed up and are not parties on the ledger or they have not yet started followiong you.
This social network is similar to Twitter and Instagram, where by following someone, say Alice, you make yourself visible to her but not vice versa.
We will see how we encode this in DAML in the next section.

   .. figure:: images/create-daml-app-bob-follows-alice.png
      :alt: In the create-daml-app users can follow each other in a similiar fashion as in Twitter or Instagram

To make this relationship reciprocal, open a new browser window/tab at http://localhost:3000.
(Having separate windows/tabs allows you to see both you and the screen of the user you are following at the same time.)
Once you log in as the user you are following - Alice, you'll notice your name in her network.
In fact, Alice can see the entire list of users you are follwing in the *Network* panel.
This is because this list is part of the user data that became visible when you started follwing her.

   .. figure:: images/create-daml-app-alice-sees-bob.png
      :alt: In the create-daml-app when you start following somone you reveal the list of people you are following

When Alice starts follwing you, you can see her in your network as well.
Just switch to the window where you are logged in as yourself - the network should update automatically.

   .. figure:: images/create-daml-app-bob-sees-alice-in-the-network.png
      :alt: In the create-daml-app when the user you are following follows you back s/he reveals the list of people they are following

Play around more with the app at your leisure: create new users and start following more users.
Observe when a user becomes visible to others - this will be important to understanding DAML's privacy model later.
When you're ready, let's move on to the :doc:`architecture of our app <app-architecture>`.
