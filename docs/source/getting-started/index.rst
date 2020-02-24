.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _new-quickstart:

Getting Started with DAML
#########################

**Disclaimer:** This guide is being actively developed.
Expect major changes to the tutorial text and minor changes to the template application.

The goal of this tutorial is to get you up and running with full-stack DAML development.
We do this through the example of a simple social networking application,
showing you three things:

    1. How to build and run the application
    2. The design of its different components (:doc:`app-architecture`)
    3. How to write a new feature for the app (:doc:`first-feature`)

We do not aim to be comprehensive in all DAML concepts and tools (covered in :doc:`Writing DAML </daml/intro/0_Intro>`) or in all deployment options (see :doc:`Deploying </deploy/index>`).
The goal is that by the end of this tutorial, you'll have a good idea of the following:

    1. What DAML contracts and ledgers are
    2. How a user interface (UI) interacts with a DAML ledger
    3. How DAML helps you build a real-life application fast.

With that, let's get started!

.. toctree::
   :hidden:

   app-architecture
   first-feature

Prerequisites
*************

If you haven't already, see :doc:`installation` for the DAML SDK and VSCode development environment.

You will also need some common software tools to build and interact with the template project.

- `Git <https://git-scm.com/>`_ version control system
- `Yarn <https://yarnpkg.com/>`_ package manager for Javascript
- A terminal application for command line interaction


Running the app
***************

We'll start by getting the app up and running, and then explain the different components which we will later extend.

First off, open a terminal, clone the template project and move to the project folder::

    git clone https://github.com/digital-asset/create-daml-app.git
    cd create-daml-app

Next we need to compile the DAML code to a DAR file::

    daml build

In order to connect the UI code to this DAML, we need to run a code generation step::

    daml codegen ts .daml/dist/create-daml-app-0.1.0.dar -o daml-ts/src

Now, use Yarn to install the project dependencies and build the app::

    yarn install
    yarn workspaces run build

You should see ``Compiled successfully.`` in the output if everything is working as expected.

.. TODO: Give instructions for possible failures.

We can now run the app in two steps.
You'll need two terminal windows running for this.

In one terminal, at the root of the ``create-daml-app`` directory, run the script::

    ./daml-start.sh

This compiles the DAML component of the project and starts a *Sandbox* ledger for the app.
The ledger in this case is stored in the Sandbox application memory; it is not persistent but is useful for testing and development.
We'll leave the Sandbox running to serve requests from the UI, which result in changes to the in-memory ledger.

In a second terminal, navigate to the ``create-daml-app/ui`` folder and run::

    yarn start

This starts the UI application connected to the already running Sandbox.
The command should automatically open a window in your default browser at http://localhost:3000.
If it doesn't, just open that link in any web browser.
(Depending on your firewall settings, you may be asked whether to allow the app to receive network connections. It is safe to accept.)

Now you should see the login page for the social network.
For simplicity of this app, there is no password required to sign up or log in.
To learn how to handle authentication, see this blog post about `DAML and Auth0 <https://blog.daml.com/daml-driven/easy-authentication-for-your-distributed-app-with-daml-and-auth0>`_ or the :doc:`full documentation </app-dev/authentication>`.

First enter your name and click "Sign up".
You should see the main screen with two panels for your friends and the entire network.
Initially these are both empty as you don't have friends yet!
Go ahead and add some using the text box and add button.

You'll notice that the newly added friends are under your name in the Friends panel but don't appear in the network panel.
The latter is because 1. they have not signed up and are not parties on the ledger and 2. they have not yet added you as a friend.
In our social network, friendships can go in a single direction.
By adding a friend, say Alice, your User contract becomes visible to Alice but not vice versa.
We will see how we encode this model in DAML in the next section.

To make your friendships reciprocal, open a new browser window to the same http://localhost:3000 address.
(Having separate windows allows you to see both you and your friend's screens together.)
Once you sign up as your friend Alice, you'll notice your name in her network.
When Alice finally adds you back as a friend, you can see her in your network as well.
(Just open the previous window for this - no need to reload the page!).

Play around more with the app at your leisure: create new users and add more friends.
Observe when a user are visible to others - this will be important to understanding DAML's privacy guarantees later.
When you're ready, move on to the :doc:`architecture of our app <app-architecture>`.

.. TODO: Add screenshots for the app above
