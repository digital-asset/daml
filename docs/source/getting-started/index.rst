.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _new-quickstart:

Full Stack DAML
###############

**Disclaimer:** This guide is being actively developed.
Expect major changes to the tutorial text and minor changes to the template application.

The goal of this tutorial is to get you up and running with full-stack, DAML-driven app development. We will provide you with the template for a miniature social networking app, get you writing your first end-to-end feature and finally deploy your new app to a persistent ledger.

By the end of the tutorial, you should have an idea of what DAML contracts and ledgers are, how the UI interacts with them, and how you might solve a potential use case with a DAML solution. We do not aim to give a comprehensive guide to all DAML concepts and tools; for that, see the later sections of the documentation. With that, let's get started!

.. TODO: reference specific sections of docs instead of saying "later sections".

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

In order to connect the DAML model to the UI code, we need to run a code generation step (more on this later)::

    daml build
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
You may be asked whether to allow the app to receive network connections, which you should allow.

At this point you should see the login page for the social network.

.. TODO: Screenshot

Enter a user name of your choice and click the calculator icon next to the password field to generate a password token.
(We do not have proper authentication in this app for simplicity.)
Once you click "Sign up", you can see a screen with panels for your friends and the entire social network.
Initially these are both empty as you don't have any friends yet!
Go ahead and add some using the form (and remove them using the cross icons if you change your mind).

Now let's grow the network. Log out and sign up using the name of one of your friends.
Let's say your name is Alice and your friend's name is Bob.
Bob should now see Alice in the network (since she added him as a friend) and he is able to add her back.
Note that in this app, friendships can be added in one direction at a time, similar to how "followers" work in Twitter.

Add a few more friends as Bob, and play around a bit more by logging in as different users and adding/removing friends from the network.
This should give you a idea of the app's functionality.
