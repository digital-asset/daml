.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Canton Console
##############

Introduction
============

Canton offers a console where administrative or debugging commands can be run.

When you run the Sandbox using ``daml start`` or ``daml sandbox``, you are effectively starting an
in-memory instance of Canton with a single domain and a single participant.

As such, you can interact with the running Sandbox using the Canton Console, just like you would
in a production environment.

The purpose of this page is to give a few pointers on how the Canton Console can be used to
interact with a running Sandbox. For an in-depth guide on how to use this tool against a production,
staging or testing environment, consult the main documentation for the Canton Console.

Run the Canton Console against the Sandbox
==========================================

Once you have a Sandbox running locally (for example after running ``daml start`` or ``daml sandbox``)
you can start the Canton Console with the following command (in a separate terminal from the one where
the Sandbox is running, if any)::

   daml canton-console

Once the Canton Console started (it might take some time the first time) you can quit the session by
running the ``exit`` command.

Built-in documentation
======================

The Canton Console comes with built-in documentation that allows you to discover its features. You
can use the ``help`` command to get online documentation for top-level commands. Many objects in the
console also have further built-in help, by invoking the ``help`` method on them.

You can for example ask for help on the ``health`` object by typing::

  health.help

Or go more in depth about specific items within that object as in the following example::

  health.help("status")

Interact with the Sandbox
=========================

One of the objects available in the Canton Console represents the Sandbox itself. The object is called
``sandbox`` and you can use it to interact with the Sandbox. For example, you can list the DARs loaded
on the Sandbox by running the following command::

  sandbox.dars.list()

Among the various features available as part of the Canton Console, you can manage parties and packages,
check the health of the Sandbox, perform pruning operations and more. You are advised to consult the
built-in documentation as mentioned above to learn about further capabilities, as well as consulting
the main documentation page for the Canton Console.

