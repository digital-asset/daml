.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Canton Console
##############

Introduction
============

Canton offers a console where you can run administrative or debugging commands.

When you run the Sandbox using ``daml start`` or ``daml sandbox``, you are effectively starting an
in-memory instance of Canton with a single domain and a single participant.

As such, you can interact with the running Sandbox using the console, just like you would
in a production environment.

The purpose of this page is to give a few pointers on how the console can be used to
interact with a running Sandbox. For an in-depth guide on how to use this tool against a production,
staging or testing environment, consult the main documentation for the Canton console.

Run the Canton Console Against the Sandbox
==========================================

Once you have a Sandbox running locally (for example after running ``daml start`` or ``daml sandbox``)
you can start the console with the following command (in a separate terminal)::

   daml canton-console

Once the console starts (it might take some time the first time) you can quit the session by
running the ``exit`` command.

Built-in Documentation
======================

The Canton console comes with built-in documentation. You
can use the ``help`` command to get online documentation for top-level commands. Many objects in the
console also have further built-in help that you can access by invoking the ``help`` method on them.

For example, you can ask for help on the ``health`` object by typing::

  health.help

Or go more in depth about specific items within that object as in the following example::

  health.help("status")

Interact With the Sandbox
=========================

One of the objects available in the Canton console represents the Sandbox itself. The object is called
``sandbox`` and you can use it to interact with the Sandbox. For example, you can list the DARs loaded
on the Sandbox by running the following command::

  sandbox.dars.list()

Among the various features available as part of the console, you can manage parties and packages,
check the health of the Sandbox, perform pruning operations and more. Consult the
built-in documentation mentioned above and the main documentation page for the Canton console to learn about further capabilities.

