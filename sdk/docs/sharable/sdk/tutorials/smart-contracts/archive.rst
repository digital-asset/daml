.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Build the Daml Archive (.dar) file
==================================

In this section, you will compile your Daml smart contract into a Daml
Archive (.dar) file, which is essential for deploying and executing
contracts on a Daml ledger. By the end, you will have a fully packaged
unit ready for distribution in a multi-party system. Let’s get
started!


Open a terminal and navigate to the root directory of your Daml
project.

Run the following command to build your `.dar` file:

.. code-block:: bash

  daml build

This command compiles your Daml code and produces a Dar file in the
`./.daml/dist/` directory.

Once the build completes, check the `./.daml/dist/` directory for your
newly created `.dar` file. The name follows this format:

.. code-block:: bash

  <project-name>-<version>.dar

If you don’t see the expected output, check the terminal logs for
errors and ensure your contract compiles correctly.

For more details on compiling your DAR file, see :ref:`How to
build Daml Archive (.dar) files <build_howto_build_dar_files>`

Once you've built your DAR file, you may want to check out
:externalref:`Manage Daml packages and archive
<manage-daml-packages-and-archives>` for guidance on loading it into
your participant.

Next up
-------

In :doc:`data` you will learn about Daml's type system, and how you
can think of templates as tables and contracts as database rows.
