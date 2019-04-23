.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Node.js bindings
################

.. toctree::
  :hidden:

  tutorial
  daml-as-json

The Node.js bindings let you access the Ledger API from Node.js code.

Getting started
***************

1. Set up NPM
=============

The Node.js bindings are published on NPM.

To set up NPM:

#. Go to the `Digital Asset NPM repository home page on Bintray <https://bintray.com/digitalassetsdk/npm>`__.
#. click on the "Set me up!" button
#. follow the instructions for scoped packages with scope `da`

2. Start a new project
======================

Use ``npm init`` to create a new project.

3. Install the bindings
=======================

Use the NPM command line interface:

.. code-block:: bash

    npm install @da/daml-ledger

4. Start coding
===============

To guide you through using the Node.js bindings, we provide a tutorial template and a :doc:`tutorial <tutorial>`.

API reference documentation
***************************

`Click here for the API reference documentation <./reference/index.html>`_.

DAML types and JSON
*******************

For information on how DAML types and contracts are represented as JSON, see :doc:`/app-dev/grpc/daml-to-ledger-api`.
