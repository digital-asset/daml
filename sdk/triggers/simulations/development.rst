.. Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Developing Trigger Simulation Code
==================================

Environment Setup
-----------------

First ensure the Daml SDK is installed with:

.. code-block:: bash
  curl -sSL https://get.daml.com/ | sh

and that the repository `Daml repository <https://github.com/digital-asset/daml>`_ has been cloned with:

.. code-block:: bash
  git clone https://github.com/digital-asset/daml

Now, setup your simulation environment using:

.. code-block:: bash
  cd daml/triggers/simulations
  nix-shell shell.nix

and compile the example trigger simulation Daml project using:

.. code-block:: bash
  daml damlc build --project-root ./daml

.. note::
  Please remember to recompile your Daml project everytime you make changes to it!
