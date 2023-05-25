.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Environment Setup
=================

Clone the `Daml repository <https://github.com/digital-asset/daml>`_

.. code-block:: bash
  git clone https://github.com/digital-asset/daml
  cd daml

Create the file ``ExampleSimulation.scala`` in directory ``./triggers/tests/digitalasset/daml/lf/engine/trigger/simulation``

Compile trigger Daml project using:

.. code-block:: bash
  daml damlc build --project-root ./triggers/simulations

Following environment variables need to be set:

- ``DAR`` - Dar file containing compiled trigger Daml code for use in simulation
- ``TRIGGER`` - name of the trigger to launch in the simulation

Run simulation using:

.. code-block:: bash
  bazel test \
    --test_env=TRIGGER=Cats:breedingTrigger \
    --test_env=DAR=`pwd`/triggers/simulations/.daml/dist/trigger-simulations-0.0.1.dar \
    --test_output=streamed \
    --cache_test_results=no \
    --test_tmpdir=`pwd`/temp \
    //triggers/tests:trigger-simulation-test-launcher
