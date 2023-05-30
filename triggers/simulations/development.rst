.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Developing Trigger Simulation Code
==================================

Environment Setup
-----------------

Clone the `Daml repository <https://github.com/digital-asset/daml>`_

.. code-block:: bash
  git clone https://github.com/digital-asset/daml
  cd daml/triggers/simulations
  nix-shell shell.nix

Ensure the Daml SDK is installed with:

.. code-block:: bash
  curl -sSL https://get.daml.com/ | sh

Running Trigger Simulation
--------------------------

Compile trigger Daml project using:

.. code-block:: bash
  daml damlc build --project-root ./daml

Following environment variables need to be set:

- ``DAR`` - Dar file containing compiled trigger Daml code for use in simulation

Run the ``SlowACSGrowth`` simulation (under the bazel target ``trigger-simulation-test-launcher_test_suite_scala_ACSGrowth.scala``) using:

.. code-block:: bash
  bazel test \
    --test_env=DAR=$(pwd)/daml/.daml/dist/trigger-simulations-0.0.1.dar \
    --test_output=streamed \
    --cache_test_results=no \
    --test_tmpdir=/tmp/ \
    --test_filter=com.daml.lf.engine.trigger.SlowACSGrowth
    //triggers/tests:trigger-simulation-test-launcher_test_suite_scala_ACSGrowth.scala
