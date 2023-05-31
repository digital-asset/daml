.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Simulation Use Case Example: Ledger Contention
==============================================

TODO: what is contention? Ref: https://docs.daml.com/canton/usermanual/troubleshooting_guide.html#contention

TODO: document contract contention use case

.. code-block:: bash
  bazel test \
    --test_env=DAR=$(pwd)/daml/.daml/dist/trigger-simulations-0.0.1.dar \
    --test_output=streamed \
    --cache_test_results=no \
    --test_tmpdir=/tmp/ \
    --test_filter=com.daml.lf.engine.trigger.LimitedContention \
    //triggers/simulations:trigger-simulation-test-launcher_test_suite_scala_Contention.scala

.. code-block:: bash
  python3 ./data/analysis/graph-simulation-data.py --title "Limited Trigger Contention" /tmp/_tmp/*/TriggerSimulation*/

.. code-block:: bash
  bazel test \
    --test_env=DAR=$(pwd)/daml/.daml/dist/trigger-simulations-0.0.1.dar \
    --test_output=streamed \
    --cache_test_results=no \
    --test_tmpdir=/tmp/ \
    --test_filter=com.daml.lf.engine.trigger.MultipleTriggerContention \
    //triggers/simulations:trigger-simulation-test-launcher_test_suite_scala_Contention.scala

.. code-block:: bash
  python3 ./data/analysis/graph-simulation-data.py --title "Multiple Trigger Contention" /tmp/_tmp/*/TriggerSimulation*/
