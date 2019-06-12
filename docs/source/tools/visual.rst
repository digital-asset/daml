.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Contract Visulization
#########

Introduction
************

You can generate visual graphs for DAML contracts visual command via command line tool. This program generates dot file which in turn can fed in to graphviz engine to produce an image.

Trying it out
*************
Using daml assitant please generate DAR for project. And to produce a dot file

.. code-block:: none

    daml damlc visual <path_to_project>/dist/<project_name.dar> --dot <project_name>.dot


The generated dot file can be fed into graphviz to generate image using the following command.

.. code-block:: none

    dot -Tpng <project_name>.dot > <project_name>.dot.png


Installing Graphviz
*******************
You need to install:

1. `Graphviz <http://www.graphviz.org/download/>`_.