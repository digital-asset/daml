.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Building Applications
=====================

The Building Applications section covers the elements that are used to create, extend, and test your Daml full-stack application (including APIs and JavaScript client libraries) and the architectural best practices for bringing those elements together.

As with the Writing Daml section, you can find the Daml code for the example application and features `here <https://github.com/digital-asset/daml/tree/main/docs/source/daml/intro/daml>`_ or download it using the Daml assistant. For example, to load the sources for section 1 into a folder called ``intro1``, run daml new intro1 --template daml-intro-1.

To run the examples, you will first need to `install the Daml SDK <https://docs.daml.com/getting-started/installation.html>`_.

.. toctree::
  :maxdepth: 2

  app-dev/app-arch
  app-dev/bindings-ts/index
  json-api/index
  app-dev/bindings-java/index
  app-dev/bindings-python
  daml-script/index
  daml-repl/index
  upgrade/index
  app-dev/authorization
  app-dev/ledger-api
  app-dev/command-deduplication
  triggers/index
  tools/trigger-service/index
  tools/auth-middleware/index
