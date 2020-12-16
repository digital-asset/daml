.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Feature and Component Statuses
==============================

This page gives an overview of the statuses of released components and features according to :doc:`status-definitions`. Anything not listed here implicitly has status "Labs", but it's possible that something accidentally slipped the list so if in doubt, please :doc:`contact us <support>`.

Ledger API
----------

.. list-table::
   :widths: 80 10 10
   :header-rows: 1

   * - Component/Feature
     - Status
     - Deprecated on
   * - :doc:`Ledger API specification </app-dev/grpc/proto-docs>` including all semantics of >= DAML-LF 1.6
     - Stable
     -   
   * - `Numbered (ie non-dev) Versions of Proto definitions distributed via GitHub Releases <protobufs_>`_
     - Stable
     - 
   * - `Dev Versions of Proto definitions distributed via GitHub Releases <protobufs_>`_
     - Alpha
     - 

Integration Components
----------------------

.. list-table::
   :widths: 80 10 10
   :header-rows: 1

   * - Component/Feature
     - Status
     - Deprecated on
   * - :doc:`Integration Kit Components </daml-integration-kit/index>`
     - Labs
     -
   * - CLI and test names of :doc:`/tools/ledger-api-test-tool/index`
     - Beta
     -

.. _runtime-components:

Runtime components
------------------

.. list-table::
   :widths: 80 10 10
   :header-rows: 1

   * - Component / Feature
     - Status
     - Deprecated on
   * - **JSON API**
     - 
     -
   * - HTTP endpoints under ``/v1/`` including status codes, authentication, query language and encoding.
     - Stable
     -
   * - ``daml json-api`` CLI :ref:`for development <dev-tools>`. (as specified using ``daml json-api --help``)
     - Stable
     -
   * - Stand-alone distribution for production use, including CLI specified in ``--help``.
     - Stable
     -
   * - **Triggers**
     -
     -
   * - :doc:`DAML API of individual Triggers </triggers/index>`
     - Beta
     -
   * - :doc:`Development CLI to start individual triggers in dev environment </triggers/index>` (``daml trigger``)
     - Beta
     -
   * - :doc:`/tools/trigger-service` (``daml trigger-service``)
     - Alpha
     -
   * - **Extractor**
     -
     -
   * - :doc:`/tools/extractor` (``daml extractor``)
     - Labs
     -

.. _libraries:

Libraries
---------

.. list-table::
   :widths: 80 10 10
   :header-rows: 1

   * - Component / Feature
     - Status
     - Deprecated on
   * - **Scala Ledger API Bindings**
     -
     -
   * - ``daml codegen scala`` :doc:`CLI and generated code </app-dev/bindings-scala/index>`
     - Stable, Deprecated
     - 2020-10-14
   * - ``bindings-scala_2.12`` :doc:`library and its public API </app-dev/bindings-scala/index>`
     - Stable, Deprecated
     - 2020-10-14
   * - **Java Ledger API Bindings**
     - 
     -
   * - ``daml codegen java`` :doc:`CLI and generated code </app-dev/bindings-java/codegen>`
     - Stable
     -
   * - ``bindings-java`` :doc:`library and its public API </app-dev/bindings-java/index>`.
     - Stable
     -
   * - ``bindings-rxjava`` :doc:`library and its public API </app-dev/bindings-java/index>` *excluding* the reactive components in package ``com.daml.ledger.rxjava.components``.
     - Stable
     -
   * - Java Reactive Components in the ``com.daml.ledger.rxjava.components`` package of ``bindings-rxjava``.
     - Stable, Deprecated
     - 2020-10-14
   * - Maven artifact ``daml-lf-1.6-archive-java-proto``
     - Stable
     -
   * - Maven artifact ``daml-lf-1.7-archive-java-proto``
     - Stable
     -
   * - Maven artifact ``daml-lf-1.8-archive-java-proto``
     - Stable
     -
   * - Maven artifact ``daml-lf-dev-archive-java-proto``
     - Alpha
     -
   * - **Node.js Ledger API Bindings**
     -
     -
   * - ``@digital-asset/bindings-js`` :doc:`Node.js library </app-dev/bindings-js>`
     - Stable, Deprecated
     - 2020-10-14
   * - **JavaScript Client Libraries**
     -
     -
   * - ``daml codegen js`` :doc:`CLI and generated code </app-dev/bindings-ts/daml2js>`
     - Stable
     -
   * - ``@daml/types`` :doc:`library and its public API </app-dev/bindings-ts/daml-types>`
     - Stable
     -
   * - ``@daml/ledger`` :doc:`library and its public API </app-dev/bindings-ts/daml-ledger>`
     - Stable
     -
   * - ``@daml/react`` :doc:`library and its public API </app-dev/bindings-ts/daml-react>`
     - Stable
     -
   * - **DAML Libraries**
     -
     -
   * - :doc:`The DAML Standard Library </daml/stdlib/index>`
     - Stable
     -
   * - :doc:`The DAML Script Library </daml-script/api/index>`
     - Stable
     -
   * - :doc:`The DAML Trigger Library </triggers/api/index>`
     - Stable
     -
   
.. _dev-tools:

Developer Tools
---------------

.. list-table::
   :widths: 80 10 10
   :header-rows: 1

   * - Component / Feature
     - Status
     - Deprecated on
   * - **SDK**
     -
     -
   * - Windows SDK (installer_)
     - Stable
     -
   * - :ref:`Mac SDK <mac-linux-sdk>`
     - Stable
     -
   * - :ref:`Linux SDK <mac-linux-sdk>`
     - Stable
     -
   * - :doc:`/tools/assistant` with top level commands

       - ``--help``
       - ``version``
       - ``install``
       - ``uninstall``
     - Stable
     -
   * - ``daml start`` helper command and associated CLI (``daml start --help``)
     - Stable
     - 
   * - ``daml deploy`` :doc:`helper command and associated CLI </deploy/generic_ledger>` (``daml deploy --help``)
     - Stable
     -
   * - Assistant commands to start Runtime Components: ``daml json-api``, ``daml trigger``, ``daml trigger-service``, and ``daml extractor``.
     - See :ref:`runtime-components`.
     -
   * - **DAML Projects**
     -
     -
   * - ``daml.yaml`` project specification
     - Stable
     - 
   * - Assistant commands ``new``, ``create-daml-app``, and ``init``. Note that the templates created by ``daml new`` and ``create-daml-app`` are considered example code, and are not covered by :ref:`semantic versioning <versioning>`.
     - Stable
     -
   * - **DAML Studio**
     -
     -
   * - VSCode Extension
     - Stable
     -
   * - ``daml studio`` assistant command
     - Stable
     -
   * - **Code Generation**
     -
     -
   * - ``daml codegen`` assistant commands
     - See :ref:`libraries`.
     -
   * - **Sandbox Development Ledger**
     -
     -
   * - ``daml sandbox`` assistant command and documented CLI under ``daml sandbox --help``.
     - Stable
     -
   * - DAML Sandbox in Memory (ie without the ``--sql-backend-jdbcurl`` flag)
     - Stable
     -
   * - DAML Sandbox on Postgres (iw with the ``--sql-backend-jdbcurl`` flag)
     - Stable, Deprecated
     - 2020-12-16
   * - DAML Sandbox Classic and associated CLIs ``daml sandbox-classic``, ``daml start --sandbox-classic``
     - Stable, Deprecated
     - 2020-04-09
   * - **DAML Compiler**
     -
     -
   * - ``daml build`` CLI
     - Stable
     -
   * - ``daml damlc`` CLI
     - Stable
     -
   * - Compilation and packaging (``daml damlc build``)
     - Stable
     -
   * - Legacy packaging command (``daml damlc package``)
     - Stable, Deprecated
     - 2020-10-14
   * - In-memory Scenario/Script testing (``daml damlc test``)
     - Stable
     -
   * - DAR File inspection (``daml damlc inspect-dar``). The exact output is only covered by :ref:`semantic versioning <versioning>` when used with the ``--json`` flag.
     - Stable
     -
   * - DAR File validation (``daml damlc validate-dar``)
     - Stable
     -
   * - DAML Linter (``daml damlc lint``)
     - Stable
     -
   * - DAML REPL (``daml damlc repl``)
     - See DAML REPL heading below
     -
   * - DAML Language Server CLI (``daml damlc ide``)
     - Labs
     -
   * - DAML Documentation Generation (``daml damlc docs``)
     - Labs
     -
   * - :doc:`DAML Model Visualization </tools/visual>` (``daml damlc visual`` and ``daml damlc visual-web``)
     - Labs
     -
   * - ``daml doctest``
     - Labs
     -
   * - **Scenarios and Script**
     -
     -
   * - Scenario DAML API
     - Stable
     -
   * - :doc:`Script DAML API </daml-script/index>`
     - Stable
     -
   * - DAML Scenario IDE integration
     - Stable
     -
   * - DAML Script IDE integration
     - Stable
     -
   * - :doc:`DAML Script Library </daml-script/api/index>`
     - See :ref:`libraries`
     -
   * - ``daml test`` in-memory Script and Scenario test CLI
     - Stable
     -
   * - ``daml test-script`` Sandbox-based Script Testing
     - Stable
     -
   * - ``daml script`` :doc:`CLI to run Scripts against live ledgers. </daml-script/index>`
     - Stable
     -
   * - **Navigator**
     -
     -
   * - :doc:`DAML Navigator Development UI </tools/navigator/index>` (``daml navigator server``)
     - Stable
     -
   * - Navigator Config File Creation (``daml navigator create-config``)
     - Stable
     -
   * - :doc:`DAML Navigator Console </tools/navigator/console>` (``daml navigator console``)
     - Labs
     -
   * - Navigator graphQL Schema (``daml navigator dump-graphql-schema``)
     - Labs
     -
   * - **DAML REPL Interactive Shell**
     -
     -
   * - ``daml repl`` :doc:`CLI </daml-repl/index>`
     - Stable
     -
   * - :doc:`DAML and meta-APIs of the REPL </daml-repl/index>`
     - Stable
     -
   * - **Ledger Administration CLI**
     -
     -
   * - ``daml ledger`` :doc:`CLI and all subcommands </deploy/generic_ledger>`.
     - Stable
     -
