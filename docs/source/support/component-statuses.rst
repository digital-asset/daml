.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Feature and Component Statuses
##############################

This page gives an overview of the statuses of released components and features according to :doc:`status-definitions`. Anything not listed here implicitly has status "Labs", but it's possible that something accidentally slipped the list so if in doubt, please :doc:`contact us <support>`.

Ledger API
**********

.. list-table::
   :widths: 80 10 10
   :header-rows: 1

   * - Component/Feature
     - Status
     - Deprecated on
   * - :doc:`Ledger API specification </app-dev/grpc/proto-docs>` including all semantics of >= Daml-LF 1.6
     - Stable
     -   
   * - `Numbered (ie non-dev) Versions of Proto definitions distributed via GitHub Releases <protobufs_>`_
     - Stable
     - 
   * - `Dev Versions of Proto definitions distributed via GitHub Releases <protobufs_>`_
     - Alpha
     -
   * - Use of divulged contracts in later transactions
     - Stable, Deprecated
     - 2021-06-16


.. _runtime-components:

Runtime Components
******************

.. list-table::
   :widths: 80 10 10
   :header-rows: 1

   * - Component / Feature
     - Status
     - Deprecated on
   * - **Canton**
     -
     -
   * - Canton Application and Console
     - Stable
     -
   * - Canton Administrative APIs for participant and domain nodes
     - Stable
     -
   * - Canton Protocol
     - Stable
     -
   * - Sequencer for PostgreSQL
     - Stable
     -
   * - Sequencer for Oracle DB
     - Stable
     -
   * - Sequencer for Hyperledger Fabric
     - Beta
     -
   * - Sequencer for Hyperledger Besu
     - Beta
     -
   * - Support for connecting a single participant to multiple domains
     - Alpha
     -
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
   * - :doc:`Daml API of individual Triggers </triggers/index>`
     - Stable
     -
   * - :doc:`Development CLI to start individual triggers in dev environment </triggers/index>` (``daml trigger``)
     - Stable
     -
   * - :doc:`/tools/trigger-service/index` (``daml trigger-service``)
     - Stable
     -
   * - **Non-repudiation**
     -
     -
   * - :doc:`/tools/non-repudiation`
     - Alpha
     -

.. _libraries:

Libraries
*********

.. list-table::
   :widths: 80 10 10
   :header-rows: 1

   * - Component / Feature
     - Status
     - Deprecated on
   * - **Java Ledger API Bindings**
     - 
     -
   * - ``daml codegen java`` :doc:`CLI and generated code </app-dev/bindings-java/codegen>`
     - Stable
     -
   * - ``bindings-java`` :doc:`library and its public API </app-dev/bindings-java/index>`.
     - Stable
     -
   * - ``bindings-rxjava`` :doc:`library and its public API </app-dev/bindings-java/index>`.
     - Stable
     -
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
   * - **Daml Libraries**
     -
     -
   * - :doc:`The Daml Standard Library </daml/stdlib/index>`
     - Stable
     -
   * - :doc:`The Daml Script Library </daml-script/api/index>`
     - Stable
     -
   * - :doc:`The Daml Trigger Library </triggers/api/index>`
     - Stable
     -
   
.. _dev-tools:

Developer Tools
***************

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
   * - Assistant commands to start Runtime Components: ``daml json-api``, ``daml trigger``, and ``daml trigger-service``.
     - See :ref:`runtime-components`.
     -
   * - **Daml Projects**
     -
     -
   * - ``daml.yaml`` project specification
     - Stable
     - 
   * - Assistant commands ``new``, ``create-daml-app``, and ``init``. Note that the templates created by ``daml new`` and ``create-daml-app`` are considered example code, and are not covered by :ref:`semantic versioning <versioning>`.
     - Stable
     -
   * - **Daml Studio**
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
   * - Daml Profiler in Sandbox
     - Stable
     -
   * - **Daml Compiler**
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
   * - Daml Linter (``daml damlc lint``)
     - Stable
     -
   * - Daml REPL (``daml damlc repl``)
     - See Daml REPL heading below
     -
   * - Daml Language Server CLI (``daml damlc ide``)
     - Labs
     -
   * - Daml Documentation Generation (``daml damlc docs``)
     - Labs
     -
   * - :doc:`Daml Model Visualization </tools/visual>` (``daml damlc visual`` and ``daml damlc visual-web``)
     - Labs
     -
   * - ``daml doctest``
     - Labs
     -
   * - **Script**
     -
     -
   * - :doc:`Script Daml API </daml-script/index>`
     - Stable
     -
   * - Daml Scenario IDE integration
     - Stable
     -
   * - Daml Script IDE integration
     - Stable
     -
   * - :doc:`Daml Script Library </daml-script/api/index>`
     - See :ref:`libraries`
     -
   * - ``daml test`` in-memory Script and Scenario test CLI
     - Stable
     -
   * - ``daml script`` :doc:`CLI to run Scripts against live ledgers. </daml-script/index>`
     - Stable
     -
   * - ``daml ledger export script`` :doc:`CLI extract Daml Script from ledgers. </tools/export/index>`
     - Alpha
     -
   * - **Navigator**
     -
     -
   * - :doc:`Daml Navigator Development UI </tools/navigator/index>` (``daml navigator server``)
     - Stable
     -
   * - Navigator Config File Creation (``daml navigator create-config``)
     - Stable
     -
   * - Navigator graphQL Schema (``daml navigator dump-graphql-schema``)
     - Labs
     -
   * - **Daml REPL Interactive Shell**
     -
     -
   * - ``daml repl`` :doc:`CLI </daml-repl/index>`
     - Stable
     -
   * - :doc:`Daml and meta-APIs of the REPL </daml-repl/index>`
     - Stable
     -
   * - **Ledger Administration CLI**
     -
     -
   * - ``daml ledger`` :doc:`CLI and all subcommands </deploy/generic_ledger>`.
     - Stable
     -
