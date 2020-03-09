.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Application architecture
########################

This section describes our recommended design of a full-stack DAML application.

.. image:: ./recommended_architecture.svg

The above image shows the recommended architecture. Of course there are many ways how you can change
the architecture and technology stack to fit your needs, which we'll mention in the corresponding
sections.

To get started quickly with the recommended application architecture clone the
``create-daml-app`` application template:

.. code-block:: bash

  git clone https://github.com/digital-asset/create-daml-app

``create-daml-app`` is a small, but fully functional demo application implementing the recommended
architecture, providing you with an excellent starting point for your own application. It showcases

- using DAML React libraries
- quick iteration against the :ref:`DAML Ledger Sandbox <sandbox-manual>`.
- authentication
- deploying your application in the cloud as a Docker container

Backend
~~~~~~~

The backend for your application can be any DAML ledger implementation running your DAR (:ref:`DAML
Archive <dar-file-dalf-file>`) file.

We recommend using the :ref:`DAML JSON API <json-api>` as an interface to your frontend. It is
served by the HTTP JSON API server connected to the ledger API server. It provides simple HTTP
endpoints to interact with the ledger via GET/POST requests. However, if you prefer, you can also
use the :ref:`gRPC API <grpc>` directly.

When you use the ``create-daml-app`` template application, you can start a local sandbox together
with a JSON API server by running

.. code-block:: bash

  ./daml-start.sh

in the root of the project. This is the most simple DAML ledger implementation. Once your
application matures and becomes ready for production, the ``daml deploy`` command helps you deploy
your frontend and DAML artefacts of your project to a production ledger. See :ref:`Deploying to DAML
Ledgers <deploy-ref_overview>` for an in depth manual for specific ledgers.

Frontend
~~~~~~~~

We recommended building your frontend with the `React <https://reactjs.org>`_ framework. However,
you can choose virtually any language for your frontend and interact with the ledger via :ref:`HTTP
JSON <json-api>` endpoints. In addition, we provide support libraries for :ref:`Java
<java-bindings>` and :ref:`Scala <scala-bindings>` and you can also interact with the :ref:`gRPC API
<grpc>` directly.

.. TODO (drsk) add and point to javascript bindings.
.. If you choose a different Javascript based frontend framework, the packages ``@daml/ledger``,
.. ``@daml/types`` and the generated ``@daml2ts`` package provide you with the necessary interface code
.. to connect and issue commands against your ledger.

We provide two libraries to build your React frontend for a DAML application.

+--------------------------------------------------------------+--------------------------------------------------------------------------+
| Name                                                         | Summary                                                                  |
+==============================================================+==========================================================================+
| `@daml/react <https://www.npmjs.com/package/@daml/react>`_   | React hooks to query/create/exercise DAML contracts                      |
+--------------------------------------------------------------+--------------------------------------------------------------------------+
| `@daml/ledger <https://www.npmjs.com/package/@daml/ledger>`_ | DAML ledger object to connect and directly submit commands to the ledger |
+--------------------------------------------------------------+--------------------------------------------------------------------------+

You can install any of these libraries by running ``yarn add <library>`` in the ``ui`` directory of
your project, e.g. ``yarn add @daml/react``. Please explore the ``create-daml-app`` example project
to see the usage of these libraries.

To make your life easy when interacting with the ledger, the DAML assistant can generate
corresponding typescript data definitions for the data types declared in the deployed DAR.

.. code-block:: bash

  daml codegen ts .daml/dist/<your-project-name.dar> -o daml-ts

This command will generate a typescript project in the ``daml-ts`` folder that needs to be connected
with your frontend code in ``ui``. To do so, navigate to ``daml-ts`` and run ``yarn install`` and
then ``yarn workspaces run build``.

.. TODO (drsk) this process is changing right now, make sure it is documented up to date here.

Authentication
~~~~~~~~~~~~~~

When you deploy your application to a production ledger, you need to authenticate the identities of
your users.

DAML ledgers support a unified interface for authentication of commands. Some DAML ledgers like for
example https://projectdabl.com offer an integrated authentication service, but you can also use an
external service provider for authentication like https://auth0.com. The DAML react libraries
support interfacing with an authenticated DAML ledger. Simply initialize your ``DamlLedger`` object
with the token obtained by an authentication service. How authentication works and the form of the
required tokens is described in the :ref:`Authentication <authentication>` section.

Developer workflow
~~~~~~~~~~~~~~~~~~

The DAML SDK enables a local development environment with fast iteration cycles. If you run
``daml-reload-on-change.sh`` of the ``create-daml-app``, a local DAML sandbox ledger is started that
is updated with your most recent DAML code on any change. Next, you can start your frontend in
development mode by changing to your ``ui`` directory and run ``yarn start``. This will reload your
frontend whenever you make changes to it. You can add unit tests for your DAML models by writing
:ref:`DAML scenarios <testing-using-scenarios>`. These will also be reevaluated on change.  A
typical DAML developer workflow is to

  #. Make a small change to your DAML data model
  #. Optionally test your DAML code and with :ref:`scenarios <testing-using-scenarios>`
  #. Edit your React components to be aligned with changes made in DAML code
  #. Extend the UI to make use of the newly introduced feature
  #. Make further changes either to your DAML and/or React code until you're happy with what you've developed

.. image:: ./developer_workflow.svg
