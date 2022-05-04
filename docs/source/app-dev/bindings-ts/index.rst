.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Using JavaScript Client Libraries with Daml
###########################################

.. _typescript:

The JavaScript Client Libraries are the recommended way to build a frontend for a Daml application.
The :doc:`JavaScript Code Generator <daml2js>` can automatically generate JavaScript containing metadata about Daml packages that is required to use these libraries.
We provide an integration for the `React <https://reactjs.org>`_ framework with the
`@daml/react <daml-react/index.html>`_ library.  However, you can choose any JavaScript/TypeScript based framework
and use the `@daml/ledger <daml-ledger/index.html>`_ library directly to connect and interact with a
Daml ledger via its :ref:`HTTP JSON API <json-api>`.

The `@daml/types <daml-types/index.html>`_ library contains TypeScript data types corresponding to
primitive Daml data types, such as ``Party`` or ``Text``. It is used by the `@daml/react <daml-react/index.html>`_
and `@daml/ledger <daml-ledger/index.html>`_ libraries.

.. toctree::
   :hidden:

   daml2js
   daml-react
   daml-ledger
   daml-types
