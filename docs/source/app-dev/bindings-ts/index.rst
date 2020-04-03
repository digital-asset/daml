.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

TypeScript bindings
###################

.. _typescript:

The DAML TypeScript bindings are the recommended way to build a frontend for a DAML application. We
provide an integration for the `React <https://reactjs.org>`_ framework with the `@daml/react
<daml-react/index.html>`_ library.  However, you can choose any JavaScript/TypeScript based framework
and use the `@daml/ledger <daml-ledger/index.html>`_ library directly to connect and interact with a
DAML ledger via its :ref:`HTTP JSON API <json-api>`.

The `@daml/types <daml-types/index.html>`_ library contains TypeScript data types corresponding to
primitive DAML data types, such as ``Party`` or ``Text``. It is used by the `@daml/react <daml-react/index.html>`_
and `@daml/ledger <daml-ledger/index.html>`_ libraries.

.. toctree::
   :hidden:

   daml-react
   daml-ledger
   daml-types
