.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Using JavaScript Client Libraries with Daml
###########################################

.. _typescript:

The JavaScript client libraries allow to easily build frontend applications that interact with the :ref:`HTTP JSON API service <json-api>`.

To quickly build a prototype or an application with relatively simple requirements, these libraries can dramatically reduce the time
necessary to develop a full-stack application by abstracting away implementation details.

The `@daml/types <ts-daml-types_>`_ library contains the TypeScript data types corresponding to primitive Daml data type, such as ``Party`` or ``Text``.
Apart from its usefulness for TypeScript developers, the library can also be pulled in as a development-type dependency for JavaScript projects
to take advantage of tooling integration with the TypeScript ecosystem, such as the availability of autocompletion on Visual Studio Code.

The `@daml/ledger <ts-daml-ledger_>` library contains functions to interact with the endpoints exposed by HTTP JSON API service and forms
the basic layer of functionality. At this layer, you can easily query for active contracts from the ledger, create new ones or exercise
choices. This layer is agnostic with regards to any specific framework required to build the frontend.

Finally, if you are a `React.js <https://react.js.org`_ user, you can take advantage of the `@daml/react <ts-daml-react_>`_ library, which
builds on top of ``@daml/ledger`` with extensions specific to React.js. This bridges the gap between the basic functionality and the
infrastructure required to build a React.js-based frontend application. If you want to start from a ready-made application that uses this
library you can start running from the following template::

    daml new --template create-daml-app <name-of-your-project>

To use these libraries, you need to use the :doc:`JavaScript Code Generator <daml2js>` to automatically generate TypeScript containing metadata about Daml packages.

.. toctree::
   :hidden:

   daml2js
   daml-react
   daml-ledger
   daml-types
