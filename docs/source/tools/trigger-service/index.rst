.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Trigger Service
###############

.. toctree::
   :hidden:

   ./authorization
   ./auth0_example

The :ref:`running-a-no-op-trigger` section shows a simple method using the ``daml trigger`` command to arrange for the execution of a single trigger. Using this method, a dedicated process is launched to host the trigger.

Complex workflows can require running many triggers for many parties and at a certain point, use of ``daml trigger`` with its process per trigger model becomes unwieldy. The Trigger Service provides the means to host multiple triggers for multiple parties running against a common ledger in a single process and provides a convenient interface for starting, stopping and monitoring them.

The Trigger Service is a ledger client that acts as an end-user agent. The Trigger Service intermediates between the ledger and end-users by running triggers on their behalf. The Trigger Service is an HTTP REST service. All requests and responses use JSON to encode data.

Starting the Trigger Service
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this example, it is assumed there is a sandbox ledger running on port 6865 on localhost.

.. code-block:: bash

   daml trigger-service --ledger-host localhost --ledger-port 6865 --wall-clock-time

The above starts the Trigger Service using a number of default parameters. Most notably, the HTTP port the Trigger Service listens on which defaults to 8088. To see all of the available parameters, their defaults and descriptions, one can execute the command ``daml trigger-service --help``.

Although as we'll see, the Trigger Service exposes an endpoint for end-users to upload DAR files to the service it is sometimes convenient to start the service pre-configured with a specific DAR. To do this, the ``--dar`` option is provided.

.. code-block:: bash

   daml trigger-service --ledger-host localhost --ledger-port 6865 --wall-clock-time

Endpoints
~~~~~~~~~

Start a trigger
***************

Start a trigger. In this example, Alice starts the trigger called ``trigger`` in a module called ``TestTrigger`` of a package with ID ``312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14``. The response contains an identifier for the running trigger that Alice can use in subsequent commands involving the trigger.

HTTP Request
============

- URL: ``/v1/triggers``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "triggerName": "312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14:TestTrigger:trigger",
      "party": "alice",
      "applicationId": "my-app-id"
    }

where

- ``triggerName`` contains the identifier for the trigger in the form
  ``${packageId}:${moduleName}:${identifierName}``. You can find the
  package id using ``daml damlc inspect path/to/trigger.dar``.
- ``party`` is the party the trigger will be running as.
- ``applicationId`` is an optional field to specify the application ID
  the trigger will use for command submissions. If omitted, the
  trigger will default to using its random UUID identifier returned in
  the start request as the application ID.

HTTP Response
=============

.. code-block:: json

    {
      "result":{"triggerId":"4d539e9c-b962-4762-be71-40a5c97a47a6"},
      "status":200
    }


Stop a trigger
**************

Stop a running trigger. Alice stops her running trigger like so.

HTTP Request
============

- URL: ``/v1/triggers/:id``
- Method: ``DELETE``
- Content-Type: ``application/json``
- Content:

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

   {
     "result": {"triggerId":"4d539e9c-b962-4762-be71-40a5c97a47a6"},
     "status":200
   }

.. _list-running-triggers:

List running triggers
*********************

List the Triggers running on behalf of a given party.

HTTP Request
============

- URL: ``/v1/triggers?party=:party``
- Method: ``GET``

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "result": {"triggerIds":["4d539e9c-b962-4762-be71-40a5c97a47a6"]},
      "status":200
    }

Status of a trigger
*******************

The status endoint returns you metadata about the trigger like the
party it is running as and the trigger id as well as the state the
trigger is in (querying the acs, running, stopped).

HTTP Request
============

- URL: ``/v1/triggers/:id``
- Method: ``GET``

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "result":
        {
          "party": "Alice",
          "triggerId":"312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14:TestTrigger:trigger",
          "status": "running"
        },
      "status":200
    }

Upload a new DAR
****************

Upload a DAR containing one or more triggers. If successful, the DAR's "main package ID" will be in the response (the main package ID for a DAR can also be obtained using ``daml damlc inspect-dar path/to/dar``).

HTTP Request
============

- URL: ``/v1/packages``
- Method: ``POST``
- Content-Type: ``multipart/form-data``
- Content:

  ``dar=$dar_content``

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "result": {"mainPackageId":"312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14"},
      "status": 200
    }

Liveness check
**************

This can be used as a liveness probe, e.g., in Kubernetes.

HTTP Request
============

- URL: ``/livez``
- Method: ``GET``

.. code-block:: json

   {"status":"pass"}

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    { "status": "pass" }
