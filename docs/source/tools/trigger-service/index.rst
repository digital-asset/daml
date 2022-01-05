.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _trigger-service:

Trigger Service
###############

.. toctree::
   :hidden:

   ./authorization
   ./auth0_example

The :ref:`running-a-no-op-trigger` section shows a simple method using the ``daml trigger`` command to arrange for the execution of a single trigger. Using this method, a dedicated process is launched to host the trigger.

Complex workflows can require running many triggers for many parties and at a certain point, use of ``daml trigger`` with its process per trigger model becomes unwieldy. The Trigger Service provides the means to host multiple triggers for multiple parties running against a common ledger in a single process and provides a convenient interface for starting, stopping and monitoring them.

The Trigger Service is a ledger client that acts as an end-user agent. The Trigger Service intermediates between the ledger and end-users by running triggers on their behalf. The Trigger Service is an HTTP service. All requests and responses use JSON to encode data.

Starting the Trigger Service
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this example, it is assumed there is a sandbox ledger running on port 6865 on localhost.

.. code-block:: bash

   daml trigger-service --config trigger-service.conf

where the corresponding config would be

The required config would look like

.. code-block:: none

    {
      //dar file containing the trigger
      dar-paths = [
        "./my-app.dar"
      ]

      //IP address that Trigger service listens on. Defaults to 127.0.0.1.
      address = "127.0.0.1"
      //Trigger service port number. Defaults to 8088. A port number of 0 will let the system pick an ephemeral port. Consider specifying `port-file` with port number 0.
      port = 8088
      //port-file = "dummy-port-file"

      ledger-api {
        address = "localhost"
        port = 6865
      }

      //Optional max inbound message size in bytes. Defaults to 4194304.
      max-inbound-message-size = 4194304
      //Minimum time interval before restarting a failed trigger. Defaults to 5 seconds.
      min-restart-interval = 5s
      //Maximum time interval between restarting a failed trigger. Defaults to 60 seconds.
      max-restart-interval = 60s
      //Optional max HTTP entity upload size in bytes. Defaults to 4194304.
      max-http-entity-upload-size = 4194304
      //Optional HTTP entity upload timeout. Defaults to 60 seconds.
      http-entity-upload-timeout = 60s
      //Use static time or wall-clock, default is wall-clock time.
      time-provider-type = "wall-clock"
      //Compiler config type to use , default or dev mode
      compiler-config = "default"
      //TTL in seconds used for commands emitted by the trigger. Defaults to 30s.
      ttl = 60s

      //Initialize database and terminate.
      init-db = "false"

      //Do not abort if there are existing tables in the database schema. EXPERT ONLY. Defaults to false.
      allow-existing-schema = "false"

      trigger-store {
        user = "postgres"
        password = "password"
        driver = "org.postgresql.Driver"
        url = "jdbc:postgresql://localhost:5432/test?&ssl=true"

        // prefix for table names to avoid collisions, empty by default
        table-prefix = "foo"

        // max pool size for the database connection pool
        pool-size = 12
        //specifies the min idle connections for database connection pool.
        min-idle = 4
        //specifies the idle timeout for the database connection pool.
        idle-timeout = 12s
        //specifies the connection timeout for database connection pool.
        connection-timeout = 90s
      }

      authorization {
        //Sets both the internal and external auth URIs.
        //auth-common-uri = "https://oauth2/common-uri"

        // Auth Client to redirect to login , defaults to no
        auth-redirect = "yes"

        //Sets the internal auth URIs (used by the trigger service to connect directly to the middleware). Overrides value set by auth-common
        auth-internal-uri = "https://oauth2/internal-uri"
        //Sets the external auth URI (the one returned to the browser). overrides value set by auth-common.
        auth-external-uri = "https://oauth2/external-uri"
        //URI to the auth login flow callback endpoint `/cb`. By default constructed from the incoming login request.
        auth-callback-uri =  "https://oauth2/callback-uri"

        //Optional max number of pending authorization requests. Defaults to 250.
        max-pending-authorizations = 250
        //Optional authorization timeout, defaults to 60 seconds
        authorization-timeout = 60s
      }
    }

The above starts the Trigger Service using a number of default parameters. Most notably, the HTTP port the Trigger Service listens on which defaults to 8088.
The above config file should list all available parameters, their defaults and descriptions.

The trigger-service can also be started using cli-args as shown below, one can execute the command ``daml trigger-service --help`` to find all available parameters.

.. note:: Configuration file is the recommended way to run trigger-service, running via cli-args is now deprecated


.. code-block:: bash

   daml trigger-service --ledger-host localhost \
                        --ledger-port 6865 \
                        --wall-clock-time

Although as we'll see, the Trigger Service exposes an endpoint for end-users to upload DAR files to the service it is sometimes convenient to start the service pre-configured with a specific DAR. To do this, the ``--dar`` option is provided.

.. code-block:: bash

   daml trigger-service --ledger-host localhost \
                        --ledger-port 6865 \
                        --wall-clock-time \
                        --dar .daml/dist/create-daml-app-0.1.0.dar

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
  package id using ``daml damlc inspect path/to/trigger.dar | head -1``.
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

The status endoint returns metadata about the trigger like the
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

Upload a DAR containing one or more triggers. If successful, the DAR's "main package ID" will be in the response (the main package ID for a DAR can also be obtained using ``daml damlc inspect path/to/dar | head -1``).

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

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    { "status": "pass" }
