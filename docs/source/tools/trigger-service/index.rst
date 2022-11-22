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

Complex workflows can require running many triggers for many parties and at a certain point, use of ``daml trigger`` with its process-per-trigger model becomes unwieldy. The Trigger Service provides the means to host multiple triggers for multiple parties running against a common ledger in a single process and provides a convenient interface for starting, stopping and monitoring them.

The Trigger Service is a ledger client that acts as an end-user agent. The Trigger Service intermediates between the ledger and end-users by running triggers on their behalf. The Trigger Service is an HTTP service. All requests and responses use JSON to encode data.

Start the Trigger Service
*************************

In this example, it is assumed there is a Ledger API server running on port 6865 on `localhost`.

.. code-block:: bash

   daml trigger-service --config trigger-service.conf

The following snippet provides an example of what a possible `trigger-service.conf` configuration file could look like,
alongside a few annotations with regards to the meaning of the configuration keys and possibly their default values.

.. code-block:: none

    {
      // Mandatory. Paths to the DAR files containing the code executed by the trigger.
      dar-paths = [
        "./my-app.dar"
      ]

      // Mandatory. Host address that the Trigger Service listens on. Defaults to 127.0.0.1.
      address = "127.0.0.1"

      // Mandatory. Trigger Service port number. Defaults to 8088.
      // A port number of 0 will let the system pick an ephemeral port.
      port = 8088
      // Optional. If using 0 as the port number, consider specifying the path to a `port-file` where the chosen port will be saved in textual format.
      //port-file = "/path/to/port-file"

      // Mandatory. Ledger API server address and port.
      ledger-api {
        address = "localhost"
        port = 6865
      }

      // Maximum inbound message size in bytes. Defaults to 4194304 (4 MB).
      max-inbound-message-size = 4194304

      // Minimum and maximum time interval before restarting a failed trigger. Defaults to 5 and 60 seconds respectively.
      min-restart-interval = 5s
      max-restart-interval = 60s

      // Maximum HTTP entity upload size in bytes. Defaults to 4194304 (4 MB).
      max-http-entity-upload-size = 4194304

      // HTTP entity upload timeout. Defaults to 60 seconds.
      http-entity-upload-timeout = 60s

      // Use static or wall-clock time. Defaults to `wall-clock`.
      time-provider-type = "wall-clock"

      // Compiler configuration type to use between `default` or `dev`. Defaults to `default`.
      compiler-config = "default"

      // Time-to-live used for commands emitted by the trigger. Defaults to 30 seconds.
      ttl = 30s

      // If true, initialize the database and terminate immediately. Defaults to false.
      init-db = "false"

      // Do not abort if there are existing tables in the database schema. EXPERT ONLY. Defaults to false.
      allow-existing-schema = "false"

      // Configuration for the persistent store that will be used to keep track of running triggers across restarts.
      // Mandatory if `init-db` is true. Otherwise optional. If not provided, the trigger state will not be persisted
      // and restored across restarts.
      trigger-store {

        // Mandatory. Database coordinates.
        user = "postgres"
        password = "password"
        driver = "org.postgresql.Driver"
        url = "jdbc:postgresql://localhost:5432/test?&ssl=true"

        // Prefix for table names to avoid collisions. EXPERT ONLY. By default, this is empty and not used.
        //table-prefix = "foo"

        // Maximum size for the database connection pool. Defaults to 8.
        pool-size = 8

        // Minimum idle connections for the database connection pool. Defaults to 8.
        min-idle = 8

        // Idle timeout for the database connection pool. Defaults to 10 seconds.
        idle-timeout = 10s

        // Timeout for database connection pool. Defaults to 5 seconds.
        connection-timeout = 5s
      }

      authorization {

        // Auth client to redirect to login. Defaults to `no`.
        auth-redirect = "no"

        // The following options configure the auth URIs.
        // Either just `auth-common-uri` or both `auth-internal-uri` and `auth-external-uri` must be specified.
        // If all are specified, `auth-internal-uri` and `auth-external-uri` take precedence.

        // Sets both the internal and external auth URIs.
        //auth-common-uri = "https://oauth2/common-uri"

        // Internal auth URI used by the Trigger Service to connect directly to the Auth Middleware.
        auth-internal-uri = "https://oauth2/internal-uri"

        // External auth URI (the one returned to the browser).
        // This value takes precedence over the one specified for `auth-common`.
        auth-external-uri = "https://oauth2/external-uri"

        // Optional. URI to the auth login flow callback endpoint `/cb`. By default it is constructed from the incoming login request.
        // auth-callback-uri = "https://oauth2/callback-uri"

        // Maximum number of pending authorization requests. Defaults to 250.
        max-pending-authorizations = 250

        // Authorization timeout. Defaults to 60 seconds.
        authorization-timeout = 60s
      }
    }

The Trigger Service can also be started using command line arguments as shown below. The command ``daml trigger-service --help`` lists all available parameters.

.. note:: Using the configuration format shown above is the recommended way to configure Trigger Service, running with command line arguments is now deprecated.

.. code-block:: bash

   daml trigger-service --ledger-host localhost \
                        --ledger-port 6865 \
                        --wall-clock-time

Although, as we'll see, the Trigger Service exposes an endpoint for end-users to upload DAR files to the service it is sometimes convenient to start the service pre-configured with a specific DAR. To do this, the ``--dar`` option is provided.

.. code-block:: bash

   daml trigger-service --ledger-host localhost \
                        --ledger-port 6865 \
                        --wall-clock-time \
                        --dar .daml/dist/create-daml-app-0.1.0.dar

Endpoints
*********

Start a Trigger
===============

Start a trigger. In this example, ``alice`` starts the trigger called ``trigger`` in a module called ``TestTrigger`` of a package with ID ``312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14``.
The response contains an identifier for the running trigger that ``alice`` can use in subsequent commands involving the trigger.

HTTP Request
------------

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
  package ID using ``daml damlc inspect path/to/trigger.dar | head -1``.
- ``party`` is the party on behalf of which the trigger is running.
- ``applicationId`` is an optional field to specify the application ID
  the trigger will use for command submissions. If omitted, the
  trigger will default to using its random UUID identifier returned in
  the start request as the application ID.

HTTP Response
-------------

.. code-block:: json

    {
      "result":{"triggerId":"4d539e9c-b962-4762-be71-40a5c97a47a6"},
      "status":200
    }


Stop a Trigger
==============

Stop a running trigger. In this example, the request asks to stop the trigger started above.

HTTP Request
------------

- URL: ``/v1/triggers/:id``
- Method: ``DELETE``
- Content-Type: ``application/json``
- Content:

HTTP Response
-------------

- Content-Type: ``application/json``
- Content:

.. code-block:: json

   {
     "result": {"triggerId":"4d539e9c-b962-4762-be71-40a5c97a47a6"},
     "status":200
   }

.. _list-running-triggers:

List Running Triggers
=====================

List the triggers running on behalf of a given party.

HTTP Request
------------

- URL: ``/v1/triggers?party=:party``
- Method: ``GET``

HTTP Response
-------------

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "result": {"triggerIds":["4d539e9c-b962-4762-be71-40a5c97a47a6"]},
      "status":200
    }

Status of a Trigger
===================

This endpoint returns data about a trigger, including the party on behalf of which it is running, its identifier,
and its current state (querying the active contract set, running, or stopped).

HTTP Request
------------

- URL: ``/v1/triggers/:id``
- Method: ``GET``

HTTP Response
-------------

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

Upload a New DAR
================

Upload a DAR containing one or more triggers. If successful, the DAR's "main package ID" will be in the response (the main package ID for a DAR can also be obtained using ``daml damlc inspect path/to/dar | head -1``).

HTTP Request
------------

- URL: ``/v1/packages``
- Method: ``POST``
- Content-Type: ``multipart/form-data``
- Content:

  ``dar=$dar_content``

HTTP Response
-------------

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "result": {"mainPackageId":"312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14"},
      "status": 200
    }

Liveness Check
==============

This can be used as a liveness probe, e.g., in Kubernetes.

HTTP Request
------------

- URL: ``/livez``
- Method: ``GET``

HTTP Response
-------------

A status code of ``200`` indicates a successful liveness check.

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    { "status": "pass" }

Readiness Check
===============

This can be used as a readiness probe, e.g., in Kubernetes.

HTTP Request
------------

- URL: ``/readyz``
- Method: ``GET``

HTTP Response
-------------

A status code of ``200`` indicates a successful readiness check.


Metrics
*******

Enable and Configure Reporting
==============================

To enable metrics and configure reporting, you can use the below config block in application config:

.. code-block:: none

    metrics {
      // Start a metrics reporter. Must be one of "console", "csv:///PATH", "graphite://HOST[:PORT][/METRIC_PREFIX]", or "prometheus://HOST[:PORT]".
      reporter = "console"
      // Set metric reporting interval , examples : 1s, 30s, 1m, 1h
      reporting-interval = 30s
    }

Reported Metrics
================

If a Prometheus metrics reporter is configured, the Trigger Service exposes the :doc:`common HTTP metrics </json-api/production-setup/metrics#common-http-metrics>` for all endpoints.
