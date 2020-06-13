.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Trigger Service
###############

**WARNING:** The Trigger Service is an early access feature that is actively being designed and is *subject to breaking changes*. The documentation here at this time is limited to basic usage. As more features become available the documentation will be updated to include them. We welcome feedback about the Trigger Service on our `our issue tracker <https://github.com/digital-asset/daml/issues/new>`_ or `on our forum <https://discuss.daml.com>`_, or on `on Slack <https://slack.daml.com>`_.

The `DAML triggers <../triggers/index.html#running-a-daml-trigger>`_ documentation shows a simple method using the ``daml trigger`` command to arrange for the execution of a single trigger. Using this method, a dedicated process is launched to host the trigger.

Complex workflows can require running many triggers for many parties and at a certain point, use of ``daml trigger`` with its process per trigger model becomes unwieldy. The Trigger Service provides the means to host multiple triggers for multiple parties running against a common ledger in a single process and provides a convenient interface for starting, stopping and monitoring them.

The trigger-service is a ledger client that acts as an end-user agent. The trigger service intermediates between the ledger and end-users by running triggers on their behalf. The trigger-service is an HTTP REST service. All requests and responses use JSON to encode data.

Starting the Trigger Service
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this example, it is assumed there is a sandbox ledger running on port 6865 on localhost.

.. code-block:: bash

   daml trigger-service --ledger-host localhost --ledger-port 6865 --wall-clock-time --no-secret-key

The above starts the Trigger Service using a number of default parameters. Most notably, the HTTP port the Trigger Service listens on which defaults to 8088. The meaning of the ``--no-secret-key`` parameter will be explained in a later section. To see all of the available parameters, their defaults and descriptions, one can execute the command ``daml trigger-service --help``.

Although as we'll see, the Trigger Service exposes an endpoint for end-users to upload DAR files to the service it is sometimes convenient to start the service pre-configured with a specific DAR. To do this, the ``--dar`` option is provided.

.. code-block:: bash

   daml trigger-service --ledger-host localhost --ledger-port 6865 --wall-clock-time --no-secret-key --dar <DAR>

End-user interaction with the Trigger Service
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``start``
*********

Start a trigger. In this example, Alice starts the trigger called ``trigger`` in a module called ``TestTrigger`` of a package with ID ``312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14``. The response contains an identifier for the running trigger that Alice can use in subsequent commands involving the trigger.

.. code-block:: bash

   $curl --user 'alice:secret' \
      -X POST localhost:8088/v1/start \
      -H "Content-type: application/json" -H "Accept: application/json" \
      -d '{"triggerName":"312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14:TestTrigger:trigger"}'
   {"result":{"triggerId":"4d539e9c-b962-4762-be71-40a5c97a47a6"},"status":200}

``stop``
********

Stop a running trigger. Alice stops her running trigger like so.

.. code-block:: bash

   $curl --user 'alice:secret' \
      -X DELETE localhost:8088/v1/stop/4d539e9c-b962-4762-be71-40a5c97a47a6 \
      -H "Content-type: application/json" -H "Accept: application/json"
   {"result":{"triggerId":"4d539e9c-b962-4762-be71-40a5c97a47a6"},"status":200}

``upload_dar``
**************

Upload an automation DAR. If successful, the DAR's "main package ID" will be in the response (the main package ID for a DAR can also be obtained using ``daml damlc inspect-dar path/to/dar``).

.. code-block:: bash

   $ curl -F 'dar=@/home/alice/test-model.dar' localhost:8088/v1/upload_dar
   {"result":{"mainPackageId":"312094804c1468e2166bae3c9ba8b5cc0d285e31356304a2e9b0ac549df59d14"},"status":200}

``list``
********

List the DARS running on behalf of a given party. Alice can check on her running triggers as follows.

.. code-block:: bash

   $curl --user 'alice:secret' \
       -X GET localhost:8088/v1/list \
       -H "Content-type: application/json" -H "Accept: application/json"
   {"result":{"triggerIds":["4d539e9c-b962-4762-be71-40a5c97a47a6"],"status":200}

``status``
**********

It's sometimes useful to get information about the history of a specific trigger. This can be done with the "status" endpoint.

.. code-block:: bash

   $curl --user 'alice:secret' \
      -X GET localhost:8088/v1/status/4d539e9c-b962-4762-be71-40a5c97a47a6 \
      -H "Content-type: application/json" -H "Accept: application/json"
  {"result":{"logs":[["2020-06-12T12:35:49.863","starting"],["2020-06-12T12:35:50.89","running"],["2020-06-12T12:51:57.557","stopped: by user request"]]},"status":200}

``health``
**********

Test connectivity.

.. code-block:: bash

   $curl -X GET localhost:8088/v1/health
   {"status":"pass"}

Identifying trigger parties
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this section, we briefly explain how parties are identified in end-user interactions with a running Trigger Service.

When an end-user interacts with a Trigger Service, they do so by sending HTTP requests to one of several Trigger Service HTTP endpoints. Such requests must contain information detailing the party the Trigger Service is to act on the behalf of in its handling of the user request.

This party information is conveyed via  HTTP basic authentication headers where the requesting party is represented by a unique username/password pair (for now passwords must be provided but are ignored - future versions will support authentication and validation).

With an understanding of the role of credentials now in mind we can explain the ``--no-secret-key`` Trigger Service option. When the Trigger Service stores credentials it first encrypts them. The encryption/decryption algorithms it uses to do so require the use of a "key". What key is to be used is expected to be provided as the value of the environment variable ``TRIGGER_SERVICE_SECRET_KEY`` (for example, before starting the Trigger Service by a command like ``export TRIGGER_SERVICE_SECRET_KEY="my secret key"``). At startup, if the Trigger Service detects that the environment variable is not defined then it will terminate unless the parameter ``--no-secret-key`` is in its startup options. If it is, the service will continue with a default key.
