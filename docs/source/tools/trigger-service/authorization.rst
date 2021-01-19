.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Authorization
#############

The trigger service issues commands to the ledger that may require authorization through an access token.
See :doc:`Ledger Authorization </app-dev/authorization>` for a description of authentication and authorization on Daml ledgers.
How to obtain an access token is defined by the ledger operator.
The trigger service interfaces with an :doc:`Authorization Middleware </tools/auth-middleware/index>`
to obtain an access token in order to decouple it from the specific authentication and authorization mechanism used for a given ledger.

Enable Authorization
~~~~~~~~~~~~~~~~~~~~

You can use the following command-line flags to configure the trigger service to interface with a given authorization middleware.

``--auth``
    The URI to the authorization middleware.
    The authorization middleware should be reachable under this URI from the client as well as the trigger service itself.

``--auth-callback``
    The login workflow may require redirection to the callback endpoint of the trigger service.
    This flag configures the URI to the trigger service's ``/cb`` endpoint, it should be reachable from the client.

For example, use the following flags if the trigger service and the authorization middleware are both running behind a reverse proxy.::

    --auth https://example.com/auth
    --auth-callback https://example.com/trigger/cb

Assuming that the authorization middleware is available under ``https://example.com/auth/``
and the trigger service is available under ``https://example.com/trigger/``.

Note that the trigger service must be able to share cookies with the authorization middleware as described in the :ref:`Deployment notes <oauth2-middleware-deployment>`.

.. TODO[AH] Explain redirect and unauthorized responses when login is required. Explain custom WWW-Authenticate header.
.. TODO[AH] Explain how to interface an auth trigger service from a simple JS frontent.
