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

Obtain Authorization
~~~~~~~~~~~~~~~~~~~~

The trigger service will redirect to the authorization middleware if a request requires authentication and authorization of the user.
HTML requests will be redirected to the middleware's ``/login`` endpoint using an HTTP redirect (302 Found).
Other requests will receive a 401 Unauthorized response.
The redirect behavior can be configured using the command-line flag ``--auth-redirect``.

The 401 Unauthorized response will include a `WWW-Authenticate header <https://tools.ietf.org/html/rfc7235#section-4.1>`_ of the form:

.. code-block:: none

    WWW-Authenticate
        DamlAuthMiddleware realm=":claims",login=":login",auth=":auth"

where

- ``claims`` are the required :ref:`Daml Ledger Claims <auth-middleware-claims>`.
- ``login`` is the URL to initiate the login flow on the authorization middleware.
- ``auth`` is the URL to check whether authorization has been granted.

The response will also include an entity with

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "realm": ":claims",
        "login": ":auth",
        "auth": ":login",
    }

An application can direct the user to the login URL,
wait until authorization has been granted,
and repeat the original request once authorization has been granted.
The auth URL can be used to poll until authorization has been granted.
Alternatively, it can append a custom ``redirect_url`` parameter to the login URL and redirect to the resulting URL.
Note that login with the IAM may require entering credentials into a web-form,
i.e. the login URL should be opened in a web browser.

.. TODO[AH] Explain how to interface an auth trigger service from a simple JS frontent.
