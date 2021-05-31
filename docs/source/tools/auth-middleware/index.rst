.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Auth Middleware
###############

.. toctree::
   :hidden:

   ./oauth2

Daml ledgers only validate authorization tokens. The issuance of those tokens however is something defined by the ledger operator and can vary significantly even if the same ledger is being used. This poses a challenge for application developers aiming to develop applications that need to be able to acquire and refresh authorization tokens but don’t want to tie themselves to any particular mechanism for token issuance.
The auth middleware aims to address this problem by providing an API that decouples Daml applications from these details.
The ledger operator can provide an auth middleware that is suitable for their authentication and authorization mechanism.
Daml Connect includes an implementation of an auth middleware that supports `OAuth 2.0 Authorization Code Grant <https://oauth.net/2/grant-types/authorization-code/>`_. If this implementation is not compatible with your mechanism for token issuance, you can implement your own auth middleware provided it conforms to the same API.

Features
~~~~~~~~

The auth middleware is designed to fulfill the following goals:

- Be agnostic of the authentication and authorization protocol required by the identity and access management (IAM) system used by the ledger operator.
- Allow fine grained access control via Daml ledger claims.
- Support token refresh for long running clients that should not require user interaction.

Auth Middleware API
~~~~~~~~~~~~~~~~~~~

An implementation of the auth middleware must provide the following API.

Obtain Access Token
*******************

The application contacts this endpoint to determine if the user is authenticated and authorized to access the given claims.
The application must forward any cookies that it itself received in the user's request.
The response will contain an access token and optionally a refresh token if the user is authenticated and authorized.
Otherwise, the response will be 401 Unauthorized.

HTTP Request
============

- URL: ``/auth?claims=:claims``
- Method: ``GET``
- Headers: ``Cookie``

where

- ``claims`` are the requested :ref:`Daml Ledger Claims <auth-middleware-claims>`.

For example::

    /auth?claims=actAs:Alice+applicationId:MyApp

HTTP Response
=============

.. code-block:: json

    {
	"access_token": "...",
	"refresh_token": "..."
    }

where

- ``access_token`` is the access token to use for Daml ledger commands.
- ``refresh_token`` (optional) can be used to refresh an expired access token on the ``/refresh`` endpoint.

Request Authorization
*********************

The application directs the user to this endoint if the ``/auth`` endpoint returned 401 Unauthorized.
This will request authentication and authorization of the user from the IAM for the given claims.
E.g. in the OAuth 2.0 based implementation included in DAML Connect, this will start an Authorization Code Grant flow.

If authorization is granted this will store the access and optional refresh token in a cookie. The request can define a callback URI, if specified this endpoint will redirect to the callback URI at the end of the flow. Otherwise, it will respond with a status code that indicates whether authorization was successful or not.

HTTP Request
============

- URL: ``/login?claims=:claims&redirect_uri=:redirect_uri&state=:state``
- Method: ``GET``

where

- ``claims`` are the requested :ref:`Daml Ledger Claims <auth-middleware-claims>`.
- ``redirect_uri`` (optional) redirect to this URI at the end of the flow.
  Passes ``error`` and optionally ``error_description`` parameters if authorization failed.
- ``state`` (optional) forward this parameter to the ``redirect_uri`` if specified.

For example::

    /login?claims=actAs:Alice+applicationId:MyApp&redirect_uri=http://example.com/cb&state=2b56cc2e-01ad-4e51-a9b3-124d4bbe0a91

Refresh Access Token
********************

The application contacts this endpoint to refresh an expired access token without requiring user input.
Token refresh is available if the ``/auth`` endpoint return a refresh token along side the access token.
This endpoint will return a new access token and optionally a new refresh token to replace the old.

HTTP Request
============

- URL: ``/refresh``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "refresh_token": "..."
    }

where

- ``refresh_token`` is the refresh token returned by ``/auth`` or a previous ``/refresh`` request.

HTTP Response
=============

.. code-block:: json

    {
	"access_token": "...",
	"refresh_token": "..."
    }

where

- ``access_token`` is the access token to use for Daml ledger commands.
- ``refresh_token`` (optional) can be used to refresh an expired access token on the ``/refresh`` endpoint.

.. _auth-middleware-claims:

Daml Ledger Claims
******************

A list of claims specifies the set of capabilities that are requested.
These are passed as a URL encoded, space separated list of individual claims of the following form:

`admin`
    Access to admin-level services.
`readAs:<Party Name>`
    Read access for the given party.
`actAs:<Party Name>`
    Issue commands on behalf of the given party.
`applicationId:<Application Id>`
    Restrict access to commands issued with the given application Id.

See :ref:`Access Tokens and Claims <authorization-claims>` for further information on Daml ledger capabilities.
