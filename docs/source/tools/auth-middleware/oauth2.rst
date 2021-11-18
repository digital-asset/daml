.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _oauth2-middleware:

OAuth 2.0 Auth Middleware
#########################

Daml Connect includes an implementation of an auth middleware that supports `OAuth 2.0 Authorization Code Grant <https://oauth.net/2/grant-types/authorization-code/>`_.
The implementation aims to be configurable to support different OAuth 2.0 providers and to allow custom mappings from Daml ledger claims to OAuth 2.0 scopes.

OAuth 2.0 Configuration
~~~~~~~~~~~~~~~~~~~~~~~

`RFC 6749 <https://tools.ietf.org/html/rfc6749#section-3>`_ specifies that OAuth 2.0 providers offer two endpoints:
The `authorization endpoint <https://tools.ietf.org/html/rfc6749#section-3.1>`_
and the `token endpoint <https://tools.ietf.org/html/rfc6749#section-3.2>`_.
The URIs for these endpoints can be configured independently using the following flags:

- ``--oauth-auth``
- ``--oauth-token``

The OAuth 2.0 provider may require that the application identify itself using a client identifier and client secret.
These can be specified using the following environment variables:

- ``DAML_CLIENT_ID``
- ``DAML_CLIENT_SECRET``

The auth middleware assumes that the OAuth 2.0 provider issues JWT access tokens.
The ``/auth`` endpoint will validate the token, if available, and ensure that it grants the requested claims.
The auth middleware accepts the same command-line flags as the :ref:`Daml Sandbox <sandbox-authorization>` to define the public key for token validation.

Request Templates
*****************

The exact format of OAuth 2.0 requests may vary between providers.
Furthermore, the mapping from Daml ledger claims to OAuth 2.0 scopes is defined by the IAM operator.
For that reason OAuth 2.0 requests made by auth middleware can be configured using user defined `Jsonnet <https://jsonnet.org/>`_ templates.
Templates are parameterized configurations expressed as top-level functions.

Authorization Request
=====================

This template defines the format of the `Authorization request <https://tools.ietf.org/html/rfc6749#section-4.1.1>`_.
Use the following command-line flag to use a custom template:

- ``--oauth-auth-template``

Arguments
^^^^^^^^^

The template will be passed the following arguments:

- ``config`` (object)
    - ``clientId`` (string) the OAuth 2.0 client identifier
    - ``clientSecret`` (string) the OAuth 2.0 client secret
- ``request`` (object)
    - ``claims`` (object) the requested claims
        - ``admin`` (bool)
        - ``applicationId`` (string or null)
        - ``actAs`` (list of string)
        - ``readAs`` (list of string)
    - ``redirectUri`` (string)
    - ``state`` (string)

Returns
^^^^^^^

The query parameters for the authorization endpoint encoded as an object with string values.

Example
^^^^^^^

.. code-block:: none

    local scope(claims) =
      local admin = if claims.admin then "admin";
      local applicationId = if claims.applicationId != null then "applicationId:" + claims.applicationId;
      local actAs = std.map(function(p) "actAs:" + p, claims.actAs);
      local readAs = std.map(function(p) "readAs:" + p, claims.readAs);
      [admin, applicationId] + actAs + readAs;

    function(config, request) {
      "audience": "https://daml.com/ledger-api",
      "client_id": config.clientId,
      "redirect_uri": request.redirectUri,
      "response_type": "code",
      "scope": std.join(" ", ["offline_access"] + scope(request.claims)),
      "state": request.state,
    }

Token Request
=============

This template defines the format of the `Token request <https://tools.ietf.org/html/rfc6749#section-4.1.3>`_.
Use the following command-line flag to use a custom template:

- ``--oauth-token-template``

Arguments
^^^^^^^^^

The template will be passed the following arguments:

- ``config`` (object)
    - ``clientId`` (string) the OAuth 2.0 client identifier
    - ``clientSecret`` (string) the OAuth 2.0 client secret
- ``request`` (object)
    - ``code`` (string)
    - ``redirectUri`` (string)

Returns
^^^^^^^

The request parameters for the token endpoint encoded as an object with string values.

Example
^^^^^^^

.. code-block:: none

    function(config, request) {
      "client_id": config.clientId,
      "client_secret": config.clientSecret,
      "code": request.code,
      "grant_type": "authorization_code",
      "redirect_uri": request.redirectUri,
    }

Refresh Request
===============

This template defines the format of the `Refresh request <https://tools.ietf.org/html/rfc6749#section-6>`_.
Use the following command-line flag to use a custom template:

- ``--oauth-refresh-template``

Arguments
^^^^^^^^^

The template will be passed the following arguments:

- ``config`` (object)
    - ``clientId`` (string) the OAuth 2.0 client identifier
    - ``clientSecret`` (string) the OAuth 2.0 client secret
- ``request`` (object)
    - ``refreshToken`` (string)

Returns
^^^^^^^

The request parameters for the authorization endpoint encoded as an object with string values.

Example
^^^^^^^

.. code-block:: none

    function(config, request) {
      "client_id": config.clientId,
      "client_secret": config.clientSecret,
      "grant_type": "refresh_code",
      "refresh_token": request.refreshToken,
    }

.. _oauth2-middleware-deployment:

Deployment Notes
~~~~~~~~~~~~~~~~

The auth middleware API relies on sharing cookies between the auth middleware and the Daml application.
One way to enable this is to expose the auth middleware and the Daml application under the same domain, e.g. through a reverse proxy.
Note that you will need to specify the external callback URI in that case using the ``--callback`` command-line flag.

For example, assuming the following nginx configuration snippet:

.. code-block:: nginx

    http {
      server {
        server_name example.com
        location /auth/ {
          proxy_pass http://localhost:3000/;
        }
      }
    }

You would invoke the OAuth 2.0 auth middleware with the following flags:

.. code-block:: shell

    oauth2-middleware \
        --callback https://example.com/auth/cb \
        --address localhost
        --http-port 3000

Some browsers reject ``Secure`` cookies on unencrypted connections even on localhost.
You can pass the command-line flag ``--cookie-secure no`` for testing and development on localhost to avoid this.
