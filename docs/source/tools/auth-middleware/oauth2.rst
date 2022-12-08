.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _oauth2-middleware:

OAuth 2.0 Auth Middleware
#########################

Daml includes an implementation of an auth middleware that supports `OAuth 2.0 Authorization Code Grant <https://oauth.net/2/grant-types/authorization-code/>`_.
The implementation aims to be configurable to support different OAuth 2.0 providers and to allow custom mappings from Daml ledger claims to OAuth 2.0 scopes.

OAuth 2.0 Configuration
~~~~~~~~~~~~~~~~~~~~~~~

`RFC 6749 <https://tools.ietf.org/html/rfc6749#section-3>`_ specifies that OAuth 2.0 providers offer two endpoints:
The `authorization endpoint <https://tools.ietf.org/html/rfc6749#section-3.1>`_
and the `token endpoint <https://tools.ietf.org/html/rfc6749#section-3.2>`_.
The URIs for these endpoints can be configured independently using the following fields:

- ``oauth-auth``
- ``oauth-token``

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
Use the following config field to use a custom template:

- ``oauth-auth-template``

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
Use the following config field to use a custom template:

- ``oauth-token-template``

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
Use the following config field to use a custom template:

- ``oauth-refresh-template``

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
        --config oauth-middleware.conf

The required config would look like

.. code-block:: none

    {
      // Environment variables:
      // DAML_CLIENT_ID      The OAuth2 client-id - must not be empty
      // DAML_CLIENT_SECRET  The OAuth2 client-secret - must not be empty
      client-id = ${DAML_CLIENT_ID}
      client-secret = ${DAML_CLIENT_SECRET}

      //IP address that OAuth2 Middleware service listens on. Defaults to 127.0.0.1.
      address = "127.0.0.1"
      //OAuth2 Middleware service port number. Defaults to 3000. A port number of 0 will let the system pick an ephemeral port. Consider specifying `--port-file` option with port number 0.
      port = 3000

      //URI to the auth middleware's callback endpoint `/cb`. By default constructed from the incoming login request.
      callback-uri = "https://example.com/auth/cb"

      //Maximum number of simultaneously pending login requests. Requests will be denied when exceeded until earlier requests have been completed or timed out.
      max-login-requests = 250

      //Login request timeout. Requests will be evicted if the callback endpoint receives no corresponding request in time.
      login-timeout = 60s

      //Enable the Secure attribute on the cookie that stores the token. Defaults to true. Only disable this for testing and development purposes.
      cookie-secure = "true"

      //URI of the OAuth2 authorization endpoint
      oauth-auth="https://oauth2-provider.com/auth_uri"

      //URI of the OAuth2 token endpoint
      oauth-token="https://oauth2-provider.com/token_uri"

      //OAuth2 authorization request Jsonnet template
      oauth-auth-template="file://path/oauth/auth/template"

      //OAuth2 token request Jsonnet template
      oauth-token-template = "file://path/oauth/token/template"

      //OAuth2 refresh request Jsonnet template
      oauth-refresh-template = "file://path/oauth/refresh/template"

      // Enables JWT-based authorization, where the JWT is signed by one of the below Jwt based token verifiers
      token-verifier {
        // type can be rs256-crt, es256-crt, es512-crt or rs256-jwks
        type = "rs256-jwks"
        // X509 certificate file (.crt)/JWKS url from where the public key is loaded
        uri = "https://example.com/.well-known/jwks.json"
      }
    }

The oauth2-middleware can also be started using cli-args.

.. note:: Configuration file is the recommended way to run oauth2-middleware, running via cli-args is now deprecated

.. code-block:: shell

    oauth2-middleware \
        --callback https://example.com/auth/cb \
        --address localhost \
        --http-port 3000 \
        --oauth-auth https://oauth2-provider.com/auth_uri \
        --oauth-token https://oauth2-provider.com/token_uri \
        --auth-jwt-rs256-jwks https://example.com/.well-known/jwks.json

Some browsers reject ``Secure`` cookies on unencrypted connections even on localhost.
You can pass the command-line flag ``--cookie-secure no`` for testing and development on localhost to avoid this.

Metrics
*******

You may configure the oauth2-middleware to expose the :doc:`common HTTP metrics </ops/common-metrics>` via Prometheus
by adding the below section to the application config:

.. code-block:: none

    metrics {
      // Start a metrics reporter. Must be one of "console", "csv:///PATH", "graphite://HOST[:PORT][/METRIC_PREFIX]", or "prometheus://HOST[:PORT]".
      reporter = "prometheus://localhost:9000"
      // Set metric reporting interval , examples : 1s, 30s, 1m, 1h
      reporting-interval = 30s
    }

Liveness and Readiness Endpoints
********************************

The following sections describe the endpoints that can be used to probe the liveness and readiness of the auth middleware service.

Liveness Check
==============

This can be used as a liveness probe, e.g., in Kubernetes.

HTTP Request
^^^^^^^^^^^^

- URL: ``/livez``
- Method: ``GET``

HTTP Response
^^^^^^^^^^^^^

A status code of ``200`` indicates a successful liveness check.

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    { "status": "pass" }

Readiness Check
===============

This can be used as a readiness probe, e.g., in Kubernetes.

HTTP Request
^^^^^^^^^^^^

- URL: ``/readyz``
- Method: ``GET``

HTTP Response
^^^^^^^^^^^^^

A status code of ``200`` indicates a successful readiness check.

