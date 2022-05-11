.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Authorization
#############

The trigger service issues commands to the ledger that may require authorization through an access token.
See :doc:`Ledger Authorization </app-dev/authorization>` for a description of authentication and authorization on Daml ledgers.
How to obtain an access token is defined by the ledger operator.
The trigger service interfaces with an :doc:`Auth Middleware </tools/auth-middleware/index>`
to obtain an access token in order to decouple it from the specific authentication and authorization mechanism used for a given ledger.
The documentation includes an :doc:`Example Configuration using Auth0 <auth0_example>`.

Enable Authorization
~~~~~~~~~~~~~~~~~~~~

You can use the following command-line flags to configure the trigger service to interface with a given auth middleware.

``--auth``
    The URI to the auth middleware.
    The auth middleware should be reachable under this URI from the client as well as the trigger service itself.

``--auth-callback``
    The login workflow may require redirection to the callback endpoint of the trigger service.
    This flag configures the URI to the trigger service's ``/cb`` endpoint, it should be reachable from the client.

For example, use the following flags if the trigger service and the auth middleware are both running behind a reverse proxy.::

    --auth https://example.com/auth
    --auth-callback https://example.com/trigger/cb

Assuming that the auth middleware is available under ``https://example.com/auth``
and the trigger service is available under ``https://example.com/trigger``.

Note that the trigger service must be able to share cookies with the auth middleware as described in the :ref:`Deployment notes <oauth2-middleware-deployment>`.

Obtain Authorization
~~~~~~~~~~~~~~~~~~~~

The trigger service will respond with 401 Unauthorized if a request requires authentication and authorization of the user.
The trigger service can be configured to redirect to the ``/login`` endpoint via HTTP redirect (302 Found)
using the command-line flag ``-auth-redirect``.
This can be useful for testing if the IAM does not require user input.

The 401 Unauthorized response will include a `WWW-Authenticate header <https://tools.ietf.org/html/rfc7235#section-4.1>`_ of the form:

.. code-block:: none

    WWW-Authenticate
        DamlAuthMiddleware realm=":claims",login=":login",auth=":auth"

where

- ``claims`` are the required :ref:`Daml Ledger Claims <auth-middleware-claims>`.
- ``login`` is the URL to initiate the login flow on the auth middleware.
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

Example
*******

This section describes how a web frontend can interact with the trigger service when authorization is required.
Note, to avoid cross-origin requests and to enable sharing of cookies
the web application and auth middleware should be exposed under the same domain,
e.g. behind a shared reverse proxy.

Let's start with a request to the :ref:`list running triggers <list-running-triggers>` endpoint.

.. code-block:: javascript

    const resp = await fetch("/trigger/v1/triggers?party=Alice");
    if (resp.status >= 200 && resp.status < 300) {
        const result = await resp.json();
        // process result ...
    } else if (resp.status === 401) {
        // handle Unauthorized ...
    } else {
        // handle other error ...
    }

If the request succeeds it decodes the JSON response body and continues processing the result,
otherwise it checks if the request failed with 401 Unauthorized or another error.
We will ignore the general error case and focus only on handling the Unauthorized response.

Login via Redirect
==================

A simple solution is to redirect the browser to the login URL after adding a ``redirect_url`` parameter that points back to the current page.

.. code-block:: javascript

    const challenge = await resp.json();
    var loginUrl = new URL(challenge.login);
    loginUrl.searchParams.append("redirect_uri", window.location.href);
    window.location.replace(loginUrl.href);

This code first decodes the JSON encoded authentication challenge included in the response body,
then it extends the login URL with a ``redirect_uri`` parameter that points back to the current page,
and redirects the browser to the login flow.
The browser will be redirected to the original page after the login flow completed
at which point authorization should have been granted and the original request should succeed.

Login via Popup
===============

Another solution is to direct the user to the login page in a separate window,
wait until authorization has been granted, and then retry the original request.

.. code-block:: javascript

    const challenge = await resp.json();
    await popupLogin(challenge.login, challenge.auth);
    // retry original request ...

The function ``popupLogin`` opens the login URL in a popup window
and polls on the auth URL until authorization has been granted.
It raises an error if the login window closes before authorization has been granted.

.. code-block:: javascript

    function popupLogin(login, auth) {
        return new Promise(function (resolve, reject) {
            var popup = window.open(login);
            var timer = setInterval(async function() {
                const closed = popup.closed;
                const resp = await fetch(auth);
                if (resp.status >= 200 && resp.status < 300) {
                    // The user logged in
                    clearInterval(timer);
                    popup.close();
                    resolve();
                } else if (closed) {
                    // The popup is closed but we are not logged in.
                    reject(new Error("Login failed"))
                }
            }, 1000);
        });
    }
