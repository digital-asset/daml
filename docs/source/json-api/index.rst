.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

HTTP JSON API Service
#####################

**WARNING:** the HTTP JSON API described in this document is actively
being designed and is *subject to breaking changes*, including all
request and response elements demonstrated below or otherwise
implemented by the API.  We welcome feedback about the API on `our issue
tracker
<https://github.com/digital-asset/daml/issues/new?milestone=HTTP+JSON+API+Maintenance>`_
or `on Slack <https://hub.daml.com/slack/>`_.

The JSON API provides a significantly simpler way than :doc:`the Ledger
API </app-dev/index>` to access *basic active contract set
functionality*:

- creating contracts,
- exercising choices on contracts, and
- querying the current active contract set.

The goal is to get you up and running writing effective
ledger-integrated applications quickly, so we have deliberately excluded
complicating concerns, including but not limited to

- inspecting transactions,
- asynchronous submit/completion workflows,
- temporal queries (e.g. active contracts *as of a certain time*), and
- ledger metaprogramming (e.g. packages and templates).

For these and other features, use :doc:`the Ledger API </app-dev/index>`
instead.

.. toctree::
   :hidden:

   lf-value-specification
   search-query-language

How to start
************

Start sandbox from a DAML project directory
===========================================

::

    $ daml sandbox --wall-clock-time --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar

Start HTTP service from a DAML project directory
================================================

::

    $ daml json-api --ledger-host localhost --ledger-port 6865 \
        --http-port 7575 --max-inbound-message-size 4194304 --package-reload-interval 5s \
        --application-id HTTP-JSON-API-Gateway --static-content "prefix=static,directory=./static-content" \
        --query-store-jdbc-config "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,createSchema=false"

::

    $ daml json-api --help
    HTTP JSON API daemon
    Usage: http-json-binary [options]

      --help
            Print this usage text
      --ledger-host <value>
            Ledger host name or IP address
      --ledger-port <value>
            Ledger port number
      --address <value>
            IP address that HTTP JSON API service listens on. Defaults to 0.0.0.0.
      --http-port <value>
            HTTP JSON API service port number
      --application-id <value>
            Optional application ID to use for ledger registration. Defaults to HTTP-JSON-API-Gateway
      --package-reload-interval <value>
            Optional interval to poll for package updates. Examples: 500ms, 5s, 10min, 1h, 1d. Defaults to 5 seconds
      --max-inbound-message-size <value>
            Optional max inbound message size in bytes. Defaults to 4194304
      --query-store-jdbc-config "driver=<JDBC driver class name>,url=<JDBC connection url>,user=<user>,password=<password>,createSchema=<true|false>"
            Optional query store JDBC configuration string. Contains comma-separated key-value pairs. Where:
            driver -- JDBC driver class name,
            url -- JDBC connection URL,
            user -- database user name,
            password -- database user password
            createSchema -- boolean flag, if set to true, the process will re-create database schema and terminate immediately.
            Example: "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,createSchema=false"
      --static-content "prefix=<URL prefix>,directory=<directory>"
            DEV MODE ONLY (not recommended for production). Optional static content configuration string. Contains comma-separated key-value pairs. Where:
            prefix -- URL prefix,
            directory -- local directory that will be mapped to the URL prefix.
            Example: "prefix=static,directory=./static-content"
      --access-token-file <value>
        provide the path from which the access token will be read, required to interact with an authenticated ledger, no default

With Authentication
===================

Apart from interacting with the Ledger API on behalf of the user, the HTTP JSON API server must also interact with the Ledger API to maintain some relevant internal state.

For this reason, you must provide an access token when you start the HTTP JSON API if you're running it against a Ledger API server that requires authentication.

Note that this token is used exclusively for maintaining the internal list of known packages and templates, and that it will not be use to authenticate client calls to the HTTP JSON API: the user is expected to provide a valid authentication token with each call.

The HTTP JSON API servers requires no access to party-specific data, only access to the ledger identity and package services: a token issued for the HTTP JSON API server should contain enough claims to contact these two services but no more than that. Please refer to your ledger operator's documentation to find out how.

Once you have retrieved your access token, you can provide it to the HTTP JSON API by storing it in a file and provide the path to it using the ``--access-token-file`` command line option.

If the token cannot be read from the provided path or the Ledger API reports an authentication error (for example due to token expiration), the HTTP JSON API will report the error via logging. The token file can be updated with a valid token and it will be picked up at the next attempt to send a request.

Example session
***************

::

    $ daml new iou-quickstart-java quickstart-java
    $ cd iou-quickstart-java/
    $ daml build
    $ daml sandbox --wall-clock-time --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar
    $ daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575

Choosing a party
================

You specify your party and other settings with JWT.  In testing
environments, you can use https://jwt.io to generate your token.

The default "header" is fine.  Under "Payload", fill in::

    {
      "ledgerId": "MyLedger",
      "applicationId": "foobar",
      "party": "Alice"
    }

Keep in mind:
- the value of ``ledgerId`` payload field has to match ``--ledgerid`` passed to the sandbox.
- you can replace ``Alice`` with whatever party you want to use.

Under "Verify Signature", put ``secret`` as the secret (_not_ base64
encoded); that is the hardcoded secret for testing.

Then the "Encoded" box should have your token; set HTTP header
``Authorization: Bearer copy-paste-token-here`` for normal requests, and
add the subprotocols ``jwt.token.copy-paste-token-here`` and
``daml.ws.auth`` for WebSockets requests.

Here are two tokens you can use for testing:

- ``{"ledgerId": "MyLedger", "applicationId": "foobar", "party": "Alice"}``
  ``eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQWxpY2UifQ.4HYfzjlYr1ApUDot0a6a4zB49zS_jrwRUOCkAiPMqo0``

- ``{"ledgerId": "MyLedger", "applicationId": "foobar", "party": "Bob"}``
  ``eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQm9iIn0.2LE3fAvUzLx495JWpuSzHye9YaH3Ddt4d2Pj0L1jSjA``
  
For production use, we have a tool in development for generating proper
RSA-encrypted tokens locally, which will arrive when the service also
supports such tokens.

GET ``/contracts/search``
=========================

List all currently active contracts for all known templates. Note that the retrieved contracts do not get persisted into query store database.

The response is the same as for the POST method below.

POST ``/contracts/search``
==========================

List currently active contracts that match a given query.

Request
-------

application/json body, formatted according to the :doc:`search-query-language`:

.. code-block:: json

    {
        "%templates": ["Iou:Iou"],
        "amount": 999.99
    }

Empty Response
--------------

.. code-block:: json

    {
        "status": 200,
        "result": []
    }

Nonempty Response
-----------------

Each contract formatted according to :doc:`lf-value-specification`.

.. code-block:: json

    {
        "result": [
            {
                "observers": [],
                "agreementText": "",
                "payload": {
                    "observers": [],
                    "issuer": "Alice",
                    "amount": "999.99",
                    "currency": "USD",
                    "owner": "Alice"
                },
                "signatories": [
                    "Alice"
                ],
                "contractId": "#52:0",
                "templateId": "b10d22d6c2f2fae41b353315cf893ed66996ecb0abe4424ea6a81576918f658a:Iou:Iou"
            }
        ],
        "status": 200
    }

Nonempty Response with Unknown Template IDs Warning
---------------------------------------------------

.. code-block:: json

    {
        "warnings": {
            "unknownTemplateIds": ["UnknownModule:UnknownEntity"]
        },
        "result": [
            {
                "observers": [],
                "agreementText": "",
                "payload": {
                    "observers": [],
                    "issuer": "Alice",
                    "amount": "999.99",
                    "currency": "USD",
                    "owner": "Alice"
                },
                "signatories": [
                    "Alice"
                ],
                "contractId": "#52:0",
                "templateId": "b10d22d6c2f2fae41b353315cf893ed66996ecb0abe4424ea6a81576918f658a:Iou:Iou"
            }
        ],
        "status": 200
    }

WebSocket ``/contracts/searchForever``
======================================

List currently active contracts that match a given query, with
continuous updates.

Two subprotocols must be passed, as described in `Choosing a party
<#choosing-a-party>`__.

application/json body must be sent first, formatted according to the
:doc:`search-query-language`::

    {"%templates": ["Iou:Iou"]}

output a series of JSON documents, each ``argument`` formatted according
to :doc:`lf-value-specification`::

    {
        "created": [{
            "observers": [],
            "agreementText": "",
            "signatories": ["Alice"],
            "contractId": "#1:0",
            "templateId": "398e67533888ab6532c3e62c2c3445182e2cdd291457732f174d2698a7d1db2d:Iou:Iou",
            "witnessParties": ["Alice"],
            "argument": {
                "observers": [],
                "issuer": "Alice",
                "amount": "999.99",
                "currency": "USD",
                "owner": "Alice"
            }
        }]
    }

To keep the stream alive, you'll occasionally see messages like this,
which can be safely ignored::

    {"heartbeat": "ping"}

After submitting an ``Iou_Split`` exercise, which creates two contracts
and archives the one above, the same stream will eventually produce::

    {
        "created": [{
            "observers": [],
            "agreementText": "",
            "signatories": ["Alice"],
            "contractId": "#2:1",
            "templateId": "398e67533888ab6532c3e62c2c3445182e2cdd291457732f174d2698a7d1db2d:Iou:Iou",
            "witnessParties": ["Alice"],
            "argument": {
                "observers": [],
                "issuer": "Alice",
                "amount": "42.42",
                "currency": "USD",
                "owner": "Alice"
            }
        }, {
            "observers": [],
            "agreementText": "",
            "signatories": ["Alice"],
            "contractId": "#2:2",
            "templateId": "398e67533888ab6532c3e62c2c3445182e2cdd291457732f174d2698a7d1db2d:Iou:Iou",
            "witnessParties": ["Alice"],
            "argument": {
                "observers": [],
                "issuer": "Alice",
                "amount": "957.57",
                "currency": "USD",
                "owner": "Alice"
            }
        }],
        "archived": ["#1:0"]
    }

Some notes on behavior:

1. Each result object means "this is what would have changed if you just
   polled ``/contracts/search`` iteratively."  In particular, just as
   polling search can "miss" contracts (as a create and archive can be
   paired between polls), such contracts may or may not appear in any
   result object.

2. No ``archived`` ever contains a contract ID occurring within an
   ``created`` in the same object.  So, for example, supposing you are
   keeping an internal map of active contracts, you can apply the
   ``created`` first or the ``archived`` first and be guaranteed to get
   the same results.

3. You will almost certainly receive contract IDs in the ``archived``
   set that you never received an ``created`` for.  These are contracts
   that query filtered out, but for which the server no longer is aware
   of that.  You can safely ignore these.  However, such "phantom
   archives" *are* guaranteed to represent an actual archival *on the
   ledger*, so if you are keeping a more global dataset outside the
   context of this specific search, you can use that archival
   information as you wish.

POST ``/command/create``
========================

Create a contract.

Request
-------

application/json body, ``argument`` formatted according to :doc:`lf-value-specification`:

.. code-block:: json

    {
      "templateId": "Iou:Iou",
      "argument": {
        "observers": [],
        "issuer": "Alice",
        "amount": "999.99",
        "currency": "USD",
        "owner": "Alice"
      }
    }

Response
--------

.. code-block:: json

    {
        "status": 200,
        "result": {
            "observers": [],
            "agreementText": "",
            "payload": {
                "observers": [],
                "issuer": "Alice",
                "amount": "999.99",
                "currency": "USD",
                "owner": "Alice"
            },
            "signatories": [
                "Alice"
            ],
            "contractId": "#124:0",
            "templateId": "11c8f3ace75868d28136adc5cfc1de265a9ee5ad73fe8f2db97510e3631096a2:Iou:Iou"
        }
    }
 
POST ``/command/exercise``
==========================

Exercise a choice on a contract.

``"contractId": "#124:0"`` is the value from the create output.

Request
-------

application/json body:

.. code-block:: json

    {
        "templateId": "Iou:Iou",
        "contractId": "#124:0",
        "choice": "Iou_Transfer",
        "argument": {
            "newOwner": "Alice"
        }
    }

Response
--------

.. code-block:: json

    {
        "status": 200,
        "result": {
            "exerciseResult": "#201:1",
            "contracts": [
                {
                    "archived": {
                        "contractId": "#124:0",
                        "templateId": "11c8f3ace75868d28136adc5cfc1de265a9ee5ad73fe8f2db97510e3631096a2:Iou:Iou"
                    }
                },
                {
                    "created": {
                        "observers": [],
                        "agreementText": "",
                        "payload": {
                            "iou": {
                                "observers": [],
                                "issuer": "Alice",
                                "amount": "999.99",
                                "currency": "USD",
                                "owner": "Alice"
                            },
                            "newOwner": "Alice"
                        },
                        "signatories": [
                            "Alice"
                        ],
                        "contractId": "#201:1",
                        "templateId": "11c8f3ace75868d28136adc5cfc1de265a9ee5ad73fe8f2db97510e3631096a2:Iou:IouTransfer"
                    }
                }
            ]
        }
    }

Where:

- ``exerciseResult`` -- the return value of the exercised contract choice.
- ``contracts`` -- an array containing contracts that were archived and created as part of the exercised choice. The array may contain: **zero or many** ``{"archived": {...}}`` and **zero or many** ``{"created": {...}}`` elements. The order of the contracts is the same as on the ledger.

GET ``/parties``
================

Response
--------

.. code-block:: json

    {
        "status": 200,
        "result": [
            {
                "party": "Alice",
                "isLocal": true
            }
        ]
    }

POST ``/contracts/lookup``
==========================

Lookup by Contract ID
---------------------

Request
~~~~~~~

application/json body:

.. code-block:: json

    {
      "contractId": "#201:1"
    }

Contract Not Found Response
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: json

    {
        "status": 200,
        "result": null
    }

Contract Found Response
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: json

    {
        "status": 200,
        "result": {
            "observers": [],
            "agreementText": "",
            "payload": {
                "iou": {
                    "observers": [],
                    "issuer": "Alice",
                    "amount": "999.99",
                    "currency": "USD",
                    "owner": "Alice"
                },
                "newOwner": "Alice"
            },
            "signatories": [
                "Alice"
            ],
            "contractId": "#201:1",
            "templateId": "11c8f3ace75868d28136adc5cfc1de265a9ee5ad73fe8f2db97510e3631096a2:Iou:IouTransfer"
        }
    }

Lookup by Contract Key
----------------------

Request
~~~~~~~

application/json body:

.. code-block:: json

    {
        "templateId": "Account:Account",
        "key": [
            "Alice",
            "abc123"
        ]
    }

Contract Not Found Response
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: json

    {
        "status": 200,
        "result": null
    }

Contract Found Response
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: json

    {
        "status": 200,
        "result": {
            "observers": [],
            "agreementText": "",
            "payload": {
                "owner": "Alice",
                "number": "abc123",
                "status": {
                    "tag": "Enabled",
                    "value": "2020-01-01T00:00:01Z"
                }
            },
            "signatories": [
                "Alice"
            ],
            "key": {
                "_1": "Alice",
                "_2": "abc123"
            },
            "contractId": "#697:0",
            "templateId": "11c8f3ace75868d28136adc5cfc1de265a9ee5ad73fe8f2db97510e3631096a2:Account:Account"
        }
    }
