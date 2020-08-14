.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _json-api:

HTTP JSON API Service
#####################

The **JSON API** provides a significantly simpler way to interact with a ledger than :doc:`the Ledger
API </app-dev/ledger-api>` by providing *basic active contract set functionality*:

- creating contracts,
- exercising choices on contracts,
- querying the current active contract set, and
- retrieving all known parties.

The goal of this API is to get your distributed ledger application up and running quickly, so we have deliberately excluded
complicating concerns including, but not limited to:

- inspecting transactions,
- asynchronous submit/completion workflows,
- temporal queries (e.g. active contracts *as of a certain time*), and
- ledger metaprogramming (e.g. retrieving packages and templates).

For these and other features, use :doc:`the Ledger API </app-dev/ledger-api>`
instead.

We welcome feedback about the JSON API on `our issue tracker
<https://github.com/digital-asset/daml/issues/new?milestone=HTTP+JSON+API+Maintenance>`_
`on our forum <https://discuss.daml.com>`_, or `on Slack
<https://slack.daml.com>`_.

.. toctree::
   :hidden:
   :maxdepth: 3

   lf-value-specification
   search-query-language

Running the JSON API
********************

Start a DAML Ledger
===================

You can run the JSON API alongside any Ledger API you want, if you don't have a Ledger API you want to run it with then start a sandbox ledger as so:

.. code-block:: shell

    daml new my_project --template quickstart-java
    cd my_project
    daml build
    daml sandbox --wall-clock-time --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar

.. _start-http-service:

Start the HTTP JSON API Service
===============================

Basic
-----

The most basic way to start the JSON API is with the command:

.. code-block:: shell

    daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575

This will connect to your ledger that is running on ``localhost:6865`` and start your JSON API server which you'll be able to query on ``localhost:7575``

With Query Store
------------------

Often when running the JSON API you'll want to cache queries, especially large queries, to decrease the workload of your underlying ledger.
The JSON API provides a way to do this by allowing you to specify a database to cache queries (currently PostgreSQL only).

In this case you can use the ``--query-store-jdbc-config`` flag, an example of which is below. 

.. note:: When you use the Query Store you'll want your first run to specify ``createSchema=true`` so that all the necessary tables are created. After the first run make sure ``createSchema=false`` so that it doesn't attempt to create the tables again.

.. code-block:: shell

    daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575 \
    --query-store-jdbc-config "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,createSchema=false"

.. note:: The JSON API provides many other useful configuration flags, run ``daml json-api --help`` to see all of them.

Authentication and Authorization
================================

The Ledger API and JSON API use the terms Authentication and Authorization to describe different aspects of their operation.
While these terms may sound similar they mean different things.

The JSON API essentially performs two separate tasks:

1. It talks to the Ledger API to get data it needs to operate, for this it needs to may need to provide an *authentication token* to the Ledger API if the Ledger API requires *authentication*.
2. It handles the requests coming from one or more Parties, for this each party needs to provide an *authorization token* with each request it sends to the JSON API.

Authentication
--------------

The authentication token is used exclusively for maintaining the internal list of known packages and templates.

.. note:: At no point should an authentication token be provided to an end user, these are for internal use only.

Every authentication token is different and will depend on what your specific ledger or ledger operator requires.
The JSON API server requires no access to party-specific data, only access to the ledger identity and package services.
A token issued for the HTTP JSON API server should contain enough claims to contact these two services but no more than that.
Please refer to your ledger operator's documentation to find out how.

Once you have retrieved your access token, you can provide it to the JSON API by storing it in a file
and starting ``daml json-api`` with the flag ``--access-token-file /path/to/your/token.file``.

If the token cannot be read from the provided path or the Ledger API reports an authentication error
(for example due to token expiration), the JSON API will report the error via logging. 

.. note:: If the token file is updated with a new token it will be picked up at the next attempt to send a request. You can use this to handle cases where an old token expires without interrupting your JSON API service.

Authorization
-------------

Every request from a client to the JSON API requires you to specify a party and some other settings,
with a JWT token.  Normal HTTP requests pass the token in an ``Authorization`` header, 
while WebSocket requests pass the token in a subprotocol.

.. note:: While the JSON API receives the token it doesn't handle it itself. Upon receiving a token it will pass it on to the underlying Ledger's AuthService which will then determine if the token is valid and authorized. As such each token format is ledger/implementation-specific and the below example reflects only the format that is used for the DAML Sandbox.

In the DAML Sandbox testing environment, you can use https://jwt.io to generate your
token.  The default "header" is fine.  Under "Payload", fill in:

.. code-block:: json

    {
      "https://daml.com/ledger-api": {
        "ledgerId": "MyLedger",
        "applicationId": "HTTP-JSON-API-Gateway",
        "actAs": ["Alice"]
      }
    }

The value of the ``ledgerId`` field has to match the ``ledgerId` of your underlying DAML Ledger.
For the ``daml sandbox`` this corresponds to the ``--ledgerid MyLedger`` flag.

The value of the ``applicationId`` field must match that provided to the ``daml json-api`` command with the flag
``--application-id`` (default is ``HTTP-JSON-API-Gateway``).

The value for ``actAs`` is specified as a list. You can replace ``Alice`` with whatever party you want to use,
provided that it is a valid party on the ledger.
For the ``daml sandbox`` any string will create a party with that name.

Under "Verify Signature", put ``secret`` as the secret (*not* base64
encoded); that is the hardcoded secret for testing.

Then the "Encoded" box should have your **token**, ready for passing to
the service as described in the following sections.

Alternatively, here are two tokens you can use for testing:

``{"https://daml.com/ledger-api": {"ledgerId": "MyLedger", "applicationId": "HTTP-JSON-API-Gateway", "actAs": ["Alice"]}}``:

.. code-block:: none

    eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJIVFRQLUpTT04tQVBJLUdhdGV3YXkiLCJhY3RBcyI6WyJBbGljZSJdfX0.34zzF_fbWv7p60r5s1kKzwndvGdsJDX-W4Xhm4oVdpk


``{"https://daml.com/ledger-api": {"ledgerId": "MyLedger", "applicationId": "HTTP-JSON-API-Gateway", "actAs": ["Bob"]}}``:

.. code-block:: none

    eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJIVFRQLUpTT04tQVBJLUdhdGV3YXkiLCJhY3RBcyI6WyJCb2IiXX19.0uPPZtM1AmKvnGixt_Qo53cMDcpnziCjKKiWLvMX2VM

For production use, we have a tool in development for generating proper
RSA-encrypted tokens locally, which will arrive when the service also
supports such tokens.

In the meantime you can also use whichever JWT libraries are available in the language of your choice.

Authorization with HTTP
^^^^^^^^^^^^^^^^^^^^^^^

Set HTTP header ``Authorization: Bearer paste-jwt-here``

Example: 

.. code-block:: none 

    Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJIVFRQLUpTT04tQVBJLUdhdGV3YXkiLCJhY3RBcyI6WyJBbGljZSJdfX0.34zzF_fbWv7p60r5s1kKzwndvGdsJDX-W4Xhm4oVdpk

Authorization with WebSockets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

WebSocket clients support a "subprotocols" argument (sometimes simply
called "protocols"); this is usually in a list form but occasionally in
comma-separated form.  Check documentation for your WebSocket library of
choice for details.

For HTTP JSON requests, you must pass two subprotocols:

- ``daml.ws.auth``
- ``jwt.token.paste-jwt-here``

Example: 

.. code-block:: none

    jwt.token.eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJIVFRQLUpTT04tQVBJLUdhdGV3YXkiLCJhY3RBcyI6WyJBbGljZSJdfX0.34zzF_fbWv7p60r5s1kKzwndvGdsJDX-W4Xhm4oVdpk``

HTTP Status Codes
*****************

The **JSON API** reports errors using standard HTTP status codes. It divides HTTP status codes into 3 groups indicating:

1. success (200)
2. failure due to a client-side problem (400, 401, 404)
3. failure due to a server-side problem (500)

The **JSON API** can return one of the following HTTP status codes:

- 200 - OK
- 400 - Bad Request (Client Error)
- 401 - Unauthorized, authentication required
- 404 - Not Found
- 500 - Internal Server Error

If a client's HTTP GET or POST request reaches an API endpoint, the corresponding response will always contain a JSON object with a ``status`` field, either an ``errors`` or ``result`` field and an optional ``warnings``:

.. code-block:: none

    {
        "status": <400 | 401 | 404 | 500>,
        "errors": <JSON array of strings>, | "result": <JSON object or array>,
        ["warnings": <JSON object> ]
    }

Where:

- ``status`` -- a JSON number which matches the HTTP response status code returned in the HTTP header,
- ``errors`` -- a JSON array of strings, each string represents one error,
- ``result`` -- a JSON object or JSON array, representing one or many results,
- ``warnings`` -- an optional field with a JSON object, representing one or many warnings.

See the following blog post for more details about error handling best practices: `REST API Error Codes 101 <https://blog.restcase.com/rest-api-error-codes-101/>`_.

Successful response, HTTP status: 200 OK
========================================

- Content-Type: ``application/json``
- Content:

.. code-block:: none

    {
        "status": 200,
        "result": <JSON object>
    }

Successful response with a warning, HTTP status: 200 OK
=======================================================

- Content-Type: ``application/json``
- Content:

.. code-block:: none

    {
        "status": 200,
        "result": <JSON object>,
        "warnings": <JSON object>
    }

.. _error-format:

Failure, HTTP status: 400 | 401 | 404 | 500
===========================================

- Content-Type: ``application/json``
- Content:

.. code-block:: none

    {
        "status": <400 | 401 | 404 | 500>,
        "errors": <JSON array of strings>
    }

Examples
========

**Result with JSON Object without Warnings:**

.. code-block:: none

    {"status": 200, "result": {...}}

**Result with JSON Array and Warnings:**

.. code-block:: none

    {"status": 200, "result": [...], "warnings": {"unknownTemplateIds": ["UnknownModule:UnknownEntity"]}}

**Bad Request Error:**

.. code-block:: json

    {"status": 400, "errors": ["JSON parser error: Unexpected character 'f' at input index 27 (line 1, position 28)"]}

**Bad Request Error with Warnings:**

.. code-block:: json

    {"status":400, "errors":["Cannot resolve any template ID from request"], "warnings":{"unknownTemplateIds":["XXX:YYY","AAA:BBB"]}}

**Authentication Error:**

.. code-block:: json

    {"status": 401, "errors": ["Authentication Required"]}

**Not Found Error:**

.. code-block:: json

    {"status": 404, "errors": ["HttpMethod(POST), uri: http://localhost:7575/v1/query1"]}

**Internal Server Error:**

.. code-block:: json

    {"status": 500, "errors": ["Cannot initialize Ledger API"]}

Create a new Contract
*********************

To create an instance of an ``Iou`` contract from the :doc:`Quickstart guide </app-dev/bindings-java/quickstart>`:

.. literalinclude:: ../app-dev/bindings-java/quickstart/template-root/daml/Iou.daml
  :language: daml
  :lines: 9-15

.. _create-request:

HTTP Request
============

- URL: ``/v1/create``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "templateId": "Iou:Iou",
      "payload": {
        "issuer": "Alice",
        "owner": "Alice",
        "currency": "USD",
        "amount": "999.99",
        "observers": []
      }
    }

Where:

- ``templateId`` is the contract template identifier, which can be formatted as either:

  + ``"<package ID>:<module>:<entity>"`` or
  + ``"<module>:<entity>"`` if contract template can be uniquely identified by its module and entity name.

- ``payload`` field contains contract fields as defined in the DAML template and formatted according to :doc:`lf-value-specification`.

.. _create-response:

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

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

Where:

- ``status`` field matches the HTTP response status code returned in the HTTP header,
- ``result`` field contains created contract details. Keep in mind that ``templateId`` in the **JSON API** response is always fully qualified (always contains package ID).

.. _create-request-with-meta:

Creating a Contract with a Command ID
*************************************

When creating a new contract you may specify an optional ``meta`` field.

.. note:: You cannot currently use ``commandIds`` anywhere else in the JSON API, but you can use it for observing the results of its commands outside the JSON API in logs or via the Ledger API's :doc:`Command Services </app-dev/services>`

.. code-block:: json

    {
      "templateId": "Iou:Iou",
      "payload": {
        "observers": [],
        "issuer": "Alice",
        "amount": "999.99",
        "currency": "USD",
        "owner": "Alice"
      },
      "meta": {
      	"commandId": "a unique ID"
      }
    }

Where:

- ``commandId`` -- optional field, a unique string identifying the command.

Exercise by Contract ID
***********************

The JSON command below, demonstrates how to exercise an ``Iou_Transfer`` choice on an ``Iou`` contract:

.. literalinclude:: ../app-dev/bindings-java/quickstart/template-root/daml/Iou.daml
  :language: daml
  :lines: 23, 52-55

HTTP Request
============

- URL: ``/v1/exercise``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "templateId": "Iou:Iou",
        "contractId": "#124:0",
        "choice": "Iou_Transfer",
        "argument": {
            "newOwner": "Alice"
        }
    }

Where:

- ``templateId`` -- contract template identifier, same as in :ref:`create request <create-request>`,
- ``contractId`` -- contract identifier, the value from the  :ref:`create response <create-response>`,
- ``choice`` -- DAML contract choice, that is being exercised,
- ``argument`` -- contract choice argument(s).

.. _exercise-response:

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "status": 200,
        "result": {
            "exerciseResult": "#201:1",
            "events": [
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

- ``status`` field matches the HTTP response status code returned in the HTTP header,

- ``result`` field contains contract choice execution details:

    + ``exerciseResult`` field contains the return value of the exercised contract choice,
    + ``events`` contains an array of contracts that were archived and created as part of the choice execution. The array may contain: **zero or many** ``{"archived": {...}}`` and **zero or many** ``{"created": {...}}`` elements. The order of the contracts is the same as on the ledger.


Exercise by Contract Key
************************

The JSON command below, demonstrates how to exercise ``Archive`` a choice on ``Account`` contract with a ``(Party, Text)`` :doc:`contract key </daml/reference/contract-keys>` defined like this:

.. code-block:: daml

    template Account with
        owner : Party
        number : Text
        status : AccountStatus
      where
        signatory owner
        key (owner, number) : (Party, Text)
        maintainer key._1


HTTP Request
============

- URL: ``/v1/exercise``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "templateId": "Account:Account",
        "key": {
            "_1": "Alice",
            "_2": "abc123"
        },
        "choice": "Archive",
        "argument": {}
    }

Where:

- ``templateId`` -- contract template identifier, same as in :ref:`create request <create-request>`,
- ``key`` -- contract key, formatted according to the :doc:`lf-value-specification`,
- ``choice`` -- DAML contract choice, that is being exercised,
- ``argument`` -- contract choice argument(s), empty, because ``Archive`` does not take any.

HTTP Response
=============

Formatted similar to :ref:`Exercise by Contract ID response <exercise-response>`.

Create and Exercise in the Same Transaction
*******************************************

This command allows creating a contract and exercising a choice on the newly created contract in the same transaction.

HTTP Request
============

- URL: ``/v1/create-and-exercise``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "templateId": "Iou:Iou",
      "payload": {
        "observers": [],
        "issuer": "Alice",
        "amount": "999.99",
        "currency": "USD",
        "owner": "Alice"
      },
      "choice": "Iou_Transfer",
      "argument": {
        "newOwner": "Bob"
      }
    }

Where:

- ``templateId`` -- the initial contract template identifier, in the same format as in the :ref:`create request <create-request>`,
- ``payload`` -- the initial contract fields as defined in the DAML template and formatted according to :doc:`lf-value-specification`,
- ``choice`` -- DAML contract choice, that is being exercised,
- ``argument`` -- contract choice argument(s).

HTTP Response
=============

Please note that the response below is for a consuming choice, so it contains:

- ``created`` and ``archived`` events for the initial contract (``"contractId": "#1:0"``), which was created and archived right away when a consuming choice was exercised on it,
- a ``created`` event for the contract that is the result of exercising the choice (``"contractId": "#1:2"``).

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "result": {
        "exerciseResult": "#1:2",
        "events": [
          {
            "created": {
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
              "contractId": "#1:0",
              "templateId": "a3b788b4dc18dc060bfb82366ae6dc055b1e361d646d5cfdb1b729607e344336:Iou:Iou"
            }
          },
          {
            "archived": {
              "contractId": "#1:0",
              "templateId": "a3b788b4dc18dc060bfb82366ae6dc055b1e361d646d5cfdb1b729607e344336:Iou:Iou"
            }
          },
          {
            "created": {
              "observers": [
                "Bob"
              ],
              "agreementText": "",
              "payload": {
                "iou": {
                  "observers": [],
                  "issuer": "Alice",
                  "amount": "999.99",
                  "currency": "USD",
                  "owner": "Alice"
                },
                "newOwner": "Bob"
              },
              "signatories": [
                "Alice"
              ],
              "contractId": "#1:2",
              "templateId": "a3b788b4dc18dc060bfb82366ae6dc055b1e361d646d5cfdb1b729607e344336:Iou:IouTransfer"
            }
          }
        ]
      },
      "status": 200
    }

Fetch Contract by Contract ID
*****************************

HTTP Request
============

- URL: ``/v1/fetch``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

application/json body:

.. code-block:: json

    {
      "contractId": "#201:1"
    }

Contract Not Found HTTP Response
================================

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "status": 200,
        "result": null
    }

Contract Found HTTP Response
============================

- Content-Type: ``application/json``
- Content:

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

Fetch Contract by Key
*********************

HTTP Request
============

- URL: ``/v1/fetch``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "templateId": "Account:Account",
        "key": {
            "_1": "Alice",
            "_2": "abc123"
        }
    }

Contract Not Found HTTP Response
================================

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "status": 200,
        "result": null
    }

Contract Found HTTP Response
============================

- Content-Type: ``application/json``
- Content:

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


Get all Active Contracts
************************

List all currently active contracts for all known templates.

.. note:: Retrieved contracts do not get persisted into a query store database. Query store is a search index and can be used to optimize search latency. See :ref:`Start HTTP service <start-http-service>` for information on how to start JSON API service with a query store enabled.

.. note:: You can only query active contracts with the ``/v1/query`` endpoint. Archived contracts (those that were archived or consumed during an exercise operation) will not be shown in the results.

HTTP Request
============

- URL: ``/v1/query``
- Method: ``GET``
- Content: <EMPTY>

HTTP Response
=============

The response is the same as for the POST method below.

Query all Active Contracts
**************************

List currently active contracts that match a given query.

HTTP Request
============

- URL: ``/v1/query``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "templateIds": ["Iou:Iou"],
        "query": {"amount": 999.99}
    }

Where:

- ``templateIds`` --  an array of contract template identifiers to search through,
- ``query`` -- search criteria to apply to the specified ``templateIds``, formatted according to the :doc:`search-query-language`.

Empty HTTP Response
===================

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "status": 200,
        "result": []
    }

Nonempty HTTP Response
======================

- Content-Type: ``application/json``
- Content:

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

Where

- ``result`` contains an array of contracts, each contract formatted according to :doc:`lf-value-specification`,
- ``status`` matches the HTTP status code returned in the HTTP header.

Nonempty HTTP Response with Unknown Template IDs Warning
========================================================

- Content-Type: ``application/json``
- Content:

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

Fetch Parties by Identifiers
****************************

- URL: ``/v1/parties``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    ["Alice", "Bob", "Dave"]

If an empty JSON array is passed: ``[]``, this endpoint returns BadRequest(400) error:

.. code-block:: json

    {
      "status": 400,
      "errors": [
        "JsonReaderError. Cannot read JSON: <[]>. Cause: spray.json.DeserializationException: must be a list with at least 1 element"
      ]
    }

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "status": 200,
      "result": [
        {
          "identifier": "Alice",
          "displayName": "Alice & Co. LLC",
          "isLocal": true
        },
        {
          "identifier": "Bob",
          "displayName": "Bob & Co. LLC",
          "isLocal": true
        },
        {
          "identifier": "Dave",
          "isLocal": true
        }
      ]
    }

Please note that the order of the party objects in the response is not guaranteed to match the order of the passed party identifiers.

Where

- ``identifier`` -- a stable unique identifier of a DAML party,
- ``displayName`` -- optional human readable name associated with the party. Might not be unique,
- ``isLocal`` -- true if party is hosted by the backing participant.

Response with Unknown Parties Warning
=====================================

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "result": [
        {
          "identifier": "Alice",
          "displayName": "Alice & Co. LLC",
          "isLocal": true
        }
      ],
      "warnings": {
        "unknownParties": ["Erin"]
      },
      "status": 200
    }

The ``result`` might be an empty JSON array if none of the requested parties is known.

Fetch All Known Parties
***********************

- URL: ``/v1/parties``
- Method: ``GET``
- Content: <EMPTY>

HTTP Response
=============

The response is the same as for the POST method above.

Allocate a New Party
********************

This endpoint is a JSON API proxy for the Ledger API's :ref:`AllocatePartyRequest <com.daml.ledger.api.v1.admin.AllocatePartyRequest>`. For more information about party management, please refer to :ref:`Provisioning Identifiers <provisioning-ledger-identifiers>` part of the Ledger API documentation.

HTTP Request
============

- URL: ``/v1/parties/allocate``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "identifierHint": "Carol",
      "displayName": "Carol & Co. LLC"
    }

Please refer to :ref:`AllocateParty <com.daml.ledger.api.v1.admin.AllocatePartyRequest>` documentation for information about the meaning of the fields.

All fields in the request are optional, this means that an empty JSON object is a valid request to allocate a new party:

.. code-block:: json

    {}

HTTP Response
=============

.. code-block:: json

    {
      "result": {
        "identifier": "Carol",
        "displayName": "Carol & Co. LLC",
        "isLocal": true
      },
      "status": 200
    }

List All DALF Packages
**********************

HTTP Request
============

- URL: ``/v1/packages``
- Method: ``GET``
- Content: <EMPTY>

HTTP Response
=============

.. code-block:: json

    {
      "result": [
        "c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd",
        "733e38d36a2759688a4b2c4cec69d48e7b55ecc8dedc8067b815926c917a182a",
        "bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f",
        "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7",
        "8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb"
      ],
      "status": 200
    }

Where ``result`` is the JSON array containing the package IDs of all loaded DALFs.

Download a DALF Package
***********************

HTTP Request
============

- URL: ``/v1/packages/<package ID>``
- Method: ``GET``
- Content: <EMPTY>

Note that the desired package ID is specified in the URL.

HTTP Response, status: 200 OK
=============================

- Transfer-Encoding: ``chunked``
- Content-Type: ``application/octet-stream``
- Content: <DALF bytes>

The content (body) of the HTTP response contains raw DALF package bytes, without any encoding. Note that the package ID specified in the URL is actually the SHA-256 hash of the downloaded DALF package and can be used to validate the integrity of the downloaded content.

HTTP Response with Error, any status different from 200 OK
==========================================================

Any status different from ``200 OK`` will be in the format specified below.

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "errors": [
            "io.grpc.StatusRuntimeException: NOT_FOUND"
        ],
        "status": 500
    }

Upload a DAR File
*****************

HTTP Request
============

- URL: ``/v1/packages``
- Method: ``POST``
- Content-Type: ``application/octet-stream``
- Content: <DAR bytes>

The content (body) of the HTTP request contains raw DAR file bytes, without any encoding.

HTTP Response, status: 200 OK
=============================

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "result": 1,
        "status": 200
    }

HTTP Response with Error
========================

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
        "errors": [
            "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid argument: Invalid DAR: package-upload, content: [}]"
        ],
        "status": 500
    }

Streaming API
*************

Two subprotocols must be passed with every request, as described in
`Passing token with WebSockets <#passing-token-with-websockets>`__.

JavaScript/Node.js example demonstrating how to establish Streaming API connection:

.. code-block:: javascript

    const wsProtocol = "daml.ws.auth";
    const tokenPrefix = "jwt.token.";
    const jwt =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJIVFRQLUpTT04tQVBJLUdhdGV3YXkiLCJhY3RBcyI6WyJBbGljZSJdfX0.34zzF_fbWv7p60r5s1kKzwndvGdsJDX-W4Xhm4oVdp";
    const subprotocols = [`${tokenPrefix}${jwt}`, wsProtocol];

    const ws = new WebSocket("ws://localhost:7575/v1/stream/query", subprotocols);

    ws.addEventListener("open", function open() {
      ws.send(JSON.stringify({templateIds: ["Iou:Iou"]}));
    });

    ws.addEventListener("message", function incoming(data) {
      console.log(data);
    });

Please note that Streaming API does not allow multiple requests over the same WebSocket connection. The server returns an error and disconnects if second request received over the same WebSocket connection.

Error and Warning Reporting
===========================

Errors and warnings reported as part of the regular ``on-message`` flow: ``ws.addEventListener("message", ...)``.

Streaming API error messages formatted the same way as :ref:`synchronous API errors <error-format>`.

Streaming API reports only one type of warnings -- unknown template IDs, which is formatted as:

.. code-block:: none

    {"warnings":{"unknownTemplateIds":<JSON Array of template ID strings>>}}

Error and Warning Examples:
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: none

    {"warnings": {"unknownTemplateIds": ["UnknownModule:UnknownEntity"]}}

    {
      "errors":["JsonReaderError. Cannot read JSON: <{\"templateIds\":[]}>. Cause: spray.json.DeserializationException: search requires at least one item in 'templateIds'"],
      "status":400
    }

    {
      "errors":["Multiple requests over the same WebSocket connection are not allowed."],
      "status":400
    }

    {
      "errors":["Could not resolve any template ID from request."],
      "status":400
    }

Contracts Query Stream
======================

- URL: ``/v1/stream/query``
- Scheme: ``ws``
- Protocol: ``WebSocket``

List currently active contracts that match a given query, with
continuous updates.

``application/json`` body must be sent first, formatted according to the
:doc:`search-query-language`::

    {"templateIds": ["Iou:Iou"]}

Multiple queries may be specified in an array, for overlapping or
different sets of template IDs::

    [
        {"templateIds": ["Iou:Iou"], "query": {"amount": {"%lte": 50}}},
        {"templateIds": ["Iou:Iou"], "query": {"amount": {"%gt": 50}}},
        {"templateIds": ["Iou:Iou"]}
    ]

An optional ``offset`` returned by a prior query (see output examples
below) may be specified *before* the above, as a separate body.  It must
be a string, and if specified, the stream will begin immediately *after*
the response body that included that offset::

    {"offset": "5609"}

The output is a series of JSON documents, each ``payload`` formatted
according to :doc:`lf-value-specification`::

    {
        "events": [{
            "created": {
                "observers": [],
                "agreementText": "",
                "payload": {
                    "observers": [],
                    "issuer": "Alice",
                    "amount": "999.99",
                    "currency": "USD",
                    "owner": "Alice"
                },
                "signatories": ["Alice"],
                "contractId": "#1:0",
                "templateId": "eb3b150383a979d6765b8570a17dd24ae8d8b63418ee5fd20df20ad2a1c13976:Iou:Iou"
            },
            "matchedQueries": [1, 2]
        }]
    }

where ``matchedQueries`` indicates the 0-based indices into the request
list of queries that matched this contract.

Every ``events`` block following the end of contracts that existed when
the request started includes an ``offset``.  The stream is guaranteed to
send an offset immediately at the beginning of this "live" data, which
may or may not contain any ``events``; if it does not contain events and
no events were emitted before, it may be ``null`` if there was no
transaction on the ledger
or a string representing the current ledger end;
otherwise, it will be a string.  For example, you might use it to turn
off an initial "loading" indicator::

    {
        "events": [],
        "offset": "2"
    }

To keep the stream alive, you'll occasionally see messages like this,
which can be safely ignored if you do not need to capture the last seen ledger offset::

    {"events":[],"offset":"5609"}

where ``offset`` is the last seen ledger offset.

After submitting an ``Iou_Split`` exercise, which creates two contracts
and archives the one above, the same stream will eventually produce::

    {
        "events": [{
            "archived": {
                "contractId": "#1:0",
                "templateId": "eb3b150383a979d6765b8570a17dd24ae8d8b63418ee5fd20df20ad2a1c13976:Iou:Iou"
            }
        }, {
            "created": {
                "observers": [],
                "agreementText": "",
                "payload": {
                    "observers": [],
                    "issuer": "Alice",
                    "amount": "42.42",
                    "currency": "USD",
                    "owner": "Alice"
                },
                "signatories": ["Alice"],
                "contractId": "#2:1",
                "templateId": "eb3b150383a979d6765b8570a17dd24ae8d8b63418ee5fd20df20ad2a1c13976:Iou:Iou"
            },
            "matchedQueries": [0, 2]
        }, {
            "created": {
                "observers": [],
                "agreementText": "",
                "payload": {
                    "observers": [],
                    "issuer": "Alice",
                    "amount": "957.57",
                    "currency": "USD",
                    "owner": "Alice"
                },
                "signatories": ["Alice"],
                "contractId": "#2:2",
                "templateId": "eb3b150383a979d6765b8570a17dd24ae8d8b63418ee5fd20df20ad2a1c13976:Iou:Iou"
            },
            "matchedQueries": [1, 2]
        }],
        "offset": "3"
    }

If any template IDs are found not to resolve, the first element of the stream will report them::

    {"warnings": {"unknownTemplateIds": ["UnknownModule:UnknownEntity"]}}

and the stream will continue, provided that at least one template ID
resolved properly.

Aside from ``"created"`` and ``"archived"`` elements, ``"error"``
elements may appear, which contain a string describing the error.  The
stream will continue in these cases, rather than terminating.

Some notes on behavior:

1. Each result array means "this is what would have changed if you just
   polled ``/v1/query`` iteratively."  In particular, just as
   polling search can "miss" contracts (as a create and archive can be
   paired between polls), such contracts may or may not appear in any
   result object.

2. No ``archived`` ever contains a contract ID occurring within a
   ``created`` in the same array.  So, for example, supposing you are
   keeping an internal map of active contracts keyed by contract ID, you
   can apply the ``created`` first or the ``archived`` first, forwards,
   backwards, or in random order, and be guaranteed to get the same
   results.

3. Within a given array, if an ``archived`` and ``created`` refer to
   contracts with the same template ID and :doc:`contract key </daml/reference/contract-keys>`, the
   ``archived`` is guaranteed to occur before the ``created``.

4. Except in cases of #3, within a single response array, the order of
   ``created`` and ``archived`` is undefined and does not imply that any
   element occurred "before" or "after" any other one.

5. You will almost certainly receive contract IDs in ``archived`` that
   you never received a ``created`` for.  These are contracts that
   query filtered out, but for which the server no longer is aware of
   that.  You can safely ignore these.  However, such "phantom archives"
   *are* guaranteed to represent an actual archival *on the ledger*, so
   if you are keeping a more global dataset outside the context of this
   specific search, you can use that archival information as you wish.

Fetch by Key Contracts Stream
=============================

- URL: ``/v1/stream/fetch``
- Scheme: ``ws``
- Protocol: ``WebSocket``

List currently active contracts that match one of the given ``{templateId, key}`` pairs, with continuous updates.

``application/json`` body must be sent first, formatted according to the following rule:

.. code-block:: none

    [
        {"templateId": "<template ID 1>", "key": <key 1>},
        {"templateId": "<template ID 2>", "key": <key 2>},
        ...
        {"templateId": "<template ID N>", "key": <key N>}
    ]

Where:

- ``templateId`` -- contract template identifier, same as in :ref:`create request <create-request>`,
- ``key`` -- contract key, formatted according to the :doc:`lf-value-specification`,

Example:

.. code-block:: json

    [
        {"templateId": "Account:Account", "key": {"_1": "Alice", "_2": "abc123"}},
        {"templateId": "Account:Account", "key": {"_1": "Alice", "_2": "def345"}}
    ]

The output stream has the same format as the output from the `Contracts
Query Stream`_. We further guarantee that for every ``archived`` event
appearing on the stream there has been a matching ``created`` event
earlier in the stream, except in the case of missing
``contractIdAtOffset`` fields in the case described below.

You may supply an optional ``offset`` for the stream, exactly as with
query streams.  However, you should supply with each ``{templateId,
key}`` pair a ``contractIdAtOffset``, which is the contract ID currently
associated with that pair at the point of the given offset, or ``null``
if no contract ID was associated with the pair at that offset.  For
example, with the above keys, if you had one ``"abc123"`` contract but
no ``"def345"`` contract, you might specify:

.. code-block:: json

    [
        {"templateId": "Account:Account", "key": {"_1": "Alice", "_2": "abc123"},
         "contractIdAtOffset": "#1:0"},
        {"templateId": "Account:Account", "key": {"_1": "Alice", "_2": "def345"},
         "contractIdAtOffset": null}
    ]

If every ``contractIdAtOffset`` is specified, as is so in the example
above, you will not receive any ``archived`` events for contracts
created before the offset *unless* those contracts are identified in a
``contractIdAtOffset``.  By contrast, if any ``contractIdAtOffset`` is
missing, ``archived`` event filtering will be disabled, and you will
receive "phantom archives" as with query streams.
