.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _json-api:

HTTP JSON API Service
#####################

**WARNING:** the HTTP JSON API described in this document is actively
being designed and is *subject to breaking changes*, including all
request and response elements demonstrated below or otherwise
implemented by the API.  We welcome feedback about the API on `our issue
tracker
<https://github.com/digital-asset/daml/issues/new?milestone=HTTP+JSON+API+Maintenance>`_
or `on Slack <https://hub.daml.com/slack/>`_.

Please keep in mind that the presence of **/v1** prefix in the the URLs below does not mean that the endpoint interfaces are stabilized.

The **JSON API** provides a significantly simpler way than :doc:`the Ledger
API </app-dev/ledger-api>` to interact with a ledger by providing *basic active contract set functionality*:

- creating contracts,
- exercising choices on contracts,
- querying the current active contract set, and
- retrieving all known parties.

The goal of this API is to get you up and running distributed ledger applications quickly, so we have deliberately excluded
complicating concerns, including but not limited to:

- inspecting transactions,
- asynchronous submit/completion workflows,
- temporal queries (e.g. active contracts *as of a certain time*), and
- ledger metaprogramming (e.g. retrieving packages and templates).

For these and other features, use :doc:`the Ledger API </app-dev/ledger-api>`
instead.

.. toctree::
   :hidden:

   lf-value-specification
   search-query-language

How to start
************

Start sandbox
=============

From a DAML project directory:

.. code-block:: shell

    $ daml sandbox --wall-clock-time --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar

.. _start-http-service:

Start HTTP service
==================

From a DAML project directory:

.. code-block:: shell

    $ daml json-api --ledger-host localhost --ledger-port 6865 \
        --http-port 7575 --max-inbound-message-size 4194304 --package-reload-interval 5s \
        --application-id HTTP-JSON-API-Gateway --static-content "prefix=static,directory=./static-content" \
        --query-store-jdbc-config "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,createSchema=false"

.. code-block:: none

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
      --default-ttl <value>
            Optional Time to Live interval to set if not provided in the command. Examples: 30s, 1min, 1h. Defaults to 30 seconds
      --max-inbound-message-size <value>
            Optional max inbound message size in bytes. Defaults to 4194304
      --query-store-jdbc-config "driver=<JDBC driver class name>,url=<JDBC connection url>,user=<user>,password=<password>,createSchema=<true|false>"
            Optional query store JDBC configuration string. Query store is a search index, use it if you need to query large active contract sets. Contains comma-separated key-value pairs. Where:
            driver -- JDBC driver class name, only org.postgresql.Driver supported right now,
            url -- JDBC connection URL, only jdbc:postgresql supported right now,
            user -- database user name,
            password -- database user password,
            createSchema -- boolean flag, if set to true, the process will re-create database schema and terminate immediately.
            Example: "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,createSchema=false"
      --static-content "prefix=<URL prefix>,directory=<directory>"
            DEV MODE ONLY (not recommended for production). Optional static content configuration string. Contains comma-separated key-value pairs. Where:
            prefix -- URL prefix,
            directory -- local directory that will be mapped to the URL prefix.
            Example: "prefix=static,directory=./static-content"
      --access-token-file <value>
            provide the path from which the access token will be read, required to interact with an authenticated ledger, no default
      --websocket-config "maxDuration=<Maximum websocket session duration in minutes>,heartBeatPer=Server-side heartBeat interval in seconds"
            Optional websocket configuration string. Contains comma-separated key-value pairs. Where:
            maxDuration -- Maximum websocket session duration in minutes
            heartBeatPer -- Server-side heartBeat interval in seconds
            Example: "maxDuration=120,heartBeatPer=5"

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

.. code-block:: shell

    $ daml new iou-quickstart-java quickstart-java
    $ cd iou-quickstart-java/
    $ daml build
    $ daml sandbox --wall-clock-time --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar
    $ daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575

Choosing a party
****************

Every request requires you to specify a party and some other settings,
with a JWT token.  Normal HTTP requests pass the token in an
``Authentication`` header, while WebSocket requests pass the token in a
subprotocol.

In testing environments, you can use https://jwt.io to generate your
token.  The default "header" is fine.  Under "Payload", fill in:

.. code-block:: json

    {
      "https://daml.com/ledger-api": {
        "ledgerId": "MyLedger",
        "applicationId": "foobar",
        "actAs": ["Alice"]
      }
    }

Keep in mind:
- the value of ``ledgerId`` payload field has to match ``--ledgerid`` passed to the sandbox.
- you can replace ``Alice`` with whatever party you want to use.

Under "Verify Signature", put ``secret`` as the secret (_not_ base64
encoded); that is the hardcoded secret for testing.

Then the "Encoded" box should have your **token**, ready for passing to
the service as described in the following sections.

Alternatively, here are two tokens you can use for testing:

- ``{"https://daml.com/ledger-api": {"ledgerId": "MyLedger", "applicationId": "foobar", "actAs": ["Alice"]}}``
  ``eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJmb29iYXIiLCJhY3RBcyI6WyJBbGljZSJdfX0.VdDI96mw5hrfM5ZNxLyetSVwcD7XtLT4dIdHIOa9lcU``

- ``{"https://daml.com/ledger-api": {"ledgerId": "MyLedger", "applicationId": "foobar", "actAs": ["Bob"]}}``
  ``eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJmb29iYXIiLCJhY3RBcyI6WyJCb2IiXX19.zU-iMSFG90na8IHacrS25xho3u6AKnSlTKbvpkaSyYw``
  
For production use, we have a tool in development for generating proper
RSA-encrypted tokens locally, which will arrive when the service also
supports such tokens.

Passing token with HTTP
=======================

Set HTTP header ``Authorization: Bearer copy-paste-token-here`` for
normal requests.

Passing token with WebSockets
=============================

WebSocket clients support a "subprotocols" argument (sometimes simply
called "protocols"); this is usually in a list form but occasionally in
comma-separated form.  Check documentation for your WebSocket library of
choice for details.

For HTTP JSON requests, you must pass two subprotocols:

- ``daml.ws.auth``
- ``jwt.token.copy-paste-token-here``

where ``copy-paste-token-here`` is the encoded JWT token described above.

Error Reporting
***************

The **JSON API** reports errors using standard HTTP status codes. It divides HTTP status codes in 3 groups indicating:

1. success (200)
2. failure due to a client-side problem (400, 401, 404)
3. failure due to a server-side problem (500)

The **JSON API** can return one of the following HTTP status codes:

- 200 - OK
- 400 - Bad Request (Client Error)
- 401 - Unauthorized, authentication required
- 404 - Not Found
- 500 - Internal Server Error

If client's HTTP GET or POST request reaches an API endpoint, the corresponding response will always contain a JSON object with ``status`` field, either ``errors`` or ``result`` and optional ``warnings``:

.. code-block:: none

    {
        "status": <400 | 401 | 404 | 500>
        ,"errors": <JSON array of strings> | ,"result": <JSON object>
        [ ,"warnings": <JSON object> ]
    }

Where:

- ``status`` -- a JSON number which matches the HTTP response status code returned in the HTTP header,
- ``errors`` -- a JSON array of strings, each string represents one error,
- ``result`` -- a JSON object or JSON array, representing one or many results,
- ``warnings`` -- optional field, a JSON object, representing one ore many warnings.

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

.. code-block:: none

    {"status": 200, "result": {...}}

.. code-block:: none

    {"status": 200, "result": [...], "warnings": {"unknownTemplateIds": ["UnknownModule:UnknownEntity"]}}

.. code-block:: json

    {"status": 401, "errors": ["Authentication Required"]}

.. code-block:: json

    {"status": 400, "errors": ["JSON parser error: Unexpected character 'f' at input index 27 (line 1, position 28)"]}

.. code-block:: json

    {"status": 500, "errors": ["Cannot initialize Ledger API"]}

Create a new Contract
*********************

See the request documentation below on how to create an instance of ``Iou`` contract from the :doc:`Quickstart guide </getting-started/quickstart>`:

.. literalinclude:: ../getting-started/quickstart/template-root/daml/Iou.daml
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
  + ``"<module>:<entity>"`` if contract template can be uniquely identified by it's module and entity name.

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

Create a new Contract with optional meta field
**********************************************

When creating a new contract, client may specify an optional ``meta`` field:

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
      	"commandId": "a unique ID",
      	"ledgerEffectiveTime": 1579730994499,
      	"maximumRecordTime": 1579731004499
      }
    }

Where:

- ``commandId`` -- optional field, a unique string identifying the command;
- ``ledgerEffectiveTime`` -- optional field, the number of milliseconds from the epoch of ``1970-01-01T00:00:00Z``, an approximation of the wall clock time on the ledger server;
- ``maximumRecordTime`` -- optional field, the number of milliseconds from the epoch of ``1970-01-01T00:00:00Z``, a deadline for observing this command in the completion stream before it can be considered to have timed out.
 
Exercise by Contract ID
***********************

The JSON command below, demonstrates how to exercise ``Iou_Transfer`` choice on ``Iou`` contract:

.. literalinclude:: ../getting-started/quickstart/template-root/daml/Iou.daml
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

The JSON command below, demonstrates how to exercise ``Archive`` choice on ``Account`` contract with a ``(Party, Text)`` key defined like this:

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

- ``templateId`` -- the initial contract template identifier, in the same format as in same as in the :ref:`create request <create-request>`,
- ``payload`` -- the initial contract fields as defined in the DAML template and formatted according to :doc:`lf-value-specification`,
- ``choice`` -- DAML contract choice, that is being exercised,
- ``argument`` -- contract choice argument(s).

HTTP Response
=============

Please note that the response below is for a consuming choice, so it contains:

- ``created`` and ``archived`` events for the initial contract (``"contractId": "#1:0"``), which was created and archived right away when a consuming choice was exercised on it,
- a ``created`` event for the contract that is the result of the choice exercise (``"contractId": "#1:2"``).

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


Contract Search, All Templates
******************************

List all currently active contracts for all known templates.

Note that the retrieved contracts do not get persisted into query store database. Query store is a search index and can be used to optimize search latency. See :ref:`Start HTTP service <start-http-service>` for information on how to start JSON API service with query store enabled.

HTTP Request
============

- URL: ``/v1/query``
- Method: ``GET``
- Content: <EMPTY>

HTTP Response
=============

The response is the same as for the POST method below.

Contract Search
***************

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
- ``query`` -- search criteria to apply to the specified ``templateIds``, formatted according to the :doc:`search-query-language`:

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
- ``status`` matches the HTTP status code returned in the HTTP header,

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

If empty JSON array is passed: ``[]``, this endpoint returns BadRequest(400) error:

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

HTTP Response with Unknown Parties Warning
============================================

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
        "unknownParties": [
          "Erin"
        ]
      },
      "status": 200
    }

Fetch All Known Parties
***********************

- URL: ``/v1/parties``
- Method: ``GET``
- Content: <EMPTY>

HTTP Response
=============

The response is the same as for the POST method above.

Streaming API
*************

Two subprotocols must be passed with every request, as described in
`Passing token with WebSockets <#passing-token-with-websockets>`__.

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

output a series of JSON documents, each ``payload`` formatted according
to :doc:`lf-value-specification`::

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
no events were emitted before, it may be ``null`` or a string;
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

2. No ``archived`` ever contains a contract ID occurring within an
   ``created`` in the same array.  So, for example, supposing you are
   keeping an internal map of active contracts, you can apply the
   ``created`` first or the ``archived`` first and be guaranteed to get
   the same results.

3. Within a given array, if an ``archived`` and ``created`` refer to
   contracts with the same template ID and contract key, the
   ``archived`` is guaranteed to occur before the ``created``.

4. You will almost certainly receive contract IDs in ``archived`` that
   you never received a ``created`` for.  These are contracts that
   query filtered out, but for which the server no longer is aware of
   that.  You can safely ignore these.  However, such "phantom archives"
   *are* guaranteed to represent an actual archival *on the ledger*, so
   if you are keeping a more global dataset outside the context of this
   specific search, you can use that archival information as you wish.

5. Within a single response array, the order of ``created`` and
   ``archived`` is undefined and does not imply that any element
   occurred "before" or "after" any other one.  As specified in note #2,
   order of application of changes doesn't matter; you will get the same
   results if you walk the array forwards, backwards, or in random
   order.

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

The output stream has the same format as the output from the `Contracts Query Stream`_. We further guarantee that for every ``archived`` event appearing on the stream there has been a matching ``created`` event earlier in the stream.
