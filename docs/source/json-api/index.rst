.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

HTTP JSON API Service
#####################

**WARNING:** the HTTP JSON API described in this document is actively
being designed and is *subject to breaking changes*, including all
request and response elements demonstrated below or otherwise
implemented by the API.  We welcome feedback about the API on `our issue
tracker
<https://github.com/digital-asset/daml/issues/new?milestone=HTTP+JSON+API+Maintenance>`_
or `on Slack <https://damldriven.slack.com/>`_.

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
        --http-port 7575 --max-inbound-message-size 4194304 --package-reload-interval 5s \\
        --application-id HTTP-JSON-API-Gateway

::

    $ daml json-api --help
    HTTP JSON API daemon
    Usage: http-json-binary [options]

      --help                   Print this usage text
      --ledger-host <value>    Ledger host name or IP address
      --ledger-port <value>    Ledger port number
      --http-port <value>      HTTP JSON API service port number
      --application-id <value>
                               Optional application ID to use for ledger registration. Defaults to HTTP-JSON-API-Gateway
      --package-reload-interval <value>
                               Optional interval to poll for package updates. Examples: 500ms, 5s, 10min, 1h, 1d. Defaults to 5 seconds
      --max-inbound-message-size <value>
                               Optional max inbound message size in bytes. Defaults to 4194304

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
``Authorization: Bearer copy-paste-token-here``.

Here are two tokens you can use for testing:

- ``{"ledgerId": "MyLedger", "applicationId": "foobar", "party": "Alice"}``
  ``eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQWxpY2UifQ.4HYfzjlYr1ApUDot0a6a4zB49zS_jrwRUOCkAiPMqo0``

- ``{"ledgerId": "MyLedger", "applicationId": "foobar", "party": "Bob"}``
  ``eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQm9iIn0.2LE3fAvUzLx495JWpuSzHye9YaH3Ddt4d2Pj0L1jSjA``
  
For production use, we have a tool in development for generating proper
RSA-encrypted tokens locally, which will arrive when the service also
supports such tokens.

GET http://localhost:7575/contracts/search
==========================================

POST http://localhost:7575/contracts/search
===========================================

List currently active contracts that match a given query.

application/json body, formatted according to the :doc:`search-query-language`::

    {"%templates": [{"moduleName": "Iou", "entityName": "Iou"}],
     "amount": 999.99}

empty output::

    {
        "status": 200,
        "result": []
    }

output, each contract formatted according to :doc:`lf-value-specification`::

    {
        "status": 200,
        "result": [
            {
                "observers": [],
                "agreementText": "",
                "signatories": [
                    "Alice"
                ],
                "contractId": "#489:0",
                "templateId": {
                    "packageId": "ac3a64908d9f6b4453329b3d7d8ddea44c83f4f5469de5f7ae19158c69bf8473",
                    "moduleName": "Iou",
                    "entityName": "Iou"
                },
                "witnessParties": [
                    "Alice"
                ],
                "argument": {
                    "observers": [],
                    "issuer": "Alice",
                    "amount": "999.99",
                    "currency": "USD",
                    "owner": "Alice"
                },
                "workflowId": "Alice Workflow"
            }
        ]
    }

POST http://localhost:7575/command/create
=========================================

Create a contract.

application/json body, ``argument`` formatted according to :doc:`lf-value-specification`::

    {
      "templateId": {
        "moduleName": "Iou",
        "entityName": "Iou"
     },
      "argument": {
        "observers": [],
        "issuer": "Alice",
        "amount": "999.99",
        "currency": "USD",
        "owner": "Alice"
      }
    }

output::

    {
        "status": 200,
        "result": {
            "observers": [],
            "agreementText": "",
            "signatories": [
                "Alice"
            ],
            "contractId": "#56:0",
            "templateId": {
                "packageId": "ac3a64908d9f6b4453329b3d7d8ddea44c83f4f5469de5f7ae19158c69bf8473",
                "moduleName": "Iou",
                "entityName": "Iou"
            },
            "witnessParties": [
                "Alice"
            ],
            "argument": {
                "observers": [],
                "issuer": "Alice",
                "amount": "999.99",
                "currency": "USD",
                "owner": "Alice"
            },
            "workflowId": "Alice Workflow"
        }
    }
 
POST http://localhost:44279/command/exercise
============================================

Exercise a choice on a contract.

``"contractId": "#56:0"`` is the value from the create output
application/json body::

    {
        "templateId": {
            "moduleName": "Iou",
            "entityName": "Iou"
        },
        "contractId": "#56:0",
        "choice": "Iou_Transfer",
        "argument": {
            "newOwner": "Alice"
        }
    }

output::

    {
        "status": 200,
        "result": [
            {
                "archived": {
                    "workflowId": "Alice Workflow",
                    "contractId": "#56:0",
                    "templateId": {
                        "packageId": "ac3a64908d9f6b4453329b3d7d8ddea44c83f4f5469de5f7ae19158c69bf8473",
                        "moduleName": "Iou",
                        "entityName": "Iou"
                    },
                    "witnessParties": [
                        "Alice"
                    ]
                }
            },
            {
                "created": {
                    "observers": [],
                    "agreementText": "",
                    "signatories": [
                        "Alice"
                    ],
                    "contractId": "#301:1",
                    "templateId": {
                        "packageId": "ac3a64908d9f6b4453329b3d7d8ddea44c83f4f5469de5f7ae19158c69bf8473",
                        "moduleName": "Iou",
                        "entityName": "IouTransfer"
                    },
                    "witnessParties": [
                        "Alice"
                    ],
                    "argument": {
                        "iou": {
                            "observers": [],
                            "issuer": "Alice",
                            "amount": "999.99",
                            "currency": "USD",
                            "owner": "Alice"
                        },
                        "newOwner": "Alice"
                    },
                    "workflowId": "Alice Workflow"
                }
            }
        ]
    }
