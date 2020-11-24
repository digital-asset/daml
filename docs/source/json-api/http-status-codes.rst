.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

HTTP Status Codes
#################

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
****************************************

- Content-Type: ``application/json``
- Content:

.. code-block:: none

    {
        "status": 200,
        "result": <JSON object>
    }

Successful response with a warning, HTTP status: 200 OK
*******************************************************

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
*******************************************

- Content-Type: ``application/json``
- Content:

.. code-block:: none

    {
        "status": <400 | 401 | 404 | 500>,
        "errors": <JSON array of strings>
    }

Examples
********

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