.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-external-93384:

DA.External
===========

The ``DA.External`` module provides a safe interface for Daml code to interact
with external deterministic services. It enables integration with systems outside
the Daml ledger, such as oracles, price feeds, or custom computation services,
while maintaining the deterministic execution guarantees required for distributed
ledger consensus.

Execution Model and Security Guarantees
---------------------------------------

External calls are fully compatible with Daml's execution model and security
guarantees. They maintain deterministic execution and consensus properties
through the following design:

**Deterministic Services**: The external service must be fully deterministic.
Given identical inputs (``functionId``, ``configHex``, and ``inputHex``), it
must always produce the same output. This is enforced by requiring all
participants to use the same service configuration and/or binary, verified through
the ``configHex`` hash parameter.

**Local Execution**: Each participant runs its own local instance of the
external service. This eliminates network dependencies and single points of
failure that could compromise ledger integrity.

**Transaction Validation**: In Canton, all transactions are validated and
re-executed on all affected participants. When a transaction contains an
external call, each participant independently executes the same call with
identical parameters. The transaction is only accepted if all participants
obtain identical results from their respective service instances.

**Consensus Preservation**: Since all participants must arrive at the same
result, external calls preserve the consensus properties of the ledger. If any
participant's service produces a different result, the transaction is rejected,
maintaining ledger integrity.

The external service becomes part of the deterministic execution environment,
similar to built-in functions, while enabling integration with external systems.

Types
-----

.. _type-da-external-byteshex-28451:

`BytesHex <type-da-external-byteshex-28451_>`_
  \: :ref:`Text <type-ghc-types-text-51952>`

  A type alias for ``Text`` representing hex-encoded bytes. The text must
  contain only hexadecimal characters (0-9, a-f, A-F) and have an even length
  (each pair of hex characters represents one byte).

  Note: This is a separate type alias from ``DA.Crypto.Text.BytesHex``, though
  both represent the same concept (``Text``). They are kept separate because
  ``DA.Crypto.Text`` is an alpha feature, while ``DA.External`` is stable. This
  avoids coupling stable functionality to alpha features.

Functions
---------

.. _function-da-external-externalcall-71234:

`externalCall <function-da-external-externalcall-71234_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`BytesHex <type-da-external-byteshex-28451>` \-\> :ref:`BytesHex <type-da-external-byteshex-28451>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`BytesHex <type-da-external-byteshex-28451>`

  Makes an external call to a deterministic service identified by ``functionId``
  with the provided configuration hash and input data, both encoded as
  hexadecimal strings.

  **Parameters:**

  * ``functionId``: The identifier of the external function to call (e.g.,
    "echo", "oracle", "price-feed"). This value is sent as the
    ``X-Daml-External-Function-Id`` HTTP header, allowing the external service
    to route requests to different handlers or endpoints.

  * ``configHex``: A hash (encoded in hex) of the external service's
    configuration and/or binary. Must be valid hex-encoded bytes (even length,
    characters 0-9, a-f, A-F). This parameter ensures all participants use the
    same service version by comparing the hash of their local service against
    this value. Essential for maintaining consensus.

  * ``inputHex``: Input data for the external call, encoded as a hex string.
    Must be valid hex-encoded bytes (even length, characters 0-9, a-f, A-F).

  **Returns:**

  * ``Some result``: If the call succeeds and both input arguments are valid
    hex-encoded bytes. The result is normalized to lowercase hexadecimal.

  * ``None``: If either input argument is invalid (not hex-encoded or odd
    length), or if the external call fails (HTTP errors, timeouts, connection
    failures).

  **Behavior:**

  * Input validation occurs before making the external call. Invalid inputs
    result in ``None`` without attempting the HTTP request.

  * All hex inputs are normalized to lowercase before being passed to the
    external call primitive.

  * Successful results are returned as lowercase hexadecimal strings.

.. _function-da-external-ishex-17968:

`isHex <function-da-external-ishex-17968_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Checks whether a text string contains only valid hexadecimal characters
  (0-9, a-f, A-F). Does not check the length of the string. An empty string
  returns ``True`` (vacuously, as all characters are valid hex).

.. _function-da-external-isbyteshex-48261:

`isBytesHex <function-da-external-isbyteshex-48261_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Checks whether a text string is a valid hex-encoded byte string. Returns
  ``True`` if the string has an even length and contains only valid hexadecimal
  characters (0-9, a-f, A-F).

Configuration
-------------

External calls are configured via environment variables or system properties:

**DAML_EXTERNAL_CALL_ENDPOINT** (or ``daml.external.call.endpoint`` system property)
  The full URL for the external call endpoint, including the path. Must be a valid HTTP or HTTPS URL.
  
  Default: ``http://127.0.0.1:1606/api/v1/external-call``
  
  Example: ``export DAML_EXTERNAL_CALL_ENDPOINT="https://api.example.com/api/v1/external-call"``

**DAML_EXTERNAL_CALL_ECHO** (or ``daml.external.call.echo`` system property)
  Enables echo mode for testing. When enabled, returns the input hex string as
  output without making an HTTP request.
  
  Accepts: ``1``, ``true``, ``yes`` (case-insensitive)
  
  Default: Echo mode is disabled (HTTP requests are made)
  
  Example: ``export DAML_EXTERNAL_CALL_ECHO="true"``

**DAML_EXTERNAL_CALL_COST_<FUNCTION_ID>** (or ``daml.external.call.cost.<functionId>`` system property)
  Configures the cost/weight for external calls on a per-function basis. This allows
  different functions to have different costs based on their computational requirements.
  
  The cost is looked up dynamically each time an external call is made, allowing
  configuration changes without code modifications.
  
  **Environment Variable Format:**
  
  * Pattern: ``DAML_EXTERNAL_CALL_COST_<FUNCTION_ID>`` where ``<FUNCTION_ID>`` is the
    function identifier in uppercase (e.g., ``ECHO``, ``ORACLE``, ``PRICE_FEED``)
  * Value: A positive integer representing the cost/weight
  * Default: ``0`` (if not set or if the value cannot be parsed)
  
  **System Property Format:**
  
  * Pattern: ``daml.external.call.cost.<functionId>`` where ``<functionId>`` is the
    function identifier in lowercase (e.g., ``echo``, ``oracle``, ``price-feed``)
  * Value: A positive integer representing the cost/weight
  * Default: ``0`` (if not set or if the value cannot be parsed)
  
  **Lookup Order:**
  
  1. Environment variable (checked first)
  2. System property (checked if environment variable is not set)
  3. Default value of ``0`` (used if neither is set or if the value is invalid)
  
  **Examples:**
  
  .. code-block:: bash
  
     # Set cost for "echo" function via environment variable
     export DAML_EXTERNAL_CALL_COST_ECHO=1000
     
     # Set cost for "oracle" function via environment variable
     export DAML_EXTERNAL_CALL_COST_ORACLE=5000
     
     # Set cost via system property (lowercase function ID)
     -Ddaml.external.call.cost.echo=1000
     -Ddaml.external.call.cost.oracle=5000
  
  **Note:** Function IDs in environment variable names must be uppercase, while system
  property names use lowercase function IDs. Invalid values (non-numeric strings) are
  silently ignored and the default cost of ``0`` is used.

HTTP Protocol
-------------

When echo mode is disabled, ``externalCall`` makes HTTP POST requests with the
following format:

**Request URL**: Configured via ``DAML_EXTERNAL_CALL_ENDPOINT`` (default:
``http://127.0.0.1:1606/api/v1/external-call``)

**HTTP Headers**:

* ``Content-Type: application/octet-stream``
* ``X-Daml-External-Function-Id: <functionId>``
* ``X-Daml-External-Config-Hash: <configHex>`` (normalized to lowercase)
* ``X-Daml-External-Mode: <mode>`` where mode is one of:
  
  * ``validation``: Transaction validation phase
  * ``submission``: Transaction submission phase
  * ``pure``: Pure computation context

**Request Body**: The normalized (lowercase) hex-encoded input data as a plain
text string.

**Timeouts**: Connection timeout 500ms, request timeout 1500ms

**HTTP Version**: HTTP/1.1

**Response**: HTTP 200 returns ``Some`` with the response body (expected to be
hex-encoded, normalized to lowercase). Any other status code or network error
returns ``None``.

Examples
--------

Basic usage:

.. code-block:: daml

   import DA.External
   import DA.Assert

   testSuccess : ()
   testSuccess =
     -- configHex should be a cryptographic hash of the service configuration
     case externalCall "echo" "a1b2c3d4e5f6" "cafebabe" of
       Some res -> assert (res == "cafebabe")
       None -> abort "unexpected failure"

Handling invalid input:

.. code-block:: daml

   import DA.External

   testFailure : ()
   testFailure =
     case externalCall "echo" "xyz" "00" of
       Some _ -> abort "expected failure"
       None -> ()  -- Invalid hex input "xyz" causes None

Using validation helpers:

.. code-block:: daml

   import DA.External

   validateInput : Text -> Optional BytesHex
   validateInput input =
     if isBytesHex input
       then Some input
       else None

   safeExternalCall : Text -> Text -> Text -> Optional BytesHex
   safeExternalCall functionId config input =
     case (validateInput config, validateInput input) of
       (Some configHex, Some inputHex) -> externalCall functionId configHex inputHex
       _ -> None

Security Best Practices
-----------------------

* **Service Determinism**: The external service must be fully deterministic.
  Same inputs must always produce same outputs. Non-deterministic services will
  cause transaction validation failures.

* **Configuration Hash**: Use a cryptographic hash (e.g., SHA-256) of the
  service's configuration and/or binary for ``configHex``. This ensures all
  participants use the same service version.

* **HTTPS in Production**: Use HTTPS endpoints to ensure data confidentiality
  and integrity in transit.

* **Response Validation**: Always validate and sanitize responses from external
  services before using them in critical business logic.

* **Timeout Handling**: External calls have strict timeouts (500ms connection,
  1500ms request). Design services to respond within these limits, or handle
  timeouts gracefully in Daml code.

* **Service Verification**: The external service should verify the
  ``X-Daml-External-Config-Hash`` header matches its own configuration hash to
  ensure it's being called with the expected configuration.
