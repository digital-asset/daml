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

**Update Monad Integration**: External calls are ``Update`` actions, ensuring
they are properly recorded in the transaction structure. This allows the ledger
to track external interactions and maintain auditability.

**Question/Answer Interface**: The engine delegates HTTP calls to the participant
via a question/answer interface. This ensures connection pooling is managed at
the participant level and the engine does not make direct network calls.

**Multiple Extension Support**: Participants can configure multiple extension
services, each with its own endpoint, authentication, and function declarations.
This enables different use cases (oracles, KYC, etc.) to coexist.

**Deterministic Services**: The external service must be fully deterministic.
Given identical inputs, it must always produce the same output. This is enforced
by requiring all participants to use the same service configuration and/or binary,
verified through the ``configHash`` parameter.

**Local Execution**: Each participant runs its own local instance of the
external service. This eliminates network dependencies and single points of
failure that could compromise ledger integrity.

**Transaction Validation**: In Canton, all transactions are validated and
re-executed on all affected participants. When a transaction contains an
external call, each participant independently executes the same call with
identical parameters. The transaction is only accepted if all participants
obtain identical results from their respective service instances.

**Startup Validation**: On participant startup, the system validates that all
DARs have their extension requirements satisfied by the participant's
configuration. This detects misconfigurations and version drift early.

Types
-----

.. _type-da-external-byteshex-28451:

`BytesHex <type-da-external-byteshex-28451_>`_
  \: :ref:`Text <type-ghc-types-text-51952>`

  A type alias for ``Text`` representing hex-encoded bytes. The text must
  contain only hexadecimal characters (0-9, a-f, A-F) and have an even length
  (each pair of hex characters represents one byte).

Functions
---------

.. _function-da-external-externalcall-71234:

`externalCall <function-da-external-externalcall-71234_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`BytesHex <type-da-external-byteshex-28451>` \-\> :ref:`BytesHex <type-da-external-byteshex-28451>` \-\> :ref:`Update <type-da-internal-lf-update-68702>` :ref:`BytesHex <type-da-external-byteshex-28451>`

  Makes an external call to a configured extension service. This is an
  ``Update`` action that records the external call in the transaction.

  **Parameters:**

  * ``extensionId``: The identifier of the configured extension (must match
    a key in the Canton participant's ``engine.extensions`` configuration).

  * ``functionId``: The identifier of the function within the extension to call
    (e.g., "get-price", "verify-identity"). This value is sent as the
    ``X-Daml-External-Function-Id`` HTTP header.

  * ``configHash``: A hash (encoded in hex) of the external service's
    configuration and/or binary. Must be valid hex-encoded bytes (even length,
    characters 0-9, a-f, A-F). This parameter ensures all participants use the
    same service version.

  * ``inputHex``: Input data for the external call, encoded as a hex string.
    Must be valid hex-encoded bytes (even length, characters 0-9, a-f, A-F).

  **Returns:**

  * The response from the external service as a hex string (normalized to
    lowercase).

  **Errors:**

  The function throws an error (aborting the transaction) if:

  * Either ``configHash`` or ``inputHex`` is invalid hex (not hex-encoded or
    odd length).

  * The extension is not configured on the participant.

  * The HTTP call fails (network error, timeout, non-200 response).

  Error messages include detailed context (HTTP status, request ID, extension
  ID, function ID) for debugging.

.. _function-da-external-ishex-17968:

`isHex <function-da-external-ishex-17968_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Checks whether a text string contains only valid hexadecimal characters
  (0-9, a-f, A-F). Does not check the length of the string.

.. _function-da-external-isbyteshex-48261:

`isBytesHex <function-da-external-isbyteshex-48261_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Checks whether a text string is a valid hex-encoded byte string. Returns
  ``True`` if the string has an even length and contains only valid hexadecimal
  characters (0-9, a-f, A-F).

Canton Configuration
--------------------

External calls are configured in the Canton participant configuration file:

.. code-block:: hocon

   canton.participants.mynode.parameters.engine {
     # Map of extension ID to extension configuration
     extensions {
       price-oracle {
         name = "Price Oracle Service"
         host = "oracle.example.com"
         port = 8443
         use-tls = true
         jwt-file = "/secrets/oracle-jwt.txt"
         connect-timeout = 500ms
         request-timeout = 8s
         max-retries = 3

         # Declared functions for startup validation
         declared-functions = [
           { function-id = "get-price", config-hash = "a1b2c3d4..." }
           { function-id = "get-volatility", config-hash = "e5f6g7h8..." }
         ]
       }

       kyc-service {
         name = "KYC Verification Service"
         host = "kyc.internal"
         port = 8080
         use-tls = false
         jwt = "eyJhbGciOiJIUzI1NiIs..."

         declared-functions = [
           { function-id = "verify-identity", config-hash = "..." }
         ]
       }
     }

     extension-settings {
       # Validate extension configurations on startup
       validate-extensions-on-startup = true

       # Fail startup if validation fails
       fail-on-extension-validation-error = true

       # Echo mode for testing (returns input as output)
       echo-mode = false
     }
   }

**Configuration Options:**

* ``name``: Human-readable name for the extension
* ``host``: Hostname of the extension service
* ``port``: Port of the extension service
* ``use-tls``: Whether to use TLS for the connection (default: true)
* ``tls-insecure``: Skip TLS certificate validation (dev only, default: false)
* ``jwt``: JWT token for authentication
* ``jwt-file``: Path to file containing JWT token
* ``connect-timeout``: Connection timeout (default: 500ms)
* ``request-timeout``: Request timeout (default: 8s)
* ``max-total-timeout``: Maximum total time including retries (default: 25s)
* ``max-retries``: Maximum retry attempts (default: 3)
* ``declared-functions``: Functions this extension provides (for validation)

HTTP Protocol
-------------

The participant makes HTTP POST requests with the following format:

**Request URL**: ``https://<host>:<port>/api/v1/external-call``

**HTTP Headers**:

* ``Content-Type: application/octet-stream``
* ``X-Daml-External-Function-Id: <functionId>``
* ``X-Daml-External-Config-Hash: <configHash>`` (normalized to lowercase)
* ``X-Daml-External-Mode: <mode>`` where mode is one of:

  * ``validation``: Transaction validation phase
  * ``submission``: Transaction submission phase

* ``X-Request-Id: <uuid>`` (for request tracking)
* ``Authorization: Bearer <jwt>`` (if JWT is configured)

**Request Body**: The normalized (lowercase) hex-encoded input data.

**Response**: HTTP 200 with the hex-encoded response body. Any other status
code triggers retries (for 408, 429, 500, 502, 503, 504) or immediate failure.

Examples
--------

Basic usage:

.. code-block:: daml

   import DA.External

   template PriceOracle
     with
       owner : Party
     where
       signatory owner

       choice GetPrice : Text
         with
           asset : Text
         controller owner
         do
           -- Make external call to price oracle
           result <- externalCall "price-oracle" "get-price" "a1b2c3d4" asset
           pure result

Using validation helpers:

.. code-block:: daml

   import DA.External

   validateAndCall : Text -> Text -> Text -> Text -> Update (Optional Text)
   validateAndCall extId funcId config input = do
     if isBytesHex config && isBytesHex input
       then do
         result <- externalCall extId funcId config input
         pure (Some result)
       else pure None

Security Best Practices
-----------------------

* **Service Determinism**: The external service must be fully deterministic.
  Same inputs must always produce same outputs.

* **Configuration Hash**: Use a cryptographic hash (e.g., SHA-256) of the
  service's configuration for ``configHash``.

* **HTTPS in Production**: Always use TLS in production environments.

* **Startup Validation**: Enable ``validate-extensions-on-startup`` to catch
  configuration issues early.

* **Function Declarations**: Declare all functions in the extension config
  to enable version drift detection.
