.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _authorization:

Authorization
#############

When developing Daml applications using SDK tools,
your local setup will most likely not perform any Ledger API request authorization --
by default, any valid Ledger API request will be accepted by the sandbox.

This is not the case for participant nodes of deployed ledgers.
For every Ledger API request, the participant node checks whether the request contains an access token that is valid and sufficient to authorize that request.
You thus need to add support for authorization using access tokens to your application to run it against a deployed ledger.

.. note:: In case of mutual (two-way) TLS authentication, the Ledger API
          client must present its certificate (in addition to an access token) to
          the Ledger API server as part of the authentication process. The provided
          certificate must be signed by a certificate authority (CA) trusted
          by the Ledger API server. Note that the identity of the application
          will not be proven by using this method, i.e. the `application_id` field in the request
          is not necessarily correlated with the CN (Common Name) in the certificate.

Introduction
************

Your Daml application sends requests to the :doc:`Ledger API </app-dev/ledger-api>` exposed by a participant node to submit changes to the ledger
(e.g., "*exercise choice X on contract Y as party Alice*"), or to read data from the ledger
(e.g., "*read all active contracts visible to party Alice*").
Your application might send these requests via a middleware like the :doc:`JSON API </json-api/index>`.

Whether a participant node *can* serve such a request depends on whether the participant node hosts the respective parties, and
whether the request is valid according to the :ref:`Daml Ledger Model <da-ledgers>`.
Whether a participant node *will* serve such a request to a Daml application depends on whether the
request includes an access token that is valid and sufficient to authorize the request for this participant node.

Acquire and Use Access Tokens
*****************************

How an application acquires access tokens depends on the participant node it talks to and is ultimately set up by the participant node operator.
Many setups use a flow in the style of `OAuth 2.0 <https://oauth.net/2/>`_.

In this scenario, the Daml application first contacts a token issuer to get an access token.
The token issuer verifies the identity of the requesting application, looks up the privileges of the application,
and generates a signed access token describing those privileges.

Once the access token is issued, the Daml application sends it along with every Ledger API request.
The Daml ledger verifies:

- that the token was issued by one of its trusted token issuers
- that the token has not been tampered with
- that the token had not expired
- that the privileges described in the token authorize the request

.. image:: ./images/Authentication.svg
   :alt: A flowchart illustrating the process of authentication described in the two paragraphs immediately above.

How you attach tokens to requests depends on the tool or library you use to interact with the Ledger API.
See the tool's or library's documentation for more information. (E.g. relevant documentation for
the :ref:`Java bindings <ledger-api-java-bindings-authorization>`
and the :ref:`JSON API <json-api-access-tokens>`.)


.. _authorization-claims:

Access Tokens and Rights
************************

Access tokens contain information about the rights granted to the bearer of the token. These rights are specific to the API being accessed.

The Daml Ledger API uses the following rights to govern request authorization:

- ``public``: the right to retrieve publicly available information, such as the ledger identity
- ``participant_admin``: the right to administer the participant node
- ``idp_admin``: the right to administer the users and parties belonging the same identity provider configuration as the authenticated user
- ``canReadAs(p)``: the right to read information off the ledger (like the active contracts) visible to the party ``p``
- ``canActsAs(p)``: same as ``canReadAs(p)``, with the added right of issuing commands on behalf of the party ``p``

The following table summarizes the rights required to access each Ledger API endpoint:

+-------------------------------------+-------------------------------+--------------------------------------------------------+
| Ledger API service                  | Endpoint                      | Required right                                         |
+=====================================+===============================+========================================================+
| LedgerIdentityService               | GetLedgerIdentity             | public                                                 |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| ActiveContractsService              | GetActiveContracts            | for each requested party p: canReadAs(p)               |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| CommandCompletionService            | CompletionEnd                 | public                                                 |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
|                                     | CompletionStream              | for each requested party p: canReadAs(p)               |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| CommandSubmissionService            | Submit                        | for submitting party p: canActAs(p)                    |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| CommandService                      | All                           | for submitting party p: canActAs(p)                    |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| EventQueryService                   | All                           | for each requesting party p: canReadAs(p)              |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| Health                              | All                           | no access token required for health checking           |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| IdentityProviderConfigService       | All                           | participant_admin                                      |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| LedgerConfigurationService          | GetLedgerConfiguration        | public                                                 |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| MeteringReportService               | All                           | participant_admin                                      |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| PackageService                      | All                           | public                                                 |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| PackageManagementService            | All                           | participant_admin                                      |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| PartyManagementService              | All                           | participant_admin                                      |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
|                                     | All (except GetParticipantId, | idp_admin                                              |
|                                     | UpdatePartyIdentityProviderId)|                                                        |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| ParticipantPruningService           | All                           | participant_admin                                      |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| ServerReflection                    | All                           | no access token required for gRPC service reflection   |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| TimeService                         | GetTime                       | public                                                 |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
|                                     | SetTime                       | participant_admin                                      |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| TransactionService                  | LedgerEnd                     | public                                                 |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
|                                     | All (except LedgerEnd)        | for each requested party p: canReadAs(p)               |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| UserManagementService               | All                           | participant_admin                                      |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
|                                     | All (except                   | idp_admin                                              |
|                                     | UpdateUserIdentityProviderId) |                                                        |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
|                                     | GetUser                       | authenticated users can get their own user             |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
|                                     | ListUserRights                | authenticated users can list their own rights          |
+-------------------------------------+-------------------------------+--------------------------------------------------------+
| VersionService                      | All                           | public                                                 |
+-------------------------------------+-------------------------------+--------------------------------------------------------+


.. _access-token-formats:

Access Token Formats
********************

Applications should treat access tokens as opaque blobs.
However, as an application developer it can be helpful to understand the format of access tokens to debug problems.

All Daml ledgers represent access tokens as `JSON Web Tokens (JWTs) <https://datatracker.ietf.org/doc/html/rfc7519>`_,
and there are two formats of the JSON payload used by Daml ledgers.

.. note:: To generate access tokens for testing purposes, you can use the `jwt.io <https://jwt.io/>`__ web site.

.. _user-access-tokens:

User Access Tokens
==================

Daml ledgers that support participant :ref:`user management <user-management-service>` also accept user access tokens.
They are useful for scenarios where an application's rights change dynamically over the application's lifetime.

User access tokens do not encode rights directly like the custom Daml claims tokens explained in the following sections.
Instead, user access tokens encode the participant user on whose behalf the request is issued.

When handling such requests, participant nodes look up the participant user's current rights
before checking request authorization per the  :ref:`table above <authorization-claims>`.
Thus the rights granted to an application can be changed dynamically using
the participant user management service *without* issuing new access tokens,
as would be required for the custom Daml claims tokens.

User access tokens are `JWTs <https://datatracker.ietf.org/doc/html/rfc7519>`_ that follow the
`OAuth 2.0 standard <https://datatracker.ietf.org/doc/html/rfc6749>`_. There are two
different JSON encodings: An audience-based token format that relies
on the audience field to specify that it is designated for a specific
Daml participant and a scope-based audience token format which relies on the
scope field to designate the purpose. Both formats can be used interchangeably but
if possible, use of the audience-based token format is recommend as it
is compatible with a wider range of IAMs, e.g., Kubernetes does not
support setting the scope field and makes the participant id mandatory
which prevents misuse of a token on a different participant.

Audience-Based Tokens
---------------------

.. code-block:: json

   {
      "aud": "https://daml.com/jwt/aud/participant/someParticipantId",
      "sub": "someUserId",
      "iss": "someIdpId",
      "exp": 1300819380
   }

To interpret the above notation:

- ``aud`` is a required field which restricts the token to participant nodes with the given ID (e.g. ``someParticipantId``)
- ``sub`` is a required field which specifies the participant user's ID
- ``iss`` is a field which specifies the identity provider id
- ``exp`` is an optional field which specifies the JWT expiration date (in seconds since EPOCH)

Scope-Based Tokens
------------------

.. code-block:: json

   {
      "aud": "someParticipantId",
      "sub": "someUserId",
      "exp": 1300819380,
      "iss": "someIdpId",
      "scope": "daml_ledger_api"
   }

To interpret the above notation:

- ``aud`` is an optional field which restricts the token to participant nodes with the given ID
- ``sub`` is a required field which specifies the participant user's ID
- ``iss`` is a field which specifies the identity provider id
- ``exp`` is an optional field which specifies the JWT expiration date (in seconds since EPOCH)
- ``scope`` is a space-separated list of `OAuth 2.0 scopes <https://datatracker.ietf.org/doc/html/rfc6749#section-3.3>`_
  that must contain the ``"daml_ledger_api"`` scope

Requirements for User IDs
-------------------------

User IDs must be non-empty strings of at most 128 characters that are either alphanumeric ASCII characters or one of the symbols "@^$.!`-#+'~_|:".

Identity providers
------------------

An identity provider configuration can be thought of as a set of participant users which:
 - Have a defined way to verify their access tokens
 - Can be administered in isolation from the rest of the users on the same participant node
 - Have an identity provider id unique per participant node
 - Have a related set of parties that share the same identity provider id

A participant node always has a statically configured default identity provider configuration whose id is the empty string ``""``.
Additionally, you can configure a small number of non-default identity providers using ``IdentityProviderConfigService``
by supplying a non-empty identity provider id and a `JWK Set <https://datatracker.ietf.org/doc/html/rfc7517>`_
URL which the participant node will use to retrieve the cryptographic data needed to verify the access tokens.

When authenticating as a user from a non-default identity provider configuration, your access tokens must
contain the ``iss`` field whose value matches the identity provider id.
In case of the default identity provider configuration, the ``iss`` field can be empty or omitted from the access tokens.

Privileged Tokens
=================

A privileged token allows the caller to gain either ``wildcard-`` or ``participant-admin-`` level access. A ``wildcard-``level access combines
the capabilities of the ``participant-admin`` with the right to ``act-as`` and ``read-as`` any party. Privileged tokens look like and are
parsed as any other token. They can follow both the audience-based and the scope-based format. Unlike with user tokens, the entry specified
in the ``sub`` field is not validated or checked and need not represent a user existing in the participant's database. 

Use of privileged tokens is inactive by default and must be explicitly enabled in the Canton configuration. Unlike normal
user tokens, privileged tokens can also be configured for the Admin APIs exposed by the participant, the sequencer, and
the mediator nodes.

Encoding and Signature
======================

Access tokens conforming to the JWT specification are embedded in a larger JSON structure with a separate
header and payload.

.. code-block:: json

   {
      "alg": "RS256",
      "typ": "JWT"
   }
   {
      "aud": "https://daml.com/jwt/aud/participant/someParticipantId",
      "sub": "someUserId",
      "iss": "someIdpId",
      "exp": 1300819380
   }

Together they are then base64 encoded, forming the final token's stem. Subsequently, the stem is
signed using the cryptographic algorithm identified in the header. The signature itself is also
base64-encoded and appended to the stem. The resulting character string takes a shape similar to

.. code-block:: bash

   eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJodHRwczovL2RhbWwuY29tL2p3dC9hdWQvcGFydGljaXBhbnQvc29tZVBhcnRpY2lwYW50SWQiLCJzdWIiOiJzb21lVXNlcklkIiwiaXNzIjoic29tZUlkcElkIiwiZXhwIjoxMzAwODE5MzgwfQ.DLVPehRLt8WiddI6mwUU1lqIgRbysLK34mgkuzSDQTThCXlEY_S57SHKEQHw-Pai0Y0OeGP7wNsT6uq51vBVbRNfxOLwy5owQRm3LEeTbSXMjnnPVrtRrhelVQCsH2AcV4J4bbrAe6YfKGYFBXZOfeRL3Gy7KIplcfxDZekHdPD8lhwK8AkvAR4IaOX72Q5jhjB2yOY9IwpVxx-pN0vWCqmxTbQqnIpSGo185Y0f38nKZeofGT5jcJZaSv7z4Ks15gs9gm1pHorEL6TZLCbX7T064hQeTBFea-kxQlUkcfcgmUOMAmA05_4a8fdFz2uHq5km7ylp6pUITogN5MJ-_CVFEwOD0GveOgiUJBBMHDBjq_V_DfRE4nZ04tFQ0DDthWpMd0F59JFIhmjZSZT9DWppj6G7VBWpu9aIFPefyX--2U_aO0Smt_dBBV5A6pvbIgX6ITF2tjEvvOCLHtLKmNTlP8cclna70DCsDIrojNVDMFpLXYLvsP6DhQWkGaRb-nz0hLjQE_PtuQzSexrZG5d8tHFS351E2-aUVTKoJuEGHH3n1it-d9yHdt4fAynIbhWUVAervxc-oXyrA3-uafrxbIiQCpnw0kQ8K-HwJpkfz_Yqf-luI1FaRiPT9F-cYzwvceNf2_2hhmiuGiYp3rVIPwkFAuBc1vgpPiWSNLc

Note that access token generation in the correct format is typically delegated to the identity provider
systems. Client application developers are unlikely to need to deal with it directly.
