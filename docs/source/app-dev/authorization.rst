.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _authorization:

Authorization
#############

When developing Daml applications using SDK tools,
your local setup will most likely not perform any ledger API request authorization -
by default, any valid ledger API request will be accepted by the sandbox.

This is not the case for participant nodes of :doc:`deployed ledgers </deploy/index>`.
They check for every ledger API request whether the request contains an access token that is valid and sufficient to authorize the request.
You thus need to add support for authorization using access token to your application to run it against a deployed ledger.

Introduction
************

Your Daml application sends requests to the :doc:`Ledger API </app-dev/ledger-api>` exposed by a participant node to submit changes to the ledger
(e.g., "*exercise choice X on contract Y as party Alice*"), or to read data from the ledger
(e.g., "*read all active contracts visible to party Alice*").
Your application might send these requests via a middleware like the :doc:`JSON API </json-api/index>`.

Whether a participant node *can* serve such a request depends on whether the participant node hosts the respective parties, and
whether the request is valid according to the :ref:`Daml Ledger Model <da-ledgers>`.
Whether a participant node *will* serve such a request to a Daml application depends on whether the application accompanied the
request with an access token that is valid and sufficient to authorize the request for this participant node.

Acquiring and using access tokens
*********************************

How an application should acquire access tokens depends on the participant node it talks to and is ultimately setup by the participant node operator.
However most setups share the following pattern:

First, the Daml application contacts a token issuer to get an access token.
The token issuer verifies the identity of the requesting application
(e.g., by checking the `OAuth 2.0 client credentials <https://datatracker.ietf.org/doc/html/rfc6749#section-2.2>`_ sent with the request),
looks up the privileges of the application,
and generates a signed access token describing those privileges.

Then, the Daml application sends the access token along with every ledger API request.
The Daml ledger verifies the signature of the token (to make sure it has not been tampered with and was issued by one of its trusted token issuers),
and then checks that the token has not yet expired and that the privileges described in the token authorize the given ledger API request.

.. image:: ./images/Authentication.svg


.. _authorization-claims:

Access tokens and rights
************************

Access tokens contain information about the rights granted to the bearer of the token. These rights are specific to the API being accessed.

The Daml ledger API uses the following rights to govern request authorization:

- ``public``: the right to retrieve publicly available information, such as the ledger identity
- ``participant_admin``: the right to adminstrate the participant node
- ``canReadAs(p)``: the right to read information off the ledger (like the active contracts) visible to the party ``p``
- ``canActsAs(p)``: same as ``canReadAs(p)``, with the added right of issuing commands on behalf of the party ``p``

The following table summarizes what kind of rights are required to access each Ledger API endpoint:

+-------------------------------------+----------------------------+------------------------------------------+
| Ledger API service                  | Endpoint                   | Required right                           |
+=====================================+============================+==========================================+
| LedgerIdentityService               | GetLedgerIdentity          | public                                   |
+-------------------------------------+----------------------------+------------------------------------------+
| ActiveContractsService              | GetActiveContracts         | for each requested party p: canReadAs(p) |
+-------------------------------------+----------------------------+------------------------------------------+
| CommandCompletionService            | CompletionEnd              | public                                   |
|                                     +----------------------------+------------------------------------------+
|                                     | CompletionStream           | for each requested party p: canReadAs(p) |
+-------------------------------------+----------------------------+------------------------------------------+
| CommandSubmissionService            | Submit                     | for submitting party p: canActAs(p)      |
+-------------------------------------+----------------------------+------------------------------------------+
| CommandService                      | All                        | for submitting party p: canActAs(p)      |
+-------------------------------------+----------------------------+------------------------------------------+
| LedgerConfigurationService          | GetLedgerConfiguration     | public                                   |
+-------------------------------------+----------------------------+------------------------------------------+
| MeteringReportService               | All                        | participant_admin                        |
+-------------------------------------+----------------------------+------------------------------------------+
| PackageService                      | All                        | public                                   |
+-------------------------------------+----------------------------+------------------------------------------+
| PackageManagementService            | All                        | participant_admin                        |
+-------------------------------------+----------------------------+------------------------------------------+
| PartyManagementService              | All                        | participant_admin                        |
+-------------------------------------+----------------------------+------------------------------------------+
| ParticipantPruningService           | All                        | participant_admin                        |
+-------------------------------------+----------------------------+------------------------------------------+
| ResetService                        | All                        | participant_admin                        |
+-------------------------------------+----------------------------+------------------------------------------+
| TimeService                         | GetTime                    | public                                   |
|                                     +----------------------------+------------------------------------------+
|                                     | SetTime                    | participant_admin                        |
+-------------------------------------+----------------------------+------------------------------------------+
| TransactionService                  | LedgerEnd                  | public                                   |
|                                     +----------------------------+------------------------------------------+
|                                     | All (except LedgerEnd)     | for each requested party p: canReadAs(p) |
+-------------------------------------+----------------------------+------------------------------------------+
| UserManagementService               | All                        | participant_admin                        |
+-------------------------------------+----------------------------+------------------------------------------+
| VersionService                      | All                        | public                                   |
+-------------------------------------+----------------------------+------------------------------------------+




Access token formats
********************

Applications should treat access tokens as opaque blobs.
However as an application developer it can be helpful to understand the format of access tokens to debug problems.

All Daml ledgers represent access tokens as `JSON Web Tokens (JWTs) <https://datatracker.ietf.org/doc/html/rfc7519>`_,
and there are two formats of their JSON payload in use by Daml ledgers.


Custom Daml claims access tokens
================================

This format represents the :ref:`rights <authorization-claims>` granted by the access token as custom claims in the JWT's payload, like so:


.. code-block:: json

   {
      "https://daml.com/ledger-api": {
        "ledgerId": null,
        "participantId": "123e4567-e89b-12d3-a456-426614174000",
        "applicationId": null,
        "admin": true,
        "actAs": ["Alice"],
        "readAs": ["Bob"]
      },
      "exp": 1300819380
   }

where all of the fields are optional, and if present,

- ``ledgerId``, ``participantId``, ``applicationId`` restrict the validity of the token to the given ledger, participant, or application
- ``exp`` is the standard JWT expiration date (in seconds since EPOCH)
- ``admin``, ``actAs`` and ``readAs`` encode the rights granted by this access token

The ``public`` right is implicitly granted to any bearing a valid JWT issued by a trusted issuer (even without being an admin or being able to act or read on behalf of any party).



User access tokens
==================


Participant user management
***************************




Getting access tokens
*********************

To learn how to receive access tokens for a deployed ledger, contact your ledger operator.
This may be a manual exchange over a secure channel,
or your application may have to request tokens at runtime using an API such as `OAuth <https://oauth.net/2/>`__.

To learn how to generate access tokens for the Sandbox,
read the :ref:`sandbox <sandbox-authorization>` documentation.


Access tokens may be represented differently based on the ledger implementation.

To learn how these claims are represented in the Sandbox,
read the :ref:`sandbox <sandbox-authorization>` documentation.
