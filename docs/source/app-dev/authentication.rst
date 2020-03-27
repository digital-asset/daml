.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _authentication:

Authentication
##############

When developing DAML applications using SDK tools,
your local setup will most likely not use any authentication -
by default, any valid ledger API request will be accepted by the sandbox.

To run your application against a :doc:`deployed ledger </deploy/index>`, you will need to add authentication.

Introduction
************

The :doc:`Ledger API </app-dev/ledger-api>` is used to request changes to the ledger (e.g., "*Alice
wants to exercise choice X on contract Y*), or to read data from the ledger (e.g., "*Alice wants to
see all active contracts*").

What requests are valid is defined by :ref:`integrity <da-model-integrity>` and :ref:`privacy <da-model-privacy>` parts the :ref:`DAML Ledger Model <da-ledgers>`.
This model is defined in terms of :ref:`DAML parties <glossary-party>`,
and does not require any cryptographic information to be sent along with requests.

In particular, this model does not talk about authentication ("*Is the request claiming to come from Alice really sent by Alice?*")
and authorization ("*Is Alice authorized to add a new DAML package to the ledger?*").

In practice, DAML ledgers will therefore need to add authentication to the ledger API.

.. note::
    Depending on the ledger topology, a DAML ledger may consist of multiple participant nodes,
    each having its own ledger API server.
    Each participant node typically hosts different DAML parties,
    and only sees data visible to the parties hosted on that node (as defined by the DAML privacy model).

    For more details on DAML ledger topologies, refer to the :ref:`DAML Ledger Topologies <daml-ledger-topologies>` documentation.

Adding authentication
=====================

How authentication is set up on a particular ledger is defined by the ledger operator.
However, most authentication setups share the following pattern:

First, the DAML application contacts a token issuer to get an access token.
The token issuer verifies the identity of the requesting user
(e.g., by checking the username/password credentials sent with the request),
looks up the privileges of the user,
and generates a signed access token describing those privileges.

Then, the DAML application sends the access token along with each ledger API request.
The DAML ledger verifies the signature of the token (to make sure it has not been tampered with),
and then checks that the privileges described in the token authorize the given ledger API request.

.. image:: ./images/Authentication.svg

Glossary:

- ``Authentication`` is the process of confirming an identity.
- ``Authorization`` is the process of checking permissions to access a resource.
- A ``token`` (or ``access token``) is a tamper-proof piece of data that contains security information, such as the user identity or its privileges.
- A ``token issuer`` is a service that generates tokens. Also known as "authentication server" or "Identity and Access Management (IAM) system".

.. _authentication-claims:

Access tokens and claims
************************

Access tokens contain information about the capabilities held by the bearer of the token. This information is represented by a *claim* to a given capability.

The claims can express the following capabilities:

- ``public``: ability to retrieve publicly available information, such as the ledger identity
- ``admin``: ability to interact with admin-level services, such as package uploading and user allocation
- ``canReadAs(p)``: ability to read information off the ledger (like the active contracts) visible to the party ``p``
- ``canActsAs(p)``: same as ``canReadAs(p)``, with the added ability of issuing commands on behalf of the party ``p``

The following table summarizes what kind of claim is required to access each Ledger API endpoint:

+-------------------------------------+----------------------------+------------------------------------------+
| Ledger API service                  | Endpoint                   | Required claim                           |
+=====================================+============================+==========================================+
| LedgerIdentityService               | GetLedgerIdentity          | public                                   |
+-------------------------------------+----------------------------+------------------------------------------+
| ActiveContractsService              | GetActiveContracts         | for each requested party p: canReadAs(p) |
+-------------------------------------+----------------------------+------------------------------------------+
| CommandSubmissionService            | Submit                     | for submitting party p: canActAs(p)      |
|                                     +----------------------------+------------------------------------------+
|                                     | CompletionEnd              | public                                   |
|                                     +----------------------------+------------------------------------------+
|                                     | CompletionStream           | for each requested party p: canReadAs(p) |
+-------------------------------------+----------------------------+------------------------------------------+
| CommandService                      | All                        | for submitting party p: canActAs(p)      |
+-------------------------------------+----------------------------+------------------------------------------+
| LedgerConfigurationService          | GetLedgerConfiguration     | public                                   |
+-------------------------------------+----------------------------+------------------------------------------+
| PackageService                      | All                        | public                                   |
+-------------------------------------+----------------------------+------------------------------------------+
| PackageManagementService            | All                        | admin                                    |
+-------------------------------------+----------------------------+------------------------------------------+
| PartyManagementService              | All                        | admin                                    |
+-------------------------------------+----------------------------+------------------------------------------+
| ResetService                        | All                        | admin                                    |
+-------------------------------------+----------------------------+------------------------------------------+
| TimeService                         | GetTime                    | public                                   |
|                                     +----------------------------+------------------------------------------+
|                                     | SetTime                    | admin                                    |
+-------------------------------------+----------------------------+------------------------------------------+
| TransactionService                  | LedgerEnd                  | public                                   |
|                                     +----------------------------+------------------------------------------+
|                                     | All (except LedgerEnd)     | for each requested party p: canReadAs(p) |
+-------------------------------------+----------------------------+------------------------------------------+

Access tokens may be represented differently based on the ledger implementation.

To learn how these claims are represented in the Sandbox,
read the :ref:`sandbox <sandbox-authentication>` documentation.

Getting access tokens
*********************

To learn how to receive access tokens for a deployed ledger, contact your ledger operator.
This may be a manual exchange over a secure channel,
or your application may have to request tokens at runtime using an API such as `OAuth <https://oauth.net/2/>`__.

To learn how to generate access tokens for the Sandbox,
read the :ref:`sandbox <sandbox-authentication>` documentation.

Using access tokens
*******************

To learn how to use access tokens in the Scala bindings, read the :ref:`Scala bindings authentication<scala-bindings-authentication>` documentation.
