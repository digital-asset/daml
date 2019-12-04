.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Authentication
##############

When developing DAML applications using SDK tools,
your local setup will most likely not use any authentication -
by default, any valid ledger API request will be accepted by the sandbox.

To run your application against a :doc:`deployed ledger </deploy/index>`, you will need to add authentication.

Introduction
************

The main way for a DAML application to interact with a DAML ledger is through the :doc:`gRPC ledger API </app-dev/grpc/index>`.

This API can be used to request changes to the ledger (e.g., "*Alice wants to exercise choice X on contract Y*),
or to read data from the ledger (e.g., "*Alice wants to see all active contracts*").

What requests are valid is defined by :ref:`integrity <da-model-integrity>` and :ref:`privacy <da-model-privacy>` parts the :ref:`DA Ledger Model <da-ledgers>`.
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
looks up the priviledges of the user,
and generates a signed access token describing those priviledges.

Then, the DAML application sends the access token along with each ledger API request.
The DAML ledger verifies the signature of the token (to make sure it has not been tampered with),
and then checks that the priviledges described in the token authorize the given ledger API request.

.. image:: ./images/Authentication.svg

Glossary:

- ``Authentication`` is the process of confirming an identity.
- ``Authorization`` is the process of checking permissions to access a resource.
- A ``token`` is a tamper-proof piece of data that contains security information, such as the user identity or its priviledges.
- A ``token issuer`` is a service that generates tokens. Also known as "authentication server" or "Identity and Access Management (IAM) system".

Getting access tokens
*********************

To learn how to receive access tokens for a deployed ledger, contact your ledger operator.
This may be a manual exchange over a secure channel,
or your application may have to request tokens at runtime using an API such as `OAuth <https://oauth.net/2/>`__.

To learn how to generate access tokens for a local sandbox,
read the :ref:`sandbox <sandbox-authentication>` documentation.


Using access tokens
*******************

To learn how to use access tokens in the Scala bindings, read the :ref:`Scala bindings authentication<scala-bindings-authentication>` documentation.
