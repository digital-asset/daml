.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Security and Privacy
####################

For an *HTTP JSON API* server, all data is maintained by the operator of the deployment.
It is the operator's responsibility to ensure that the data abides by the necessary
regulations and confidentiality expectations.

We recommend using the tools documented by PostgreSQL to protect data at
rest, and using a secure communication channel between the *HTTP JSON API* server and the PostgreSQL server.

 The *HTTP JSON API* server provides
TLS support to protect data in transit and over untrusted networks. To enable TLS you must specify both the private key for your server and the
certificate chain via the below config block that specifies the ``cert-chain-file``, ``private-key-file``. You can also set
a custom root CA certificate that will be used to validate client certificates via the ``trust-collection-file`` parameter:

.. code-block:: none

    ledger-api {
      address = "127.0.0.1"
      port = 6400
      tls {
        enabled = "true"
        // the certificate to be used by the server
        cert-chain-file = "cert-chain.crt"
        // private key of the server
        private-key-file = "pvt-key.pem"
        // trust collection, which means that all client certificates will be verified using the trusted
        // certificates in this store. if omitted, the JVM default trust store is used.
        trust-collection-file = "root-ca.crt"
      }
    }

Using the cli options (deprecated), you can specify tls options using``daml json-api --pem server.pem --crt server.crt``.
Custom root CA certificate can be set via ``--cacrt ca.crt``

For more details on secure Daml infrastructure setup please see this `reference implementation <https://github.com/digital-asset/ex-secure-daml-infra>`__

