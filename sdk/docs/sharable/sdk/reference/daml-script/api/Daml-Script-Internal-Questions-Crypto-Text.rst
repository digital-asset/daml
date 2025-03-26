.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-crypto-text-2436:

Daml.Script.Internal.Questions.Crypto.Text
==========================================

Daml Script support for working with Crypto builtins\.

Data Types
----------

.. _type-daml-script-internal-questions-crypto-text-privatekeyhex-82732:

**type** `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_
  \= `BytesHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  A DER formatted private key to be used for ECDSA message signing

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"privateKey\" `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_ `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"privateKey\" `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_ `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_

.. _type-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-99276:

**data** `Secp256k1GenerateKeyPair <type-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-99276_>`_

  .. _constr-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-46259:

  `Secp256k1GenerateKeyPair <constr-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-46259_>`_

    (no fields)

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `Secp256k1GenerateKeyPair <type-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-99276_>`_ `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_

.. _type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395:

**data** `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_

  .. _constr-daml-script-internal-questions-crypto-text-secp256k1keypair-60460:

  `Secp256k1KeyPair <constr-daml-script-internal-questions-crypto-text-secp256k1keypair-60460_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - privateKey
         - `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_
         -
       * - publicKey
         - `PublicKeyHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-publickeyhex-51359>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `Secp256k1GenerateKeyPair <type-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-99276_>`_ `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"privateKey\" `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_ `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"publicKey\" `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_ `PublicKeyHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-publickeyhex-51359>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"privateKey\" `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_ `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"publicKey\" `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_ `PublicKeyHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-publickeyhex-51359>`_

.. _type-daml-script-internal-questions-crypto-text-secp256k1sign-62642:

**data** `Secp256k1Sign <type-daml-script-internal-questions-crypto-text-secp256k1sign-62642_>`_

  .. _constr-daml-script-internal-questions-crypto-text-secp256k1sign-47199:

  `Secp256k1Sign <constr-daml-script-internal-questions-crypto-text-secp256k1sign-47199_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - privateKey
         - `PublicKeyHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-publickeyhex-51359>`_
         -
       * - message
         - `BytesHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `Secp256k1Sign <type-daml-script-internal-questions-crypto-text-secp256k1sign-62642_>`_ `BytesHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"message\" `Secp256k1Sign <type-daml-script-internal-questions-crypto-text-secp256k1sign-62642_>`_ `BytesHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"privateKey\" `Secp256k1Sign <type-daml-script-internal-questions-crypto-text-secp256k1sign-62642_>`_ `PublicKeyHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-publickeyhex-51359>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"message\" `Secp256k1Sign <type-daml-script-internal-questions-crypto-text-secp256k1sign-62642_>`_ `BytesHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"privateKey\" `Secp256k1Sign <type-daml-script-internal-questions-crypto-text-secp256k1sign-62642_>`_ `PublicKeyHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-publickeyhex-51359>`_

Functions
---------

.. _function-daml-script-internal-questions-crypto-text-secp256k1sign-72886:

`secp256k1sign <function-daml-script-internal-questions-crypto-text-secp256k1sign-72886_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_ \-\> `BytesHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `BytesHex <https://docs.daml.com/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  Using a DER formatted private key (encoded as a hex string) use Secp256k1 to sign a hex encoded string message\.

  Note that this implementation uses a random source with a fixed PRNG and seed, ensuring it behaves deterministically during testing\.

  For example, CCTP attestation services may be mocked in daml\-script code\.

.. _function-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-90200:

`secp256k1generatekeypair <function-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-90200_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_

  Generate DER formatted Secp256k1 public/private key pairs\.

