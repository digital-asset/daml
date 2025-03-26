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

