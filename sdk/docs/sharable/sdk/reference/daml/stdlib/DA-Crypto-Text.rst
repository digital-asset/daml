.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-crypto-text-67266:

DA.Crypto.Text
==============

Functions for working with Crypto builtins\.
For example, as used to implement CCTP functionality\.

Typeclasses
-----------

.. _class-da-crypto-text-hastohex-92431:

**class** `HasToHex <class-da-crypto-text-hastohex-92431_>`_ a **where**

  .. _function-da-crypto-text-tohex-12193:

  `toHex <function-da-crypto-text-tohex-12193_>`_
    \: a \-\> `BytesHex <type-da-crypto-text-byteshex-47880_>`_

    Converts a typed data value into a hex encoded string\.

  **instance** `HasToHex <class-da-crypto-text-hastohex-92431_>`_ :ref:`Text <type-ghc-types-text-51952>`

.. _class-da-crypto-text-hasfromhex-84972:

**class** `HasFromHex <class-da-crypto-text-hasfromhex-84972_>`_ a **where**

  .. _function-da-crypto-text-fromhex-45182:

  `fromHex <function-da-crypto-text-fromhex-45182_>`_
    \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> a

    Converts a hex encoded string into a typed data value\.

  **instance** `HasFromHex <class-da-crypto-text-hasfromhex-84972_>`_ :ref:`Text <type-ghc-types-text-51952>`

Data Types
----------

.. _type-da-crypto-text-byteshex-47880:

**type** `BytesHex <type-da-crypto-text-byteshex-47880_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-crypto-text-publickeyhex-51359:

**type** `PublicKeyHex <type-da-crypto-text-publickeyhex-51359_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

  A DER formatted public key to be used for ECDSA signature verification

.. _type-da-crypto-text-signaturehex-12945:

**type** `SignatureHex <type-da-crypto-text-signaturehex-12945_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

  A DER formatted SECP256K1 signature

Functions
---------

.. _function-da-crypto-text-keccak256-57106:

`keccak256 <function-da-crypto-text-keccak256-57106_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Computes the KECCAK256 hash of the UTF8 bytes of the ``Text``, and returns it in its hex\-encoded
  form\. The hex encoding uses lowercase letters\.

.. _function-da-crypto-text-secp256k1-38075:

`secp256k1 <function-da-crypto-text-secp256k1-38075_>`_
  \: `SignatureHex <type-da-crypto-text-signaturehex-12945_>`_ \-\> `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> `PublicKeyHex <type-da-crypto-text-publickeyhex-51359_>`_ \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Validate the SECP256K1 signature given a hex encoded message and a hex encoded DER formatted public key\.
