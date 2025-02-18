.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-crypto-81150:

DA.Crypto
=========

Functions for working with Crypto builtins\.
For example, as used to implement CCTP functionality\.

Data Types
----------

.. _type-da-crypto-bytes32hex-15707:

**type** `Bytes32Hex <type-da-crypto-bytes32hex-15707_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-crypto-byteshex-78696:

**type** `BytesHex <type-da-crypto-byteshex-78696_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-crypto-publickeyhex-94719:

**type** `PublicKeyHex <type-da-crypto-publickeyhex-94719_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

  A DER formatted public key to be used for ECDSA signature verification

.. _type-da-crypto-signaturehex-55409:

**type** `SignatureHex <type-da-crypto-signaturehex-55409_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

  A DER formatted SECP256K1 signature

.. _type-da-crypto-uint256hex-28900:

**type** `UInt256Hex <type-da-crypto-uint256hex-28900_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-crypto-uint32hex-24241:

**type** `UInt32Hex <type-da-crypto-uint32hex-24241_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-crypto-uint64hex-39034:

**type** `UInt64Hex <type-da-crypto-uint64hex-39034_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

Functions
---------

.. _function-da-crypto-keccak256-87938:

`keccak256 <function-da-crypto-keccak256-87938_>`_
  \: `BytesHex <type-da-crypto-byteshex-78696_>`_ \-\> `Bytes32Hex <type-da-crypto-bytes32hex-15707_>`_

  Computes the KECCAK256 hash of the UTF8 bytes of the ``Text``, and returns it in its hex\-encoded
  form\. The hex encoding uses lowercase letters\.

.. _function-da-crypto-secp256k1-14763:

`secp256k1 <function-da-crypto-secp256k1-14763_>`_
  \: `SignatureHex <type-da-crypto-signaturehex-55409_>`_ \-\> `BytesHex <type-da-crypto-byteshex-78696_>`_ \-\> `PublicKeyHex <type-da-crypto-publickeyhex-94719_>`_ \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Validate the SECP256K1 signature given a hex encoded message and a hex encoded DER formatted public key\.
