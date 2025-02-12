.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-cctp-85211:

DA.CCTP
=======

Functions for working with CCTP\.

Data Types
----------

.. _type-da-cctp-bytes32hex-93758:

**type** `Bytes32Hex <type-da-cctp-bytes32hex-93758_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-cctp-byteshex-82533:

**type** `BytesHex <type-da-cctp-byteshex-82533_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-cctp-publickeyhex-12002:

**type** `PublicKeyHex <type-da-cctp-publickeyhex-12002_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

  A DER formatted public key to be used for ECDSA signature verification

.. _type-da-cctp-signaturehex-17076:

**type** `SignatureHex <type-da-cctp-signaturehex-17076_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

  A DER formatted SECP256K1 signature

.. _type-da-cctp-uint256hex-38529:

**type** `UInt256Hex <type-da-cctp-uint256hex-38529_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-cctp-uint32hex-2410:

**type** `UInt32Hex <type-da-cctp-uint32hex-2410_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

.. _type-da-cctp-uint64hex-15029:

**type** `UInt64Hex <type-da-cctp-uint64hex-15029_>`_
  \= :ref:`Text <type-ghc-types-text-51952>`

Functions
---------

.. _function-da-cctp-keccak256-24237:

`keccak256 <function-da-cctp-keccak256-24237_>`_
  \: `BytesHex <type-da-cctp-byteshex-82533_>`_ \-\> `Bytes32Hex <type-da-cctp-bytes32hex-93758_>`_

  Computes the KECCAK256 hash of the UTF8 bytes of the ``Text``, and returns it in its hex\-encoded
  form\. The hex encoding uses lowercase letters\.

.. _function-da-cctp-secp256k1-88912:

`secp256k1 <function-da-cctp-secp256k1-88912_>`_
  \: `SignatureHex <type-da-cctp-signaturehex-17076_>`_ \-\> `BytesHex <type-da-cctp-byteshex-82533_>`_ \-\> `PublicKeyHex <type-da-cctp-publickeyhex-12002_>`_ \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Validate the SECP256K1 signature given a hex encoded message and a hex encoded DER formatted public key\.
