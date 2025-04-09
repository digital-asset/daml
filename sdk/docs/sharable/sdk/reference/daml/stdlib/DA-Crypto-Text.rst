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

  **instance** `HasToHex <class-da-crypto-text-hastohex-92431_>`_ :ref:`Party <type-da-internal-lf-party-57932>`

  **instance** `HasToHex <class-da-crypto-text-hastohex-92431_>`_ :ref:`Int <type-ghc-types-int-37261>`

  **instance** `HasToHex <class-da-crypto-text-hastohex-92431_>`_ :ref:`Text <type-ghc-types-text-51952>`

.. _class-da-crypto-text-hasfromhex-84972:

**class** `HasFromHex <class-da-crypto-text-hasfromhex-84972_>`_ a **where**

  .. _function-da-crypto-text-fromhex-45182:

  `fromHex <function-da-crypto-text-fromhex-45182_>`_
    \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> a

    Converts a hex encoded string into a typed data value\.

  **instance** `HasFromHex <class-da-crypto-text-hasfromhex-84972_>`_ (:ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Party <type-da-internal-lf-party-57932>`)

  **instance** `HasFromHex <class-da-crypto-text-hasfromhex-84972_>`_ (:ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Int <type-ghc-types-int-37261>`)

  **instance** `HasFromHex <class-da-crypto-text-hasfromhex-84972_>`_ (:ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Text <type-ghc-types-text-51952>`)

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

.. _function-da-crypto-text-ishex-17968:

`isHex <function-da-crypto-text-ishex-17968_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isHex`` is ``True`` if ``t`` is not empty and consists only of
  hex or hexadecimal characters\.

.. _function-da-crypto-text-keccak256-57106:

`keccak256 <function-da-crypto-text-keccak256-57106_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Computes the KECCAK256 hash of the UTF8 bytes of the ``Text``, and returns it in its hex\-encoded
  form\. The hex encoding uses lowercase letters\.

.. _function-da-crypto-text-secp256k1-38075:

`secp256k1 <function-da-crypto-text-secp256k1-38075_>`_
  \: `SignatureHex <type-da-crypto-text-signaturehex-12945_>`_ \-\> `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> `PublicKeyHex <type-da-crypto-text-publickeyhex-51359_>`_ \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Validate the SECP256K1 signature given a hex encoded message and a hex encoded DER formatted public key\.

.. _function-da-crypto-text-numericviastringtohex-44461:

`numericViaStringToHex <function-da-crypto-text-numericviastringtohex-44461_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n \=\> :ref:`Numeric <type-ghc-types-numeric-891>` n \-\> `BytesHex <type-da-crypto-text-byteshex-47880_>`_

.. _function-da-crypto-text-numericviastringfromhex-60098:

`numericViaStringFromHex <function-da-crypto-text-numericviastringfromhex-60098_>`_
  \: :ref:`NumericScale <class-ghc-classes-numericscale-83720>` n \=\> `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` (:ref:`Numeric <type-ghc-types-numeric-891>` n)

.. _function-da-crypto-text-bytecount-29784:

`byteCount <function-da-crypto-text-bytecount-29784_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>`

  Number of bytes present in a byte encoded string\.

.. _function-da-crypto-text-minbytes32hex-29458:

`minBytes32Hex <function-da-crypto-text-minbytes32hex-29458_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Minimum Bytes32 hex value

.. _function-da-crypto-text-maxbytes32hex-56560:

`maxBytes32Hex <function-da-crypto-text-maxbytes32hex-56560_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Maximum Bytes32 hex value

.. _function-da-crypto-text-isbytes32hex-1801:

`isBytes32Hex <function-da-crypto-text-isbytes32hex-1801_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Validate that the byte encoded string is Bytes32Hex

.. _function-da-crypto-text-minuint32hex-58146:

`minUInt32Hex <function-da-crypto-text-minuint32hex-58146_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Minimum UInt32 hex value

.. _function-da-crypto-text-maxuint32hex-80016:

`maxUInt32Hex <function-da-crypto-text-maxuint32hex-80016_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Maximum UInt32 hex value

.. _function-da-crypto-text-isuint32hex-65583:

`isUInt32Hex <function-da-crypto-text-isuint32hex-65583_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Validate that the byte encoded string is UInt32Hex

.. _function-da-crypto-text-minuint64hex-67161:

`minUInt64Hex <function-da-crypto-text-minuint64hex-67161_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Minimum UInt64 hex value

.. _function-da-crypto-text-maxuint64hex-40555:

`maxUInt64Hex <function-da-crypto-text-maxuint64hex-40555_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Maximum UInt64 hex value

.. _function-da-crypto-text-isuint64hex-49912:

`isUInt64Hex <function-da-crypto-text-isuint64hex-49912_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Validate that the byte encoded string is UInt64Hex

.. _function-da-crypto-text-minuint256hex-23801:

`minUInt256Hex <function-da-crypto-text-minuint256hex-23801_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Minimum UInt256 hex value

.. _function-da-crypto-text-maxuint256hex-58651:

`maxUInt256Hex <function-da-crypto-text-maxuint256hex-58651_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Maximum UInt256 hex value

.. _function-da-crypto-text-isuint256hex-33362:

`isUInt256Hex <function-da-crypto-text-isuint256hex-33362_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Validate that the byte encoded string is UInt256Hex

.. _function-da-crypto-text-packhexbytes-55939:

`packHexBytes <function-da-crypto-text-packhexbytes-55939_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Pack a byte encoded string to a given byte count size\. If the byte string is shorter than the pad
  size, then prefix with 00 byte strings\. If the byte string is larger, then truncate the byte string\.

.. _function-da-crypto-text-slicehexbytes-22633:

`sliceHexBytes <function-da-crypto-text-slicehexbytes-22633_>`_
  \: `BytesHex <type-da-crypto-text-byteshex-47880_>`_ \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> :ref:`Either <type-da-types-either-56020>` :ref:`Text <type-ghc-types-text-51952>` `BytesHex <type-da-crypto-text-byteshex-47880_>`_

  Extract the byte string starting at startByte up to, but excluding, endByte\. Byte indexing starts at 1\.
