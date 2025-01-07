// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  CryptoKeyFormat,
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  HashAlgorithm,
  PbkdfScheme,
  RequiredEncryptionSpecs,
  RequiredSigningSpecs,
  SigningAlgorithmSpec,
  SigningKeySpec,
  SymmetricKeyScheme,
}

object SymbolicCryptoProvider {
  // The schemes are ignored by symbolic crypto

  val supportedSigningSpecs: RequiredSigningSpecs =
    RequiredSigningSpecs(
      NonEmpty.mk(Set, SigningAlgorithmSpec.Ed25519),
      NonEmpty.mk(Set, SigningKeySpec.EcCurve25519),
    )
  val supportedSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]] =
    NonEmpty.mk(Set, SymmetricKeyScheme.Aes128Gcm)
  val supportedEncryptionSpecs: RequiredEncryptionSpecs =
    RequiredEncryptionSpecs(
      NonEmpty.mk(Set, EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm),
      NonEmpty.mk(Set, EncryptionKeySpec.EcP256),
    )
  val supportedHashAlgorithms: NonEmpty[Set[HashAlgorithm]] = NonEmpty.mk(Set, HashAlgorithm.Sha256)
  val supportedCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]] =
    NonEmpty.mk(Set, CryptoKeyFormat.Symbolic)
  val supportedPbkdfSchemes: NonEmpty[Set[PbkdfScheme]] =
    NonEmpty.mk(Set, PbkdfScheme.Argon2idMode1)

}
