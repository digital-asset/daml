// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  HashAlgorithm,
  PbkdfScheme,
  SigningKeyScheme,
  SymmetricKeyScheme,
}

final case class CryptoProviderScheme[S](default: S, supported: NonEmpty[Set[S]]) {
  require(supported.contains(default))
}

/** Configures the optional default and allowed schemes of kind S.
  *
  * @param default The optional scheme to use. If none is specified, use the provider's default scheme of kind S.
  * @param allowed The optional allowed schemes to use. If none is specified, all the provider's supported schemes of kind S are allowed.
  */
final case class CryptoSchemeConfig[S](
    default: Option[S] = None,
    allowed: Option[NonEmpty[Set[S]]] = None,
)

/** Cryptography configuration. */
trait CryptoConfig {

  /** the crypto provider implementation to use */
  def provider: CryptoProvider

  /** the signing key scheme configuration */
  def signing: CryptoSchemeConfig[SigningKeyScheme]

  /** the encryption algorithm configuration */
  def encryptionAlgorithms: CryptoSchemeConfig[EncryptionAlgorithmSpec]

  /** the encryption key configuration */
  def encryptionKeys: CryptoSchemeConfig[EncryptionKeySpec]

  /** the symmetric key scheme configuration */
  def symmetric: CryptoSchemeConfig[SymmetricKeyScheme]

  /** the hash algorithm configuration */
  def hash: CryptoSchemeConfig[HashAlgorithm]

  /** the password-based key derivation function configuration */
  def pbkdf: CryptoSchemeConfig[PbkdfScheme]
}

final case class CommunityCryptoConfig(
    provider: CommunityCryptoProvider = CommunityCryptoProvider.Jce,
    signing: CryptoSchemeConfig[SigningKeyScheme] = CryptoSchemeConfig(),
    encryptionAlgorithms: CryptoSchemeConfig[EncryptionAlgorithmSpec] = CryptoSchemeConfig(),
    encryptionKeys: CryptoSchemeConfig[EncryptionKeySpec] = CryptoSchemeConfig(),
    symmetric: CryptoSchemeConfig[SymmetricKeyScheme] = CryptoSchemeConfig(),
    hash: CryptoSchemeConfig[HashAlgorithm] = CryptoSchemeConfig(),
    pbkdf: CryptoSchemeConfig[PbkdfScheme] = CryptoSchemeConfig(),
) extends CryptoConfig
