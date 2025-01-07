// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  HashAlgorithm,
  PbkdfScheme,
  SigningAlgorithmSpec,
  SigningKeySpec,
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

/** Stores the configuration of the signing scheme.
  *
  * @param algorithms the algorithm specifications
  * @param keys the key specifications
  */
final case class SigningSchemeConfig(
    algorithms: CryptoSchemeConfig[SigningAlgorithmSpec] = CryptoSchemeConfig(),
    keys: CryptoSchemeConfig[SigningKeySpec] = CryptoSchemeConfig(),
)

/** Stores the configuration of the encryption scheme.
  *
  * @param algorithms the algorithm specifications
  * @param keys the key specifications
  */
final case class EncryptionSchemeConfig(
    algorithms: CryptoSchemeConfig[EncryptionAlgorithmSpec] = CryptoSchemeConfig(),
    keys: CryptoSchemeConfig[EncryptionKeySpec] = CryptoSchemeConfig(),
)

/** Cryptography configuration. */
trait CryptoConfig {

  /** the crypto provider implementation to use */
  def provider: CryptoProvider

  /** the signing key scheme configuration */
  def signing: SigningSchemeConfig

  /** the encryption scheme configuration */
  def encryption: EncryptionSchemeConfig

  /** the symmetric key scheme configuration */
  def symmetric: CryptoSchemeConfig[SymmetricKeyScheme]

  /** the hash algorithm configuration */
  def hash: CryptoSchemeConfig[HashAlgorithm]

  /** the password-based key derivation function configuration */
  def pbkdf: CryptoSchemeConfig[PbkdfScheme]

  /** optional support for a KMS */
  def kms: Option[KmsConfig]
}

final case class CommunityCryptoConfig(
    provider: CryptoProvider = CryptoProvider.Jce,
    signing: SigningSchemeConfig = SigningSchemeConfig(),
    encryption: EncryptionSchemeConfig = EncryptionSchemeConfig(),
    symmetric: CryptoSchemeConfig[SymmetricKeyScheme] = CryptoSchemeConfig(),
    hash: CryptoSchemeConfig[HashAlgorithm] = CryptoSchemeConfig(),
    pbkdf: CryptoSchemeConfig[PbkdfScheme] = CryptoSchemeConfig(),
    kms: Option[CommunityKmsConfig] = None,
) extends CryptoConfig
