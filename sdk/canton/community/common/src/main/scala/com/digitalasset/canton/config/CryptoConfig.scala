// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  EncryptionKeyScheme,
  HashAlgorithm,
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

  /** the encryption key scheme configuration */
  def encryption: CryptoSchemeConfig[EncryptionKeyScheme]

  /** the symmetric key scheme configuration */
  def symmetric: CryptoSchemeConfig[SymmetricKeyScheme]

  /** the hash algorithm configuration */
  def hash: CryptoSchemeConfig[HashAlgorithm]
}

final case class CommunityCryptoConfig(
    provider: CommunityCryptoProvider =
      CommunityCryptoProvider.Tink, // TODO(i12244): Migrate to JCE.
    signing: CryptoSchemeConfig[SigningKeyScheme] = CryptoSchemeConfig(),
    encryption: CryptoSchemeConfig[EncryptionKeyScheme] = CryptoSchemeConfig(),
    symmetric: CryptoSchemeConfig[SymmetricKeyScheme] = CryptoSchemeConfig(),
    hash: CryptoSchemeConfig[HashAlgorithm] = CryptoSchemeConfig(),
) extends CryptoConfig
