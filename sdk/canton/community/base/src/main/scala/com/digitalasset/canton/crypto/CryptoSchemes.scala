// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{CryptoConfig, CryptoProviderScheme, CryptoSchemeConfig}

// TODO(#18934): Ensure required/allowed schemes are enforced by private/pure crypto classes
final case class CryptoSchemes(
    signingKeySpecs: CryptoScheme[SigningKeySpec],
    signingAlgoSpecs: CryptoScheme[SigningAlgorithmSpec],
    encryptionKeySpecs: CryptoScheme[EncryptionKeySpec],
    encryptionAlgoSpecs: CryptoScheme[EncryptionAlgorithmSpec],
    symmetricKeySchemes: CryptoScheme[SymmetricKeyScheme],
    hashAlgorithms: CryptoScheme[HashAlgorithm],
    pbkdfSchemes: Option[CryptoScheme[PbkdfScheme]],
)

object CryptoSchemes {
  def fromConfig(config: CryptoConfig): Either[String, CryptoSchemes] =
    for {
      signingKeySpecs <- CryptoScheme.create(config.signing.keys, config.provider.signingKeys)
      signingAlgoSpecs <- CryptoScheme.create(
        config.signing.algorithms,
        config.provider.signingAlgorithms,
      )

      encryptionKeySpecs <- CryptoScheme.create(
        config.encryption.keys,
        config.provider.encryptionKeys,
      )
      encryptionAlgoSpecs <- CryptoScheme.create(
        config.encryption.algorithms,
        config.provider.encryptionAlgorithms,
      )

      symmetricKeySchemes <- CryptoScheme.create(config.symmetric, config.provider.symmetric)
      hashAlgorithms <- CryptoScheme.create(config.hash, config.provider.hash)
      pbkdfSchemesO <- config.provider.pbkdf.traverse(CryptoScheme.create(config.pbkdf, _))
    } yield CryptoSchemes(
      signingKeySpecs,
      signingAlgoSpecs,
      encryptionKeySpecs,
      encryptionAlgoSpecs,
      symmetricKeySchemes,
      hashAlgorithms,
      pbkdfSchemesO,
    )
}

final case class CryptoScheme[S](default: S, allowed: NonEmpty[Set[S]])

object CryptoScheme {

  /** Creates a [[CryptoScheme]] based on the configuration and the provider's capabilities.
    *
    * Default scheme is either explicitly configured or the provider's default scheme. Allowed
    * schemes are either explicitly configured and must be supported by the provider, or all
    * supported schemes by the provider.
    */
  def create[S](
      configured: CryptoSchemeConfig[S],
      provider: CryptoProviderScheme[S],
  ): Either[String, CryptoScheme[S]] = {
    val supported = provider.supported

    // If no allowed schemes are configured, all supported schemes are allowed.
    val allowed = configured.allowed.getOrElse(supported)

    // If no scheme is configured, use the default scheme of the provider
    val default = configured.default.getOrElse(provider.default)

    // The allowed schemes that are not in the supported set
    val unsupported = allowed.diff(supported)

    for {
      _ <- Either.cond(unsupported.isEmpty, (), s"Allowed schemes $unsupported are not supported")
      _ <- Either.cond(allowed.contains(default), (), s"Scheme $default is not allowed: $allowed")
    } yield CryptoScheme(default, allowed)
  }

}
