// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{CryptoConfig, CryptoProviderScheme, CryptoSchemeConfig}
import com.digitalasset.canton.crypto.kms.Kms
import com.digitalasset.canton.util.EitherUtil
import com.google.common.annotations.VisibleForTesting

final case class CryptoSchemes private (
    signingSchemes: SigningCryptoSchemes,
    encryptionSchemes: EncryptionCryptoSchemes,
    symmetricKeySchemes: CryptoScheme[SymmetricKeyScheme],
    hashAlgorithms: CryptoScheme[HashAlgorithm],
    pbkdfSchemes: Option[CryptoScheme[PbkdfScheme]],
)

object CryptoSchemes {

  /** Check that all allowed schemes are actually supported by the KMS (i.e., driver or equivalent).
    *
    * The default scheme MUST be supported by the KMS. If an allowed scheme is not supported by the
    * KMS, the scheme will not be actively supported but only used, for example, to verify a
    * signature or perform asymmetric encryption. The driver's private cryptography does not need to
    * support them.
    */
  private def selectKmsScheme[S](
      cryptoScheme: CryptoScheme[S],
      kmsSupported: Set[S],
      description: String,
  ): Either[String, NonEmpty[Set[S]]] =
    for {
      _ <- EitherUtil.condUnit(
        kmsSupported.contains(cryptoScheme.default),
        s"The configured default scheme ${cryptoScheme.default} not supported by the KMS: $kmsSupported",
      )
      supported = cryptoScheme.allowed.intersect(kmsSupported)
      supportedNE <- NonEmpty
        .from(supported)
        .toRight(
          s"None of the allowed $description ${cryptoScheme.allowed.mkString(", ")} are supported by" +
            s"the KMS: $kmsSupported"
        )
    } yield supportedNE

  def selectKmsSchemes(
      cryptoSchemes: CryptoSchemes,
      kms: Kms.SupportedSchemes,
  ): Either[String, CryptoSchemes] =
    for {
      // use only schemes allowed by configuration and supported by the KMS
      selectedSigningKeySpecs <-
        selectKmsScheme(
          cryptoSchemes.signingSchemes.keySpecs,
          kms.supportedSigningKeySpecs,
          "signing key specs",
        )

      selectedSigningAlgoSpecs <-
        selectKmsScheme(
          cryptoSchemes.signingSchemes.algorithmSpecs,
          kms.supportedSigningAlgoSpecs,
          "signing algorithm specs",
        )

      signingSchemes = SigningCryptoSchemes(
        keySpecs =
          CryptoScheme(cryptoSchemes.signingSchemes.keySpecs.default, selectedSigningKeySpecs),
        algorithmSpecs = CryptoScheme(
          cryptoSchemes.signingSchemes.algorithmSpecs.default,
          selectedSigningAlgoSpecs,
        ),
      )

      selectedEncryptionKeySpecs <-
        selectKmsScheme(
          cryptoSchemes.encryptionSchemes.keySpecs,
          kms.supportedEncryptionKeySpecs,
          "encryption key specs",
        )

      selectedEncryptionAlgoSpecs <-
        selectKmsScheme(
          cryptoSchemes.encryptionSchemes.algorithmSpecs,
          kms.supportedEncryptionAlgoSpecs,
          "encryption algorithm specs",
        )

      encryptionSchemes = EncryptionCryptoSchemes(
        keySpecs = CryptoScheme(
          cryptoSchemes.encryptionSchemes.keySpecs.default,
          selectedEncryptionKeySpecs,
        ),
        algorithmSpecs = CryptoScheme(
          cryptoSchemes.encryptionSchemes.algorithmSpecs.default,
          selectedEncryptionAlgoSpecs,
        ),
      )
    } yield cryptoSchemes.copy(
      signingSchemes = signingSchemes,
      encryptionSchemes = encryptionSchemes,
    )

  @VisibleForTesting
  def tryFromConfig(config: CryptoConfig): CryptoSchemes =
    fromConfig(config)
      .getOrElse(
        throw new RuntimeException(
          "Could not validate the selected crypto schemes from the configuration file."
        )
      )

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
      SigningCryptoSchemes(
        signingKeySpecs,
        signingAlgoSpecs,
      ),
      EncryptionCryptoSchemes(
        encryptionKeySpecs,
        encryptionAlgoSpecs,
      ),
      symmetricKeySchemes,
      hashAlgorithms,
      pbkdfSchemesO,
    )
}

/** The default and supported key and algorithm signing specifications. */
final case class SigningCryptoSchemes(
    keySpecs: CryptoScheme[SigningKeySpec],
    algorithmSpecs: CryptoScheme[SigningAlgorithmSpec],
)

/** The default and supported key and algorithm encryption specifications. */
final case class EncryptionCryptoSchemes(
    keySpecs: CryptoScheme[EncryptionKeySpec],
    algorithmSpecs: CryptoScheme[EncryptionAlgorithmSpec],
)

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
