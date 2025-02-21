// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CryptoConfig
import com.digitalasset.canton.protocol.StaticSynchronizerParameters

object CryptoHandshakeValidator {

  private def validateScheme[S](
      required: NonEmpty[Set[S]],
      scheme: CryptoScheme[S],
  ): Either[String, Unit] = {
    val unsupported = required.diff(scheme.allowed)
    for {
      // Required but not allowed
      _ <- Either.cond(
        unsupported.isEmpty,
        (),
        s"Required schemes $unsupported are not supported/allowed (${scheme.allowed})",
      )

      // The default scheme must be a required scheme, otherwise another node may not allow and support our default scheme.
      _ <- Either.cond(
        required.contains(scheme.default),
        (),
        s"The default ${scheme.default} scheme is not a required scheme: $required",
      )
    } yield ()
  }

  private def validateFormats[F](
      requiredFormats: NonEmpty[Set[F]],
      supportedFormats: NonEmpty[Set[F]],
  ): Either[String, Unit] = {
    val unsupportedFormats = requiredFormats.diff(supportedFormats)

    Either.cond(
      unsupportedFormats.isEmpty,
      (),
      s"Required formats $unsupportedFormats are not supported ($supportedFormats)",
    )
  }

  /** Validates that the required crypto schemes are allowed and supported. The default scheme must
    * be one of the required schemes.
    *
    * The synchronizer defines for each signing, encryption, symmetric, and hashing a set of
    * required schemes. A connecting member must be configured to allow (and thus support) all
    * required schemes of the synchronizer.
    */
  def validate(
      parameters: StaticSynchronizerParameters,
      config: CryptoConfig,
  ): Either[String, Unit] =
    for {
      schemes <- CryptoSchemes.fromConfig(config)
      _ <- validateScheme(
        parameters.requiredSigningSpecs.algorithms,
        schemes.signingAlgoSpecs,
      )
      _ <- validateScheme(
        parameters.requiredSigningSpecs.keys,
        schemes.signingKeySpecs,
      )
      _ <- validateScheme(
        parameters.requiredEncryptionSpecs.algorithms,
        schemes.encryptionAlgoSpecs,
      )
      _ <- validateScheme(
        parameters.requiredEncryptionSpecs.keys,
        schemes.encryptionKeySpecs,
      )
      _ <- validateScheme(
        parameters.requiredSymmetricKeySchemes,
        schemes.symmetricKeySchemes,
      )
      _ <- validateScheme(
        parameters.requiredHashAlgorithms,
        schemes.hashAlgorithms,
      )
      _ <- validateFormats(
        parameters.requiredCryptoKeyFormats,
        config.provider.supportedCryptoKeyFormats,
      )
      _ <- validateFormats(
        parameters.requiredSignatureFormats,
        config.provider.supportedSignatureFormats,
      )
    } yield ()

}
