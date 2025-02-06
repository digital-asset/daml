// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{
  CommunityCryptoConfig,
  CryptoConfig,
  ProtocolConfig,
  SessionSigningKeysConfig,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.CryptoFactory.{
  selectAllowedEncryptionAlgorithmSpecs,
  selectAllowedEncryptionKeySpecs,
  selectAllowedHashAlgorithms,
  selectAllowedSigningAlgorithmSpecs,
  selectAllowedSigningKeySpecs,
  selectAllowedSymmetricKeySchemes,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.version.ProtocolVersion

/** Configuration of synchronizer parameters that all members connecting to a synchronizer must adhere to.
  *
  * To set these parameters, you need to be familiar with the Canton architecture.
  * See <a href="https://docs.daml.com/canton/architecture/overview.html">the Canton architecture overview</a>
  * for further information.
  *
  * @param requiredSigningAlgorithmSpecs         The optional required signing algorithm specifications that a member has to support. If none is specified, all the allowed specifications are required.
  * @param requiredSigningKeySpecs      The optional required signing key specifications that a member has to support. If none is specified, all the allowed specifications are required.
  * @param requiredEncryptionAlgorithmSpecs      The optional required encryption algorithm specifications that a member has to support. If none is specified, all the allowed specifications are required.
  * @param requiredEncryptionKeySpecs   The optional required encryption key specifications that a member has to support. If none is specified, all the allowed specifications are required.
  * @param requiredSymmetricKeySchemes  The optional required symmetric key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredHashAlgorithms       The optional required hash algorithms that a member has to support. If none is specified, all the allowed algorithms are required.
  * @param requiredCryptoKeyFormats     The optional required crypto key formats that a member has to support. If none is specified, all the supported algorithms are required.
  * @param dontWarnOnDeprecatedPV       If true, then this synchronizer will not emit a warning when configured to use a deprecated protocol version (such as 2.0.0).
  */
final case class SynchronizerParametersConfig(
    requiredSigningAlgorithmSpecs: Option[NonEmpty[Set[SigningAlgorithmSpec]]] = None,
    requiredSigningKeySpecs: Option[NonEmpty[Set[SigningKeySpec]]] = None,
    requiredEncryptionAlgorithmSpecs: Option[NonEmpty[Set[EncryptionAlgorithmSpec]]] = None,
    requiredEncryptionKeySpecs: Option[NonEmpty[Set[EncryptionKeySpec]]] = None,
    requiredSymmetricKeySchemes: Option[NonEmpty[Set[SymmetricKeyScheme]]] = None,
    requiredHashAlgorithms: Option[NonEmpty[Set[HashAlgorithm]]] = None,
    requiredCryptoKeyFormats: Option[NonEmpty[Set[CryptoKeyFormat]]] = None,
    requiredSignatureFormats: Option[NonEmpty[Set[SignatureFormat]]] = None,
    override val sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    override val alphaVersionSupport: Boolean = true,
    override val betaVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
) extends ProtocolConfig
    with PrettyPrinting {

  override protected def pretty: Pretty[SynchronizerParametersConfig] = prettyOfClass(
    param("requiredSigningAlgorithmSpecs", _.requiredSigningAlgorithmSpecs),
    param("requiredSigningKeySpecs", _.requiredSigningKeySpecs),
    param("requiredEncryptionAlgorithmSpecs", _.requiredEncryptionAlgorithmSpecs),
    param("requiredEncryptionKeySpecs", _.requiredEncryptionKeySpecs),
    param("requiredSymmetricKeySchemes", _.requiredSymmetricKeySchemes),
    param("requiredHashAlgorithms", _.requiredHashAlgorithms),
    param("requiredCryptoKeyFormats", _.requiredCryptoKeyFormats),
    param("requiredSignatureFormats", _.requiredSignatureFormats),
    param("sessionSigningKeys", _.sessionSigningKeys),
    param("alphaVersionSupport", _.alphaVersionSupport),
    param("betaVersionSupport", _.betaVersionSupport),
    param("dontWarnOnDeprecatedPV", _.dontWarnOnDeprecatedPV),
  )

  /** Converts the synchronizer parameters config into a synchronizer parameters protocol message.
    *
    * Sets the required crypto schemes based on the provided crypto config if they are unset in the config.
    */
  def toStaticSynchronizerParameters(
      cryptoConfig: CryptoConfig = CommunityCryptoConfig(),
      protocolVersion: ProtocolVersion,
  ): Either[String, StaticSynchronizerParameters] = {

    def selectSchemes[S](
        configuredRequired: Option[NonEmpty[Set[S]]],
        allowedFn: CryptoConfig => Either[String, NonEmpty[Set[S]]],
    ): Either[String, NonEmpty[Set[S]]] =
      for {
        allowed <- allowedFn(cryptoConfig)
        required = configuredRequired.getOrElse(allowed)
        // All required schemes must be allowed
        _ <- Either.cond(
          required.forall(r => allowed.contains(r)),
          (),
          s"Required schemes $required are not all allowed $allowed",
        )
      } yield required

    // Set to allowed schemes if none required schemes are specified
    for {
      newRequiredSigningAlgorithmSpecs <- selectSchemes(
        requiredSigningAlgorithmSpecs,
        selectAllowedSigningAlgorithmSpecs,
      )
      newRequiredSigningKeySpecs <- selectSchemes(
        requiredSigningKeySpecs,
        selectAllowedSigningKeySpecs,
      )
      newRequiredEncryptionAlgorithmSpecs <- selectSchemes(
        requiredEncryptionAlgorithmSpecs,
        selectAllowedEncryptionAlgorithmSpecs,
      )
      newRequiredEncryptionKeySpecs <- selectSchemes(
        requiredEncryptionKeySpecs,
        selectAllowedEncryptionKeySpecs,
      )
      newRequiredSymmetricKeySchemes <- selectSchemes(
        requiredSymmetricKeySchemes,
        selectAllowedSymmetricKeySchemes,
      )
      newRequiredHashAlgorithms <- selectSchemes(
        requiredHashAlgorithms,
        selectAllowedHashAlgorithms,
      )
      newCryptoKeyFormats = requiredCryptoKeyFormats.getOrElse(
        cryptoConfig.provider.supportedCryptoKeyFormatsForProtocol(protocolVersion)
      )
      newSignatureFormats = requiredSignatureFormats.getOrElse(
        cryptoConfig.provider.supportedSignatureFormatsForProtocol(protocolVersion)
      )
    } yield {
      StaticSynchronizerParameters(
        requiredSigningSpecs = RequiredSigningSpecs(
          newRequiredSigningAlgorithmSpecs,
          newRequiredSigningKeySpecs,
        ),
        requiredEncryptionSpecs = RequiredEncryptionSpecs(
          newRequiredEncryptionAlgorithmSpecs,
          newRequiredEncryptionKeySpecs,
        ),
        requiredSymmetricKeySchemes = newRequiredSymmetricKeySchemes,
        requiredHashAlgorithms = newRequiredHashAlgorithms,
        requiredCryptoKeyFormats = newCryptoKeyFormats,
        requiredSignatureFormats = newSignatureFormats,
        protocolVersion = protocolVersion,
      )
    }
  }
}
