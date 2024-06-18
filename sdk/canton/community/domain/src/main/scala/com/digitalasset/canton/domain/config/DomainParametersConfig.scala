// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import cats.syntax.contravariantSemigroupal.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{CryptoConfig, PositiveDurationSeconds, ProtocolConfig}
import com.digitalasset.canton.crypto.CryptoFactory.{
  selectAllowedEncryptionKeyScheme,
  selectAllowedHashAlgorithms,
  selectAllowedSigningKeyScheme,
  selectAllowedSymmetricKeySchemes,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.version.{DomainProtocolVersion, ProtocolVersion}

/** Configuration of domain parameters that all members connecting to a domain must adhere to.
  *
  * To set these parameters, you need to be familiar with the Canton architecture.
  * See <a href="https://docs.daml.com/canton/architecture/overview.html">the Canton architecture overview</a>
  * for further information.
  *
  * @param reconciliationInterval determines the time between sending two successive ACS commitments.
  *                               Must be a multiple of 1 second.
  * @param maxRatePerParticipant maximum number of messages sent per participant per second
  * @param maxInboundMessageSize maximum size of messages (in bytes) that the domain can receive through the public API
  * @param uniqueContractKeys When set, participants connected to this domain will check that contract keys are unique.
  *                           When a participant is connected to a domain with unique contract keys support,
  *                           it must not connect nor have ever been connected to any other domain.
  * @param requiredSigningKeySchemes The optional required signing key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredEncryptionKeySchemes The optional required encryption key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredSymmetricKeySchemes The optional required symmetric key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredHashAlgorithms The optional required hash algorithms that a member has to support. If none is specified, all the allowed algorithms are required.
  * @param requiredCryptoKeyFormats The optional required crypto key formats that a member has to support. If none is specified, all the supported algorithms are required.
  * @param protocolVersion                        The protocol version spoken on the domain. All participants and domain nodes attempting to connect to the sequencer need to support this protocol version to connect.
  * @param dontWarnOnDeprecatedPV If true, then this domain will not emit a warning when configured to use a deprecated protocol version (such as 2.0.0).
  * @param resetStoredStaticConfig DANGEROUS: If true, then the stored static configuration parameters will be reset to the ones in the configuration file
  */
final case class DomainParametersConfig(
    reconciliationInterval: PositiveDurationSeconds =
      StaticDomainParameters.defaultReconciliationInterval.toConfig,
    maxRatePerParticipant: NonNegativeInt = StaticDomainParameters.defaultMaxRatePerParticipant,
    maxInboundMessageSize: MaxRequestSize = StaticDomainParameters.defaultMaxRequestSize,
    uniqueContractKeys: Boolean = true,
    requiredSigningKeySchemes: Option[NonEmpty[Set[SigningKeyScheme]]] = None,
    requiredEncryptionKeySchemes: Option[NonEmpty[Set[EncryptionKeyScheme]]] = None,
    requiredSymmetricKeySchemes: Option[NonEmpty[Set[SymmetricKeyScheme]]] = None,
    requiredHashAlgorithms: Option[NonEmpty[Set[HashAlgorithm]]] = None,
    requiredCryptoKeyFormats: Option[NonEmpty[Set[CryptoKeyFormat]]] = None,
    protocolVersion: DomainProtocolVersion,
    override val devVersionSupport: Boolean = false,
    override val previewVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    resetStoredStaticConfig: Boolean = false,
) extends ProtocolConfig
    with PrettyPrinting {

  override def pretty: Pretty[DomainParametersConfig] = prettyOfClass(
    param("reconciliationInterval", _.reconciliationInterval),
    param("maxRatePerParticipant", _.maxRatePerParticipant),
    param("maxInboundMessageSize", _.maxInboundMessageSize.value),
    param("uniqueContractKeys", _.uniqueContractKeys),
    param("requiredSigningKeySchemes", _.requiredSigningKeySchemes),
    param("requiredEncryptionKeySchemes", _.requiredEncryptionKeySchemes),
    param("requiredSymmetricKeySchemes", _.requiredSymmetricKeySchemes),
    param("requiredHashAlgorithms", _.requiredHashAlgorithms),
    param("requiredCryptoKeyFormats", _.requiredCryptoKeyFormats),
    param("protocolVersion", _.protocolVersion.version),
    param("devVersionSupport", _.devVersionSupport),
    param("dontWarnOnDeprecatedPV", _.dontWarnOnDeprecatedPV),
    param("resetStoredStaticConfig", _.resetStoredStaticConfig),
  )

  override def initialProtocolVersion: ProtocolVersion = protocolVersion.version

  /** Converts the domain parameters config into a domain parameters protocol message.
    *
    * Sets the required crypto schemes based on the provided crypto config if they are unset in the config.
    */
  def toStaticDomainParameters(
      cryptoConfig: CryptoConfig
  ): Either[String, StaticDomainParameters] = {

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
      _ <- validateNonDefaultValues()

      newRequiredSigningKeySchemes <- selectSchemes(
        requiredSigningKeySchemes,
        selectAllowedSigningKeyScheme,
      )
      newRequiredEncryptionKeySchemes <- selectSchemes(
        requiredEncryptionKeySchemes,
        selectAllowedEncryptionKeyScheme,
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
        cryptoConfig.provider.supportedCryptoKeyFormatsForProtocol(protocolVersion.unwrap)
      )
    } yield {
      StaticDomainParameters.create(
        reconciliationInterval = reconciliationInterval.toInternal,
        maxRatePerParticipant = maxRatePerParticipant,
        maxRequestSize = maxInboundMessageSize,
        uniqueContractKeys = uniqueContractKeys,
        requiredSigningKeySchemes = newRequiredSigningKeySchemes,
        requiredEncryptionKeySchemes = newRequiredEncryptionKeySchemes,
        requiredSymmetricKeySchemes = newRequiredSymmetricKeySchemes,
        requiredHashAlgorithms = newRequiredHashAlgorithms,
        requiredCryptoKeyFormats = newCryptoKeyFormats,
        protocolVersion = protocolVersion.unwrap,
      )
    }
  }

  /** Return an error if one parameter which is dynamic has non-default
    * value specified in the config. The reason for the error is that
    * such a config value would be ignored.
    */
  private def validateNonDefaultValues(): Either[String, Unit] = {

    def errorMessage(
        name: String,
        setConsoleCommand: String,
        configuredValue: String,
        defaultValue: String,
    ) =
      s"""|Starting from protocol version ${ProtocolVersion.v4}, $name is a dynamic parameter that cannot be configured within the configuration file.
          |The configured value `$configuredValue` is ignored. The default value is ${defaultValue}.
          |Please use the admin api to set this parameter: domain-name.service.$setConsoleCommand($configuredValue)
          |""".stripMargin

    val reconciliationIntervalValid = Either.cond(
      reconciliationInterval == StaticDomainParameters.defaultReconciliationInterval.toConfig,
      (),
      errorMessage(
        "reconciliation interval",
        "set_reconciliation_interval",
        reconciliationInterval.underlying.toString(),
        StaticDomainParameters.defaultReconciliationInterval.toFiniteDuration.toString(),
      ),
    )

    val maxRatePerParticipantValid = Either.cond(
      maxRatePerParticipant == StaticDomainParameters.defaultMaxRatePerParticipant,
      (),
      errorMessage(
        "max rate per participant",
        "set_max_rate_per_participant",
        maxRatePerParticipant.value.toString,
        StaticDomainParameters.defaultMaxRatePerParticipant.value.toString,
      ),
    )

    val maxRequestSizeValid = Either.cond(
      maxInboundMessageSize == StaticDomainParameters.defaultMaxRequestSize,
      (),
      errorMessage(
        "max request size (previously: max inbound message size)",
        "set_max_request_size",
        maxInboundMessageSize.unwrap.toString,
        StaticDomainParameters.defaultMaxRequestSize.unwrap.toString,
      ),
    )

    (reconciliationIntervalValid, maxRatePerParticipantValid, maxRequestSizeValid).tupled.map(_ =>
      ()
    )
  }
}

object DomainParametersConfig {
  def defaults(protocolVersion: DomainProtocolVersion): DomainParametersConfig =
    DomainParametersConfig(protocolVersion = protocolVersion)
}
