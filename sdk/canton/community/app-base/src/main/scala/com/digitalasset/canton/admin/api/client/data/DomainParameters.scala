// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{
  CommunityCryptoConfig,
  CryptoConfig,
  NonNegativeFiniteDuration,
  PositiveDurationSeconds,
}
import com.digitalasset.canton.crypto.{
  CryptoKeyFormat,
  EncryptionKeyScheme,
  HashAlgorithm,
  SigningKeyScheme,
  SymmetricKeyScheme,
}
import com.digitalasset.canton.domain.config.DomainParametersConfig
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DynamicDomainParameters.InvalidDynamicDomainParameters
import com.digitalasset.canton.protocol.{
  AcsCommitmentsCatchUpConfig,
  DynamicDomainParameters as DynamicDomainParametersInternal,
  OnboardingRestriction,
  StaticDomainParameters as StaticDomainParametersInternal,
  v30,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration as InternalNonNegativeFiniteDuration,
  PositiveSeconds,
}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion}
import com.digitalasset.canton.{config, crypto as DomainCrypto}
import com.google.common.annotations.VisibleForTesting
import io.scalaland.chimney.dsl.*

import scala.Ordering.Implicits.*

final case class StaticDomainParameters(
    requiredSigningKeySchemes: Set[SigningKeyScheme],
    requiredEncryptionKeySchemes: Set[EncryptionKeyScheme],
    requiredSymmetricKeySchemes: Set[SymmetricKeyScheme],
    requiredHashAlgorithms: Set[HashAlgorithm],
    requiredCryptoKeyFormats: Set[CryptoKeyFormat],
    protocolVersion: ProtocolVersion,
) {
  def writeToFile(outputFile: String): Unit =
    BinaryFileUtil.writeByteStringToFile(outputFile, toInternal.toByteString)

  private[canton] def toInternal: StaticDomainParametersInternal =
    StaticDomainParametersInternal.create(
      requiredSigningKeySchemes = NonEmptyUtil.fromUnsafe(
        requiredSigningKeySchemes.map(_.transformInto[DomainCrypto.SigningKeyScheme])
      ),
      requiredEncryptionKeySchemes = NonEmptyUtil.fromUnsafe(
        requiredEncryptionKeySchemes.map(_.transformInto[DomainCrypto.EncryptionKeyScheme])
      ),
      requiredSymmetricKeySchemes = NonEmptyUtil.fromUnsafe(
        requiredSymmetricKeySchemes.map(_.transformInto[DomainCrypto.SymmetricKeyScheme])
      ),
      requiredHashAlgorithms = NonEmptyUtil.fromUnsafe(
        requiredHashAlgorithms.map(_.transformInto[DomainCrypto.HashAlgorithm])
      ),
      requiredCryptoKeyFormats = NonEmptyUtil.fromUnsafe(
        requiredCryptoKeyFormats.map(_.transformInto[DomainCrypto.CryptoKeyFormat])
      ),
      protocolVersion = protocolVersion,
    )
}

object StaticDomainParameters {

  // This method is unsafe. Not prefixing by `try` to have nicer docs snippets.
  def fromConfig(
      config: DomainParametersConfig,
      cryptoConfig: CryptoConfig,
      protocolVersion: ProtocolVersion,
  ): StaticDomainParameters = {
    val internal = config
      .toStaticDomainParameters(cryptoConfig, protocolVersion)
      .valueOr(err =>
        throw new IllegalArgumentException(s"Cannot instantiate static domain parameters: $err")
      )

    StaticDomainParameters(internal)
  }

  def defaultsWithoutKMS(protocolVersion: ProtocolVersion): StaticDomainParameters =
    defaults(CommunityCryptoConfig(), protocolVersion)

  // This method is unsafe. Not prefixing by `try` to have nicer docs snippets.
  def defaults(
      cryptoConfig: CryptoConfig,
      protocolVersion: ProtocolVersion,
  ): StaticDomainParameters = {
    val internal = DomainParametersConfig()
      .toStaticDomainParameters(cryptoConfig, protocolVersion)
      .valueOr(err =>
        throw new IllegalArgumentException(s"Cannot instantiate static domain parameters: $err")
      )

    StaticDomainParameters(internal)
  }

  def apply(
      domain: StaticDomainParametersInternal
  ): StaticDomainParameters =
    StaticDomainParameters(
      requiredSigningKeySchemes =
        domain.requiredSigningKeySchemes.forgetNE.map(_.transformInto[SigningKeyScheme]),
      requiredEncryptionKeySchemes =
        domain.requiredEncryptionKeySchemes.forgetNE.map(_.transformInto[EncryptionKeyScheme]),
      requiredSymmetricKeySchemes =
        domain.requiredSymmetricKeySchemes.forgetNE.map(_.transformInto[SymmetricKeyScheme]),
      requiredHashAlgorithms =
        domain.requiredHashAlgorithms.forgetNE.map(_.transformInto[HashAlgorithm]),
      requiredCryptoKeyFormats =
        domain.requiredCryptoKeyFormats.forgetNE.map(_.transformInto[CryptoKeyFormat]),
      protocolVersion = domain.protocolVersion,
    )

  def tryReadFromFile(inputFile: String): StaticDomainParameters = {
    val staticDomainParametersInternal = StaticDomainParametersInternal
      .readFromTrustedFile(inputFile)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Reading static domain parameters from file $inputFile failed: $err"
        )
      )

    StaticDomainParameters(staticDomainParametersInternal)
  }

  private def requiredKeySchemes[P, A](
      field: String,
      content: Seq[P],
      parse: (String, P) => ParsingResult[A],
  ): ParsingResult[NonEmpty[Set[A]]] =
    ProtoConverter.parseRequiredNonEmpty(parse(field, _), field, content).map(_.toSet)

  def fromProtoV30(
      domainParametersP: v30.StaticDomainParameters
  ): ParsingResult[StaticDomainParameters] = {
    val v30.StaticDomainParameters(
      requiredSigningKeySchemesP,
      requiredEncryptionKeySchemesP,
      requiredSymmetricKeySchemesP,
      requiredHashAlgorithmsP,
      requiredCryptoKeyFormatsP,
      protocolVersionP,
    ) = domainParametersP

    for {
      requiredSigningKeySchemes <- requiredKeySchemes(
        "requiredSigningKeySchemes",
        requiredSigningKeySchemesP,
        SigningKeyScheme.fromProtoEnum,
      )
      requiredEncryptionKeySchemes <- requiredKeySchemes(
        "requiredEncryptionKeySchemes",
        requiredEncryptionKeySchemesP,
        EncryptionKeyScheme.fromProtoEnum,
      )
      requiredSymmetricKeySchemes <- requiredKeySchemes(
        "requiredSymmetricKeySchemes",
        requiredSymmetricKeySchemesP,
        SymmetricKeyScheme.fromProtoEnum,
      )
      requiredHashAlgorithms <- requiredKeySchemes(
        "requiredHashAlgorithms",
        requiredHashAlgorithmsP,
        HashAlgorithm.fromProtoEnum,
      )
      requiredCryptoKeyFormats <- requiredKeySchemes(
        "requiredCryptoKeyFormats",
        requiredCryptoKeyFormatsP,
        CryptoKeyFormat.fromProtoEnum,
      )
      // Data in the console is not really validated, so we allow for deleted
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP, allowDeleted = true)
    } yield StaticDomainParameters(
      requiredSigningKeySchemes,
      requiredEncryptionKeySchemes,
      requiredSymmetricKeySchemes,
      requiredHashAlgorithms,
      requiredCryptoKeyFormats,
      protocolVersion,
    )
  }
}

// TODO(#15650) Properly expose new BFT parameters and domain limits
final case class DynamicDomainParameters(
    confirmationResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveDurationSeconds,
    confirmationRequestsMaxRate: NonNegativeInt,
    maxRequestSize: NonNegativeInt,
    sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
    trafficControlParameters: Option[TrafficControlParameters],
    onboardingRestriction: OnboardingRestriction,
    acsCommitmentsCatchUpConfig: Option[AcsCommitmentsCatchUpConfig],
) {

  def decisionTimeout: config.NonNegativeFiniteDuration =
    confirmationResponseTimeout + mediatorReactionTimeout

  if (ledgerTimeRecordTimeTolerance * 2 > mediatorDeduplicationTimeout)
    throw new InvalidDynamicDomainParameters(
      s"The ledgerTimeRecordTimeTolerance ($ledgerTimeRecordTimeTolerance) must be at most half of the " +
        s"mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
    )

  // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.1dzc6dxxlpca
  private[canton] def compatibleWithNewLedgerTimeRecordTimeTolerance(
      newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
  ): Boolean = {
    // If false, a new request may receive the same ledger time as a previous request and the previous
    // request may be evicted too early from the mediator's deduplication store.
    // Thus, an attacker may assign the same UUID to both requests.
    // See i9028 for a detailed design. (This is the second clause of item 2 of Lemma 2).
    ledgerTimeRecordTimeTolerance + newLedgerTimeRecordTimeTolerance <= mediatorDeduplicationTimeout
  }

  def update(
      confirmationResponseTimeout: NonNegativeFiniteDuration = confirmationResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration = mediatorDeduplicationTimeout,
      reconciliationInterval: PositiveDurationSeconds = reconciliationInterval,
      confirmationRequestsMaxRate: NonNegativeInt = confirmationRequestsMaxRate,
      maxRequestSize: NonNegativeInt = maxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
        sequencerAggregateSubmissionTimeout,
      trafficControlParameters: Option[TrafficControlParameters] = trafficControlParameters,
      onboardingRestriction: OnboardingRestriction = onboardingRestriction,
      acsCommitmentsCatchUpConfig: Option[AcsCommitmentsCatchUpConfig] = acsCommitmentsCatchUpConfig,
  ): DynamicDomainParameters = this.copy(
    confirmationResponseTimeout = confirmationResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    transferExclusivityTimeout = transferExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
    mediatorDeduplicationTimeout = mediatorDeduplicationTimeout,
    reconciliationInterval = reconciliationInterval,
    confirmationRequestsMaxRate = confirmationRequestsMaxRate,
    maxRequestSize = maxRequestSize,
    sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
    trafficControlParameters = trafficControlParameters,
    onboardingRestriction = onboardingRestriction,
    acsCommitmentsCatchUpConfig = acsCommitmentsCatchUpConfig,
  )

  private[canton] def toInternal: Either[String, DynamicDomainParametersInternal] =
    DynamicDomainParametersInternal
      .protocolVersionRepresentativeFor(ProtoVersion(30))
      .leftMap(_.message)
      .map { rpv =>
        DynamicDomainParametersInternal.tryCreate(
          confirmationResponseTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(confirmationResponseTimeout),
          mediatorReactionTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(mediatorReactionTimeout),
          transferExclusivityTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(transferExclusivityTimeout),
          topologyChangeDelay = InternalNonNegativeFiniteDuration.fromConfig(topologyChangeDelay),
          ledgerTimeRecordTimeTolerance =
            InternalNonNegativeFiniteDuration.fromConfig(ledgerTimeRecordTimeTolerance),
          mediatorDeduplicationTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(mediatorDeduplicationTimeout),
          reconciliationInterval = PositiveSeconds.fromConfig(reconciliationInterval),
          confirmationRequestsMaxRate = confirmationRequestsMaxRate,
          maxRequestSize = MaxRequestSize(maxRequestSize),
          sequencerAggregateSubmissionTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(sequencerAggregateSubmissionTimeout),
          trafficControlParameters = trafficControlParameters.map(_.toInternal),
          onboardingRestriction = onboardingRestriction,
          acsCommitmentsCatchUpConfigParameter = acsCommitmentsCatchUpConfig,
        )(rpv)
      }
}

object DynamicDomainParameters {

  /** Default dynamic domain parameters for non-static clocks */
  @VisibleForTesting
  def defaultValues(protocolVersion: ProtocolVersion): DynamicDomainParameters =
    DynamicDomainParameters(
      DynamicDomainParametersInternal.defaultValues(protocolVersion)
    )

  private[canton] def initialValues(clock: Clock, protocolVersion: ProtocolVersion) =
    DynamicDomainParameters(
      DynamicDomainParametersInternal.initialValues(clock, protocolVersion)
    )

  def apply(
      domain: DynamicDomainParametersInternal
  ): DynamicDomainParameters =
    domain.transformInto[DynamicDomainParameters]
}
