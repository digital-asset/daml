// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.admin.api.client.data.crypto.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{
  CommunityCryptoConfig,
  CryptoConfig,
  NonNegativeFiniteDuration,
  PositiveDurationSeconds,
}
import com.digitalasset.canton.domain.config.DomainParametersConfig
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DynamicDomainParameters.{
  InvalidDynamicDomainParameters,
  protocolVersionRepresentativeFor,
}
import com.digitalasset.canton.protocol.{
  CatchUpConfig,
  DynamicDomainParameters as DynamicDomainParametersInternal,
  OnboardingRestriction,
  StaticDomainParameters as StaticDomainParametersInternal,
  v30 as protocolV30,
}
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration as InternalNonNegativeFiniteDuration,
  PositiveSeconds,
}
import com.digitalasset.canton.topology.admin.v30old.DomainParametersChangeAuthorization
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion}
import com.digitalasset.canton.crypto as DomainCrypto
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
    catchUpParameters: Option[CatchUpConfig],
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
      catchUpParameters = catchUpParameters,
    )
}

object StaticDomainParameters {

  // This method is unsafe. Not prefixing by `try` to have nicer docs snippets.
  def fromConfig(
      config: DomainParametersConfig,
      cryptoConfig: CryptoConfig,
  ): StaticDomainParameters = {
    val internal = config
      .toStaticDomainParameters(cryptoConfig)
      .valueOr(err =>
        throw new IllegalArgumentException(s"Cannot instantiate static domain parameters: $err")
      )

    StaticDomainParameters(internal)
  }

  lazy val defaultsWithoutKMS: StaticDomainParameters =
    defaults(CommunityCryptoConfig())

  // This method is unsafe. Not prefixing by `try` to have nicer docs snippets.
  def defaults(
      cryptoConfig: CryptoConfig
  ): StaticDomainParameters = {
    val internal = DomainParametersConfig()
      .toStaticDomainParameters(cryptoConfig)
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
      catchUpParameters = domain.catchUpParameters,
    )

  def tryReadFromFile(inputFile: String): StaticDomainParameters = {
    val staticDomainParametersInternal = StaticDomainParametersInternal
      .readFromFileUnsafe(inputFile)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Reading static domain parameters from file $inputFile failed: $err"
        )
      )

    StaticDomainParameters(staticDomainParametersInternal)
  }
}

// TODO(#15650) Properly expose new BFT parameters and domain limits
final case class DynamicDomainParameters(
    participantResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveDurationSeconds,
    maxRatePerParticipant: NonNegativeInt,
    maxRequestSize: NonNegativeInt,
    sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
    trafficControlParameters: Option[TrafficControlParameters],
    onboardingRestriction: OnboardingRestriction,
) {

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
      participantResponseTimeout: NonNegativeFiniteDuration = participantResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration = mediatorDeduplicationTimeout,
      reconciliationInterval: PositiveDurationSeconds = reconciliationInterval,
      maxRatePerParticipant: NonNegativeInt = maxRatePerParticipant,
      maxRequestSize: NonNegativeInt = maxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
        sequencerAggregateSubmissionTimeout,
      trafficControlParameters: Option[TrafficControlParameters] = trafficControlParameters,
      onboardingRestriction: OnboardingRestriction = onboardingRestriction,
  ): DynamicDomainParameters = this.copy(
    participantResponseTimeout = participantResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    transferExclusivityTimeout = transferExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
    mediatorDeduplicationTimeout = mediatorDeduplicationTimeout,
    reconciliationInterval = reconciliationInterval,
    maxRatePerParticipant = maxRatePerParticipant,
    maxRequestSize = maxRequestSize,
    sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
    trafficControlParameters = trafficControlParameters,
    onboardingRestriction = onboardingRestriction,
  )

  def toProto: DomainParametersChangeAuthorization.Parameters =
    DomainParametersChangeAuthorization.Parameters.ParametersV1(
      protocolV30.DynamicDomainParameters(
        participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
        mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
        transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
        topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
        ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
        mediatorDeduplicationTimeout = Some(mediatorDeduplicationTimeout.toProtoPrimitive),
        reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
        defaultParticipantLimits = Some(
          protocolV30.ParticipantDomainLimits(
            maxRate = maxRatePerParticipant.unwrap,
            maxNumParties = 0,
            maxNumPackages = 0,
          )
        ),
        maxRequestSize = maxRequestSize.unwrap,
        onboardingRestriction = onboardingRestriction.toProtoV30,
        requiredPackages = Nil,
        onlyRequiredPackagesPermitted = false,
        defaultMaxHostingParticipantsPerParty = 0,
        sequencerAggregateSubmissionTimeout =
          Some(sequencerAggregateSubmissionTimeout.toProtoPrimitive),
        trafficControlParameters = None,
      )
    )

  private[canton] def toInternal: DynamicDomainParametersInternal =
    DynamicDomainParametersInternal.tryCreate(
      participantResponseTimeout =
        InternalNonNegativeFiniteDuration.fromConfig(participantResponseTimeout),
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
      maxRatePerParticipant = maxRatePerParticipant,
      maxRequestSize = MaxRequestSize(maxRequestSize),
      sequencerAggregateSubmissionTimeout =
        InternalNonNegativeFiniteDuration.fromConfig(sequencerAggregateSubmissionTimeout),
      trafficControlParameters = trafficControlParameters.map(_.toInternal),
      onboardingRestriction = onboardingRestriction,
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))
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
