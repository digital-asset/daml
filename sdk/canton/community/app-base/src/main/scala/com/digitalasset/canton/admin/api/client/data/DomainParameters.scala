// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyUtil.instances.*
import com.digitalasset.canton.admin.api.client.data.crypto.{
  CryptoKeyFormat,
  HashAlgorithm,
  RequiredEncryptionSpecs,
  RequiredSigningSpecs,
  SymmetricKeyScheme,
}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{
  CommunityCryptoConfig,
  CryptoConfig,
  NonNegativeFiniteDuration,
  PositiveDurationSeconds,
}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters.InvalidDynamicSynchronizerParameters
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.protocol.{
  AcsCommitmentsCatchUpConfig,
  DynamicSynchronizerParameters as DynamicSynchronizerParametersInternal,
  OnboardingRestriction,
  StaticSynchronizerParameters as StaticSynchronizerParametersInternal,
  v30,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration as InternalNonNegativeFiniteDuration,
  PositiveSeconds,
}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion}
import com.digitalasset.canton.{ProtoDeserializationError, config, crypto as SynchronizerCrypto}
import com.google.common.annotations.VisibleForTesting
import io.scalaland.chimney.dsl.*

import scala.Ordering.Implicits.*
import scala.annotation.nowarn

final case class StaticSynchronizerParameters(
    requiredSigningSpecs: RequiredSigningSpecs,
    requiredEncryptionSpecs: RequiredEncryptionSpecs,
    requiredSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]],
    requiredHashAlgorithms: NonEmpty[Set[HashAlgorithm]],
    requiredCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]],
    protocolVersion: ProtocolVersion,
) {
  def writeToFile(outputFile: String): Unit =
    BinaryFileUtil.writeByteStringToFile(outputFile, toInternal.toByteString)

  private[canton] def toInternal: StaticSynchronizerParametersInternal =
    this.transformInto[StaticSynchronizerParametersInternal]: @nowarn(
      "msg=Der in object CryptoKeyFormat is deprecated"
    )
}

object StaticSynchronizerParameters {

  // This method is unsafe. Not prefixing by `try` to have nicer docs snippets.
  def fromConfig(
      config: SynchronizerParametersConfig,
      cryptoConfig: CryptoConfig,
      protocolVersion: ProtocolVersion,
  ): StaticSynchronizerParameters = {
    val internal = config
      .toStaticSynchronizerParameters(cryptoConfig, protocolVersion)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Cannot instantiate static synchronizer parameters: $err"
        )
      )

    StaticSynchronizerParameters(internal)
  }

  def defaultsWithoutKMS(protocolVersion: ProtocolVersion): StaticSynchronizerParameters =
    defaults(CommunityCryptoConfig(), protocolVersion)

  // This method is unsafe. Not prefixing by `try` to have nicer docs snippets.
  def defaults(
      cryptoConfig: CryptoConfig,
      protocolVersion: ProtocolVersion,
  ): StaticSynchronizerParameters = {
    val internal = SynchronizerParametersConfig()
      .toStaticSynchronizerParameters(cryptoConfig, protocolVersion)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Cannot instantiate static synchronizer parameters: $err"
        )
      )

    StaticSynchronizerParameters(internal)
  }

  def apply(
      synchronizer: StaticSynchronizerParametersInternal
  ): StaticSynchronizerParameters =
    synchronizer.transformInto[StaticSynchronizerParameters]: @nowarn(
      "msg=Der in object CryptoKeyFormat is deprecated"
    )

  def tryReadFromFile(inputFile: String): StaticSynchronizerParameters = {
    val staticSynchronizerParametersInternal = StaticSynchronizerParametersInternal
      .readFromTrustedFile(inputFile)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Reading static synchronizer parameters from file $inputFile failed: $err"
        )
      )

    StaticSynchronizerParameters(staticSynchronizerParametersInternal)
  }

  private def requiredKeySchemes[P, A](
      field: String,
      content: Seq[P],
      parse: (String, P) => ParsingResult[A],
  ): ParsingResult[NonEmpty[Set[A]]] =
    ProtoConverter.parseRequiredNonEmpty(parse(field, _), field, content).map(_.toSet)

  def fromProtoV30(
      synchronizerParametersP: v30.StaticSynchronizerParameters
  ): ParsingResult[StaticSynchronizerParameters] = {
    val v30.StaticSynchronizerParameters(
      requiredSigningSpecsOP,
      requiredEncryptionSpecsOP,
      requiredSymmetricKeySchemesP,
      requiredHashAlgorithmsP,
      requiredCryptoKeyFormatsP,
      protocolVersionP,
    ) = synchronizerParametersP

    for {
      requiredSigningSpecsP <- requiredSigningSpecsOP.toRight(
        ProtoDeserializationError.FieldNotSet(
          "required_signing_specs"
        )
      )
      requiredSigningAlgorithmSpecs <- requiredKeySchemes(
        "required_signing_algorithm_specs",
        requiredSigningSpecsP.algorithms,
        SynchronizerCrypto.SigningAlgorithmSpec.fromProtoEnum,
      )
      requiredSigningKeySpecs <- requiredKeySchemes(
        "required_signing_key_specs",
        requiredSigningSpecsP.keys,
        SynchronizerCrypto.SigningKeySpec.fromProtoEnum,
      )
      requiredEncryptionSpecsP <- requiredEncryptionSpecsOP.toRight(
        ProtoDeserializationError.FieldNotSet(
          "required_encryption_specs"
        )
      )
      requiredEncryptionAlgorithmSpecs <- requiredKeySchemes(
        "required_encryption_algorithm_specs",
        requiredEncryptionSpecsP.algorithms,
        SynchronizerCrypto.EncryptionAlgorithmSpec.fromProtoEnum,
      )
      requiredEncryptionKeySpecs <- requiredKeySchemes(
        "required_encryption_key_specs",
        requiredEncryptionSpecsP.keys,
        SynchronizerCrypto.EncryptionKeySpec.fromProtoEnum,
      )
      requiredSymmetricKeySchemes <- requiredKeySchemes(
        "required_symmetric_key_schemes",
        requiredSymmetricKeySchemesP,
        SynchronizerCrypto.SymmetricKeyScheme.fromProtoEnum,
      )
      requiredHashAlgorithms <- requiredKeySchemes(
        "required_hash_algorithms",
        requiredHashAlgorithmsP,
        SynchronizerCrypto.HashAlgorithm.fromProtoEnum,
      )
      requiredCryptoKeyFormats <- requiredKeySchemes(
        "required_crypto_key_formats",
        requiredCryptoKeyFormatsP,
        SynchronizerCrypto.CryptoKeyFormat.fromProtoEnum,
      )
      // Data in the console is not really validated, so we allow for deleted
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP, allowDeleted = true)
    } yield StaticSynchronizerParameters(
      StaticSynchronizerParametersInternal(
        SynchronizerCrypto
          .RequiredSigningSpecs(requiredSigningAlgorithmSpecs, requiredSigningKeySpecs),
        SynchronizerCrypto
          .RequiredEncryptionSpecs(requiredEncryptionAlgorithmSpecs, requiredEncryptionKeySpecs),
        requiredSymmetricKeySchemes,
        requiredHashAlgorithms,
        requiredCryptoKeyFormats,
        protocolVersion,
      )
    )
  }
}

// TODO(#15650) Properly expose new BFT parameters and synchronizer limits
final case class DynamicSynchronizerParameters(
    confirmationResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    assignmentExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveDurationSeconds,
    maxRequestSize: NonNegativeInt,
    sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
    trafficControlParameters: Option[TrafficControlParameters],
    onboardingRestriction: OnboardingRestriction,
    acsCommitmentsCatchUpConfig: Option[AcsCommitmentsCatchUpConfig],
    participantSynchronizerLimits: ParticipantSynchronizerLimits,
    submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration,
) {

  def decisionTimeout: config.NonNegativeFiniteDuration =
    confirmationResponseTimeout + mediatorReactionTimeout

  @inline def confirmationRequestsMaxRate: NonNegativeInt =
    participantSynchronizerLimits.confirmationRequestsMaxRate

  if (submissionTimeRecordTimeTolerance * 2 > mediatorDeduplicationTimeout)
    throw new InvalidDynamicSynchronizerParameters(
      s"The submissionTimeRecordTimeTolerance ($submissionTimeRecordTimeTolerance) must be at most half of the " +
        s"mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
    )

  // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.1dzc6dxxlpca
  // Originally the validation was done on ledgerTimeRecordTimeTolerance, but was moved to submissionTimeRecordTimeTolerance
  // instead when the parameter was introduced
  private[canton] def compatibleWithNewSubmissionTimeRecordTimeTolerance(
      newSubmissionTimeRecordTimeTolerance: NonNegativeFiniteDuration
  ): Boolean =
    // If false, a new request may receive the same submission time as a previous request and the previous
    // request may be evicted too early from the mediator's deduplication store.
    // Thus, an attacker may assign the same UUID to both requests.
    // See i9028 for a detailed design. (This is the second clause of item 2 of Lemma 2).
    submissionTimeRecordTimeTolerance + newSubmissionTimeRecordTimeTolerance <= mediatorDeduplicationTimeout

  def update(
      confirmationResponseTimeout: NonNegativeFiniteDuration = confirmationResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      assignmentExclusivityTimeout: NonNegativeFiniteDuration = assignmentExclusivityTimeout,
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
      acsCommitmentsCatchUpConfig: Option[AcsCommitmentsCatchUpConfig] =
        acsCommitmentsCatchUpConfig,
      submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration =
        submissionTimeRecordTimeTolerance,
  ): DynamicSynchronizerParameters = this.copy(
    confirmationResponseTimeout = confirmationResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    assignmentExclusivityTimeout = assignmentExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
    mediatorDeduplicationTimeout = mediatorDeduplicationTimeout,
    reconciliationInterval = reconciliationInterval,
    maxRequestSize = maxRequestSize,
    sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
    trafficControlParameters = trafficControlParameters,
    onboardingRestriction = onboardingRestriction,
    acsCommitmentsCatchUpConfig = acsCommitmentsCatchUpConfig,
    participantSynchronizerLimits = ParticipantSynchronizerLimits(confirmationRequestsMaxRate),
    submissionTimeRecordTimeTolerance = submissionTimeRecordTimeTolerance,
  )

  private[canton] def toInternal: Either[String, DynamicSynchronizerParametersInternal] =
    DynamicSynchronizerParametersInternal
      .protocolVersionRepresentativeFor(ProtoVersion(30))
      .leftMap(_.message)
      .map { rpv =>
        DynamicSynchronizerParametersInternal.tryCreate(
          confirmationResponseTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(confirmationResponseTimeout),
          mediatorReactionTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(mediatorReactionTimeout),
          assignmentExclusivityTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(assignmentExclusivityTimeout),
          topologyChangeDelay = InternalNonNegativeFiniteDuration.fromConfig(topologyChangeDelay),
          ledgerTimeRecordTimeTolerance =
            InternalNonNegativeFiniteDuration.fromConfig(ledgerTimeRecordTimeTolerance),
          mediatorDeduplicationTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(mediatorDeduplicationTimeout),
          reconciliationInterval = PositiveSeconds.fromConfig(reconciliationInterval),
          maxRequestSize = MaxRequestSize(maxRequestSize),
          sequencerAggregateSubmissionTimeout =
            InternalNonNegativeFiniteDuration.fromConfig(sequencerAggregateSubmissionTimeout),
          trafficControlParameters = trafficControlParameters.map(_.toInternal),
          onboardingRestriction = onboardingRestriction,
          acsCommitmentsCatchUpConfigParameter = acsCommitmentsCatchUpConfig,
          participantSynchronizerLimits = participantSynchronizerLimits.toInternal,
          submissionTimeRecordTimeTolerance =
            InternalNonNegativeFiniteDuration.fromConfig(submissionTimeRecordTimeTolerance),
        )(rpv)
      }
}

object DynamicSynchronizerParameters {

  /** Default dynamic synchronizer parameters for non-static clocks */
  @VisibleForTesting
  def defaultValues(protocolVersion: ProtocolVersion): DynamicSynchronizerParameters =
    DynamicSynchronizerParameters(
      DynamicSynchronizerParametersInternal.defaultValues(protocolVersion)
    )

  private[canton] def initialValues(clock: Clock, protocolVersion: ProtocolVersion) =
    DynamicSynchronizerParameters(
      DynamicSynchronizerParametersInternal.initialValues(clock, protocolVersion)
    )

  def apply(
      synchronizer: DynamicSynchronizerParametersInternal
  ): DynamicSynchronizerParameters =
    synchronizer.transformInto[DynamicSynchronizerParameters]
}
