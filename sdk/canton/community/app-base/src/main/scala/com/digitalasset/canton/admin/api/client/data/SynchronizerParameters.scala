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
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  CryptoConfig,
  NonNegativeFiniteDuration,
  PositiveDurationSeconds,
}
import com.digitalasset.canton.crypto.SignatureFormat
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters.InvalidDynamicSynchronizerParameters
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.protocol.{
  AcsCommitmentsCatchUpParameters as AcsCommitmentsCatchUpParametersInternal,
  DynamicSynchronizerParameters as DynamicSynchronizerParametersInternal,
  OnboardingRestriction as OnboardingRestrictionInternal,
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
    requiredSignatureFormats: NonEmpty[Set[SignatureFormat]],
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
    defaults(CryptoConfig(), protocolVersion)

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

  private def parseRequiredSet[P, A](
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
      requiredSignatureFormatsP,
      protocolVersionP,
    ) = synchronizerParametersP

    for {
      requiredSigningSpecsP <- requiredSigningSpecsOP.toRight(
        ProtoDeserializationError.FieldNotSet(
          "required_signing_specs"
        )
      )
      requiredSigningAlgorithmSpecs <- parseRequiredSet(
        "required_signing_algorithm_specs",
        requiredSigningSpecsP.algorithms,
        SynchronizerCrypto.SigningAlgorithmSpec.fromProtoEnum,
      )
      requiredSigningKeySpecs <- parseRequiredSet(
        "required_signing_key_specs",
        requiredSigningSpecsP.keys,
        SynchronizerCrypto.SigningKeySpec.fromProtoEnum,
      )
      requiredEncryptionSpecsP <- requiredEncryptionSpecsOP.toRight(
        ProtoDeserializationError.FieldNotSet(
          "required_encryption_specs"
        )
      )
      requiredEncryptionAlgorithmSpecs <- parseRequiredSet(
        "required_encryption_algorithm_specs",
        requiredEncryptionSpecsP.algorithms,
        SynchronizerCrypto.EncryptionAlgorithmSpec.fromProtoEnum,
      )
      requiredEncryptionKeySpecs <- parseRequiredSet(
        "required_encryption_key_specs",
        requiredEncryptionSpecsP.keys,
        SynchronizerCrypto.EncryptionKeySpec.fromProtoEnum,
      )
      requiredSymmetricKeySchemes <- parseRequiredSet(
        "required_symmetric_key_schemes",
        requiredSymmetricKeySchemesP,
        SynchronizerCrypto.SymmetricKeyScheme.fromProtoEnum,
      )
      requiredHashAlgorithms <- parseRequiredSet(
        "required_hash_algorithms",
        requiredHashAlgorithmsP,
        SynchronizerCrypto.HashAlgorithm.fromProtoEnum,
      )
      requiredCryptoKeyFormats <- parseRequiredSet(
        "required_crypto_key_formats",
        requiredCryptoKeyFormatsP,
        SynchronizerCrypto.CryptoKeyFormat.fromProtoEnum,
      )
      requiredSignatureFormats <- parseRequiredSet(
        "required_signature_formats",
        requiredSignatureFormatsP,
        SynchronizerCrypto.SignatureFormat.fromProtoEnum,
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
        requiredSignatureFormats,
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
    trafficControl: Option[TrafficControlParameters],
    onboardingRestriction: OnboardingRestriction,
    acsCommitmentsCatchUp: Option[AcsCommitmentsCatchUpParameters],
    participantSynchronizerLimits: ParticipantSynchronizerLimits,
    submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration,
) extends PrettyPrinting {

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

  override protected def pretty: Pretty[DynamicSynchronizerParameters] =
    prettyOfClass(
      param("confirmation response timeout", _.confirmationResponseTimeout),
      param("mediator reaction timeout", _.mediatorReactionTimeout),
      param("assignment exclusivity timeout", _.assignmentExclusivityTimeout),
      param("topology change delay", _.topologyChangeDelay),
      param("ledger time record time tolerance", _.ledgerTimeRecordTimeTolerance),
      param("mediator deduplication timeout", _.mediatorDeduplicationTimeout),
      param("reconciliation interval", _.reconciliationInterval),
      param("confirmation requests max rate", _.confirmationRequestsMaxRate),
      param("max request size", _.maxRequestSize.value),
      param("sequencer aggregate submission timeout", _.sequencerAggregateSubmissionTimeout),
      paramIfDefined("traffic control", _.trafficControl),
      paramIfDefined("ACS commitment catchup", _.acsCommitmentsCatchUp),
      param("participant synchronizer limits", _.participantSynchronizerLimits),
      param("submission time record time tolerance", _.submissionTimeRecordTimeTolerance),
      param("onboarding restriction", _.onboardingRestriction),
    )

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
      trafficControl: Option[TrafficControlParameters] = trafficControl,
      onboardingRestriction: OnboardingRestriction = onboardingRestriction,
      acsCommitmentsCatchUpParameters: Option[AcsCommitmentsCatchUpParameters] =
        acsCommitmentsCatchUp,
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
    trafficControl = trafficControl,
    onboardingRestriction = onboardingRestriction,
    acsCommitmentsCatchUp = acsCommitmentsCatchUpParameters,
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
          trafficControl = trafficControl.map(_.toInternal),
          onboardingRestriction =
            onboardingRestriction.transformInto[OnboardingRestrictionInternal],
          acsCommitmentsCatchUpParameters = acsCommitmentsCatchUp
            .map(_.transformInto[AcsCommitmentsCatchUpParametersInternal]),
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

sealed trait OnboardingRestriction extends Product with Serializable with PrettyPrinting {
  def toProtoV30: v30.OnboardingRestriction
  def isLocked: Boolean
  def isRestricted: Boolean
  final def isOpen: Boolean = !isLocked
}

object OnboardingRestriction {
  def fromProtoV30(
      onboardingRestrictionP: v30.OnboardingRestriction
  ): ParsingResult[OnboardingRestriction] = onboardingRestrictionP match {
    case v30.OnboardingRestriction.ONBOARDING_RESTRICTION_UNRESTRICTED_OPEN =>
      Right(UnrestrictedOpen)
    case v30.OnboardingRestriction.ONBOARDING_RESTRICTION_UNRESTRICTED_LOCKED =>
      Right(UnrestrictedLocked)
    case v30.OnboardingRestriction.ONBOARDING_RESTRICTION_RESTRICTED_OPEN => Right(RestrictedOpen)
    case v30.OnboardingRestriction.ONBOARDING_RESTRICTION_RESTRICTED_LOCKED =>
      Right(RestrictedLocked)
    case v30.OnboardingRestriction.Unrecognized(value) =>
      Left(ProtoDeserializationError.UnrecognizedEnum("onboarding_restriction", value))
    case v30.OnboardingRestriction.ONBOARDING_RESTRICTION_UNSPECIFIED =>
      Left(ProtoDeserializationError.FieldNotSet("onboarding_restriction"))
  }

  /** Anyone can join */
  final case object UnrestrictedOpen extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_UNRESTRICTED_OPEN

    override def isLocked: Boolean = false
    override def isRestricted: Boolean = false

    override def pretty: Pretty[UnrestrictedOpen.type] = prettyOfObject[UnrestrictedOpen.type]
  }

  /** In theory, anyone can join, except now, the registration procedure is closed */
  final case object UnrestrictedLocked extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_UNRESTRICTED_LOCKED

    override def isLocked: Boolean = true
    override def isRestricted: Boolean = false

    override def pretty: Pretty[UnrestrictedLocked.type] = prettyOfObject[UnrestrictedLocked.type]
  }

  /** Only participants on the allowlist can join
    *
    * Requires the synchronizer owners to issue a valid ParticipantSynchronizerPermission transaction
    */
  final case object RestrictedOpen extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_RESTRICTED_OPEN

    override def isLocked: Boolean = false
    override def isRestricted: Boolean = true

    override def pretty: Pretty[RestrictedOpen.type] = prettyOfObject[RestrictedOpen.type]

  }

  /** Only participants on the allowlist can join in theory, except now, the registration procedure is closed */
  final case object RestrictedLocked extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_RESTRICTED_LOCKED

    override def isLocked: Boolean = true
    override def isRestricted: Boolean = true

    override def pretty: Pretty[RestrictedLocked.type] = prettyOfObject[RestrictedLocked.type]
  }
}

final case class AcsCommitmentsCatchUpParameters(
    catchUpIntervalSkip: PositiveInt,
    nrIntervalsToTriggerCatchUp: PositiveInt,
) extends PrettyPrinting {
  override protected def pretty: Pretty[AcsCommitmentsCatchUpParameters] =
    prettyOfClass(
      param("catch up interval skip", _.catchUpIntervalSkip),
      param("number of intervals to trigger catch up", _.nrIntervalsToTriggerCatchUp),
    )
}
