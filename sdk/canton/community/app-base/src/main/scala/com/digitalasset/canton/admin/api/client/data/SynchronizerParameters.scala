// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.config.CryptoConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
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
  RemoteClock,
  SimClock,
}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion}
import com.digitalasset.canton.{ProtoDeserializationError, config, crypto as SynchronizerCrypto}
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
    topologyChangeDelay: config.NonNegativeFiniteDuration,
    enableTransparencyChecks: Boolean,
    protocolVersion: ProtocolVersion,
    serial: NonNegativeInt,
) extends PrettyPrinting {
  def writeToFile(outputFile: String): Unit =
    BinaryFileUtil.writeByteStringToFile(outputFile, toInternal.toByteString)

  def toInternal: StaticSynchronizerParametersInternal =
    this.transformInto[StaticSynchronizerParametersInternal]: @nowarn(
      "msg=Der in object CryptoKeyFormat is deprecated"
    )

  override protected def pretty: Pretty[StaticSynchronizerParameters] = prettyOfClass(
    param("required signing specs", _.requiredSigningSpecs),
    param("required encryption specs", _.requiredEncryptionSpecs),
    param("required symmetric key schemes", _.requiredSymmetricKeySchemes),
    param("required hash algorithms", _.requiredHashAlgorithms),
    param("required crypto key formats", _.requiredCryptoKeyFormats),
    param("topology change delay", _.topologyChangeDelay),
    param("protocol version", _.protocolVersion),
    param("serial", _.serial),
  )
}

object StaticSynchronizerParameters {

  // This method is unsafe. Not prefixing by `try` to have nicer docs snippets.
  def fromConfig(
      config: SynchronizerParametersConfig,
      cryptoConfig: CryptoConfig,
      protocolVersion: ProtocolVersion,
      serial: NonNegativeInt = NonNegativeInt.zero,
  ): StaticSynchronizerParameters = {
    val internal = config
      .toStaticSynchronizerParameters(cryptoConfig, protocolVersion, serial)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Cannot instantiate static synchronizer parameters: $err"
        )
      )

    StaticSynchronizerParameters(internal)
  }

  def defaultsWithoutKMS(
      protocolVersion: ProtocolVersion,
      serial: NonNegativeInt = NonNegativeInt.zero,
      topologyChangeDelay: config.NonNegativeFiniteDuration =
        StaticSynchronizerParametersInternal.defaultTopologyChangeDelay.toConfig,
  ): StaticSynchronizerParameters =
    defaults(CryptoConfig(), protocolVersion, serial, topologyChangeDelay)

  // This method is unsafe. Not prefixing by `try` to have nicer docs snippets.
  def defaults(
      cryptoConfig: CryptoConfig,
      protocolVersion: ProtocolVersion,
      serial: NonNegativeInt = NonNegativeInt.zero,
      topologyChangeDelay: config.NonNegativeFiniteDuration =
        StaticSynchronizerParametersInternal.defaultTopologyChangeDelay.toConfig,
  ): StaticSynchronizerParameters = {
    val internal = SynchronizerParametersConfig(topologyChangeDelay = Some(topologyChangeDelay))
      .toStaticSynchronizerParameters(cryptoConfig, protocolVersion, serial)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Cannot instantiate static synchronizer parameters: $err"
        )
      )
    StaticSynchronizerParameters(internal)
  }

  private[canton] def initialValues(
      clock: Clock,
      protocolVersion: ProtocolVersion,
      serial: NonNegativeInt = NonNegativeInt.zero,
  ): StaticSynchronizerParameters = {
    val topologyChangeDelay = clock match {
      case _: SimClock | _: RemoteClock =>
        StaticSynchronizerParametersInternal.defaultTopologyChangeDelayNonStandardClock
      case _ => StaticSynchronizerParametersInternal.defaultTopologyChangeDelay
    }
    defaultsWithoutKMS(protocolVersion, serial, topologyChangeDelay.toConfig)
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
      serialP,
      enableTransparencyChecks,
      topologyChangeDelayP,
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
      topologyChangeDelay <- ProtoConverter.parseRequired(
        config.NonNegativeFiniteDuration.fromProtoPrimitive("topology_change_delay")(_),
        "topology_change_delay",
        topologyChangeDelayP,
      )
      // Data in the console is not really validated, so we allow for deleted
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP, allowDeleted = true)
      serial <- ProtoConverter.parseNonNegativeInt("serial", serialP)
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
        topologyChangeDelay.toInternal,
        enableTransparencyChecks,
        protocolVersion,
        serial,
      )
    )
  }
}

// TODO(#15650) Properly expose new BFT parameters and synchronizer limits
final case class DynamicSynchronizerParameters(
    confirmationResponseTimeout: config.NonNegativeFiniteDuration,
    mediatorReactionTimeout: config.NonNegativeFiniteDuration,
    assignmentExclusivityTimeout: config.NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: config.NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: config.NonNegativeFiniteDuration,
    reconciliationInterval: config.PositiveDurationSeconds,
    maxRequestSize: NonNegativeInt,
    sequencerAggregateSubmissionTimeout: config.NonNegativeFiniteDuration,
    trafficControl: Option[TrafficControlParameters],
    onboardingRestriction: OnboardingRestriction,
    acsCommitmentsCatchUp: Option[AcsCommitmentsCatchUpParameters],
    participantSynchronizerLimits: ParticipantSynchronizerLimits,
    preparationTimeRecordTimeTolerance: config.NonNegativeFiniteDuration,
) extends PrettyPrinting {

  def decisionTimeout: config.NonNegativeFiniteDuration =
    confirmationResponseTimeout + mediatorReactionTimeout

  @inline def confirmationRequestsMaxRate: NonNegativeInt =
    participantSynchronizerLimits.confirmationRequestsMaxRate

  if (preparationTimeRecordTimeTolerance * 2 > mediatorDeduplicationTimeout)
    throw new InvalidDynamicSynchronizerParameters(
      s"The preparationTimeRecordTimeTolerance ($preparationTimeRecordTimeTolerance) must be at most half of the " +
        s"mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
    )

  // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.1dzc6dxxlpca
  // Originally the validation was done on ledgerTimeRecordTimeTolerance, but was moved to preparationTimeRecordTimeTolerance
  // instead when the parameter was introduced
  private[canton] def compatibleWithNewPreparationTimeRecordTimeTolerance(
      newPreparationTimeRecordTimeTolerance: config.NonNegativeFiniteDuration
  ): Boolean =
    // If false, a new request may receive the same submission time as a previous request and the previous
    // request may be evicted too early from the mediator's deduplication store.
    // Thus, an attacker may assign the same UUID to both requests.
    // See i9028 for a detailed design. (This is the second clause of item 2 of Lemma 2).
    preparationTimeRecordTimeTolerance + newPreparationTimeRecordTimeTolerance <= mediatorDeduplicationTimeout

  override protected def pretty: Pretty[DynamicSynchronizerParameters] =
    prettyOfClass(
      param("confirmation response timeout", _.confirmationResponseTimeout),
      param("mediator reaction timeout", _.mediatorReactionTimeout),
      param("assignment exclusivity timeout", _.assignmentExclusivityTimeout),
      param("ledger time record time tolerance", _.ledgerTimeRecordTimeTolerance),
      param("mediator deduplication timeout", _.mediatorDeduplicationTimeout),
      param("reconciliation interval", _.reconciliationInterval),
      param("confirmation requests max rate", _.confirmationRequestsMaxRate),
      param("max request size", _.maxRequestSize.value),
      param("sequencer aggregate submission timeout", _.sequencerAggregateSubmissionTimeout),
      paramIfDefined("traffic control", _.trafficControl),
      paramIfDefined("ACS commitment catchup", _.acsCommitmentsCatchUp),
      param("participant synchronizer limits", _.participantSynchronizerLimits),
      param("preparation time record time tolerance", _.preparationTimeRecordTimeTolerance),
      param("onboarding restriction", _.onboardingRestriction),
    )

  def update(
      confirmationResponseTimeout: config.NonNegativeFiniteDuration = confirmationResponseTimeout,
      mediatorReactionTimeout: config.NonNegativeFiniteDuration = mediatorReactionTimeout,
      assignmentExclusivityTimeout: config.NonNegativeFiniteDuration = assignmentExclusivityTimeout,
      ledgerTimeRecordTimeTolerance: config.NonNegativeFiniteDuration =
        ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout: config.NonNegativeFiniteDuration = mediatorDeduplicationTimeout,
      reconciliationInterval: config.PositiveDurationSeconds = reconciliationInterval,
      confirmationRequestsMaxRate: NonNegativeInt = confirmationRequestsMaxRate,
      maxRequestSize: NonNegativeInt = maxRequestSize,
      sequencerAggregateSubmissionTimeout: config.NonNegativeFiniteDuration =
        sequencerAggregateSubmissionTimeout,
      trafficControl: Option[TrafficControlParameters] = trafficControl,
      onboardingRestriction: OnboardingRestriction = onboardingRestriction,
      acsCommitmentsCatchUpParameters: Option[AcsCommitmentsCatchUpParameters] =
        acsCommitmentsCatchUp,
      preparationTimeRecordTimeTolerance: config.NonNegativeFiniteDuration =
        preparationTimeRecordTimeTolerance,
  ): DynamicSynchronizerParameters = this.copy(
    confirmationResponseTimeout = confirmationResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    assignmentExclusivityTimeout = assignmentExclusivityTimeout,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
    mediatorDeduplicationTimeout = mediatorDeduplicationTimeout,
    reconciliationInterval = reconciliationInterval,
    maxRequestSize = maxRequestSize,
    sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
    trafficControl = trafficControl,
    onboardingRestriction = onboardingRestriction,
    acsCommitmentsCatchUp = acsCommitmentsCatchUpParameters,
    participantSynchronizerLimits = ParticipantSynchronizerLimits(confirmationRequestsMaxRate),
    preparationTimeRecordTimeTolerance = preparationTimeRecordTimeTolerance,
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
          preparationTimeRecordTimeTolerance =
            InternalNonNegativeFiniteDuration.fromConfig(preparationTimeRecordTimeTolerance),
        )(rpv)
      }
}

object DynamicSynchronizerParameters {

  private[canton] def initialValues(protocolVersion: ProtocolVersion) =
    DynamicSynchronizerParameters(
      DynamicSynchronizerParametersInternal.initialValues(protocolVersion)
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
    * Requires the synchronizer owners to issue a valid ParticipantSynchronizerPermission
    * transaction
    */
  final case object RestrictedOpen extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_RESTRICTED_OPEN

    override def isLocked: Boolean = false
    override def isRestricted: Boolean = true

    override def pretty: Pretty[RestrictedOpen.type] = prettyOfObject[RestrictedOpen.type]

  }

  /** Only participants on the allowlist can join in theory, except now, the registration procedure
    * is closed
    */
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
