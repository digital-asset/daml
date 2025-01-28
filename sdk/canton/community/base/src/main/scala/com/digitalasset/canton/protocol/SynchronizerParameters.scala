// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.instances.option.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters.InvalidDynamicSynchronizerParameters
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration,
  PositiveSeconds,
  RemoteClock,
  SimClock,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.transaction.ParticipantSynchronizerLimits
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{ProtoDeserializationError, checked}

import scala.concurrent.Future

object SynchronizerParameters {

  /** This class is used to represent synchronizer parameter(s) that can come from static
    * synchronizer parameters or dynamic ones, depending on the protocol version.
    * @param validFrom If the parameter comes from dynamic parameters, exclusive
    *                  timestamp coming from the topology transaction, otherwise, CantonTimestamp.MinValue
    * @param validUntil If the parameter comes from dynamic parameters, timestamp
    *                   coming from the topology transaction, otherwise None
    */
  final case class WithValidity[+P](
      validFrom: CantonTimestamp,
      validUntil: Option[CantonTimestamp],
      parameter: P,
  ) {
    def map[T](f: P => T): WithValidity[T] =
      WithValidity(validFrom, validUntil, f(parameter))
    def isValidAt(ts: CantonTimestamp) = validFrom < ts && validUntil.forall(ts <= _)

    def emptyInterval: Boolean = validUntil.contains(validFrom)
  }

  final case class MaxRequestSize(value: NonNegativeInt) extends AnyVal {
    def unwrap: Int = value.unwrap
  }
}

final case class StaticSynchronizerParameters(
    requiredSigningSpecs: RequiredSigningSpecs,
    requiredEncryptionSpecs: RequiredEncryptionSpecs,
    requiredSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]],
    requiredHashAlgorithms: NonEmpty[Set[HashAlgorithm]],
    requiredCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]],
    protocolVersion: ProtocolVersion,
) extends HasProtocolVersionedWrapper[StaticSynchronizerParameters] {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    StaticSynchronizerParameters.type
  ] = StaticSynchronizerParameters.protocolVersionRepresentativeFor(protocolVersion)

  @transient override protected lazy val companionObj: StaticSynchronizerParameters.type =
    StaticSynchronizerParameters

  def toProtoV30: v30.StaticSynchronizerParameters =
    v30.StaticSynchronizerParameters(
      requiredSigningSpecs = Some(requiredSigningSpecs.toProtoV30),
      requiredEncryptionSpecs = Some(requiredEncryptionSpecs.toProtoV30),
      requiredSymmetricKeySchemes = requiredSymmetricKeySchemes.toSeq.map(_.toProtoEnum),
      requiredHashAlgorithms = requiredHashAlgorithms.toSeq.map(_.toProtoEnum),
      requiredCryptoKeyFormats = requiredCryptoKeyFormats.toSeq.map(_.toProtoEnum),
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
}
object StaticSynchronizerParameters
    extends VersioningCompanion[StaticSynchronizerParameters]
    with ProtocolVersionedCompanionDbHelpers[StaticSynchronizerParameters] {

  // Note: if you need static synchronizer parameters for testing, look at BaseTest.defaultStaticSynchronizerParametersWith

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(
      v30.StaticSynchronizerParameters
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "static synchronizer parameters"

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
      requiredSigningSpecs <- RequiredSigningSpecs.fromProtoV30(requiredSigningSpecsP)
      requiredEncryptionSpecsP <- requiredEncryptionSpecsOP.toRight(
        ProtoDeserializationError.FieldNotSet(
          "required_encryption_specs"
        )
      )
      requiredEncryptionSpecs <- RequiredEncryptionSpecs.fromProtoV30(requiredEncryptionSpecsP)
      requiredSymmetricKeySchemes <- requiredKeySchemes(
        "required_symmetric_key_schemes",
        requiredSymmetricKeySchemesP,
        SymmetricKeyScheme.fromProtoEnum,
      )
      requiredHashAlgorithms <- requiredKeySchemes(
        "required_hash_algorithms",
        requiredHashAlgorithmsP,
        HashAlgorithm.fromProtoEnum,
      )
      requiredCryptoKeyFormats <- requiredKeySchemes(
        "required_crypto_key_formats",
        requiredCryptoKeyFormatsP,
        CryptoKeyFormat.fromProtoEnum,
      )
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP)
    } yield StaticSynchronizerParameters(
      requiredSigningSpecs,
      requiredEncryptionSpecs,
      requiredSymmetricKeySchemes,
      requiredHashAlgorithms,
      requiredCryptoKeyFormats,
      protocolVersion,
    )
  }
}

/** Onboarding restrictions for new participants joining a synchronizer
  *
  * The synchronizer administrators can set onboarding restrictions to control
  * which participant can join the synchronizer.
  */
sealed trait OnboardingRestriction extends Product with Serializable {
  def toProtoV30: v30.OnboardingRestriction
  def isLocked: Boolean
  def isRestricted: Boolean
  final def isOpen: Boolean = !isLocked
  final def isUnrestricted: Boolean = !isRestricted
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
  }

  /** In theory, anyone can join, except now, the registration procedure is closed */
  final case object UnrestrictedLocked extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_UNRESTRICTED_LOCKED

    override def isLocked: Boolean = true
    override def isRestricted: Boolean = false
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
  }

  /** Only participants on the allowlist can join in theory, except now, the registration procedure is closed */
  final case object RestrictedLocked extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_RESTRICTED_LOCKED

    override def isLocked: Boolean = true
    override def isRestricted: Boolean = true
  }

}

/** @param confirmationResponseTimeout the amount of time (w.r.t. the sequencer clock) that a participant may take
  *                                   to validate a command and send a response.
  *                                   Once the timeout has elapsed for a request,
  *                                   the mediator will discard all responses for that request.
  *                                   Choose a lower value to reduce the time to reject a command in case one of the
  *                                   involved participants has high load / operational problems.
  *                                   Choose a higher value to reduce the likelihood of commands being rejected
  *                                   due to timeouts.
  * @param mediatorReactionTimeout the maximum amount of time (w.r.t. the sequencer clock) that the mediator may take
  *                                to validate the responses for a request and broadcast the result message.
  *                                The mediator reaction timeout starts when the confirmation response timeout has elapsed.
  *                                If the mediator does not send a result message within that timeout,
  *                                participants must rollback the transaction underlying the request.
  *                                Also applies to determine the max-sequencing-time of daml 3.x topology transactions
  *                                governed by mediator group.
  *                                Choose a lower value to reduce the time to learn whether a command
  *                                has been accepted.
  *                                Choose a higher value to reduce the likelihood of commands being rejected
  *                                due to timeouts.
  * @param assignmentExclusivityTimeout this timeout affects who can initiate the assignment.
  *                                   Before the timeout, only the submitter of the unassignment can initiate the
  *                                   corresponding assignment.
  *                                   From the timeout onwards, every stakeholder of the contract can initiate the assignment,
  *                                   if it has not yet happened.
  *                                   Moreover, if this timeout is zero, no automatic assignments will occur.
  *                                   Choose a low value, if you want to lower the time that contracts can be inactive
  *                                   due to ongoing reassignments.
  *                                   Choosing a high value currently has no practical benefit, but
  *                                   will have benefits in a future version.
  * TODO(M41): Document those benefits
  * @param topologyChangeDelay determines the offset applied to the topology transactions before they become active,
  *                            in order to support parallel transaction processing
  * @param ledgerTimeRecordTimeTolerance the maximum absolute difference between the ledger time and the
  *                                      record time of a command.
  *                                      If the absolute difference would be larger for a command,
  *                                      then the command must be rejected.
  * @param mediatorDeduplicationTimeout the time for how long a request will be stored at the mediator for deduplication
  *                                     purposes. This must be at least twice the `submissionTimeRecordTimeTolerance`.
  *                                     It is fine to choose the minimal value, unless you plan to subsequently
  *                                     increase `submissionTimeRecordTimeTolerance.`
  * @param reconciliationInterval The size of the reconciliation interval (minimum duration between two ACS commitments).
  *                               Note: default to [[StaticSynchronizerParameters.defaultReconciliationInterval]] for backward
  *                               compatibility.
  *                               Should be significantly longer than the period of time it takes to compute the commitment and have it sequenced of the synchronizer.
  *                               Otherwise, ACS commitments will keep being exchanged continuously on an idle synchronizer.
  * @param maxRequestSize maximum size of messages (in bytes) that the synchronizer can receive through the public API
  * @param sequencerAggregateSubmissionTimeout the maximum time for how long an incomplete aggregate submission request is
  *                                            allowed to stay pending in the sequencer's state before it's removed.
  *                                            Must be at least `confirmationResponseTimeout` + `mediatorReactionTimeout` in a practical system.
  *                                            Must be greater than `maxSequencingTime` specified by a participant,
  *                                            practically also requires extra slack to allow clock skew between participant and sequencer.
  * @param onboardingRestriction current onboarding restrictions for participants
  * @param acsCommitmentsCatchUpConfig   Optional parameters of type [[com.digitalasset.canton.protocol.AcsCommitmentsCatchUpConfig]].
  *                                      Defined starting with protobuf version v2 and protocol version v30.
  *                                      If None, the catch-up mode is disabled: the participant does not trigger the
  *                                      catch-up mode when lagging behind.
  *                                      If not None, it specifies the number of reconciliation intervals that the
  *                                      participant skips in catch-up mode, and the number of catch-up intervals
  *                                      intervals a participant should lag behind in order to enter catch-up mode.
  * @param submissionTimeRecordTimeTolerance the maximum absolute difference between the submission time and the
  *                                 record time of a command.
  *                                 If the absolute difference would be larger for a command,
  *                                 then the command must be rejected.
  *                                 Defaults to [[ledgerTimeRecordTimeTolerance]] if not set when deserializing from proto.
  * @throws DynamicSynchronizerParameters$.InvalidDynamicSynchronizerParameters
  *   if `mediatorDeduplicationTimeout` is less than twice of `submissionTimeRecordTimeTolerance`.
  */
final case class DynamicSynchronizerParameters private (
    confirmationResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    assignmentExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveSeconds,
    maxRequestSize: MaxRequestSize,
    sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
    trafficControlParameters: Option[TrafficControlParameters],
    onboardingRestriction: OnboardingRestriction,
    acsCommitmentsCatchUpConfig: Option[AcsCommitmentsCatchUpConfig],
    participantSynchronizerLimits: ParticipantSynchronizerLimits,
    submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      DynamicSynchronizerParameters.type
    ]
) extends HasProtocolVersionedWrapper[DynamicSynchronizerParameters]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: DynamicSynchronizerParameters.type =
    DynamicSynchronizerParameters

  @inline def confirmationRequestsMaxRate: NonNegativeInt =
    participantSynchronizerLimits.confirmationRequestsMaxRate

  // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.jtqcu52qpf82
  if (
    submissionTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2) > mediatorDeduplicationTimeout
  )
    throw new InvalidDynamicSynchronizerParameters(
      s"The submissionTimeRecordTimeTolerance ($submissionTimeRecordTimeTolerance) must be at most half of the " +
        s"mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
    )

  /** In some situations, the sequencer processes submission requests with a slightly outdated topology snapshot.
    * This is to allow recipients to verify sequencer signatures when the sequencer keys have been rolled over and
    * they have not yet received the new keys, and to resolve group addresses using a sender-specified snapshot.
    * This parameter determines how much outdated the signing key or the group address resolution can be.
    * Choose a higher value to avoid that the sequencer refuses to sign and send messages.
    * Choose a lower value to reduce the latency of sequencer key rollovers and updates of group addresses.
    * The sequencer topology tolerance must be at least `confirmationResponseTimeout + mediatorReactionTimeout`.
    */
  def sequencerTopologyTimestampTolerance: NonNegativeFiniteDuration =
    (confirmationResponseTimeout + mediatorReactionTimeout) * NonNegativeInt.tryCreate(2)

  /** Submitters compute the submission cost of their request before sending it, using the same topology used to sign
    * the SubmissionRequest. This is to provide an upper bound to the synchronizer on how much they commit to spend
    * for the sequencing and delivery of the submission.
    * Concurrent topology changes to the submission can result in the cost computed by the submitter being wrong at
    * sequencing time. This parameter determines how outdated the topology used to compute the cost can be.
    * If within the tolerance window, the submitted cost will be deducted (pending enough traffic is available) and the event
    * will be delivered.
    * If outside the tolerance window, no cost will be deducted but the event will not be delivered.
    * It is the responsibility of the sequencer that received the request to do this check ahead of time and not
    * let outdated requests be sequenced. After sequencing, such events will be reported via metrics as "wasted" traffic
    * and tied to the sequencer who processed the request for tracing and accountability.
    * The timestamp checked against this parameter will be the one used to sign the submission request, not
    * the one in the submission request itself.
    *
    * Note: Current value is equal to [[sequencerTopologyTimestampTolerance]] to get the same behavior in terms of
    * tolerance as senders get for the topology timestamp specified in the SubmissionRequest.
    */
  def submissionCostTimestampTopologyTolerance: NonNegativeFiniteDuration =
    sequencerTopologyTimestampTolerance

  def automaticAssignmentEnabled: Boolean =
    assignmentExclusivityTimeout > NonNegativeFiniteDuration.Zero

  def update(
      assignmentExclusivityTimeout: NonNegativeFiniteDuration = assignmentExclusivityTimeout,
      reconciliationInterval: PositiveSeconds = reconciliationInterval,
      confirmationRequestsMaxRate: NonNegativeInt = confirmationRequestsMaxRate,
      maxRequestSize: MaxRequestSize = maxRequestSize,
  ): DynamicSynchronizerParameters =
    this.copy(
      assignmentExclusivityTimeout = assignmentExclusivityTimeout,
      reconciliationInterval = reconciliationInterval,
      maxRequestSize = maxRequestSize,
      participantSynchronizerLimits =
        ParticipantSynchronizerLimits(confirmationRequestsMaxRate = confirmationRequestsMaxRate),
    )(representativeProtocolVersion)

  def tryUpdate(
      confirmationResponseTimeout: NonNegativeFiniteDuration = confirmationResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      assignmentExclusivityTimeout: NonNegativeFiniteDuration = assignmentExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration = mediatorDeduplicationTimeout,
      reconciliationInterval: PositiveSeconds = reconciliationInterval,
      confirmationRequestsMaxRate: NonNegativeInt = confirmationRequestsMaxRate,
      maxRequestSize: MaxRequestSize = maxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
        sequencerAggregateSubmissionTimeout,
      trafficControlParameters: Option[TrafficControlParameters] = trafficControlParameters,
      onboardingRestriction: OnboardingRestriction = onboardingRestriction,
      acsCommitmentsCatchUpConfigParameter: Option[AcsCommitmentsCatchUpConfig] =
        acsCommitmentsCatchUpConfig,
      submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration =
        submissionTimeRecordTimeTolerance,
  ): DynamicSynchronizerParameters = DynamicSynchronizerParameters.tryCreate(
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
    acsCommitmentsCatchUpConfigParameter = acsCommitmentsCatchUpConfigParameter,
    participantSynchronizerLimits = ParticipantSynchronizerLimits(confirmationRequestsMaxRate),
    submissionTimeRecordTimeTolerance = submissionTimeRecordTimeTolerance,
  )(representativeProtocolVersion)

  def toProtoV30: v30.DynamicSynchronizerParameters = v30.DynamicSynchronizerParameters(
    confirmationResponseTimeout = Some(confirmationResponseTimeout.toProtoPrimitive),
    mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
    assignmentExclusivityTimeout = Some(assignmentExclusivityTimeout.toProtoPrimitive),
    topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
    ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
    mediatorDeduplicationTimeout = Some(mediatorDeduplicationTimeout.toProtoPrimitive),
    reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
    maxRequestSize = maxRequestSize.unwrap,
    onboardingRestriction = onboardingRestriction.toProtoV30,
    participantSynchronizerLimits = Some(participantSynchronizerLimits.toProto),
    sequencerAggregateSubmissionTimeout =
      Some(sequencerAggregateSubmissionTimeout.toProtoPrimitive),
    trafficControlParameters = trafficControlParameters.map(_.toProtoV30),
    acsCommitmentsCatchupConfig = acsCommitmentsCatchUpConfig.map(_.toProtoV30),
    submissionTimeRecordTimeTolerance = Some(submissionTimeRecordTimeTolerance.toProtoPrimitive),
  )

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
      paramIfDefined("traffic control config", _.trafficControlParameters),
      paramIfDefined("ACS commitment catchup config", _.acsCommitmentsCatchUpConfig),
      param("participant synchronizer limits", _.participantSynchronizerLimits),
      param("submission time record time tolerance", _.submissionTimeRecordTimeTolerance),
    )
}

object DynamicSynchronizerParameters extends VersioningCompanion[DynamicSynchronizerParameters] {

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(
      v30.DynamicSynchronizerParameters
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "dynamic synchronizer parameters"

  lazy val defaultReconciliationInterval: PositiveSeconds = PositiveSeconds.tryOfSeconds(60)
  lazy val defaultConfirmationRequestsMaxRate: NonNegativeInt = NonNegativeInt.tryCreate(1000000)
  lazy val defaultParticipantSynchronizerLimits: ParticipantSynchronizerLimits =
    ParticipantSynchronizerLimits(
      defaultConfirmationRequestsMaxRate
    )
  lazy val defaultMaxRequestSize: MaxRequestSize = MaxRequestSize(
    NonNegativeInt.tryCreate(10 * 1024 * 1024)
  )

  private val defaultConfirmationResponseTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(30)
  private val defaultMediatorReactionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(30)

  private val defaultAssignmentExclusivityTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(60)

  private val defaultTrafficControlParameters: Option[TrafficControlParameters] =
    Option.empty[TrafficControlParameters]

  private val defaultTopologyChangeDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfMillis(250)
  private val defaultTopologyChangeDelayNonStandardClock: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.Zero // SimClock, RemoteClock

  private val defaultLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(60)

  private val defaultSubmissionTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    defaultLedgerTimeRecordTimeTolerance

  private val defaultMediatorDeduplicationTimeout: NonNegativeFiniteDuration =
    defaultLedgerTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2)

  // Based on SequencerClientConfig.defaultMaxSequencingTimeOffset + 1 minute of slack for the clock drift
  private val defaultSequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfMinutes(6)

  private val defaultOnboardingRestriction: OnboardingRestriction =
    OnboardingRestriction.UnrestrictedOpen

  private val defaultAcsCommitmentsCatchUp: Option[AcsCommitmentsCatchUpConfig] = Some(
    AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(5), PositiveInt.tryCreate(2))
  )

  /** Safely creates DynamicSynchronizerParameters.
    *
    * @return `Left(...)` if `mediatorDeduplicationTimeout` is less than twice of `submissionTimeRecordTimeTolerance`.
    */
  private def create(
      confirmationResponseTimeout: NonNegativeFiniteDuration,
      mediatorReactionTimeout: NonNegativeFiniteDuration,
      assignmentExclusivityTimeout: NonNegativeFiniteDuration,
      topologyChangeDelay: NonNegativeFiniteDuration,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
      reconciliationInterval: PositiveSeconds,
      maxRequestSize: MaxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
      trafficControlConfig: Option[TrafficControlParameters],
      onboardingRestriction: OnboardingRestriction,
      acsCommitmentsCatchUpConfig: Option[AcsCommitmentsCatchUpConfig],
      participantSynchronizerLimits: ParticipantSynchronizerLimits,
      submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration,
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion[
        DynamicSynchronizerParameters.type
      ]
  ): Either[InvalidDynamicSynchronizerParameters, DynamicSynchronizerParameters] =
    Either.catchOnly[InvalidDynamicSynchronizerParameters](
      tryCreate(
        confirmationResponseTimeout,
        mediatorReactionTimeout,
        assignmentExclusivityTimeout,
        topologyChangeDelay,
        ledgerTimeRecordTimeTolerance,
        mediatorDeduplicationTimeout,
        reconciliationInterval,
        maxRequestSize,
        sequencerAggregateSubmissionTimeout,
        trafficControlConfig,
        onboardingRestriction,
        acsCommitmentsCatchUpConfig,
        participantSynchronizerLimits,
        submissionTimeRecordTimeTolerance,
      )(representativeProtocolVersion)
    )

  /** Creates DynamicSynchronizerParameters
    *
    * @throws InvalidDynamicSynchronizerParameters if `mediatorDeduplicationTimeout` is less than twice of `submissionTimeRecordTimeTolerance`.
    */
  def tryCreate(
      confirmationResponseTimeout: NonNegativeFiniteDuration,
      mediatorReactionTimeout: NonNegativeFiniteDuration,
      assignmentExclusivityTimeout: NonNegativeFiniteDuration,
      topologyChangeDelay: NonNegativeFiniteDuration,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
      reconciliationInterval: PositiveSeconds,
      maxRequestSize: MaxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
      trafficControlParameters: Option[TrafficControlParameters],
      onboardingRestriction: OnboardingRestriction,
      acsCommitmentsCatchUpConfigParameter: Option[AcsCommitmentsCatchUpConfig],
      participantSynchronizerLimits: ParticipantSynchronizerLimits,
      submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration,
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion[
        DynamicSynchronizerParameters.type
      ]
  ): DynamicSynchronizerParameters =
    DynamicSynchronizerParameters(
      confirmationResponseTimeout,
      mediatorReactionTimeout,
      assignmentExclusivityTimeout,
      topologyChangeDelay,
      ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout,
      reconciliationInterval,
      maxRequestSize,
      sequencerAggregateSubmissionTimeout,
      trafficControlParameters,
      onboardingRestriction,
      acsCommitmentsCatchUpConfigParameter,
      participantSynchronizerLimits,
      submissionTimeRecordTimeTolerance,
    )(representativeProtocolVersion)

  /** Default dynamic synchronizer parameters for non-static clocks */
  def defaultValues(protocolVersion: ProtocolVersion): DynamicSynchronizerParameters =
    initialValues(defaultTopologyChangeDelay, protocolVersion)

  def initialValues(
      topologyChangeDelay: NonNegativeFiniteDuration,
      protocolVersion: ProtocolVersion,
      mediatorReactionTimeout: NonNegativeFiniteDuration = defaultMediatorReactionTimeout,
  ): DynamicSynchronizerParameters = checked( // safe because default values are safe
    DynamicSynchronizerParameters.tryCreate(
      confirmationResponseTimeout = defaultConfirmationResponseTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      assignmentExclusivityTimeout = defaultAssignmentExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = defaultLedgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout = defaultMediatorDeduplicationTimeout,
      reconciliationInterval = DynamicSynchronizerParameters.defaultReconciliationInterval,
      maxRequestSize = DynamicSynchronizerParameters.defaultMaxRequestSize,
      sequencerAggregateSubmissionTimeout = defaultSequencerAggregateSubmissionTimeout,
      trafficControlParameters = defaultTrafficControlParameters,
      onboardingRestriction = defaultOnboardingRestriction,
      acsCommitmentsCatchUpConfigParameter = defaultAcsCommitmentsCatchUp,
      participantSynchronizerLimits =
        DynamicSynchronizerParameters.defaultParticipantSynchronizerLimits,
      submissionTimeRecordTimeTolerance = defaultSubmissionTimeRecordTimeTolerance,
    )(
      protocolVersionRepresentativeFor(protocolVersion)
    )
  )

  def tryInitialValues(
      topologyChangeDelay: NonNegativeFiniteDuration,
      protocolVersion: ProtocolVersion,
      confirmationRequestsMaxRate: NonNegativeInt =
        DynamicSynchronizerParameters.defaultConfirmationRequestsMaxRate,
      maxRequestSize: MaxRequestSize = DynamicSynchronizerParameters.defaultMaxRequestSize,
      mediatorReactionTimeout: NonNegativeFiniteDuration = defaultMediatorReactionTimeout,
      reconciliationInterval: PositiveSeconds =
        DynamicSynchronizerParameters.defaultReconciliationInterval,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
        defaultSequencerAggregateSubmissionTimeout,
      submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration =
        defaultSubmissionTimeRecordTimeTolerance,
  ) =
    DynamicSynchronizerParameters.tryCreate(
      confirmationResponseTimeout = defaultConfirmationResponseTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      assignmentExclusivityTimeout = defaultAssignmentExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = defaultLedgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout = defaultMediatorDeduplicationTimeout,
      reconciliationInterval = reconciliationInterval,
      maxRequestSize = maxRequestSize,
      sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
      trafficControlParameters = defaultTrafficControlParameters,
      onboardingRestriction = defaultOnboardingRestriction,
      acsCommitmentsCatchUpConfigParameter = defaultAcsCommitmentsCatchUp,
      participantSynchronizerLimits = ParticipantSynchronizerLimits(confirmationRequestsMaxRate),
      submissionTimeRecordTimeTolerance = submissionTimeRecordTimeTolerance,
    )(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def initialValues(
      clock: Clock,
      protocolVersion: ProtocolVersion,
  ): DynamicSynchronizerParameters = {
    val topologyChangeDelay = clock match {
      case _: RemoteClock | _: SimClock => defaultTopologyChangeDelayNonStandardClock
      case _ => defaultTopologyChangeDelay
    }
    initialValues(topologyChangeDelay, protocolVersion)
  }

  // if there is no topology change delay defined (or not yet propagated), we'll use this one
  val topologyChangeDelayIfAbsent: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero

  def fromProtoV30(
      synchronizerParametersP: v30.DynamicSynchronizerParameters
  ): ParsingResult[DynamicSynchronizerParameters] = {
    val v30.DynamicSynchronizerParameters(
      confirmationResponseTimeoutP,
      mediatorReactionTimeoutP,
      assignmentExclusivityTimeoutP,
      topologyChangeDelayP,
      ledgerTimeRecordTimeToleranceP,
      reconciliationIntervalP,
      mediatorDeduplicationTimeoutP,
      maxRequestSizeP,
      onboardingRestrictionP,
      defaultLimitsP,
      sequencerAggregateSubmissionTimeoutP,
      trafficControlConfigP,
      acsCommitmentCatchupConfigP,
      submissionTimeRecordTimeToleranceP,
    ) = synchronizerParametersP
    for {

      confirmationResponseTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "confirmationResponseTimeout"
      )(
        confirmationResponseTimeoutP
      )
      mediatorReactionTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "mediatorReactionTimeout"
      )(
        mediatorReactionTimeoutP
      )
      assignmentExclusivityTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "assignmentExclusivityTimeout"
      )(
        assignmentExclusivityTimeoutP
      )
      topologyChangeDelay <- NonNegativeFiniteDuration.fromProtoPrimitiveO("topologyChangeDelay")(
        topologyChangeDelayP
      )
      ledgerTimeRecordTimeTolerance <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "ledgerTimeRecordTimeTolerance"
      )(
        ledgerTimeRecordTimeToleranceP
      )

      reconciliationInterval <- PositiveSeconds.fromProtoPrimitiveO(
        "reconciliationInterval"
      )(
        reconciliationIntervalP
      )
      mediatorDeduplicationTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "mediatorDeduplicationTimeout"
      )(
        mediatorDeduplicationTimeoutP
      )

      maxRequestSize <- NonNegativeInt
        .create(maxRequestSizeP)
        .map(MaxRequestSize.apply)
        .leftMap(InvariantViolation.toProtoDeserializationError("max_request_size", _))

      sequencerAggregateSubmissionTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "sequencerAggregateSubmissionTimeout"
      )(
        sequencerAggregateSubmissionTimeoutP
      )

      trafficControlConfig <- trafficControlConfigP.traverse(TrafficControlParameters.fromProtoV30)

      onboardingRestriction <- OnboardingRestriction.fromProtoV30(onboardingRestrictionP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))

      acsCommitmentCatchupConfig <- acsCommitmentCatchupConfigP.traverse(
        AcsCommitmentsCatchUpConfig.fromProtoV30
      )

      participantSynchronizerLimits <- ProtoConverter
        .required("participant_synchronizer_limits", defaultLimitsP)
        .flatMap(ParticipantSynchronizerLimits.fromProtoV30)

      submissionTimeRecordTimeTolerance <- submissionTimeRecordTimeToleranceP
        .traverse(
          NonNegativeFiniteDuration.fromProtoPrimitive(
            "submissionTimeRecordTimeTolerance"
          )
        )
        // TODO(i16458) enforce this field is always set when 3.x is stable
        .map(_.getOrElse(ledgerTimeRecordTimeTolerance))

      synchronizerParameters <-
        create(
          confirmationResponseTimeout = confirmationResponseTimeout,
          mediatorReactionTimeout = mediatorReactionTimeout,
          assignmentExclusivityTimeout = assignmentExclusivityTimeout,
          topologyChangeDelay = topologyChangeDelay,
          ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
          mediatorDeduplicationTimeout = mediatorDeduplicationTimeout,
          reconciliationInterval = reconciliationInterval,
          maxRequestSize = maxRequestSize,
          sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
          trafficControlConfig = trafficControlConfig,
          onboardingRestriction = onboardingRestriction,
          acsCommitmentsCatchUpConfig = acsCommitmentCatchupConfig,
          participantSynchronizerLimits = participantSynchronizerLimits,
          submissionTimeRecordTimeTolerance = submissionTimeRecordTimeTolerance,
        )(rpv).leftMap(_.toProtoDeserializationError)
    } yield synchronizerParameters
  }

  class InvalidDynamicSynchronizerParameters(message: String) extends RuntimeException(message) {
    lazy val toProtoDeserializationError: ProtoDeserializationError.InvariantViolation =
      ProtoDeserializationError.InvariantViolation(field = None, error = message)
  }
}

/** Dynamic synchronizer parameters and their validity interval.
  * Mostly so that we can perform additional checks.
  *
  * @param validFrom Start point of the validity interval (exclusive)
  * @param validUntil End point of the validity interval (inclusive)
  */
final case class DynamicSynchronizerParametersWithValidity(
    parameters: DynamicSynchronizerParameters,
    validFrom: CantonTimestamp,
    validUntil: Option[CantonTimestamp],
    synchronizerId: SynchronizerId,
) {
  def map[T](f: DynamicSynchronizerParameters => T): SynchronizerParameters.WithValidity[T] =
    SynchronizerParameters.WithValidity(validFrom, validUntil, f(parameters))

  def isValidAt(ts: CantonTimestamp): Boolean =
    validFrom < ts && validUntil.forall(ts <= _)

  private def checkValidity(ts: CantonTimestamp, goal: String): Either[String, Unit] = Either.cond(
    isValidAt(ts),
    (),
    s"Cannot compute $goal for `$ts` because validity of parameters is ($validFrom, $validUntil]",
  )

  /** Computes the decision time for the given activeness time.
    *
    * @return Left if the synchronizer parameters are not valid at `activenessTime`, the decision time otherwise
    */
  def decisionTimeFor(activenessTime: CantonTimestamp): Either[String, CantonTimestamp] =
    checkValidity(activenessTime, "decision time").map(_ =>
      activenessTime
        .add(parameters.confirmationResponseTimeout.unwrap)
        .add(parameters.mediatorReactionTimeout.unwrap)
    )

  /** Computes the decision time for the given activeness time.
    *
    * @return Left if the synchronizer parameters are not valid at `activenessTime`, the decision time otherwise
    */
  def decisionTimeForF(activenessTime: CantonTimestamp): FutureUnlessShutdown[CantonTimestamp] =
    decisionTimeFor(activenessTime).fold(
      err => FutureUnlessShutdown.failed(new IllegalStateException(err)),
      FutureUnlessShutdown.pure,
    )

  def assignmentExclusivityLimitFor(baseline: CantonTimestamp): Either[String, CantonTimestamp] =
    checkValidity(baseline, "assignment exclusivity limit").map(_ =>
      baseline.add(assignmentExclusivityTimeout.unwrap)
    )

  /** Computes the participant response time for the given timestamp.
    *
    * @return Left if the synchronizer parameters are not valid at `timestamp`.
    */
  def participantResponseDeadlineFor(timestamp: CantonTimestamp): Either[String, CantonTimestamp] =
    checkValidity(timestamp, "participant response deadline").map(_ =>
      timestamp.add(parameters.confirmationResponseTimeout.unwrap)
    )

  /** Computes the participant response time for the given timestamp.
    *
    * @throws java.lang.IllegalStateException if the synchronizer parameters are not valid at `timestamp`.
    */
  def participantResponseDeadlineForF(timestamp: CantonTimestamp): Future[CantonTimestamp] =
    participantResponseDeadlineFor(timestamp).toFuture(new IllegalStateException(_))

  def automaticAssignmentEnabled: Boolean = parameters.automaticAssignmentEnabled
  def mediatorDeduplicationTimeout: NonNegativeFiniteDuration =
    parameters.mediatorDeduplicationTimeout

  def topologyChangeDelay: NonNegativeFiniteDuration = parameters.topologyChangeDelay
  def assignmentExclusivityTimeout: NonNegativeFiniteDuration =
    parameters.assignmentExclusivityTimeout
  def sequencerTopologyTimestampTolerance: NonNegativeFiniteDuration =
    parameters.sequencerTopologyTimestampTolerance
  def submissionCostTimestampTopologyTolerance: NonNegativeFiniteDuration =
    parameters.submissionCostTimestampTopologyTolerance
}

/** The class specifies the catch-up parameters governing the catch-up mode of a participant lagging behind with its
  * ACS commitments computation.
  * ***** Parameter recommendations
  * A high [[catchUpIntervalSkip]] outputs more commitments and is slower to catch-up.
  * For equal [[catchUpIntervalSkip]], a high [[nrIntervalsToTriggerCatchUp]] is less aggressive to trigger the
  * catch-up mode.
  *
  * ***** Examples
  * (5,2) and (2,5) both trigger the catch-up mode when the processor lags behind by at least 10
  * reconciliation intervals. The former catches up quicker, but computes fewer commitments, whereas the latter
  * computes more commitments but is slower to catch-up.
  *
  * @param catchUpIntervalSkip         The number of reconciliation intervals that the participant skips in
  *                                    catch-up mode.
  *                                    A catch-up interval thus has a length of
  *                                    reconciliation interval * `catchUpIntervalSkip`.
  *                                    All participants must catch up to the same timestamp. To ensure this, the
  *                                    interval count starts at EPOCH and gets incremented in catch-up intervals.
  *                                    For example, a reconciliation interval of 5 seconds,
  *                                    and a catchUpIntervalSkip of 2 (intervals), when a participant receiving a
  *                                    valid commitment at 15 seconds with timestamp 20 seconds, will perform catch-up
  *                                    from 10 seconds to 20 seconds (skipping 15 seconds commitment).
  * @param nrIntervalsToTriggerCatchUp The number of intervals a participant should lag behind in
  *                                    order to trigger catch-up mode. If a participant's current timestamp is behind
  *                                    the timestamp of valid received commitments by `reconciliationInterval` *
  *                                    `catchUpIntervalSkip` * `nrIntervalsToTriggerCatchUp`,
  *                                     then the participant triggers catch-up mode.
  *
  * @throws java.lang.IllegalArgumentException when [[catchUpIntervalSkip]] * [[nrIntervalsToTriggerCatchUp]] overflows.
  */
final case class AcsCommitmentsCatchUpConfig(
    catchUpIntervalSkip: PositiveInt,
    nrIntervalsToTriggerCatchUp: PositiveInt,
) extends PrettyPrinting {

  require(
    Either
      .catchOnly[ArithmeticException](
        Math.multiplyExact(catchUpIntervalSkip.value, nrIntervalsToTriggerCatchUp.value)
      )
      .isRight,
    s"Catch up parameters ($catchUpIntervalSkip, $nrIntervalsToTriggerCatchUp) are too large and cause overflow when computing the catch-up interval",
  )

  require(
    catchUpIntervalSkip.value != 1 || nrIntervalsToTriggerCatchUp.value != 1,
    s"Catch up config ($catchUpIntervalSkip, $nrIntervalsToTriggerCatchUp) is ambiguous. " +
      s"It is not possible to catch up with a single interval. Did you intend to disable catch-up " +
      s"(please use AcsCommitmentsCatchUpConfig.disabledCatchUp()) or did you intend a different config?",
  )

  override protected def pretty: Pretty[AcsCommitmentsCatchUpConfig] = prettyOfClass(
    param("catchUpIntervalSkip", _.catchUpIntervalSkip),
    param("nrIntervalsToTriggerCatchUp", _.nrIntervalsToTriggerCatchUp),
  )

  def toProtoV30: v30.AcsCommitmentsCatchUpConfig = v30.AcsCommitmentsCatchUpConfig(
    catchUpIntervalSkip.value,
    nrIntervalsToTriggerCatchUp.value,
  )

  // the catch-up mode is effectively disabled when the nr of intervals is Int.MaxValue
  def isCatchUpEnabled(): Boolean =
    !(catchUpIntervalSkip.value == 1 && nrIntervalsToTriggerCatchUp.value == Int.MaxValue)
}

object AcsCommitmentsCatchUpConfig {
  def fromProtoV30(
      value: v30.AcsCommitmentsCatchUpConfig
  ): ParsingResult[AcsCommitmentsCatchUpConfig] = {
    val v30.AcsCommitmentsCatchUpConfig(catchUpIntervalSkipP, nrIntervalsToTriggerCatchUpP) = value
    for {
      catchUpIntervalSkip <- ProtoConverter.parsePositiveInt(
        "catchup_interval_skip",
        catchUpIntervalSkipP,
      )
      nrIntervalsToTriggerCatchUp <- ProtoConverter.parsePositiveInt(
        "nr_intervals_to_trigger_catch_up",
        nrIntervalsToTriggerCatchUpP,
      )
    } yield AcsCommitmentsCatchUpConfig(catchUpIntervalSkip, nrIntervalsToTriggerCatchUp)
  }

  def disabledCatchUp(): AcsCommitmentsCatchUpConfig =
    AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(1), PositiveInt.tryCreate(Integer.MAX_VALUE))
}
