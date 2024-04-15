// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.instances.option.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DynamicDomainParameters.InvalidDynamicDomainParameters
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
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.transaction.ParticipantDomainLimits
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{ProtoDeserializationError, checked, protocol}

import scala.concurrent.Future
import scala.math.Ordered.orderingToOrdered

object DomainParameters {

  /** This class is used to represent domain parameter(s) that can come from static
    * domain parameters or dynamic ones, depending on the protocol version.
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
    def unwrap = value.unwrap
  }
}

/** @param AcsCommitmentsCatchUp Optional parameters of type [[com.digitalasset.canton.protocol.AcsCommitmentsCatchUpConfig]].
  *                              Defined starting with protobuf version v2 and protocol version v6.
  *                              If None, the catch-up mode is disabled: the participant does not trigger the
  *                              catch-up mode when lagging behind.
  *                              If not None, it specifies the number of reconciliation intervals that the
  *                              participant skips in catch-up mode, and the number of catch-up intervals
  *                              intervals a participant should lag behind in order to enter catch-up mode.
  */
final case class StaticDomainParameters private (
    requiredSigningKeySchemes: NonEmpty[Set[SigningKeyScheme]],
    requiredEncryptionKeySchemes: NonEmpty[Set[EncryptionKeyScheme]],
    requiredSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]],
    requiredHashAlgorithms: NonEmpty[Set[HashAlgorithm]],
    requiredCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]],
    protocolVersion: ProtocolVersion,
) extends HasProtocolVersionedWrapper[StaticDomainParameters] {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    StaticDomainParameters.type
  ] = StaticDomainParameters.protocolVersionRepresentativeFor(protocolVersion)

  // Ensures the invariants related to default values hold
  validateInstance().valueOr(err => throw new IllegalArgumentException(err))

  @transient override protected lazy val companionObj: StaticDomainParameters.type =
    StaticDomainParameters

  def toProtoV30: v30.StaticDomainParameters =
    v30.StaticDomainParameters(
      requiredSigningKeySchemes = requiredSigningKeySchemes.toSeq.map(_.toProtoEnum),
      requiredEncryptionKeySchemes = requiredEncryptionKeySchemes.toSeq.map(_.toProtoEnum),
      requiredSymmetricKeySchemes = requiredSymmetricKeySchemes.toSeq.map(_.toProtoEnum),
      requiredHashAlgorithms = requiredHashAlgorithms.toSeq.map(_.toProtoEnum),
      requiredCryptoKeyFormats = requiredCryptoKeyFormats.toSeq.map(_.toProtoEnum),
      protocolVersion = protocolVersion.toProtoPrimitive,
    )
}
object StaticDomainParameters
    extends HasProtocolVersionedCompanion[StaticDomainParameters]
    with ProtocolVersionedCompanionDbHelpers[StaticDomainParameters] {

  // Note: if you need static domain parameters for testing, look at BaseTest.defaultStaticDomainParametersWith

  val supportedProtoVersions: protocol.StaticDomainParameters.SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(
        v30.StaticDomainParameters
      )(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

  override def name: String = "static domain parameters"

  def create(
      requiredSigningKeySchemes: NonEmpty[Set[SigningKeyScheme]],
      requiredEncryptionKeySchemes: NonEmpty[Set[EncryptionKeyScheme]],
      requiredSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]],
      requiredHashAlgorithms: NonEmpty[Set[HashAlgorithm]],
      requiredCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]],
      protocolVersion: ProtocolVersion,
  ): StaticDomainParameters = StaticDomainParameters(
    requiredSigningKeySchemes = requiredSigningKeySchemes,
    requiredEncryptionKeySchemes = requiredEncryptionKeySchemes,
    requiredSymmetricKeySchemes = requiredSymmetricKeySchemes,
    requiredHashAlgorithms = requiredHashAlgorithms,
    requiredCryptoKeyFormats = requiredCryptoKeyFormats,
    protocolVersion = protocolVersion,
  )

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
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP)
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

/** Onboarding restrictions for new participants joining a domain
  *
  * The domain administrators can set onboarding restrictions to control
  * which participant can join the domain.
  */
sealed trait OnboardingRestriction extends Product with Serializable {
  def toProtoV30: v30.OnboardingRestriction
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
  }

  /** In theory, anyone can join, except now, the registration procedure is closed */
  final case object UnrestrictedLocked extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_UNRESTRICTED_LOCKED
  }

  /** Only participants on the allowlist can join
    *
    * Requires the domain owners to issue a valid ParticipantDomainPermission transaction
    */
  final case object RestrictedOpen extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_RESTRICTED_OPEN
  }

  /** Only participants on the allowlist can join in theory, except now, the registration procedure is closed */
  final case object RestrictedLocked extends OnboardingRestriction {
    override def toProtoV30: v30.OnboardingRestriction =
      v30.OnboardingRestriction.ONBOARDING_RESTRICTION_RESTRICTED_LOCKED
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
  * @param transferExclusivityTimeout this timeout affects who can initiate a transfer-in.
  *                                   Before the timeout, only the submitter of the transfer-out can initiate the
  *                                   corresponding transfer-in.
  *                                   From the timeout onwards, every stakeholder of the contract can initiate a transfer-in,
  *                                   if it has not yet happened.
  *                                   Moreover, if this timeout is zero, no automatic transfer-ins will occur.
  *                                   Choose a low value, if you want to lower the time that contracts can be inactive
  *                                   due to ongoing transfers.
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
  *                                     purposes. This must be at least twice the `ledgerTimeRecordTimeTolerance`.
  *                                     It is fine to choose the minimal value, unless you plan to subsequently
  *                                     increase `ledgerTimeRecordTimeTolerance.`
  * @param reconciliationInterval The size of the reconciliation interval (minimum duration between two ACS commitments).
  *                               Note: default to [[StaticDomainParameters.defaultReconciliationInterval]] for backward
  *                               compatibility.
  *                               Should be significantly longer than the period of time it takes to compute the commitment and have it sequenced of the domain.
  *                               Otherwise, ACS commitments will keep being exchanged continuously on an idle domain.
  * @param confirmationRequestsMaxRate maximum number of mediator confirmation requests sent per participant per second
  * @param maxRequestSize maximum size of messages (in bytes) that the domain can receive through the public API
  * @param sequencerAggregateSubmissionTimeout the maximum time for how long an incomplete aggregate submission request is
  *                                            allowed to stay pending in the sequencer's state before it's removed.
  *                                            Must be at least `confirmationResponseTimeout` + `mediatorReactionTimeout` in a practical system.
  *                                            Must be greater than `maxSequencingTime` specified by a participant,
  *                                            practically also requires extra slack to allow clock skew between participant and sequencer.
  * @param onboardingRestriction current onboarding restrictions for participants
  *  @param catchUpParameters   Optional parameters of type [[com.digitalasset.canton.protocol.AcsCommitmentsCatchUpConfig]].
  *                            Defined starting with protobuf version v2 and protocol version v30.
  *                            If None, the catch-up mode is disabled: the participant does not trigger the
  *                            catch-up mode when lagging behind.
  *                            If not None, it specifies the number of reconciliation intervals that the
  *                            participant skips in catch-up mode, and the number of catch-up intervals
  *                           intervals a participant should lag behind in order to enter catch-up mode.
  *
  * @throws DynamicDomainParameters$.InvalidDynamicDomainParameters
  *   if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
  */
final case class DynamicDomainParameters private (
    confirmationResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveSeconds,
    confirmationRequestsMaxRate: NonNegativeInt,
    maxRequestSize: MaxRequestSize,
    sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
    trafficControlParameters: Option[TrafficControlParameters],
    onboardingRestriction: OnboardingRestriction,
    acsCommitmentsCatchUpConfig: Option[AcsCommitmentsCatchUpConfig],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      DynamicDomainParameters.type
    ]
) extends HasProtocolVersionedWrapper[DynamicDomainParameters]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: DynamicDomainParameters.type =
    DynamicDomainParameters

  // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.jtqcu52qpf82
  if (ledgerTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2) > mediatorDeduplicationTimeout)
    throw new InvalidDynamicDomainParameters(
      s"The ledgerTimeRecordTimeTolerance ($ledgerTimeRecordTimeTolerance) must be at most half of the " +
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

  def automaticTransferInEnabled: Boolean =
    transferExclusivityTimeout > NonNegativeFiniteDuration.Zero

  def update(
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      reconciliationInterval: PositiveSeconds = reconciliationInterval,
      confirmationRequestsMaxRate: NonNegativeInt = confirmationRequestsMaxRate,
      maxRequestSize: MaxRequestSize = maxRequestSize,
  ): DynamicDomainParameters =
    this.copy(
      transferExclusivityTimeout = transferExclusivityTimeout,
      reconciliationInterval = reconciliationInterval,
      confirmationRequestsMaxRate = confirmationRequestsMaxRate,
      maxRequestSize = maxRequestSize,
    )(representativeProtocolVersion)

  def tryUpdate(
      confirmationResponseTimeout: NonNegativeFiniteDuration = confirmationResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
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
  ): DynamicDomainParameters = DynamicDomainParameters.tryCreate(
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
    acsCommitmentsCatchUpConfigParameter = acsCommitmentsCatchUpConfigParameter,
  )(representativeProtocolVersion)

  def toProtoV30: v30.DynamicDomainParameters = v30.DynamicDomainParameters(
    confirmationResponseTimeout = Some(confirmationResponseTimeout.toProtoPrimitive),
    mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
    transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
    topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
    ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
    mediatorDeduplicationTimeout = Some(mediatorDeduplicationTimeout.toProtoPrimitive),
    reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
    maxRequestSize = maxRequestSize.unwrap,
    onboardingRestriction = onboardingRestriction.toProtoV30,
    // TODO(#14054) add restricted packages mode
    requiredPackages = Seq.empty,
    // TODO(#14054) add only restricted packages supported
    onlyRequiredPackagesPermitted = false,
    defaultParticipantLimits = Some(v2DefaultParticipantLimits.toProto),
    // TODO(#14050) limit number of participants that can be allocated to a given party
    defaultMaxHostingParticipantsPerParty = 0,
    sequencerAggregateSubmissionTimeout =
      Some(sequencerAggregateSubmissionTimeout.toProtoPrimitive),
    trafficControlParameters = trafficControlParameters.map(_.toProtoV30),
    acsCommitmentsCatchupConfig = acsCommitmentsCatchUpConfig.map(_.toProtoV30),
  )

  // TODO(#14052) add topology limits
  def v2DefaultParticipantLimits: ParticipantDomainLimits = ParticipantDomainLimits(
    confirmationRequestsMaxRate = confirmationRequestsMaxRate.unwrap,
    maxNumParties = Int.MaxValue,
    maxNumPackages = Int.MaxValue,
  )

  override def pretty: Pretty[DynamicDomainParameters] = {
    if (
      representativeProtocolVersion >= companionObj.protocolVersionRepresentativeFor(
        ProtocolVersion.v31
      )
    ) {
      prettyOfClass(
        param("confirmation response timeout", _.confirmationResponseTimeout),
        param("mediator reaction timeout", _.mediatorReactionTimeout),
        param("transfer exclusivity timeout", _.transferExclusivityTimeout),
        param("topology change delay", _.topologyChangeDelay),
        param("ledger time record time tolerance", _.ledgerTimeRecordTimeTolerance),
        param("mediator deduplication timeout", _.mediatorDeduplicationTimeout),
        param("reconciliation interval", _.reconciliationInterval),
        param("confirmation requests max rate", _.confirmationRequestsMaxRate),
        param("max request size", _.maxRequestSize.value),
        param("sequencer aggregate submission timeout", _.sequencerAggregateSubmissionTimeout),
        paramIfDefined("traffic control config", _.trafficControlParameters),
        paramIfDefined("ACS commitment catchup config", _.acsCommitmentsCatchUpConfig),
      )
    } else {
      prettyOfClass(
        param("confirmation response timeout", _.confirmationResponseTimeout),
        param("mediator reaction timeout", _.mediatorReactionTimeout),
        param("transfer exclusivity timeout", _.transferExclusivityTimeout),
        param("topology change delay", _.topologyChangeDelay),
        param("ledger time record time tolerance", _.ledgerTimeRecordTimeTolerance),
        param("mediator deduplication timeout", _.mediatorDeduplicationTimeout),
        param("reconciliation interval", _.reconciliationInterval),
        param("confirmation requests max rate", _.confirmationRequestsMaxRate),
        param("max request size", _.maxRequestSize.value),
      )
    }
  }
}

object DynamicDomainParameters extends HasProtocolVersionedCompanion[DynamicDomainParameters] {

  val supportedProtoVersions: canton.protocol.DynamicDomainParameters.SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(
        v30.DynamicDomainParameters
      )(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

  override def name: String = "dynamic domain parameters"

  lazy val defaultReconciliationInterval: PositiveSeconds = PositiveSeconds.tryOfSeconds(60)
  lazy val defaultConfirmationRequestsMaxRate: NonNegativeInt = NonNegativeInt.tryCreate(1000000)
  lazy val defaultMaxRequestSize: MaxRequestSize = MaxRequestSize(
    NonNegativeInt.tryCreate(10 * 1024 * 1024)
  )

  private val defaultConfirmationResponseTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(30)
  private val defaultMediatorReactionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(30)

  private val defaultTransferExclusivityTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(60)

  private val defaultTrafficControlParameters: Option[TrafficControlParameters] =
    Option.empty[TrafficControlParameters]

  private val defaultTopologyChangeDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfMillis(250)
  private val defaultTopologyChangeDelayNonStandardClock: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.Zero // SimClock, RemoteClock

  private val defaultLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(60)

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

  /** Safely creates DynamicDomainParameters.
    *
    * @return `Left(...)` if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
    */
  private def create(
      confirmationResponseTimeout: NonNegativeFiniteDuration,
      mediatorReactionTimeout: NonNegativeFiniteDuration,
      transferExclusivityTimeout: NonNegativeFiniteDuration,
      topologyChangeDelay: NonNegativeFiniteDuration,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
      reconciliationInterval: PositiveSeconds,
      confirmationRequestsMaxRate: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
      trafficControlConfig: Option[TrafficControlParameters],
      onboardingRestriction: OnboardingRestriction,
      acsCommitmentsCatchUpConfig: Option[AcsCommitmentsCatchUpConfig],
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion[DynamicDomainParameters.type]
  ): Either[InvalidDynamicDomainParameters, DynamicDomainParameters] =
    Either.catchOnly[InvalidDynamicDomainParameters](
      tryCreate(
        confirmationResponseTimeout,
        mediatorReactionTimeout,
        transferExclusivityTimeout,
        topologyChangeDelay,
        ledgerTimeRecordTimeTolerance,
        mediatorDeduplicationTimeout,
        reconciliationInterval,
        confirmationRequestsMaxRate,
        maxRequestSize,
        sequencerAggregateSubmissionTimeout,
        trafficControlConfig,
        onboardingRestriction,
        acsCommitmentsCatchUpConfig,
      )(representativeProtocolVersion)
    )

  /** Creates DynamicDomainParameters
    *
    * @throws InvalidDynamicDomainParameters if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
    */
  def tryCreate(
      confirmationResponseTimeout: NonNegativeFiniteDuration,
      mediatorReactionTimeout: NonNegativeFiniteDuration,
      transferExclusivityTimeout: NonNegativeFiniteDuration,
      topologyChangeDelay: NonNegativeFiniteDuration,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
      reconciliationInterval: PositiveSeconds,
      confirmationRequestsMaxRate: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
      trafficControlParameters: Option[TrafficControlParameters],
      onboardingRestriction: OnboardingRestriction,
      acsCommitmentsCatchUpConfigParameter: Option[AcsCommitmentsCatchUpConfig],
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion[DynamicDomainParameters.type]
  ): DynamicDomainParameters = {
    DynamicDomainParameters(
      confirmationResponseTimeout,
      mediatorReactionTimeout,
      transferExclusivityTimeout,
      topologyChangeDelay,
      ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout,
      reconciliationInterval,
      confirmationRequestsMaxRate,
      maxRequestSize,
      sequencerAggregateSubmissionTimeout,
      trafficControlParameters,
      onboardingRestriction,
      acsCommitmentsCatchUpConfigParameter,
    )(representativeProtocolVersion)
  }

  /** Default dynamic domain parameters for non-static clocks */
  def defaultValues(protocolVersion: ProtocolVersion): DynamicDomainParameters =
    initialValues(defaultTopologyChangeDelay, protocolVersion)

  /** Default mediator-X dynamic parameters allowing to specify more generous mediator-x timeouts for BFT-distribution */
  def defaultXValues(
      protocolVersion: ProtocolVersion,
      mediatorReactionTimeout: NonNegativeFiniteDuration = defaultMediatorReactionTimeout,
  ): DynamicDomainParameters =
    initialValues(
      defaultTopologyChangeDelay,
      protocolVersion,
      mediatorReactionTimeout = mediatorReactionTimeout,
    )

  // TODO(#15161) Rework this when old nodes are killed
  def initialValues(
      topologyChangeDelay: NonNegativeFiniteDuration,
      protocolVersion: ProtocolVersion,
      mediatorReactionTimeout: NonNegativeFiniteDuration = defaultMediatorReactionTimeout,
  ): DynamicDomainParameters = checked( // safe because default values are safe
    DynamicDomainParameters.tryCreate(
      confirmationResponseTimeout = defaultConfirmationResponseTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      transferExclusivityTimeout = defaultTransferExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = defaultLedgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout = defaultMediatorDeduplicationTimeout,
      reconciliationInterval = DynamicDomainParameters.defaultReconciliationInterval,
      confirmationRequestsMaxRate = DynamicDomainParameters.defaultConfirmationRequestsMaxRate,
      maxRequestSize = DynamicDomainParameters.defaultMaxRequestSize,
      sequencerAggregateSubmissionTimeout = defaultSequencerAggregateSubmissionTimeout,
      trafficControlParameters = defaultTrafficControlParameters,
      onboardingRestriction = defaultOnboardingRestriction,
      acsCommitmentsCatchUpConfigParameter = defaultAcsCommitmentsCatchUp,
    )(
      protocolVersionRepresentativeFor(protocolVersion)
    )
  )

  def tryInitialValues(
      topologyChangeDelay: NonNegativeFiniteDuration,
      protocolVersion: ProtocolVersion,
      confirmationRequestsMaxRate: NonNegativeInt =
        DynamicDomainParameters.defaultConfirmationRequestsMaxRate,
      maxRequestSize: MaxRequestSize = DynamicDomainParameters.defaultMaxRequestSize,
      mediatorReactionTimeout: NonNegativeFiniteDuration = defaultMediatorReactionTimeout,
      reconciliationInterval: PositiveSeconds =
        DynamicDomainParameters.defaultReconciliationInterval,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
        defaultSequencerAggregateSubmissionTimeout,
  ) =
    DynamicDomainParameters.tryCreate(
      confirmationResponseTimeout = defaultConfirmationResponseTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      transferExclusivityTimeout = defaultTransferExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = defaultLedgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout = defaultMediatorDeduplicationTimeout,
      reconciliationInterval = reconciliationInterval,
      confirmationRequestsMaxRate = confirmationRequestsMaxRate,
      maxRequestSize = maxRequestSize,
      sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
      trafficControlParameters = defaultTrafficControlParameters,
      onboardingRestriction = defaultOnboardingRestriction,
      acsCommitmentsCatchUpConfigParameter = defaultAcsCommitmentsCatchUp,
    )(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def initialValues(clock: Clock, protocolVersion: ProtocolVersion): DynamicDomainParameters = {
    val topologyChangeDelay = clock match {
      case _: RemoteClock | _: SimClock => defaultTopologyChangeDelayNonStandardClock
      case _ => defaultTopologyChangeDelay
    }
    initialValues(topologyChangeDelay, protocolVersion)
  }

  // if there is no topology change delay defined (or not yet propagated), we'll use this one
  val topologyChangeDelayIfAbsent: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero

  def fromProtoV30(
      domainParametersP: v30.DynamicDomainParameters
  ): ParsingResult[DynamicDomainParameters] = {
    val v30.DynamicDomainParameters(
      confirmationResponseTimeoutP,
      mediatorReactionTimeoutP,
      transferExclusivityTimeoutP,
      topologyChangeDelayP,
      ledgerTimeRecordTimeToleranceP,
      reconciliationIntervalP,
      mediatorDeduplicationTimeoutP,
      maxRequestSizeP,
      onboardingRestrictionP,
      _requiredPackages,
      _onlyRequiredPackagesPermitted,
      defaultLimitsP,
      _partyHostingLimits,
      sequencerAggregateSubmissionTimeoutP,
      trafficControlConfigP,
      acsCommitmentCatchupConfigP,
    ) = domainParametersP
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
      transferExclusivityTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "transferExclusivityTimeout"
      )(
        transferExclusivityTimeoutP
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

      confirmationRequestsMaxRateP <- ProtoConverter
        .parseRequired[Int, v30.ParticipantDomainLimits](
          item => Right(item.confirmationRequestsMaxRate),
          "default_limits",
          defaultLimitsP,
        )

      confirmationRequestsMaxRate <- NonNegativeInt
        .create(confirmationRequestsMaxRateP)
        .leftMap(InvariantViolation.toProtoDeserializationError)

      maxRequestSize <- NonNegativeInt
        .create(maxRequestSizeP)
        .map(MaxRequestSize)
        .leftMap(InvariantViolation.toProtoDeserializationError)

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

      domainParameters <-
        create(
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
          trafficControlConfig = trafficControlConfig,
          onboardingRestriction = onboardingRestriction,
          acsCommitmentsCatchUpConfig = acsCommitmentCatchupConfig,
        )(rpv).leftMap(_.toProtoDeserializationError)
    } yield domainParameters
  }

  class InvalidDynamicDomainParameters(message: String) extends RuntimeException(message) {
    lazy val toProtoDeserializationError: ProtoDeserializationError.InvariantViolation =
      ProtoDeserializationError.InvariantViolation(message)
  }
}

/** Dynamic domain parameters and their validity interval.
  * Mostly so that we can perform additional checks.
  *
  * @param validFrom Start point of the validity interval (exclusive)
  * @param validUntil End point of the validity interval (inclusive)
  */
final case class DynamicDomainParametersWithValidity(
    parameters: DynamicDomainParameters,
    validFrom: CantonTimestamp,
    validUntil: Option[CantonTimestamp],
    domainId: DomainId,
) {
  def map[T](f: DynamicDomainParameters => T): DomainParameters.WithValidity[T] =
    DomainParameters.WithValidity(validFrom, validUntil, f(parameters))

  def isValidAt(ts: CantonTimestamp): Boolean =
    validFrom < ts && validUntil.forall(ts <= _)

  private def checkValidity(ts: CantonTimestamp, goal: String): Either[String, Unit] = Either.cond(
    isValidAt(ts),
    (),
    s"Cannot compute $goal for `$ts` because validity of parameters is ($validFrom, $validUntil]",
  )

  /** Computes the decision time for the given activeness time.
    *
    * @param activenessTime
    * @return Left in case of error, the decision time otherwise
    */
  def decisionTimeFor(activenessTime: CantonTimestamp): Either[String, CantonTimestamp] =
    checkValidity(activenessTime, "decision time").map(_ =>
      activenessTime
        .add(parameters.confirmationResponseTimeout.unwrap)
        .add(parameters.mediatorReactionTimeout.unwrap)
    )

  /** Computes the decision time for the given activeness time.
    *
    * @param activenessTime
    * @return Decision time or a failed future in case of error
    */
  def decisionTimeForF(activenessTime: CantonTimestamp): Future[CantonTimestamp] =
    decisionTimeFor(activenessTime).fold(
      err => Future.failed(new IllegalStateException(err)),
      Future.successful,
    )

  def transferExclusivityLimitFor(baseline: CantonTimestamp): Either[String, CantonTimestamp] =
    checkValidity(baseline, "transfer exclusivity limit").map(_ =>
      baseline.add(transferExclusivityTimeout.unwrap)
    )

  def participantResponseDeadlineFor(timestamp: CantonTimestamp): Either[String, CantonTimestamp] =
    checkValidity(timestamp, "participant response deadline").map(_ =>
      timestamp.add(parameters.confirmationResponseTimeout.unwrap)
    )

  def participantResponseDeadlineForF(timestamp: CantonTimestamp): Future[CantonTimestamp] =
    participantResponseDeadlineFor(timestamp).toFuture(new IllegalStateException(_))

  def automaticTransferInEnabled: Boolean = parameters.automaticTransferInEnabled
  def mediatorDeduplicationTimeout: NonNegativeFiniteDuration =
    parameters.mediatorDeduplicationTimeout

  def topologyChangeDelay: NonNegativeFiniteDuration = parameters.topologyChangeDelay
  def transferExclusivityTimeout: NonNegativeFiniteDuration = parameters.transferExclusivityTimeout
  def sequencerTopologyTimestampTolerance: NonNegativeFiniteDuration =
    parameters.sequencerTopologyTimestampTolerance
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

  override def pretty: Pretty[AcsCommitmentsCatchUpConfig] = prettyOfClass(
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
      catchUpIntervalSkip <- ProtoConverter.parsePositiveInt(catchUpIntervalSkipP)
      nrIntervalsToTriggerCatchUp <- ProtoConverter.parsePositiveInt(
        nrIntervalsToTriggerCatchUpP
      )
    } yield AcsCommitmentsCatchUpConfig(catchUpIntervalSkip, nrIntervalsToTriggerCatchUp)
  }

  def disabledCatchUp(): AcsCommitmentsCatchUpConfig =
    AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(1), PositiveInt.tryCreate(Integer.MAX_VALUE))
}
