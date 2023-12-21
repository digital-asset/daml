// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.instances.option.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DynamicDomainParameters.InvalidDynamicDomainParameters
import com.digitalasset.canton.protocol.{v1 as protoV1, v2 as protoV2}
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
import com.digitalasset.canton.{ProtoDeserializationError, checked}

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
    def map[T](f: P => T): WithValidity[T] = WithValidity(validFrom, validUntil, f(parameter))
    def isValidAt(ts: CantonTimestamp) = validFrom < ts && validUntil.forall(ts <= _)
  }
  final case class MaxRequestSize(value: NonNegativeInt) extends AnyVal {
    def unwrap = value.unwrap
  }
}

final case class StaticDomainParameters private (
    uniqueContractKeys: Boolean, // TODO(i13235) remove when UCK is gone
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

  def update(uniqueContractKeys: Boolean = uniqueContractKeys): StaticDomainParameters =
    this.copy(uniqueContractKeys = uniqueContractKeys)

  def toProtoV1: protoV1.StaticDomainParameters =
    protoV1.StaticDomainParameters(
      uniqueContractKeys = uniqueContractKeys,
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
  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v30)(
      protoV1.StaticDomainParameters
    )(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    )
  )

  override def name: String = "static domain parameters"

  def create(
      uniqueContractKeys: Boolean,
      requiredSigningKeySchemes: NonEmpty[Set[SigningKeyScheme]],
      requiredEncryptionKeySchemes: NonEmpty[Set[EncryptionKeyScheme]],
      requiredSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]],
      requiredHashAlgorithms: NonEmpty[Set[HashAlgorithm]],
      requiredCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]],
      protocolVersion: ProtocolVersion,
  ): StaticDomainParameters = StaticDomainParameters(
    uniqueContractKeys = uniqueContractKeys,
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

  def fromProtoV1(
      domainParametersP: protoV1.StaticDomainParameters
  ): ParsingResult[StaticDomainParameters] = {
    val protoV1.StaticDomainParameters(
      uniqueContractKeys,
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
      uniqueContractKeys,
      requiredSigningKeySchemes,
      requiredEncryptionKeySchemes,
      requiredSymmetricKeySchemes,
      requiredHashAlgorithms,
      requiredCryptoKeyFormats,
      protocolVersion,
    )
  }
}

/** @param participantResponseTimeout the amount of time (w.r.t. the sequencer clock) that a participant may take
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
  * @param maxRatePerParticipant maximum number of messages sent per participant per second
  * @param maxRequestSize maximum size of messages (in bytes) that the domain can receive through the public API
  * @param sequencerAggregateSubmissionTimeout the maximum time for how long an incomplete aggregate submission request is
  *                                            allowed to stay pending in the sequencer's state before it's removed.
  *                                            Must be at least `participantResponseTimeout` + `mediatorReactionTimeout` in a practical system.
  * @throws DynamicDomainParameters$.InvalidDynamicDomainParameters
  *   if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
  */
final case class DynamicDomainParameters private (
    participantResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveSeconds,
    maxRatePerParticipant: NonNegativeInt,
    maxRequestSize: MaxRequestSize,
    sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
    trafficControlParameters: Option[TrafficControlParameters],
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

  /** In some situations, the sequencer signs transaction with slightly outdated keys.
    * This is to allow recipients to verify sequencer signatures when the sequencer keys have been rolled over and
    * they have not yet received the new keys.
    * This parameter determines how much outdated a signing key can be.
    * Choose a higher value to avoid that the sequencer refuses to sign and send messages.
    * Choose a lower value to reduce the latency of sequencer key rollovers.
    * The sequencer signing tolerance must be at least `participantResponseTimeout + mediatorReactionTimeout`.
    */
  def sequencerSigningTolerance: NonNegativeFiniteDuration =
    (participantResponseTimeout + mediatorReactionTimeout) * NonNegativeInt.tryCreate(2)

  def automaticTransferInEnabled: Boolean =
    transferExclusivityTimeout > NonNegativeFiniteDuration.Zero

  def update(
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      reconciliationInterval: PositiveSeconds = reconciliationInterval,
      maxRatePerParticipant: NonNegativeInt = maxRatePerParticipant,
      maxRequestSize: MaxRequestSize = maxRequestSize,
  ): DynamicDomainParameters =
    this.copy(
      transferExclusivityTimeout = transferExclusivityTimeout,
      reconciliationInterval = reconciliationInterval,
      maxRatePerParticipant = maxRatePerParticipant,
      maxRequestSize = maxRequestSize,
    )(representativeProtocolVersion)

  def tryUpdate(
      participantResponseTimeout: NonNegativeFiniteDuration = participantResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration = mediatorDeduplicationTimeout,
      reconciliationInterval: PositiveSeconds = reconciliationInterval,
      maxRatePerParticipant: NonNegativeInt = maxRatePerParticipant,
      maxRequestSize: MaxRequestSize = maxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
        sequencerAggregateSubmissionTimeout,
      trafficControlParameters: Option[TrafficControlParameters] = trafficControlParameters,
  ): DynamicDomainParameters = DynamicDomainParameters.tryCreate(
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
  )(representativeProtocolVersion)

  def toProtoV2: protoV2.DynamicDomainParameters = protoV2.DynamicDomainParameters(
    participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
    mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
    transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
    topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
    ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
    mediatorDeduplicationTimeout = Some(mediatorDeduplicationTimeout.toProtoPrimitive),
    reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
    maxRequestSize = maxRequestSize.unwrap,
    // TODO(#14053) add permissioned domain mode
    permissionedDomain = false,
    // TODO(#14054) add restricted packages mode
    requiredPackages = Seq.empty,
    // TODO(#14054) add only restricted packages supported
    onlyRequiredPackagesPermitted = false,
    defaultParticipantLimits = Some(v2DefaultParticipantLimits.toProto),
    // TODO(#14050) limit number of participants that can be allocated to a given party
    defaultMaxHostingParticipantsPerParty = 0,
    sequencerAggregateSubmissionTimeout =
      Some(sequencerAggregateSubmissionTimeout.toProtoPrimitive),
    trafficControlParameters = trafficControlParameters.map(_.toProtoV0),
  )

  // TODO(#14052) add topology limits
  def v2DefaultParticipantLimits: ParticipantDomainLimits = ParticipantDomainLimits(
    maxRate = maxRatePerParticipant.unwrap,
    maxNumParties = Int.MaxValue,
    maxNumPackages = Int.MaxValue,
  )

  override def pretty: Pretty[DynamicDomainParameters] = {
    if (
      representativeProtocolVersion >= companionObj.protocolVersionRepresentativeFor(
        ProtocolVersion.v30
      )
    ) {
      prettyOfClass(
        param("participant response timeout", _.participantResponseTimeout),
        param("mediator reaction timeout", _.mediatorReactionTimeout),
        param("transfer exclusivity timeout", _.transferExclusivityTimeout),
        param("topology change delay", _.topologyChangeDelay),
        param("ledger time record time tolerance", _.ledgerTimeRecordTimeTolerance),
        param("mediator deduplication timeout", _.mediatorDeduplicationTimeout),
        param("reconciliation interval", _.reconciliationInterval),
        param("max rate per participant", _.maxRatePerParticipant),
        param("max request size", _.maxRequestSize.value),
        param("sequencer aggregate submission timeout", _.sequencerAggregateSubmissionTimeout),
        paramIfDefined("traffic control config", _.trafficControlParameters),
      )
    } else {
      prettyOfClass(
        param("participant response timeout", _.participantResponseTimeout),
        param("mediator reaction timeout", _.mediatorReactionTimeout),
        param("transfer exclusivity timeout", _.transferExclusivityTimeout),
        param("topology change delay", _.topologyChangeDelay),
        param("ledger time record time tolerance", _.ledgerTimeRecordTimeTolerance),
        param("mediator deduplication timeout", _.mediatorDeduplicationTimeout),
        param("reconciliation interval", _.reconciliationInterval),
        param("max rate per participant", _.maxRatePerParticipant),
        param("max request size", _.maxRequestSize.value),
      )
    }
  }
}

object DynamicDomainParameters extends HasProtocolVersionedCompanion[DynamicDomainParameters] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v30)(
      protoV2.DynamicDomainParameters
    )(
      supportedProtoVersion(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    )
  )

  override def name: String = "dynamic domain parameters"

  lazy val defaultReconciliationInterval: PositiveSeconds = PositiveSeconds.tryOfSeconds(60)
  lazy val defaultMaxRatePerParticipant: NonNegativeInt = NonNegativeInt.tryCreate(1000000)
  lazy val defaultMaxRequestSize: MaxRequestSize = MaxRequestSize(
    NonNegativeInt.tryCreate(10 * 1024 * 1024)
  )

  private val defaultParticipantResponseTimeout: NonNegativeFiniteDuration =
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

  // Based on SequencerClientConfig.defaultMaxSequencingTimeOffset
  private val defaultSequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfMinutes(5)

  /** Safely creates DynamicDomainParameters.
    *
    * @return `Left(...)` if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
    */
  private def create(
      participantResponseTimeout: NonNegativeFiniteDuration,
      mediatorReactionTimeout: NonNegativeFiniteDuration,
      transferExclusivityTimeout: NonNegativeFiniteDuration,
      topologyChangeDelay: NonNegativeFiniteDuration,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
      reconciliationInterval: PositiveSeconds,
      maxRatePerParticipant: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
      trafficControlConfig: Option[TrafficControlParameters],
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion[DynamicDomainParameters.type]
  ): Either[InvalidDynamicDomainParameters, DynamicDomainParameters] =
    Either.catchOnly[InvalidDynamicDomainParameters](
      tryCreate(
        participantResponseTimeout,
        mediatorReactionTimeout,
        transferExclusivityTimeout,
        topologyChangeDelay,
        ledgerTimeRecordTimeTolerance,
        mediatorDeduplicationTimeout,
        reconciliationInterval,
        maxRatePerParticipant,
        maxRequestSize,
        sequencerAggregateSubmissionTimeout,
        trafficControlConfig,
      )(representativeProtocolVersion)
    )

  /** Creates DynamicDomainParameters
    *
    * @throws InvalidDynamicDomainParameters if `mediatorDeduplicationTimeout` is less than twice of `ledgerTimeRecordTimeTolerance`.
    */
  def tryCreate(
      participantResponseTimeout: NonNegativeFiniteDuration,
      mediatorReactionTimeout: NonNegativeFiniteDuration,
      transferExclusivityTimeout: NonNegativeFiniteDuration,
      topologyChangeDelay: NonNegativeFiniteDuration,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
      reconciliationInterval: PositiveSeconds,
      maxRatePerParticipant: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration,
      trafficControlParameters: Option[TrafficControlParameters],
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion[DynamicDomainParameters.type]
  ): DynamicDomainParameters = {
    DynamicDomainParameters(
      participantResponseTimeout,
      mediatorReactionTimeout,
      transferExclusivityTimeout,
      topologyChangeDelay,
      ledgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout,
      reconciliationInterval,
      maxRatePerParticipant,
      maxRequestSize,
      sequencerAggregateSubmissionTimeout,
      trafficControlParameters,
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
      participantResponseTimeout = defaultParticipantResponseTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      transferExclusivityTimeout = defaultTransferExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = defaultLedgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout = defaultMediatorDeduplicationTimeout,
      reconciliationInterval = DynamicDomainParameters.defaultReconciliationInterval,
      maxRatePerParticipant = DynamicDomainParameters.defaultMaxRatePerParticipant,
      maxRequestSize = DynamicDomainParameters.defaultMaxRequestSize,
      sequencerAggregateSubmissionTimeout = defaultSequencerAggregateSubmissionTimeout,
      trafficControlParameters = defaultTrafficControlParameters,
    )(
      protocolVersionRepresentativeFor(protocolVersion)
    )
  )

  def tryInitialValues(
      topologyChangeDelay: NonNegativeFiniteDuration,
      protocolVersion: ProtocolVersion,
      maxRatePerParticipant: NonNegativeInt = DynamicDomainParameters.defaultMaxRatePerParticipant,
      maxRequestSize: MaxRequestSize = DynamicDomainParameters.defaultMaxRequestSize,
      mediatorReactionTimeout: NonNegativeFiniteDuration = defaultMediatorReactionTimeout,
      reconciliationInterval: PositiveSeconds =
        DynamicDomainParameters.defaultReconciliationInterval,
      sequencerAggregateSubmissionTimeout: NonNegativeFiniteDuration =
        defaultSequencerAggregateSubmissionTimeout,
  ) =
    DynamicDomainParameters.tryCreate(
      participantResponseTimeout = defaultParticipantResponseTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      transferExclusivityTimeout = defaultTransferExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = defaultLedgerTimeRecordTimeTolerance,
      mediatorDeduplicationTimeout = defaultMediatorDeduplicationTimeout,
      reconciliationInterval = reconciliationInterval,
      maxRatePerParticipant = maxRatePerParticipant,
      maxRequestSize = maxRequestSize,
      sequencerAggregateSubmissionTimeout = sequencerAggregateSubmissionTimeout,
      trafficControlParameters = defaultTrafficControlParameters,
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

  def fromProtoV2(
      domainParametersP: protoV2.DynamicDomainParameters
  ): ParsingResult[DynamicDomainParameters] = {
    val protoV2.DynamicDomainParameters(
      participantResponseTimeoutP,
      mediatorReactionTimeoutP,
      transferExclusivityTimeoutP,
      topologyChangeDelayP,
      ledgerTimeRecordTimeToleranceP,
      reconciliationIntervalP,
      mediatorDeduplicationTimeoutP,
      maxRequestSizeP,
      _permissionedDomain,
      _requiredPackages,
      _onlyRequiredPackagesPermitted,
      defaultLimitsP,
      _partyHostingLimits,
      sequencerAggregateSubmissionTimeoutP,
      trafficControlConfigP,
    ) = domainParametersP
    for {

      participantResponseTimeout <- NonNegativeFiniteDuration.fromProtoPrimitiveO(
        "participantResponseTimeout"
      )(
        participantResponseTimeoutP
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

      maxRatePerParticipantP <- ProtoConverter.parseRequired[Int, v2.ParticipantDomainLimits](
        item => Right(item.maxRate),
        "default_limits",
        defaultLimitsP,
      )

      maxRatePerParticipant <- NonNegativeInt
        .create(maxRatePerParticipantP)
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

      trafficControlConfig <- trafficControlConfigP.traverse(TrafficControlParameters.fromProtoV0)

      domainParameters <-
        create(
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
          trafficControlConfig = trafficControlConfig,
        )(protocolVersionRepresentativeFor(ProtoVersion(2)))
          .leftMap(_.toProtoDeserializationError)
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
        .add(parameters.participantResponseTimeout.unwrap)
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
      timestamp.add(parameters.participantResponseTimeout.unwrap)
    )

  def participantResponseDeadlineForF(timestamp: CantonTimestamp): Future[CantonTimestamp] =
    participantResponseDeadlineFor(timestamp).toFuture(new IllegalStateException(_))

  def automaticTransferInEnabled: Boolean = parameters.automaticTransferInEnabled
  def mediatorDeduplicationTimeout: NonNegativeFiniteDuration =
    parameters.mediatorDeduplicationTimeout

  def topologyChangeDelay: NonNegativeFiniteDuration = parameters.topologyChangeDelay
  def transferExclusivityTimeout: NonNegativeFiniteDuration = parameters.transferExclusivityTimeout
  def sequencerSigningTolerance: NonNegativeFiniteDuration = parameters.sequencerSigningTolerance
}
