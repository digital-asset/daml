// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AcsIndexingProgress,
  AcsReplicationProgress,
  Disconnected,
  EphemeralSequencerChannelProgress,
  PartyReplicationAuthorization,
  PartyReplicationError,
  PartyReplicationFailed,
  ReplicationParams,
  SequencerChannelAgreement,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.protocol.party.{
  PartyReplicationFileImporter,
  PartyReplicationProcessor,
}
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.protocol.{LfContractId, v30 as v30Topology}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{ProtoDeserializationError, RepairCounter}
import io.scalaland.chimney.dsl.*

/** Internal state representation of the party replication process. Refer to party_replication.proto
  * PartyReplicationStatus for the semantics.
  */
final case class PartyReplicationStatus(
    params: ReplicationParams,
    agreementO: Option[SequencerChannelAgreement],
    authorizationO: Option[PartyReplicationAuthorization],
    replicationO: Option[AcsReplicationProgress],
    indexingO: Option[AcsIndexingProgress],
    hasCompleted: Boolean,
    errorO: Option[PartyReplicationError],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PartyReplicationStatus.type
    ]
) extends HasProtocolVersionedWrapper[PartyReplicationStatus]
    with PrettyPrinting {
  @transient override protected lazy val companionObj: PartyReplicationStatus.type =
    PartyReplicationStatus

  require(
    indexingO.isEmpty || replicationO.nonEmpty,
    s"cannot begin indexing $indexingO before replication has started",
  )

  def setProcessor(
      processor: PartyReplicationProcessor
  ): PartyReplicationStatus = modifyReplication {
    case None => AcsReplicationProgress.initialize(processor)
    case Some(previous) =>
      EphemeralSequencerChannelProgress(
        previous.processedContractCount,
        previous.nextPersistenceCounter,
        previous.fullyProcessedAcs,
        processor,
      )
  }

  def setAgreementO(newAgreementO: Option[SequencerChannelAgreement]): PartyReplicationStatus =
    copy(agreementO = newAgreementO)(representativeProtocolVersion)
  def setAuthorization(newAuthorization: PartyReplicationAuthorization): PartyReplicationStatus =
    copy(authorizationO = Some(newAuthorization))(representativeProtocolVersion)
  def modifyReplication(
      modify: Option[AcsReplicationProgress] => AcsReplicationProgress
  ): PartyReplicationStatus =
    copy(replicationO = Some(modify(replicationO)))(representativeProtocolVersion)
  def setCompleted(): PartyReplicationStatus =
    copy(hasCompleted = true)(representativeProtocolVersion)
  def modifyErrorO(
      modify: Option[PartyReplicationError] => Option[PartyReplicationError]
  ): PartyReplicationStatus = copy(errorO = modify(errorO))(representativeProtocolVersion)

  def ensureCanSetAgreement(paramsReceived: ReplicationParams): Either[String, Unit] = for {
    _ <- Either.cond(
      agreementO.isEmpty,
      (),
      s"Party replication ${params.requestId} already has an agreement $agreementO",
    )
    _ <- Either.cond(
      paramsReceived == params,
      (),
      s"The party replication ${params.requestId} agreement parameters received $paramsReceived do not match the locally stored agreement parameters $params",
    )
  } yield ()

  /** Indicates whether party replication is active and expected to be progressing, i.e. whether
    * monitoring for progress and initiating state transitions are needed in contrast to having
    * completed or having failed in such a way that requires operator intervention.
    */
  def isProgressExpected: Boolean = !hasCompleted && !errorO.exists {
    case PartyReplicationFailed(_) => true
    case Disconnected(_) => false
  }

  def toProtoV30: v30.PartyReplicationStatus = v30.PartyReplicationStatus(
    Some(params.toProtoV30),
    agreementO.map(_.toProtoV30),
    authorizationO.map(_.toProtoV30),
    replicationO.map(_.toProtoV30),
    indexingO.map(_.toProtoV30),
    hasCompleted = hasCompleted,
    errorO.map(_.toProtoV30),
  )

  override protected def pretty: Pretty[PartyReplicationStatus] = {
    import com.digitalasset.canton.logging.pretty.PrettyInstances.*
    prettyOfClass(
      param("params", _.params),
      paramIfDefined("agreement", _.agreementO),
      paramIfDefined("authorization", _.authorizationO),
      paramIfDefined("replication", _.replicationO),
      paramIfDefined("indexing", _.indexingO),
      paramIfDefined("error", _.errorO),
      paramIfTrue("complete", _.hasCompleted),
    )
  }
}

object PartyReplicationStatus extends VersioningCompanion[PartyReplicationStatus] {

  override val name: String = "PartyReplicationStatus"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v30.PartyReplicationStatus
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def fromProtoV30(
      proto: v30.PartyReplicationStatus
  ): ParsingResult[PartyReplicationStatus] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    parametersP <- ProtoConverter.required("parameters", proto.parameters)
    parameters <- ReplicationParams.fromProtoV30(parametersP)
    agreementO <- proto.agreement.traverse(SequencerChannelAgreement.fromProtoV30)
    authorizationO <- proto.authorization.traverse(PartyReplicationAuthorization.fromProtoV30)
    replicationO <- proto.replication.traverse(AcsReplicationProgress.fromProtoV30)
    indexingO <- proto.indexing.traverse(AcsIndexingProgress.fromProtoV30)
    hasCompleted = proto.hasCompleted
    errorO <- proto.errorMessage.traverse(PartyReplicationError.fromProtoV30)
  } yield PartyReplicationStatus(
    parameters,
    agreementO,
    authorizationO,
    replicationO,
    indexingO,
    hasCompleted,
    errorO,
  )(rpv)

  def apply(
      params: ReplicationParams,
      pv: ProtocolVersion,
      agreementO: Option[SequencerChannelAgreement] = None,
      authorizationO: Option[PartyReplicationAuthorization] = None,
      replicationO: Option[AcsReplicationProgress] = None,
      indexingO: Option[AcsIndexingProgress] = None,
      hasCompleted: Boolean = false,
      errorO: Option[PartyReplicationError] = None,
  ): PartyReplicationStatus = PartyReplicationStatus(
    params,
    agreementO,
    authorizationO,
    replicationO,
    indexingO,
    hasCompleted,
    errorO,
  )(
    protocolVersionRepresentativeFor(pv)
  )

  final case class ReplicationParams(
      requestId: AddPartyRequestId,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  ) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.ReplicationParameters =
      v30.PartyReplicationStatus.ReplicationParameters(
        requestId.toHexString,
        partyId.toProtoPrimitive,
        synchronizerId.uid.toProtoPrimitive,
        sourceParticipantId.uid.toProtoPrimitive,
        targetParticipantId.uid.toProtoPrimitive,
        serial.unwrap,
        toPartyReplicationPermissionProtoV30(participantPermission),
      )

    private def toPartyReplicationPermissionProtoV30(
        permission: ParticipantPermission
    ): v30Topology.Enums.ParticipantPermission =
      permission match {
        case ParticipantPermission.Submission =>
          v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
        case ParticipantPermission.Confirmation =>
          v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
        case ParticipantPermission.Observation =>
          v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
      }

    override protected def pretty: Pretty[ReplicationParams] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("request", _.requestId),
        param("party", _.partyId),
        param("synchronizer", _.synchronizerId),
        param("source participant", _.sourceParticipantId),
        param("target participant", _.targetParticipantId),
        param("serial", _.serial),
        param("permission", _.participantPermission.showType),
      )
    }
  }

  object ReplicationParams {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.ReplicationParameters
    ): ParsingResult[ReplicationParams] = for {
      requestIdBytes <- HexString
        .parseToByteString(proto.requestId)
        .toRight(
          ProtoDeserializationError
            .ValueDeserializationError("request_id", s"not a hex string \"${proto.requestId}\"")
        )
      requestId <- Hash.fromProtoPrimitive(requestIdBytes)
      partyId <- PartyId.fromProtoPrimitive(proto.partyId, "party_id")
      synchronizerId <- SynchronizerId.fromProtoPrimitive(
        proto.synchronizerId,
        "synchronizer_id",
      )
      sourceParticipantId <- UniqueIdentifier
        .fromProtoPrimitive(
          proto.sourceParticipantUid,
          "source_participant_uid",
        )
        .map(ParticipantId(_))
      targetParticipantId <- UniqueIdentifier
        .fromProtoPrimitive(
          proto.targetParticipantUid,
          "target_participant_uid",
        )
        .map(ParticipantId(_))
      topologySerial <- ProtoConverter.parsePositiveInt(
        "topology_serial",
        proto.topologySerial,
      )
      participantPermission <- fromParticipantPermissionProtoV30(proto.participantPermission)
    } yield ReplicationParams(
      requestId,
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      topologySerial,
      participantPermission,
    )

    private def fromParticipantPermissionProtoV30(
        proto: v30Topology.Enums.ParticipantPermission
    ): ParsingResult[ParticipantPermission] =
      ProtoConverter.parseEnum[ParticipantPermission, v30Topology.Enums.ParticipantPermission](
        {
          case v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION =>
            Right(Some(ParticipantPermission.Submission))
          case v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION =>
            Right(Some(ParticipantPermission.Confirmation))
          case v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION =>
            Right(Some(ParticipantPermission.Observation))
          case v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED =>
            Right(None)
          case v30Topology.Enums.ParticipantPermission.Unrecognized(unknown) =>
            Left(ProtoDeserializationError.UnrecognizedEnum(proto.name, unknown))
        },
        "participant_permission",
        proto,
      )

    def fromAgreementParams(agreement: PartyReplicationAgreementParams): ReplicationParams =
      agreement.transformInto[ReplicationParams]
  }

  final case class SequencerChannelAgreement(
      // daml agreement contract id to be archived upon completion or disruptions
      damlAgreementContractId: LfContractId,
      sequencerId: SequencerId,
  ) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.SequencerChannelAgreement =
      v30.PartyReplicationStatus.SequencerChannelAgreement(
        damlAgreementContractId.coid,
        sequencerId.uid.toProtoPrimitive,
      )

    override protected def pretty: Pretty[SequencerChannelAgreement] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("contract id", _.damlAgreementContractId),
        param("sequencer", _.sequencerId),
      )
    }
  }

  object SequencerChannelAgreement {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.SequencerChannelAgreement
    ): ParsingResult[SequencerChannelAgreement] =
      for {
        contractId <- ProtoConverter.parseLfContractId(proto.contractId)
        sequencerId <- UniqueIdentifier
          .fromProtoPrimitive(proto.sequencerUid, "sequencer_uid")
          .map(SequencerId(_))
      } yield SequencerChannelAgreement(contractId, sequencerId)
  }

  final case class PartyReplicationAuthorization(
      onboardingAt: EffectiveTime,
      isOnboardingFlagCleared: Boolean,
  ) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationAuthorization =
      v30.PartyReplicationStatus.PartyReplicationAuthorization(
        Some(onboardingAt.value.toProtoTimestamp),
        isOnboardingFlagCleared,
      )

    override protected def pretty: Pretty[PartyReplicationAuthorization] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("onboarding at", _.onboardingAt.value),
        paramIfTrue("onboarding cleared", _.isOnboardingFlagCleared),
      )
    }
  }

  object PartyReplicationAuthorization {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.PartyReplicationAuthorization
    ): ParsingResult[PartyReplicationAuthorization] =
      for {
        onboardingAtP <- ProtoConverter.required("onboarding_at", proto.onboardingAt)
        onboardingAt <- CantonTimestamp.fromProtoTimestamp(onboardingAtP)
      } yield PartyReplicationAuthorization(
        EffectiveTime(onboardingAt),
        proto.isOnboardingFlagCleared,
      )
  }

  sealed trait AcsReplicationProgress extends PrettyPrinting {
    def processedContractCount: NonNegativeInt
    def nextPersistenceCounter: RepairCounter
    def fullyProcessedAcs: Boolean
    def processorO: Option[PartyReplicationProcessor]
    def fileImporterO: Option[PartyReplicationFileImporter]

    def toProtoV30: v30.PartyReplicationStatus.AcsReplicationProgress =
      v30.PartyReplicationStatus.AcsReplicationProgress(
        processedContractCount.unwrap,
        nextPersistenceCounter.unwrap,
        fullyProcessedAcs,
      )

    override protected def pretty: Pretty[AcsReplicationProgress] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("contracts", _.processedContractCount),
        param("next counter", _.nextPersistenceCounter),
        paramIfTrue("fully replicated", _.fullyProcessedAcs),
        paramIfDefined("processor", _.processorO.map(_.showType)),
      )
    }
  }

  /** PersistentProgress contains the db-persisted portion of the ACS replication progress. Before
    * the progress needs to be updated, this case class needs to be turned into one of the ephemeral
    * AcsReplicationProgress case classes.
    */
  final case class PersistentProgress(
      processedContractCount: NonNegativeInt,
      nextPersistenceCounter: RepairCounter,
      fullyProcessedAcs: Boolean,
  ) extends AcsReplicationProgress {
    override def processorO: Option[PartyReplicationProcessor] = None
    override def fileImporterO: Option[PartyReplicationFileImporter] = None
  }

  /** EphemeralSequencerChannelProgress holds the ephemeral sequencer channel based ACS replication
    * status of a party replication source or target participant processor.
    */
  final case class EphemeralSequencerChannelProgress(
      processedContractCount: NonNegativeInt,
      nextPersistenceCounter: RepairCounter,
      fullyProcessedAcs: Boolean,
      processor: PartyReplicationProcessor,
  ) extends AcsReplicationProgress {
    override def processorO: Option[PartyReplicationProcessor] = Some(processor)
    override def fileImporterO: Option[PartyReplicationFileImporter] = None
  }

  /** EphemeralFileImporterProgress holds the ephemeral file-based ACS import status of a party
    * replication target participant.
    */
  final case class EphemeralFileImporterProgress(
      processedContractCount: NonNegativeInt,
      nextPersistenceCounter: RepairCounter,
      fullyProcessedAcs: Boolean,
      fileImporter: PartyReplicationFileImporter,
  ) extends AcsReplicationProgress {
    override def processorO: Option[PartyReplicationProcessor] = None
    override def fileImporterO: Option[PartyReplicationFileImporter] = Some(fileImporter)
  }

  object AcsReplicationProgress {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.AcsReplicationProgress
    ): ParsingResult[PersistentProgress] = for {
      replicatedContractCount <- ProtoConverter.parseNonNegativeInt(
        "replicated_contract_count",
        proto.processedContractCount,
      )
      nextPersistenceCounter <- ProtoConverter.parseNonNegativeLong(
        "next_persistence_counter",
        proto.nextPersistenceCounter,
      )
    } yield PersistentProgress(
      replicatedContractCount,
      RepairCounter(nextPersistenceCounter.unwrap),
      proto.fullyProcessedAcs,
    )

    def initialize(processor: PartyReplicationProcessor): AcsReplicationProgress =
      EphemeralSequencerChannelProgress(
        NonNegativeInt.zero,
        RepairCounter.Genesis,
        fullyProcessedAcs = false,
        processor,
      )

    def initialize(fileImporter: PartyReplicationFileImporter): AcsReplicationProgress =
      EphemeralFileImporterProgress(
        NonNegativeInt.zero,
        RepairCounter.Genesis,
        fullyProcessedAcs = false,
        fileImporter,
      )
  }

  final case class AcsIndexingProgress(
      indexedContractCount: NonNegativeInt,
      nextIndexingCounter: RepairCounter,
  ) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.AcsIndexingProgress =
      v30.PartyReplicationStatus.AcsIndexingProgress(
        indexedContractCount.unwrap,
        nextIndexingCounter.unwrap,
      )

    override protected def pretty: Pretty[AcsIndexingProgress] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("contracts", _.indexedContractCount),
        param("next counter", _.nextIndexingCounter),
      )
    }
  }

  object AcsIndexingProgress {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.AcsIndexingProgress
    ): ParsingResult[AcsIndexingProgress] =
      for {
        indexedContractCount <- ProtoConverter.parseNonNegativeInt(
          "indexed_contract_count",
          proto.indexedContractCount,
        )
        nextIndexingCounter <- ProtoConverter.parseNonNegativeLong(
          "next_indexing_counter",
          proto.nextIndexingCounter,
        )
      } yield AcsIndexingProgress(indexedContractCount, RepairCounter(nextIndexingCounter.unwrap))
  }

  sealed trait PartyReplicationError extends PrettyPrinting {
    def message: String
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationError
    override protected def pretty: Pretty[PartyReplicationError] = prettyOfString(_.message)
  }

  final case class Disconnected(message: String) extends PartyReplicationError {
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationError =
      v30.PartyReplicationStatus.PartyReplicationError(
        v30.PartyReplicationStatus.PartyReplicationError.ErrorType.ERROR_TYPE_DISCONNECTED,
        message,
      )
  }

  final case class PartyReplicationFailed(message: String) extends PartyReplicationError {
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationError =
      v30.PartyReplicationStatus.PartyReplicationError(
        v30.PartyReplicationStatus.PartyReplicationError.ErrorType.ERROR_TYPE_FAILED,
        message,
      )
  }

  object PartyReplicationError {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.PartyReplicationError
    ): ParsingResult[PartyReplicationError] = {
      import v30.PartyReplicationStatus.PartyReplicationError.ErrorType
      ProtoConverter.parseEnum[PartyReplicationError, ErrorType](
        {
          case ErrorType.ERROR_TYPE_DISCONNECTED =>
            Right(Some(Disconnected(proto.errorMessage)))
          case ErrorType.ERROR_TYPE_FAILED =>
            Right(Some(PartyReplicationFailed(proto.errorMessage)))
          case ErrorType.ERROR_TYPE_UNSPECIFIED => Right(None)
          case ErrorType.Unrecognized(unknown) =>
            Left(ProtoDeserializationError.UnrecognizedEnum("error_type", unknown))
        },
        "error_type",
        proto.errorType,
      )
    }
  }
}
