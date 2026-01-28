// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.syntax.traverse.*
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.admin.data.PartyReplicationStatus.*
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus as InternalStatus
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*

import scala.annotation.unused

/** External console representation of the party replication process. Refer to
  * party_management_service.proto PartyReplicationStatus for the semantics.
  */
final case class PartyReplicationStatus(
    parameters: ReplicationParameters,
    agreementO: Option[SequencerChannelAgreement],
    authorizationO: Option[PartyReplicationAuthorization],
    replicationO: Option[AcsReplicationProgress],
    indexingO: Option[AcsIndexingProgress.type],
    hasCompleted: Boolean,
    errorO: Option[PartyReplicationError],
) extends PrettyPrinting {

  require(
    indexingO.isEmpty || replicationO.nonEmpty,
    s"cannot begin indexing $indexingO before replication has started",
  )

  def toProtoV30: v30.PartyReplicationStatus = v30.PartyReplicationStatus(
    Some(parameters.toProtoV30),
    agreementO.map(_.toProtoV30),
    authorizationO.map(_.toProtoV30),
    replicationO.map(_.toProtoV30),
    indexingO.map(_.toProtoV30),
    hasCompleted = hasCompleted,
    errorO.map(_.toProtoV30),
  )

  override protected def pretty: Pretty[PartyReplicationStatus] =
    prettyOfClass(
      param("parameters", _.parameters),
      paramIfDefined("agreement", _.agreementO),
      paramIfDefined("authorization", _.authorizationO),
      paramIfDefined("replication", _.replicationO),
      paramIfDefined("indexing", _.indexingO),
      paramIfDefined("error", _.errorO),
      paramIfTrue("complete", _.hasCompleted),
    )
}

object PartyReplicationStatus {
  def fromInternal: InternalStatus => PartyReplicationStatus = {
    // When new fields are added, adapt this deciding which fields may need to be exposed externally erring
    // on the side of cautious not exposing fields that don't have clear external utility and can be preserved
    // in a backward compatible way.
    case InternalStatus(
          params,
          agreementO,
          authorizationO,
          replicationO,
          indexingO,
          hasCompleted,
          errorO,
        ) =>
      PartyReplicationStatus(
        ReplicationParameters.fromInternal(params),
        agreementO.map(SequencerChannelAgreement.fromInternal),
        authorizationO.map(PartyReplicationAuthorization.fromInternal),
        replicationO.map(AcsReplicationProgress.fromInternal),
        indexingO.map(AcsIndexingProgress.fromInternal),
        hasCompleted,
        errorO.map(PartyReplicationError.fromInternal),
      )
  }

  def fromProtoV30(proto: v30.PartyReplicationStatus): ParsingResult[PartyReplicationStatus] =
    for {
      paramsP <- ProtoConverter.required("parameters", proto.parameters)
      params <- ReplicationParameters.fromProtoV30(paramsP)
      agreementO <- proto.agreement.traverse(SequencerChannelAgreement.fromProtoV30)
      authorizationO <- proto.authorization.traverse(PartyReplicationAuthorization.fromProtoV30)
      replicationO <- proto.replication.traverse(AcsReplicationProgress.fromProtoV30)
      indexingO <- proto.indexing.traverse(AcsIndexingProgress.fromProtoV30)
      hasCompleted = proto.hasCompleted
      errorO <- proto.errorMessage.traverse(PartyReplicationError.fromProtoV30)
    } yield PartyReplicationStatus(
      params,
      agreementO,
      authorizationO,
      replicationO,
      indexingO,
      hasCompleted,
      errorO,
    )

  final case class ReplicationParameters(
      requestId: String,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
  ) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.ReplicationParameters =
      v30.PartyReplicationStatus.ReplicationParameters(
        requestId,
        partyId.toProtoPrimitive,
        synchronizerId.uid.toProtoPrimitive,
        sourceParticipantId.uid.toProtoPrimitive,
        targetParticipantId.uid.toProtoPrimitive,
        serial.unwrap,
      )

    override protected def pretty: Pretty[ReplicationParameters] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("request", _.requestId.doubleQuoted),
        param("party", _.partyId),
        param("synchronizer", _.synchronizerId),
        param("source participant", _.sourceParticipantId),
        param("target participant", _.targetParticipantId),
        param("serial", _.serial),
      )
    }
  }
  private object ReplicationParameters {
    def fromInternal: InternalStatus.ReplicationParams => ReplicationParameters = {
      case InternalStatus.ReplicationParams(
            requestId,
            partyId,
            synchronizerId,
            sourceParticipantId,
            targetParticipantId,
            serial,
            _participantPermission,
          ) =>
        ReplicationParameters(
          requestId.toHexString,
          partyId,
          synchronizerId,
          sourceParticipantId,
          targetParticipantId,
          serial,
        )
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.ReplicationParameters
    ): ParsingResult[ReplicationParameters] = for {
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
    } yield ReplicationParameters(
      proto.requestId,
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      topologySerial,
    )
  }

  final case class SequencerChannelAgreement(sequencerId: SequencerId) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.SequencerChannelAgreement =
      v30.PartyReplicationStatus.SequencerChannelAgreement(sequencerId.uid.toProtoPrimitive)

    override protected def pretty: Pretty[SequencerChannelAgreement] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(param("sequencer", _.sequencerId))
    }
  }
  private object SequencerChannelAgreement {
    def fromInternal: InternalStatus.SequencerChannelAgreement => SequencerChannelAgreement = {
      case InternalStatus.SequencerChannelAgreement(_contractId, sequencerId) =>
        SequencerChannelAgreement(sequencerId)
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.SequencerChannelAgreement
    ): ParsingResult[SequencerChannelAgreement] =
      for {
        sequencerId <- UniqueIdentifier
          .fromProtoPrimitive(proto.sequencerUid, "sequencer_uid")
          .map(SequencerId(_))
      } yield SequencerChannelAgreement(sequencerId)
  }

  final case class PartyReplicationAuthorization(
      onboardingAt: CantonTimestamp,
      isOnboardingFlagCleared: Boolean,
  ) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationAuthorization =
      v30.PartyReplicationStatus.PartyReplicationAuthorization(
        Some(onboardingAt.toProtoTimestamp),
        isOnboardingFlagCleared,
      )

    override protected def pretty: Pretty[PartyReplicationAuthorization] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("onboarding at", _.onboardingAt),
        paramIfTrue("onboarding cleared", _.isOnboardingFlagCleared),
      )
    }
  }
  private object PartyReplicationAuthorization {
    def fromInternal
        : InternalStatus.PartyReplicationAuthorization => PartyReplicationAuthorization = {
      case InternalStatus.PartyReplicationAuthorization(onboardingAt, isOnboardingFlagCleared) =>
        PartyReplicationAuthorization(onboardingAt.value, isOnboardingFlagCleared)
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.PartyReplicationAuthorization
    ): ParsingResult[PartyReplicationAuthorization] = for {
      onboardingAtP <- ProtoConverter.required("onboarding_at", proto.onboardingAt)
      onboardingAt <- CantonTimestamp.fromProtoTimestamp(onboardingAtP)
    } yield PartyReplicationAuthorization(onboardingAt, proto.isOnboardingFlagCleared)
  }

  final case class AcsReplicationProgress(
      processedContractCount: NonNegativeInt,
      fullyProcessedAcs: Boolean,
  ) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.AcsReplicationProgress =
      v30.PartyReplicationStatus.AcsReplicationProgress(
        processedContractCount.unwrap,
        fullyProcessedAcs,
      )

    override protected def pretty: Pretty[AcsReplicationProgress] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("contracts", _.processedContractCount),
        paramIfTrue("fully replicated", _.fullyProcessedAcs),
      )
    }
  }
  private object AcsReplicationProgress {
    def fromInternal(internal: InternalStatus.AcsReplicationProgress): AcsReplicationProgress = {
      val (processedContractCount, fullyProcessedAcs) = internal match {
        case InternalStatus.PersistentProgress(count, _, done) => (count, done)
        case InternalStatus.EphemeralSequencerChannelProgress(count, _, done, _) =>
          (count, done)
        case InternalStatus.EphemeralFileImporterProgress(count, _, done, _) => (count, done)
      }
      AcsReplicationProgress(processedContractCount, fullyProcessedAcs)
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.AcsReplicationProgress
    ): ParsingResult[AcsReplicationProgress] = for {
      replicatedContractCount <- ProtoConverter.parseNonNegativeInt(
        "replicated_contract_count",
        proto.processedContractCount,
      )
    } yield AcsReplicationProgress(replicatedContractCount, proto.fullyProcessedAcs)
  }

  case object AcsIndexingProgress extends PrettyPrinting {
    override protected def pretty: Pretty[AcsIndexingProgress.type] =
      prettyOfObject[AcsIndexingProgress.type]

    def fromInternal: InternalStatus.AcsIndexingProgress => AcsIndexingProgress.type = {
      case InternalStatus.AcsIndexingProgress(_indexedContractCount, _nextIndexingCounter) =>
        AcsIndexingProgress
    }

    def toProtoV30: v30.PartyReplicationStatus.AcsIndexingProgress =
      v30.PartyReplicationStatus.AcsIndexingProgress()

    def fromProtoV30(
        @unused
        _proto: v30.PartyReplicationStatus.AcsIndexingProgress
    ): ParsingResult[AcsIndexingProgress.type] = Right(AcsIndexingProgress)
  }

  final case class PartyReplicationError(message: String) extends PrettyPrinting {
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationError =
      v30.PartyReplicationStatus.PartyReplicationError(message)
    override protected def pretty: Pretty[PartyReplicationError] = prettyOfString(_.message)
  }
  private object PartyReplicationError {
    def fromInternal: InternalStatus.PartyReplicationError => PartyReplicationError = err =>
      PartyReplicationError(err.message)

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.PartyReplicationError
    ): ParsingResult[PartyReplicationError] = Right(PartyReplicationError(proto.errorMessage))
  }
}
