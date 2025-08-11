// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.google.protobuf

final case class AddPartyStatus(
    partyId: PartyId,
    synchronizerId: SynchronizerId,
    sourceParticipantId: ParticipantId,
    targetParticipantId: ParticipantId,
    topologySerial: PositiveInt,
    status: AddPartyStatus.Status,
)

object AddPartyStatus {
  def fromProtoV30(proto: v30.GetAddPartyStatusResponse): ParsingResult[AddPartyStatus] =
    for {
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
      statusP <- ProtoConverter.required("status", proto.status).map(_.status)
      status <- parseStatus(statusP)
    } yield AddPartyStatus(
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      topologySerial,
      status,
    )

  private def parseStatus(
      statusP: v30.GetAddPartyStatusResponse.Status.Status
  ): ParsingResult[Status] = {
    def parseConnectedStatus(status: v30.GetAddPartyStatusResponse.Status.ConnectionEstablished) =
      for {
        commonFields <- parseCommonFields(status.sequencerUid, status.timestamp)
        (sequencerId, timestamp) = commonFields
      } yield ConnectionEstablished(sequencerId, timestamp)

    def parseReplicatingStatus(status: v30.GetAddPartyStatusResponse.Status.ReplicatingAcs) =
      for {
        commonFields <- parseCommonFields(status.sequencerUid, status.timestamp)
        (sequencerId, timestamp) = commonFields
        contractsReplicated <- parseContractsReplicated(status.contractsReplicated)
      } yield ReplicatingAcs(sequencerId, timestamp, contractsReplicated)

    statusP match {
      case v30.GetAddPartyStatusResponse.Status.Status.ProposalProcessed(_) =>
        Right(ProposalProcessed)
      case v30.GetAddPartyStatusResponse.Status.Status.AgreementAccepted(status) =>
        for {
          sequencerId <- UniqueIdentifier
            .fromProtoPrimitive(status.sequencerUid, "sequencer_id")
            .map(SequencerId(_))
        } yield AgreementAccepted(sequencerId)
      case v30.GetAddPartyStatusResponse.Status.Status.TopologyAuthorized(status) =>
        for {
          commonFields <- parseCommonFields(status.sequencerUid, status.timestamp)
          (sequencerId, timestamp) = commonFields
        } yield TopologyAuthorized(sequencerId, timestamp)
      case v30.GetAddPartyStatusResponse.Status.Status.ConnectionEstablished(status) =>
        parseConnectedStatus(status)
      case v30.GetAddPartyStatusResponse.Status.Status.ReplicatingAcs(status) =>
        parseReplicatingStatus(status)
      case v30.GetAddPartyStatusResponse.Status.Status.FullyReplicatedAcs(status) =>
        for {
          commonFields <- parseCommonFields(status.sequencerUid, status.timestamp)
          (sequencerId, timestamp) = commonFields
          contractsReplicated <- parseContractsReplicated(status.contractsReplicated)
        } yield FullyReplicatedAcs(sequencerId, timestamp, contractsReplicated)
      case v30.GetAddPartyStatusResponse.Status.Status.Completed(status) =>
        for {
          commonFields <- parseCommonFields(status.sequencerUid, status.timestamp)
          (sequencerId, timestamp) = commonFields
          contractsReplicated <- parseContractsReplicated(status.contractsReplicated)
        } yield Completed(sequencerId, timestamp, contractsReplicated)
      case v30.GetAddPartyStatusResponse.Status.Status.Error(status) =>
        for {
          statusPriorToErrorP <- ProtoConverter.required(
            "status_prior_to_error",
            status.statusPriorToError,
          )
          // Enforce constraint on prior status to error in part to avoid multi-level recursion
          // with bad proto.
          validStatusPriorToError <- statusPriorToErrorP.status match {
            case v30.GetAddPartyStatusResponse.Status.Status.Error(_) =>
              Left(
                ProtoDeserializationError.InvariantViolation(
                  "status_prior_to_error",
                  "Cannot be another Error",
                )
              )
            case _ =>
              Right(statusPriorToErrorP.status)
          }
          statusPriorToError <- parseStatus(validStatusPriorToError)
        } yield Error(status.errorMessage, statusPriorToError)
      case v30.GetAddPartyStatusResponse.Status.Status.Disconnected(status) =>
        for {
          statusPriorToDisconnectP <- ProtoConverter.required(
            "status_prior_to_disconnect",
            status.statusPriorToDisconnect,
          )
          // Enforce constraint on prior status to disconnected.
          statusPriorToDisconnect <- statusPriorToDisconnectP.status match {
            case v30.GetAddPartyStatusResponse.Status.Status.ConnectionEstablished(status) =>
              parseConnectedStatus(status)
            case v30.GetAddPartyStatusResponse.Status.Status.ReplicatingAcs(status) =>
              parseReplicatingStatus(status)
            case invalidStatus =>
              Left[ProtoDeserializationError, ActivelyReplicatingStatus](
                ProtoDeserializationError.InvariantViolation(
                  "status_prior_to_disconnect",
                  s"Invalid value ${invalidStatus.getClass.getSimpleName}",
                )
              )
          }
        } yield Disconnected(status.disconnectMessage, statusPriorToDisconnect)
      case v30.GetAddPartyStatusResponse.Status.Status.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("status"))
    }
  }

  private def parseCommonFields(
      sequencerUidP: String,
      timestampPO: Option[protobuf.timestamp.Timestamp],
  ): ParsingResult[(SequencerId, CantonTimestamp)] = for {
    sequencerId <- UniqueIdentifier
      .fromProtoPrimitive(sequencerUidP, "sequencer_id")
      .map(SequencerId(_))
    timestampP <- ProtoConverter.required("timestamp", timestampPO)
    timestamp <- CantonTimestamp.fromProtoTimestamp(timestampP)
  } yield (sequencerId, timestamp)

  private def parseContractsReplicated(contractsReplicatedP: Int): ParsingResult[NonNegativeInt] =
    ProtoConverter.parseNonNegativeInt("contracts_replicated", contractsReplicatedP)

  sealed trait Status
  final case object ProposalProcessed extends Status
  final case class AgreementAccepted(sequencerId: SequencerId) extends Status
  final case class TopologyAuthorized(
      sequencerId: SequencerId,
      timestamp: CantonTimestamp,
  ) extends Status
  sealed trait ActivelyReplicatingStatus extends Status
  final case class ConnectionEstablished(
      sequencerId: SequencerId,
      timestamp: CantonTimestamp,
  ) extends ActivelyReplicatingStatus
  final case class ReplicatingAcs(
      sequencerId: SequencerId,
      timestamp: CantonTimestamp,
      contractsReplicated: NonNegativeInt,
  ) extends ActivelyReplicatingStatus
  final case class FullyReplicatedAcs(
      sequencerId: SequencerId,
      timestamp: CantonTimestamp,
      contractsReplicated: NonNegativeInt,
  ) extends Status
  final case class Completed(
      sequencerId: SequencerId,
      timestamp: CantonTimestamp,
      contractsReplicated: NonNegativeInt,
  ) extends Status
  final case class Error(
      error: String,
      statusPriorToError: Status,
  ) extends Status
  final case class Disconnected(
      message: String,
      statusPriorToDisconnect: ActivelyReplicatingStatus,
  ) extends Status
}
