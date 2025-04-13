// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.traverse.*
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
      statusP <- ProtoConverter.required("status", proto.status).map(_.status)
      status <- parseStatus(statusP)
    } yield AddPartyStatus(
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      status,
    )

  private def parseStatus(
      statusP: v30.GetAddPartyStatusResponse.Status.Status
  ): ParsingResult[Status] =
    statusP match {
      case v30.GetAddPartyStatusResponse.Status.Status.ProposalProcessed(status) =>
        for {
          topologySerialO <- status.topologySerial.traverse(
            ProtoConverter.parsePositiveInt("topology_serial", _)
          )
        } yield ProposalProcessed(topologySerialO)
      case v30.GetAddPartyStatusResponse.Status.Status.AgreementAccepted(status) =>
        for {
          sequencerId <- UniqueIdentifier
            .fromProtoPrimitive(status.sequencerUid, "sequencer_id")
            .map(SequencerId(_))
          topologySerialO <- status.topologySerial.traverse(
            ProtoConverter.parsePositiveInt("topology_serial", _)
          )
        } yield AgreementAccepted(sequencerId, topologySerialO)
      case v30.GetAddPartyStatusResponse.Status.Status.TopologyAuthorized(status) =>
        for {
          commonFields <- parseCommonFields(
            status.sequencerUid,
            status.topologySerial,
            status.timestamp,
          )
          (sequencerId, topologySerial, timestamp) = commonFields
        } yield TopologyAuthorized(sequencerId, topologySerial, timestamp)
      case v30.GetAddPartyStatusResponse.Status.Status.ConnectionEstablished(status) =>
        for {
          commonFields <- parseCommonFields(
            status.sequencerUid,
            status.topologySerial,
            status.timestamp,
          )
          (sequencerId, topologySerial, timestamp) = commonFields
        } yield ConnectionEstablished(sequencerId, topologySerial, timestamp)
      case v30.GetAddPartyStatusResponse.Status.Status.ReplicatingAcs(status) =>
        for {
          commonFields <- parseCommonFields(
            status.sequencerUid,
            status.topologySerial,
            status.timestamp,
          )
          (sequencerId, topologySerial, timestamp) = commonFields
          contractsReplicated <- ProtoConverter.parseNonNegativeInt(
            "contracts_replicated",
            status.contractsReplicated,
          )
        } yield ReplicatingAcs(sequencerId, topologySerial, timestamp, contractsReplicated)
      case v30.GetAddPartyStatusResponse.Status.Status.Completed(status) =>
        for {
          commonFields <- parseCommonFields(
            status.sequencerUid,
            status.topologySerial,
            status.timestamp,
          )
          (sequencerId, topologySerial, timestamp) = commonFields
          contractsReplicated <- ProtoConverter.parseNonNegativeInt(
            "contracts_replicated",
            status.contractsReplicated,
          )
        } yield Completed(sequencerId, topologySerial, timestamp, contractsReplicated)
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
      case v30.GetAddPartyStatusResponse.Status.Status.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("status"))
    }

  private def parseCommonFields(
      sequencerUidP: String,
      topologySerialP: Int,
      timestampPO: Option[protobuf.timestamp.Timestamp],
  ): ParsingResult[(SequencerId, PositiveInt, CantonTimestamp)] = for {
    sequencerId <- UniqueIdentifier
      .fromProtoPrimitive(sequencerUidP, "sequencer_id")
      .map(SequencerId(_))
    topologySerial <- ProtoConverter.parsePositiveInt("topology_serial", topologySerialP)
    timestampP <- ProtoConverter.required("timestamp", timestampPO)
    timestamp <- CantonTimestamp.fromProtoTimestamp(timestampP)
  } yield (sequencerId, topologySerial, timestamp)

  sealed trait Status
  final case class ProposalProcessed(topologySerial: Option[PositiveInt]) extends Status
  final case class AgreementAccepted(sequencerId: SequencerId, topologySerial: Option[PositiveInt])
      extends Status
  final case class TopologyAuthorized(
      sequencerId: SequencerId,
      topologySerial: PositiveInt,
      timestamp: CantonTimestamp,
  ) extends Status
  final case class ConnectionEstablished(
      sequencerId: SequencerId,
      topologySerial: PositiveInt,
      timestamp: CantonTimestamp,
  ) extends Status
  final case class ReplicatingAcs(
      sequencerId: SequencerId,
      topologySerial: PositiveInt,
      timestamp: CantonTimestamp,
      contractsReplicated: NonNegativeInt,
  ) extends Status
  final case class Completed(
      sequencerId: SequencerId,
      topologySerial: PositiveInt,
      timestamp: CantonTimestamp,
      contractsReplicated: NonNegativeInt,
  ) extends Status
  final case class Error(
      error: String,
      statusPriorToError: Status,
  ) extends Status
}
