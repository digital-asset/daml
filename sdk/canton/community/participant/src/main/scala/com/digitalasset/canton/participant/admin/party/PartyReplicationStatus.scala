// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.party.PartyReplicationProcessor
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}

import scala.reflect.ClassTag

/** Internal state representation of the party replication process. Refer to the proto
  * GetAddPartyStatusResponse for the semantics.
  */
object PartyReplicationStatus {
  sealed trait PartyReplicationStatusCode
  object PartyReplicationStatusCode {
    case object ProposalProcessed extends PartyReplicationStatusCode
    case object AgreementAccepted extends PartyReplicationStatusCode
    case object TopologyAuthorized extends PartyReplicationStatusCode
    case object ConnectionEstablished extends PartyReplicationStatusCode
    case object ReplicatingAcs extends PartyReplicationStatusCode
    case object Completed extends PartyReplicationStatusCode
    case object Error extends PartyReplicationStatusCode
    case object Disconnected extends PartyReplicationStatusCode
  }

  /** Indicates whether party replication is active and expected to be progressing, i.e. whether
    * monitoring for progress and initiating state transitions are needed in contrast to having
    * completed or having failed in such a way that requires operator intervention.
    */
  sealed trait ProgressIsExpected

  sealed trait PartyReplicationStatus {
    def params: ReplicationParams
    def code: PartyReplicationStatusCode

    // Used to get the current status if it matches the expected status type.
    // This is typically used for error checking when enforcing the current status
    // prior to a status change.
    final def select[PRS <: PartyReplicationStatus](implicit
        M: ClassTag[PRS]
    ): Option[PRS] = M.unapply(this)

    def toProto: v30.GetAddPartyStatusResponse.Status.Status
  }

  final case class ProposalProcessed(params: ReplicationParams)
      extends PartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.ProposalProcessed

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.ProposalProcessed(
        v30.GetAddPartyStatusResponse.Status.ProposalProcessed()
      )
  }

  object ProposalProcessed {
    def apply(
        addPartyRequestId: Hash,
        partyId: PartyId,
        synchronizerId: SynchronizerId,
        sourceParticipantId: ParticipantId,
        targetParticipantId: ParticipantId,
        serial: PositiveInt,
        participantPermission: ParticipantPermission,
    ): ProposalProcessed =
      ProposalProcessed(
        ReplicationParams(
          addPartyRequestId,
          partyId,
          synchronizerId,
          sourceParticipantId,
          targetParticipantId,
          serial,
          participantPermission,
        )
      )
  }
  final case class AgreementAccepted(
      params: ReplicationParams,
      sequencerId: SequencerId,
  ) extends PartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.AgreementAccepted

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.AgreementAccepted(
        v30.GetAddPartyStatusResponse.Status
          .AgreementAccepted(sequencerId.uid.toProtoPrimitive)
      )
  }
  object AgreementAccepted {
    def apply(agreement: PartyReplicationAgreementParams): AgreementAccepted =
      AgreementAccepted(
        ReplicationParams(
          agreement.requestId,
          agreement.partyId,
          agreement.synchronizerId,
          agreement.sourceParticipantId,
          agreement.targetParticipantId,
          agreement.serial,
          agreement.participantPermission,
        ),
        agreement.sequencerId,
      )
  }
  sealed trait AuthorizedPartyReplicationStatus extends PartyReplicationStatus {
    def authorizedParams: AuthorizedReplicationParams
    def params: ReplicationParams = authorizedParams.replicationParams
  }

  final case class TopologyAuthorized(authorizedParams: AuthorizedReplicationParams)
      extends AuthorizedPartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.TopologyAuthorized

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.TopologyAuthorized(
        v30.GetAddPartyStatusResponse.Status
          .TopologyAuthorized(
            authorizedParams.sequencerId.uid.toProtoPrimitive,
            Some(authorizedParams.effectiveAt.toProtoTimestamp),
          )
      )
  }

  object TopologyAuthorized {
    def apply(
        agreement: ReplicationParams,
        sequencerId: SequencerId,
        effectiveAt: CantonTimestamp,
    ): TopologyAuthorized =
      TopologyAuthorized(AuthorizedReplicationParams(agreement, sequencerId, effectiveAt))
  }

  sealed trait ConnectedPartyReplicationStatus extends AuthorizedPartyReplicationStatus {
    def connectedParams: ConnectedReplicationParams
    def authorizedParams: AuthorizedReplicationParams = connectedParams.authorizedReplicationParams
  }

  final case class ConnectionEstablished(
      connectedParams: ConnectedReplicationParams
  ) extends ConnectedPartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.ConnectionEstablished

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.ConnectionEstablished(
        v30.GetAddPartyStatusResponse.Status
          .ConnectionEstablished(
            authorizedParams.sequencerId.uid.toProtoPrimitive,
            Some(authorizedParams.effectiveAt.toProtoTimestamp),
          )
      )
  }

  final case class ReplicatingAcs(
      connectedParams: ConnectedReplicationParams
  ) extends ConnectedPartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.ReplicatingAcs

    def replicatedContractsCount: NonNegativeInt =
      connectedParams.partyReplicationProcessor.replicatedContractsCount

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.ReplicatingAcs(
        v30.GetAddPartyStatusResponse.Status
          .ReplicatingAcs(
            authorizedParams.sequencerId.uid.toProtoPrimitive,
            Some(authorizedParams.effectiveAt.toProtoTimestamp),
            replicatedContractsCount.unwrap,
          )
      )
  }

  final case class Completed(
      connectedParams: ConnectedReplicationParams
  ) extends ConnectedPartyReplicationStatus {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.Completed

    def replicatedContractsCount: NonNegativeInt =
      connectedParams.partyReplicationProcessor.replicatedContractsCount

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.Completed(
        v30.GetAddPartyStatusResponse.Status
          .Completed(
            authorizedParams.sequencerId.uid.toProtoPrimitive,
            Some(authorizedParams.effectiveAt.toProtoTimestamp),
            replicatedContractsCount.unwrap,
          )
      )
  }

  final case class Error(
      error: String,
      previousStatus: PartyReplicationStatus,
  ) extends PartyReplicationStatus {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.Error
    override def params: ReplicationParams = previousStatus.params

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.Error(
        v30.GetAddPartyStatusResponse.Status
          .Error(
            error,
            Some(v30.GetAddPartyStatusResponse.Status(previousStatus.toProto)),
          )
      )
  }

  final case class Disconnected(
      message: String,
      previousConnectedStatus: ConnectedPartyReplicationStatus,
  ) extends PartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.Disconnected
    override def params: ReplicationParams =
      previousConnectedStatus.authorizedParams.replicationParams

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.Disconnected(
        v30.GetAddPartyStatusResponse.Status
          .Disconnected(
            message,
            Some(v30.GetAddPartyStatusResponse.Status(previousConnectedStatus.toProto)),
          )
      )
  }

  final case class ReplicationParams(
      requestId: Hash,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  )

  final case class AuthorizedReplicationParams(
      replicationParams: ReplicationParams,
      sequencerId: SequencerId,
      effectiveAt: CantonTimestamp,
  ) {
    def partyId: PartyId = replicationParams.partyId
  }

  final case class ConnectedReplicationParams(
      authorizedReplicationParams: AuthorizedReplicationParams,
      partyReplicationProcessor: PartyReplicationProcessor,
  ) {
    def partyId: PartyId = authorizedReplicationParams.partyId

    def replicatedContractsCount: NonNegativeInt =
      partyReplicationProcessor.replicatedContractsCount
  }
}
