// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
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
  }

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

  final case class ProposalProcessed(params: ReplicationParams) extends PartyReplicationStatus {
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
    ): ProposalProcessed =
      ProposalProcessed(
        ReplicationParams(
          addPartyRequestId,
          partyId,
          synchronizerId,
          sourceParticipantId,
          targetParticipantId,
          serial,
        )
      )
  }
  final case class AgreementAccepted(
      params: ReplicationParams,
      sequencerId: SequencerId,
  ) extends PartyReplicationStatus {
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
        ),
        agreement.sequencerId,
      )
  }
  sealed trait AuthorizedPartyReplicationStatus extends PartyReplicationStatus {
    def authorizedParams: AuthorizedReplicationParams
    def params: ReplicationParams = authorizedParams.toBasic
  }

  final case class TopologyAuthorized(authorizedParams: AuthorizedReplicationParams)
      extends AuthorizedPartyReplicationStatus {
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
      TopologyAuthorized(
        AuthorizedReplicationParams(
          agreement.requestId,
          agreement.partyId,
          agreement.synchronizerId,
          agreement.sourceParticipantId,
          agreement.targetParticipantId,
          agreement.serial,
          sequencerId,
          effectiveAt,
        )
      )
  }

  final case class ConnectionEstablished(authorizedParams: AuthorizedReplicationParams)
      extends AuthorizedPartyReplicationStatus {
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
      authorizedParams: AuthorizedReplicationParams,
      numberOfContractsReplicated: NonNegativeInt,
  ) extends AuthorizedPartyReplicationStatus {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.ReplicatingAcs

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.ReplicatingAcs(
        v30.GetAddPartyStatusResponse.Status
          .ReplicatingAcs(
            authorizedParams.sequencerId.uid.toProtoPrimitive,
            Some(authorizedParams.effectiveAt.toProtoTimestamp),
            numberOfContractsReplicated.unwrap,
          )
      )
  }

  final case class Completed(
      authorizedParams: AuthorizedReplicationParams,
      numberOfContractsReplicated: NonNegativeInt,
  ) extends AuthorizedPartyReplicationStatus {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.Completed

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.Completed(
        v30.GetAddPartyStatusResponse.Status
          .Completed(
            authorizedParams.sequencerId.uid.toProtoPrimitive,
            Some(authorizedParams.effectiveAt.toProtoTimestamp),
            numberOfContractsReplicated.unwrap,
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

  final case class ReplicationParams(
      requestId: Hash,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
  ) {
    implicit def toAuthorized(
        sequencerId: SequencerId,
        effectiveAt: CantonTimestamp,
    ): AuthorizedReplicationParams =
      AuthorizedReplicationParams(
        requestId,
        partyId,
        synchronizerId,
        sourceParticipantId,
        targetParticipantId,
        serial,
        sequencerId,
        effectiveAt,
      )
  }

  final case class AuthorizedReplicationParams(
      requestId: Hash,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
      sequencerId: SequencerId,
      effectiveAt: CantonTimestamp,
  ) {
    implicit def toBasic: ReplicationParams =
      ReplicationParams(
        requestId,
        partyId,
        synchronizerId,
        sourceParticipantId,
        targetParticipantId,
        serial,
      )
  }
}
