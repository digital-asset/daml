// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.party.PartyReplicationProcessor
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}
import io.scalaland.chimney.dsl.*

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
  sealed trait AgreedReplicationStatus extends PartyReplicationStatus {
    def sequencerId: SequencerId
    def damlAgreementCid: LfContractId
  }
  final case class AgreementAccepted(
      params: ReplicationParams,
      sequencerId: SequencerId,
      damlAgreementCid: LfContractId,
  ) extends AgreedReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.AgreementAccepted

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.AgreementAccepted(
        v30.GetAddPartyStatusResponse.Status
          .AgreementAccepted(sequencerId.uid.toProtoPrimitive)
      )
  }
  object AgreementAccepted {
    def apply(
        agreement: PartyReplicationAgreementParams,
        damlAgreementCid: LfContractId,
    ): AgreementAccepted =
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
        damlAgreementCid,
      )
  }
  sealed trait AuthorizedPartyReplicationStatus extends AgreedReplicationStatus {
    def sequencerId: SequencerId
    def effectiveAt: CantonTimestamp
  }

  final case class TopologyAuthorized(
      params: ReplicationParams,
      sequencerId: SequencerId,
      damlAgreementCid: LfContractId,
      effectiveAt: CantonTimestamp,
  ) extends AuthorizedPartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.TopologyAuthorized

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.TopologyAuthorized(
        v30.GetAddPartyStatusResponse.Status
          .TopologyAuthorized(
            sequencerId.uid.toProtoPrimitive,
            Some(effectiveAt.toProtoTimestamp),
          )
      )
  }

  sealed trait ConnectedPartyReplicationStatus extends AuthorizedPartyReplicationStatus {
    def partyReplicationProcessor: PartyReplicationProcessor
    final def replicatedContractsCount: NonNegativeInt =
      partyReplicationProcessor.replicatedContractsCount
  }

  final case class ConnectionEstablished(
      params: ReplicationParams,
      sequencerId: SequencerId,
      damlAgreementCid: LfContractId,
      effectiveAt: CantonTimestamp,
      partyReplicationProcessor: PartyReplicationProcessor,
  ) extends ConnectedPartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.ConnectionEstablished

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.ConnectionEstablished(
        v30.GetAddPartyStatusResponse.Status
          .ConnectionEstablished(
            sequencerId.uid.toProtoPrimitive,
            Some(effectiveAt.toProtoTimestamp),
          )
      )
  }

  final case class ReplicatingAcs(
      params: ReplicationParams,
      sequencerId: SequencerId,
      damlAgreementCid: LfContractId,
      effectiveAt: CantonTimestamp,
      partyReplicationProcessor: PartyReplicationProcessor,
  ) extends ConnectedPartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.ReplicatingAcs

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.ReplicatingAcs(
        v30.GetAddPartyStatusResponse.Status
          .ReplicatingAcs(
            sequencerId.uid.toProtoPrimitive,
            Some(effectiveAt.toProtoTimestamp),
            replicatedContractsCount.unwrap,
          )
      )
  }
  object ReplicatingAcs {
    def apply(connectionEstablished: ConnectionEstablished): ReplicatingAcs =
      connectionEstablished.transformInto[ReplicatingAcs]
  }

  final case class FullyReplicatedAcs(
      params: ReplicationParams,
      sequencerId: SequencerId,
      damlAgreementCid: LfContractId,
      effectiveAt: CantonTimestamp,
      partyReplicationProcessor: PartyReplicationProcessor,
  ) extends ConnectedPartyReplicationStatus
      with ProgressIsExpected {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.ReplicatingAcs

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.FullyReplicatedAcs(
        v30.GetAddPartyStatusResponse.Status
          .FullyReplicatedAcs(
            sequencerId.uid.toProtoPrimitive,
            Some(effectiveAt.toProtoTimestamp),
            replicatedContractsCount.unwrap,
          )
      )
  }
  object FullyReplicatedAcs {
    def apply(replicatingAcs: ReplicatingAcs): FullyReplicatedAcs =
      replicatingAcs.transformInto[FullyReplicatedAcs]
    def apply(connectionEstablished: ConnectionEstablished): FullyReplicatedAcs =
      connectionEstablished.transformInto[FullyReplicatedAcs]
  }

  final case class Completed(
      params: ReplicationParams,
      sequencerId: SequencerId,
      damlAgreementCid: LfContractId,
      effectiveAt: CantonTimestamp,
      partyReplicationProcessor: PartyReplicationProcessor,
  ) extends ConnectedPartyReplicationStatus {
    override def code: PartyReplicationStatusCode = PartyReplicationStatusCode.Completed

    override def toProto: v30.GetAddPartyStatusResponse.Status.Status =
      v30.GetAddPartyStatusResponse.Status.Status.Completed(
        v30.GetAddPartyStatusResponse.Status
          .Completed(
            sequencerId.uid.toProtoPrimitive,
            Some(effectiveAt.toProtoTimestamp),
            replicatedContractsCount.unwrap,
          )
      )
  }
  object Completed {
    def apply(fullyReplicatedAcs: FullyReplicatedAcs): Completed =
      fullyReplicatedAcs.transformInto[Completed]
  }

  final case class Error(
      error: String,
      previousStatus: PartyReplicationStatus,
  ) extends PartyReplicationStatus {
    require(
      previousStatus.code != PartyReplicationStatusCode.Error,
      s"Cannot chain previous error $previousStatus and new error $error",
    )
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
    override def params: ReplicationParams = previousConnectedStatus.params

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
}
