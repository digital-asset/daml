// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import io.scalaland.chimney.dsl.*

final case class PartyReplicationAgreementParams(
    requestId: Hash,
    partyId: PartyId,
    synchronizerId: SynchronizerId,
    sourceParticipantId: ParticipantId,
    targetParticipantId: ParticipantId,
    sequencerId: SequencerId,
    serial: PositiveInt,
    participantPermission: ParticipantPermission,
)

object PartyReplicationAgreementParams {
  def fromDaml(
      c: M.partyreplication.PartyReplicationAgreement,
      synchronizer: String,
  ): Either[String, PartyReplicationAgreementParams] =
    for {
      _ <- Either.cond(c.partyReplicationId.nonEmpty, (), "Empty party replication id")
      requestId <- Hash
        .fromHexString(c.partyReplicationId)
        .leftMap(err => s"Invalid party replication id: $err")
      partyId <-
        PartyId
          .fromProtoPrimitive(c.partyId, "partyId")
          .leftMap(err => s"Invalid partyId $err")
      sourceParticipantId <-
        PartyId
          .fromProtoPrimitive(c.sourceParticipant, "sourceParticipant")
          .bimap(
            err => s"Invalid sourceParticipant admin party $err",
            adminPartyId => ParticipantId(adminPartyId.uid),
          )
      targetParticipantId <-
        PartyId
          .fromProtoPrimitive(c.targetParticipant, "targetParticipant")
          .bimap(
            err => s"Invalid targetParticipant admin party $err",
            adminPartyId => ParticipantId(adminPartyId.uid),
          )
      sequencerId <-
        UniqueIdentifier
          .fromProtoPrimitive(c.sequencerUid, "sequencerId")
          .bimap(err => s"Invalid sequencerId $err", SequencerId(_))
      synchronizerId <-
        SynchronizerId
          .fromProtoPrimitive(synchronizer, "synchronizer")
          // The following error is impossible to trigger as the ledger-api does not emit invalid synchronizer ids
          .leftMap(err => s"Invalid synchronizerId $err")
      serialInt <- Either.cond(
        c.topologySerial.toInt.toLong == c.topologySerial,
        c.topologySerial.toInt,
        s"Non-integer serial ${c.topologySerial}",
      )
      serial <- PositiveInt.create(serialInt).leftMap(_.message)
      participantPermission = PartyParticipantPermission.fromDaml(c.participantPermission)
    } yield PartyReplicationAgreementParams(
      requestId,
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      sequencerId,
      serial,
      participantPermission,
    )

  def fromProposal(
      proposal: PartyReplicationProposalParams,
      sourceParticipantId: ParticipantId,
      sequencerId: SequencerId,
  ): PartyReplicationAgreementParams = proposal
    .into[PartyReplicationAgreementParams]
    .withFieldConst(_.sourceParticipantId, sourceParticipantId)
    .withFieldConst(_.sequencerId, sequencerId)
    .transform

  def fromAgreedReplicationStatus(
      params: PartyReplicationStatus.ReplicationParams,
      sequencerId: SequencerId,
  ): PartyReplicationAgreementParams = params
    .into[PartyReplicationAgreementParams]
    .withFieldConst(_.sequencerId, sequencerId)
    .transform
}
