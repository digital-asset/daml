// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.topology.*

final case class PartyReplicationAgreementParams private (
    partyReplicationId: Hash,
    partyId: PartyId,
    synchronizerId: SynchronizerId,
    sourceParticipantId: ParticipantId,
    targetParticipantId: ParticipantId,
    sequencerId: SequencerId,
    serialO: Option[PositiveInt],
)

object PartyReplicationAgreementParams {
  def fromDaml(
      c: M.partyreplication.PartyReplicationAgreement,
      synchronizer: String,
  ): Either[String, PartyReplicationAgreementParams] =
    for {
      _ <- Either.cond(c.partyReplicationId.nonEmpty, (), "Empty party replication id")
      partyReplicationId <- Hash
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
      serialIntO <- Either.cond(
        c.topologySerial.toInt.toLong == c.topologySerial,
        Option.when(c.topologySerial.toInt != 0)(c.topologySerial.toInt),
        s"Non-integer serial ${c.topologySerial}",
      )
      serial <- serialIntO.traverse(PositiveInt.create).leftMap(_.message)
    } yield PartyReplicationAgreementParams(
      partyReplicationId,
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      sequencerId,
      serial,
    )
}
