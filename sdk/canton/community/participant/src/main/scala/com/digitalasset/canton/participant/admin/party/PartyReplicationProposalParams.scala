// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}

import scala.jdk.CollectionConverters.*

final case class PartyReplicationProposalParams private (
    requestId: Hash,
    partyId: PartyId,
    synchronizerId: SynchronizerId,
    targetParticipantId: ParticipantId,
    sequencerIds: NonEmpty[List[SequencerId]],
    serial: Option[PositiveInt],
)

object PartyReplicationProposalParams {
  def fromDaml(
      c: M.partyreplication.PartyReplicationProposal,
      synchronizer: String,
  ): Either[String, PartyReplicationProposalParams] =
    for {
      _ <- Either.cond(c.partyReplicationId.nonEmpty, (), "Empty party replication id")
      requestId <- Hash
        .fromHexString(c.partyReplicationId)
        .leftMap(err => s"Invalid party replication id: $err")
      partyId <-
        PartyId
          .fromProtoPrimitive(c.partyId, "partyId")
          .leftMap(err => s"Invalid partyId $err")
      // Check the target participant. The source participant has already been checked by the transaction filter.
      targetParticipantId <-
        PartyId
          .fromProtoPrimitive(c.targetParticipant, "targetParticipant")
          .bimap(
            err => s"Invalid targetParticipant admin party $err",
            adminPartyId => ParticipantId(adminPartyId.uid),
          )
      sequencerIds <-
        c.sequencerUids.asScala.toList.traverse(sequencerUid =>
          UniqueIdentifier
            .fromProtoPrimitive(sequencerUid, "sequencerUids")
            .bimap(err => s"Invalid unique identifier $sequencerUid: $err", SequencerId(_))
        )
      sequencerIdsNE <- NonEmpty.from(sequencerIds).toRight("Empty sequencerIds")
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
    } yield PartyReplicationProposalParams(
      requestId,
      partyId,
      synchronizerId,
      targetParticipantId,
      sequencerIdsNE,
      serial,
    )
}
