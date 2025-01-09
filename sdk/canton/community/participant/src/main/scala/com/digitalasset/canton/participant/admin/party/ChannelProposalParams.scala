// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.ChannelId
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}

import scala.jdk.CollectionConverters.*

final case class ChannelProposalParams private (
    ts: CantonTimestamp,
    partyId: PartyId,
    targetParticipantId: ParticipantId,
    sequencerIds: NonEmpty[List[SequencerId]],
    synchronizerId: SynchronizerId,
)

object ChannelProposalParams {
  def fromDaml(
      c: M.partyreplication.ChannelProposal,
      synchronizer: String,
  ): Either[String, ChannelProposalParams] =
    for {
      ts <-
        CantonTimestamp
          .fromInstant(c.payloadMetadata.timestamp)
          // the following error is actually impossible to trigger as the lf-engine catches bad timestamps
          .leftMap(err => s"Invalid timestamp $err")
      partyId <-
        PartyId
          .fromProtoPrimitive(c.payloadMetadata.partyId, "partyId")
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
      _ <- ChannelId.fromString(c.payloadMetadata.id)
      _ <- NonNegativeLong
        .create(c.payloadMetadata.startAtWatermark)
        .leftMap(_ => s"Invalid, negative startAtWatermark ${c.payloadMetadata.startAtWatermark}")
    } yield ChannelProposalParams(ts, partyId, targetParticipantId, sequencerIdsNE, synchronizerId)
}
