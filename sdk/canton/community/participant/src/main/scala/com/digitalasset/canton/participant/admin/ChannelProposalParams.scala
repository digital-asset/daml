// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.PartyReplicationCoordinator.ChannelId
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.topology.{
  DomainId,
  ParticipantId,
  PartyId,
  SequencerId,
  UniqueIdentifier,
}

final case class ChannelProposalParams private (
    ts: CantonTimestamp,
    partyId: PartyId,
    targetParticipantId: ParticipantId,
    sequencerId: SequencerId,
    domainId: DomainId,
)

object ChannelProposalParams {
  def fromDaml(
      c: M.partyreplication.ChannelProposal,
      domain: String,
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
      sequencerId <-
        UniqueIdentifier
          .fromProtoPrimitive(c.sequencerUid, "sequencerUid")
          .bimap(err => s"Invalid sequencerUid $err", SequencerId(_))
      domainId <-
        DomainId
          .fromProtoPrimitive(domain, "domain")
          // The following error is impossible to trigger as the ledger-api does not emit invalid domain ids
          .leftMap(err => s"Invalid domainId $err")
      _ <- ChannelId.fromString(c.payloadMetadata.id)
      _ <- NonNegativeLong
        .create(c.payloadMetadata.startAtWatermark)
        .leftMap(_ => s"Invalid, negative startAtWatermark ${c.payloadMetadata.startAtWatermark}")
    } yield ChannelProposalParams(ts, partyId, targetParticipantId, sequencerId, domainId)
}
