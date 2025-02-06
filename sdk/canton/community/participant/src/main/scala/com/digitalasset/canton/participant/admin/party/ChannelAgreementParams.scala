// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.ChannelId
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.topology.*

final case class ChannelAgreementParams private (
    ts: CantonTimestamp,
    partyId: PartyId,
    sourceParticipantId: ParticipantId,
    targetParticipantId: ParticipantId,
    sequencerId: SequencerId,
    synchronizerId: SynchronizerId,
)

object ChannelAgreementParams {
  def fromDaml(
      c: M.partyreplication.ChannelAgreement,
      synchronizer: String,
  ): Either[String, ChannelAgreementParams] =
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
      _ <- ChannelId.fromString(c.payloadMetadata.id)
      _ <- NonNegativeLong
        .create(c.payloadMetadata.startAtWatermark)
        .leftMap(_ => s"Invalid, negative startAtWatermark ${c.payloadMetadata.startAtWatermark}")
    } yield ChannelAgreementParams(
      ts,
      partyId,
      sourceParticipantId,
      targetParticipantId,
      sequencerId,
      synchronizerId,
    )
}
