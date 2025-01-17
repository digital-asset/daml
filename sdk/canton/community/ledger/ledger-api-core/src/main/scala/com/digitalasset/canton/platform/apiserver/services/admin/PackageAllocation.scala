// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.platform.apiserver.services.tracking.StreamTracker
import com.digitalasset.daml.lf.data.Ref

object PartyAllocation {

  final case class TrackerKey private (val submissionId: Ref.SubmissionId)
  object TrackerKey {
    def of(
        party: String,
        participantId: Ref.ParticipantId,
        level: AuthorizationLevel,
    ): TrackerKey = {
      import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}

      val builder = Hash.build(HashPurpose.PartyUpdateId, HashAlgorithm.Sha256)
      builder.add(party.split("::")(0))
      builder.add(participantId)
      builder.add(level.toString)
      val hash = builder.finish()

      TrackerKey(Ref.SubmissionId.assertFromString(hash.toHexString))
    }

    private[admin] def forTests(submissionId: Ref.SubmissionId) = TrackerKey(submissionId)
  }

  final case class Completed(submissionId: TrackerKey, partyDetails: IndexerPartyDetails)

  type Tracker = StreamTracker[TrackerKey, Completed]
}
