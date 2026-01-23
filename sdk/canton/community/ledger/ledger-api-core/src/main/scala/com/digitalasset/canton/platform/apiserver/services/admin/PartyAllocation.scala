// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.platform.apiserver.services.tracking.StreamTracker
import com.digitalasset.daml.lf.data.Ref

object PartyAllocation {

  final case class TrackerKey(
      partyId: LfPartyId,
      participantId: Ref.ParticipantId,
      authorizationEvent: AuthorizationEvent,
  ) {
    lazy val submissionId = {
      val builder = Hash.build(HashPurpose.PartyUpdateId, HashAlgorithm.Sha256)
      builder.addString(partyId)
      builder.addString(participantId)
      builder.addString(authorizationEvent.toString)
      val hash = builder.finish()

      Ref.SubmissionId.assertFromString(hash.toHexString)
    }

    // Override hashCode and equals to only consider submissionId for equality and hashing
    // Needed for when they key is used in HashMaps etc...
    override def hashCode(): Int = submissionId.hashCode
    override def equals(obj: Any): Boolean = obj match {
      case otherTrackerKey: TrackerKey => submissionId.equals(otherTrackerKey.submissionId)
      case _ => false
    }
  }
  final case class Completed(submissionId: TrackerKey, partyDetails: IndexerPartyDetails)

  type Tracker = StreamTracker[TrackerKey, Completed]
}
