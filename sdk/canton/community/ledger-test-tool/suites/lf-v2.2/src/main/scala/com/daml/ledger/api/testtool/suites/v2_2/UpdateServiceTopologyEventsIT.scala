// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction

import scala.concurrent.{ExecutionContext, Future}

class UpdateServiceTopologyEventsIT extends LedgerTestSuite {

  testSubscription(
    "USPartyEventsInUpdatesTailing",
    "Events should be served in update stream if parties added during subscription",
    implicit ec =>
      ledger => {
        val futureTopos = ledger
          .participantAuthorizationTransaction("USPartyEventsInUpdatesTailing")
        for {
          party <- ledger.allocateParty()
          topos <- futureTopos
        } yield (party, topos)
      },
  )

  testSubscription(
    "USPartyEventsInUpdatesNotTailing",
    "Events should be served in update stream if parties added before subscription",
    implicit ec =>
      ledger =>
        for {
          begin <- ledger.currentEnd()
          party <- ledger.allocateParty()
          // For synchronization
          _ <- ledger.participantAuthorizationTransaction(
            partyIdSubstring = "USPartyEventsInUpdatesNotTailing",
            begin = Some(begin),
          )
          end <- ledger.currentEnd()
          topo <- ledger.participantAuthorizationTransaction(
            partyIdSubstring = "USPartyEventsInUpdatesNotTailing",
            begin = Some(begin),
            end = Some(end),
          )
        } yield (party, topo),
  )

  private def testSubscription(
      shortIdentifier: String,
      description: String,
      query: ExecutionContext => ParticipantTestContext => Future[
        (Party, TopologyTransaction)
      ],
  ): Unit =
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      partyAllocation = allocate(NoParties),
      runConcurrently = false,
    )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
      query(ec)(ledger).map { case (party, topo) =>
        val events = topo.events
        assert(
          events.nonEmpty,
          "expected to observe a topology event after party allocation",
        )
        assert(
          topo.offset > 0,
          "expected to populate valid offset",
        )
        assert(
          topo.updateId.nonEmpty,
          "expected to populate valid update ID",
        )
        assert(
          topo.recordTime.nonEmpty,
          "expected to populate valid record time",
        )
        assert(
          topo.synchronizerId.nonEmpty,
          "expected to populate valid synchronizer ID",
        )
        val authorization = events.headOption.flatMap(_.event.participantAuthorizationAdded)
        assert(
          authorization.nonEmpty,
          "expected to observe a participant authorization added event",
        )
        val actualParty = authorization.fold("<empty>")(_.partyId)
        assert(
          actualParty == party.underlying.getValue,
          s"expected to observe topology event for ${party.underlying.getValue} but got $actualParty",
        )
      }
    })
}
