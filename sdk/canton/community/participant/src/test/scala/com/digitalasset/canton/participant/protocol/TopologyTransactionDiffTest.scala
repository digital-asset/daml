// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.topology.DefaultTestIdentities.sequencerId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  SynchronizerId,
  TestingOwnerWithKeys,
  UniqueIdentifier,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

class TopologyTransactionDiffTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {
  private lazy val topologyFactory =
    new TestingOwnerWithKeys(sequencerId, loggerFactory, executorService)

  private lazy val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("domain::mydomain")
  )

  private def ptp(
      partyId: PartyId,
      participants: List[ParticipantId],
  ): SignedTopologyTransaction[Replace, PartyToParticipant] = {

    val mapping = PartyToParticipant.tryCreate(
      partyId,
      PositiveInt.one,
      participants.map(HostingParticipant(_, ParticipantPermission.Submission)),
    )

    val tx: TopologyTransaction[Replace, PartyToParticipant] = TopologyTransaction(
      Replace,
      PositiveInt.one,
      mapping,
      testedProtocolVersion,
    )

    topologyFactory.mkTrans[Replace, PartyToParticipant](trans = tx)
  }

  "TopologyTransactionDiff" should {

    "compute adds and removes" in {
      val p1 = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::participant1"))
      val p2 = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::participant2"))

      val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::alice"))
      val bob = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::bob"))
      val charlie = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::charlie"))
      val donald = PartyId(UniqueIdentifier.tryFromProtoPrimitive("da::donald"))

      /*
        Initial topology:
          alice -> p1, p2
          bob -> p1
          charlie -> p2
       */
      val initialTxs = List(
        ptp(alice, List(p1, p2)),
        ptp(bob, List(p1)),
        ptp(charlie, List(p2)),
      )

      def diffInitialWith(
          newState: Seq[SignedTopologyTransaction[Replace, TopologyMapping]]
      ) = TopologyTransactionDiff(
        synchronizerId,
        initialTxs,
        newState,
        p1,
        testedProtocolVersion,
      )
        .map { case TopologyTransactionDiff(events, _, _) =>
          events
        }
      // Same transactions
      diffInitialWith(initialTxs) shouldBe None

      // Empty target -> everything is removed
      diffInitialWith(Nil).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked),
        PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked),
        PartyToParticipantAuthorization(bob.toLf, p1.toLf, Revoked),
        PartyToParticipantAuthorization(charlie.toLf, p2.toLf, Revoked),
      )

      diffInitialWith(
        List(
          ptp(alice, List(p2)), // no p1

          ptp(bob, List(p1)),
          ptp(charlie, List(p2)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked)
      )

      diffInitialWith(
        List(
          ptp(alice, List()), // nobody
          ptp(bob, List(p1)),
          ptp(charlie, List(p2)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked),
        PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked),
      )

      diffInitialWith(
        List(
          ptp(alice, List(p1, p2)),
          ptp(bob, List(p1, p2)), // p2 added
          ptp(charlie, List(p2)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(bob.toLf, p2.toLf, Submission)
      )

      diffInitialWith(
        List(
          ptp(donald, List(p1)) // new
        ) ++ initialTxs
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(donald.toLf, p1.toLf, Submission)
      )
    }

  }
}
