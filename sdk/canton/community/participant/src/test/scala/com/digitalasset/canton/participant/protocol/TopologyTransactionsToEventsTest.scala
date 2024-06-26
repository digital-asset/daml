// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.topology.DefaultTestIdentities.sequencerId
import com.digitalasset.canton.topology.store.SignedTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  TestingOwnerWithKeys,
  UniqueIdentifier,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

class TopologyTransactionsToEventsTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {
  private lazy val topologyFactory =
    new TestingOwnerWithKeys(sequencerId, loggerFactory, executorService)

  private def ptp(
      partyId: PartyId,
      participants: List[ParticipantId],
  ): SignedTopologyTransaction[Replace, PartyToParticipant] = {

    val mapping = PartyToParticipant.tryCreate(
      partyId,
      None,
      PositiveInt.one,
      participants.map(HostingParticipant(_, ParticipantPermission.Submission)),
      groupAddressing = false,
    )

    val tx: TopologyTransaction[Replace, PartyToParticipant] = TopologyTransaction(
      Replace,
      PositiveInt.one,
      mapping,
      testedProtocolVersion,
    )

    topologyFactory.mkTrans[Replace, PartyToParticipant](trans = tx)
  }

  private lazy val converter: TopologyTransactionsToEvents = new TopologyTransactionsToEvents(
    loggerFactory
  )

  "TopologyTransactionsToEvents" should {

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
      val initialState = SignedTopologyTransactions(initialTxs)

      def compute(
          newState: Seq[SignedTopologyTransaction[Replace, TopologyMapping]]
      ): (Map[PartyId, Set[ParticipantId]], Map[PartyId, Set[ParticipantId]]) =
        converter.computePartiesAddedRemoved(
          initialState,
          SignedTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](newState),
        )

      val noChange = (Map.empty, Map.empty)

      // Same transactions
      compute(initialState.result) shouldBe noChange

      // Empty target -> everything is removed
      compute(Nil) shouldBe (Map.empty, Map(
        alice -> Set(p1, p2),
        bob -> Set(p1),
        charlie -> Set(p2),
      ))

      compute(
        List(
          ptp(alice, List(p2)), // no p1

          ptp(bob, List(p1)),
          ptp(charlie, List(p2)),
        )
      ) shouldBe (Map.empty, Map(alice -> Set(p1)))

      compute(
        List(
          ptp(alice, List()), // nobody
          ptp(bob, List(p1)),
          ptp(charlie, List(p2)),
        )
      ) shouldBe (Map.empty, Map(alice -> Set(p1, p2)))

      compute(
        List(
          ptp(donald, List(p1)) // new
        ) ++ initialTxs
      ) shouldBe (Map(donald -> Set(p1)), Map.empty)
    }

  }
}
