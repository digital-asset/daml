// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::mysync")
  )

  private def ptp(
      partyId: PartyId,
      participants: List[(ParticipantId, ParticipantPermission)],
  ): SignedTopologyTransaction[Replace, PartyToParticipant] = {

    val mapping = PartyToParticipant.tryCreate(
      partyId,
      PositiveInt.one,
      participants.map { case (participant, permission) =>
        HostingParticipant(participant, permission)
      },
    )

    val tx: TopologyTransaction[Replace, PartyToParticipant] = TopologyTransaction(
      Replace,
      PositiveInt.one,
      mapping,
      testedProtocolVersion,
    )

    topologyFactory.mkTrans[Replace, PartyToParticipant](trans = tx)
  }

  private def synchronizerTrustCertificate(
      participantId: ParticipantId
  ): SignedTopologyTransaction[Replace, SynchronizerTrustCertificate] = {

    val tx: TopologyTransaction[Replace, SynchronizerTrustCertificate] = TopologyTransaction(
      Replace,
      PositiveInt.one,
      SynchronizerTrustCertificate(participantId, synchronizerId),
      testedProtocolVersion,
    )

    topologyFactory.mkTrans[Replace, SynchronizerTrustCertificate](trans = tx)
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
        ptp(
          alice,
          List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
        ),
        ptp(bob, List(p1 -> ParticipantPermission.Submission)),
        ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
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
          ptp(alice, List(p2 -> ParticipantPermission.Submission)), // no p1

          ptp(bob, List(p1 -> ParticipantPermission.Submission)),
          ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked)
      )

      diffInitialWith(
        List(
          ptp(alice, List()), // nobody
          ptp(bob, List(p1 -> ParticipantPermission.Submission)),
          ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(alice.toLf, p1.toLf, Revoked),
        PartyToParticipantAuthorization(alice.toLf, p2.toLf, Revoked),
      )

      diffInitialWith(
        List(
          ptp(
            alice,
            List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
          ),
          ptp(
            bob,
            List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
          ), // p2 added
          ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(bob.toLf, p2.toLf, Submission)
      )

      diffInitialWith(
        List(
          ptp(donald, List(p1 -> ParticipantPermission.Submission)) // new
        ) ++ initialTxs
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(donald.toLf, p1.toLf, Submission)
      )

      diffInitialWith(
        List(
          synchronizerTrustCertificate(p1), // new
          synchronizerTrustCertificate(p2), // new
        ) ++ initialTxs
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(p1.adminParty.toLf, p1.toLf, Submission),
        PartyToParticipantAuthorization(p2.adminParty.toLf, p2.toLf, Submission),
      )

      diffInitialWith(
        List(
          ptp(
            bob,
            List(p1 -> ParticipantPermission.Confirmation, p2 -> ParticipantPermission.Observation),
          ), // p2 added, p2 overridden
          ptp(
            alice,
            List(p1 -> ParticipantPermission.Submission, p2 -> ParticipantPermission.Submission),
          ),
          ptp(charlie, List(p2 -> ParticipantPermission.Submission)),
        )
      ).value.forgetNE should contain theSameElementsAs Set(
        PartyToParticipantAuthorization(bob.toLf, p1.toLf, Confirmation),
        PartyToParticipantAuthorization(bob.toLf, p2.toLf, Observation),
      )
    }

  }
}
