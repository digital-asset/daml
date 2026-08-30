// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.integration.tests.multihostedparties.CoinFactoryHelpers.*
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import org.scalatest.Assertion

/*
The goal of this trait is support testing around a consortium party (svc).
We use a `Coin` contract with two signatories: the svc and an owner.

The test trait contains the following test setup steps:
- Create a consortium party (svc) hosted on three participants (P1, P2, P3) + authorized parties on a single BFT synchronizer
- Create a few coins
- Transfer a coin
 */
private[multihostedparties] trait ConsortiumPartyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  protected var consortiumPartyId: PartyId = _

  // current coins ownership
  protected var currentCoins: Set[(Set[PartyId], Double)] = _ // (signatories, amount)

  protected val threshold: PositiveInt = PositiveInt.tryCreate(3) // party confirmation threshold

  protected def checkCoins(participant: ParticipantReference, expectedAmounts: Seq[Double])(implicit
      env: TestConsoleEnvironment
  ): Assertion = {
    import env.*

    val expectedCoins: Set[(Set[PartyId], Double)] =
      expectedAmounts.map(amount => (Set(participant.adminParty, consortiumPartyId), amount)).toSet

    getCoins(participant, participant.adminParty) shouldBe expectedCoins
  }

  protected def coinsFor(
      coins: Set[(Set[PartyId], Double)],
      party: PartyId,
  ): Set[(Set[PartyId], Double)] = coins.filter { case (signatories, _) =>
    signatories.contains(party)
  }

  protected def changePartyPermissions(
      partyId: PartyId,
      newThreshold: PositiveInt,
      newPermissions: ParticipantPermission,
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*

    val allParticipants = Seq(participant1, participant2, participant3)

    logger.debug(s"Change permissions of $partyId: $allParticipants")
    allParticipants.foreach(
      _.topology.party_to_participant_mappings.propose(
        party = partyId,
        newParticipants = allParticipants.map(_.id -> newPermissions),
        threshold = newThreshold,
        store = daId,
      )
    )

    eventually() {
      participant1.topology.party_to_participant_mappings.is_known(
        daId,
        partyId,
        allParticipants,
        Some(newPermissions),
        Some(newThreshold),
      ) shouldBe true
    }
  }

  "Allocate consortium party on three participants" in { implicit env =>
    // Initially with permissions rights + threshold=1 so that we can create the initial contracts
    changePartyPermissions(consortiumPartyId, PositiveInt.one, ParticipantPermission.Submission)
  }

  "Create coins for each participant" in { implicit env =>
    import env.*

    Seq(participant1, participant2, participant3).foreach(p =>
      createCoinsFactory(consortiumPartyId, p.adminParty, p)
    )

    createCoins(participant1.adminParty, participant1, Seq(10.0, 11.0, 12.0, 32.0))
    checkCoins(participant1, Seq(10.0, 11.0, 12.0, 32.0))

    createCoins(participant2.adminParty, participant2, Seq(20.0, 21.0, 22.0))
    checkCoins(participant2, Seq(20.0, 21.0, 22.0))

    createCoins(participant3.adminParty, participant3, Seq(30.0, 31.0))
    checkCoins(participant3, Seq(30.0, 31.0))
  }

  "Change permissions of consortium party so that it cannot submit anymore" in { implicit env =>
    changePartyPermissions(consortiumPartyId, threshold, ParticipantPermission.Confirmation)
  }

  /*
    Coins before this test:       Coins after this test:
      P1: 10, 11, 12, 32            P1: 10, 11, 12
      P2: 20, 21, 22                P2: 20, 21, 22
      P3: 30, 31                    P3: 30, 31, 32
   */
  "Transfer coins" in { implicit env =>
    import env.*

    transferCoin(
      consortiumPartyId,
      participant1.adminParty,
      participant1,
      participant3.adminParty,
      participant3,
      32.0,
    )

    currentCoins = Set(
      // P1
      (Set(participant1.adminParty, consortiumPartyId), 10.0),
      (Set(participant1.adminParty, consortiumPartyId), 11.0),
      (Set(participant1.adminParty, consortiumPartyId), 12.0),
      // P2
      (Set(participant2.adminParty, consortiumPartyId), 20.0),
      (Set(participant2.adminParty, consortiumPartyId), 21.0),
      (Set(participant2.adminParty, consortiumPartyId), 22.0),
      // P3
      (Set(participant3.adminParty, consortiumPartyId), 30.0),
      (Set(participant3.adminParty, consortiumPartyId), 31.0),
      (Set(participant3.adminParty, consortiumPartyId), 32.0),
    )

    getCoins(participant3, as = consortiumPartyId) shouldBe currentCoins

    Seq(participant1, participant2, participant3).foreach { p =>
      getCoins(p, as = p.adminParty) shouldBe coinsFor(currentCoins, p.adminParty)
    }
  }
}
