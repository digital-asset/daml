// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  ParticipantPermission,
}
import com.digitalasset.canton.topology.{Namespace, PartyId, UniqueIdentifier}

/*
The goal of this test is to check the behaviour around offboarding of a consortium party (svc).
We use a `Coin` contract with two signatories: the svc and an owner.

The suite contains the following steps:
- Create a consortium party (svc) hosted on three participants (P1, P2, P3) + authorized parties on a single BFT synchronizer
- Create a few coins
- Transfer a coin
- Offboard the consortium party from P3
- Check that the distributed namespace owning the svc can be reduced to (P1, P2)
- Check that P3 stills sees the contracts for the svc
- Check that P3 can receive and transfer coins
 */
sealed trait OffboardingConsortiumPartyIntegrationTest extends ConsortiumPartyIntegrationTest {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, daName)
      participants.all.dars.upload(CantonLfV21)

      hostingParticipants = Seq(participant1, participant2, participant3)

      val decentralizedNamespace = Seq(participant1, participant2, participant3)
        .map(
          _.topology.decentralized_namespaces
            .propose_new(
              owners = hostingParticipants.map(_.namespace).toSet,
              threshold = PositiveInt.tryCreate(2),
              store = daId,
            )
            .transaction
            .mapping
        )
        .toSet
        .loneElement

      initialDecentralizedNamespace = decentralizedNamespace.namespace

      consortiumPartyId =
        PartyId(UniqueIdentifier.tryCreate("consortium-party", initialDecentralizedNamespace))
    }

  private var hostingParticipants: Seq[LocalParticipantReference] = _
  private var initialDecentralizedNamespace: Namespace = _

  /*
   Coins before this test:
    P1: 10, 11, 12
    P2: 20, 21, 22
    P3: 30, 31, 32
   */
  "Offboard the consortium party from participant 3 (P3)" in { implicit env =>
    import env.*

    hostingParticipants = Seq(participant1, participant2)
    val threshold = PositiveInt.two // 3 -> 2

    logger.debug("Offboarding: Sequencer 1 proposes that P1 and P2 host the consortium party")
    hostingParticipants.foreach(
      _.topology.party_to_participant_mappings.propose(
        party = consortiumPartyId,
        newParticipants = hostingParticipants.map(_.id -> ParticipantPermission.Confirmation),
        threshold = threshold,
        store = daId,
      )
    )

    logger.debug("Offboarding: Check that the consortium party is offboarded")
    eventually() {
      participant1.topology.party_to_participant_mappings
        .is_known(
          daId,
          consortiumPartyId,
          hostingParticipants,
          threshold = Some(threshold),
        ) shouldBe true
    }
  }

  "Remove P3 from the owners of the decentralized namespace" in { implicit env =>
    import env.*

    val updatedNamespace = DecentralizedNamespaceDefinition
      .create(
        decentralizedNamespace = initialDecentralizedNamespace,
        threshold = PositiveInt.tryCreate(hostingParticipants.size),
        owners = NonEmpty.from(hostingParticipants).value.toSet.map(_.namespace),
      )
      .value

    hostingParticipants.foreach { instance =>
      instance.topology.decentralized_namespaces.propose(
        decentralizedNamespace = updatedNamespace,
        store = daId,
        signedBy = Seq(instance.fingerprint),
      )
    }

    eventually() {
      participants.all.foreach(
        _.topology.decentralized_namespaces
          .list(
            filterNamespace = initialDecentralizedNamespace.filterString,
            store = daId,
            // using snapshot at the current time (as opposed to the default headsnapshot)
            // so that the follow up commands only happen when the offboard has already become effective
            timeQuery = TimeQuery.Snapshot(env.environment.clock.now),
          )
          .map(_.item)
          .loneElement shouldBe updatedNamespace
      )
    }
  }

  /*
   Coins before this test:
    P1: 10, 11, 12
    P2: 20, 21, 22
    P3: 30, 31, 32
   */
  "P3 still sees all the coins in the ACS" in { implicit env =>
    import env.*

    /*
      Although P3 does not host the consortium party anymore, we can still request ACS snaphots for that party.
      Also, it stills sees all the coins in the ACS.
      The reason is that the hosted witnesses is not updated in the indexer.
     */

    CoinFactoryHelpers.getCoins(participant3, consortiumPartyId) shouldBe currentCoins
    CoinFactoryHelpers.getCoins(participant3, participant3.adminParty) shouldBe coinsFor(
      currentCoins,
      participant3.adminParty,
    )
  }

  "P3 can receive and transfer coins" in { implicit env =>
    import env.*

    CoinFactoryHelpers.transferCoin(
      consortiumPartyId,
      participant1.adminParty,
      participant1,
      participant3.adminParty,
      participant3,
      12.0,
    )
    CoinFactoryHelpers.transferCoin(
      consortiumPartyId,
      participant3.adminParty,
      participant3,
      participant2.adminParty,
      participant2,
      31.0,
    )

    currentCoins = Set(
      // P1
      (Set(participant1.adminParty, consortiumPartyId), 10.0),
      (Set(participant1.adminParty, consortiumPartyId), 11.0),
      // P2
      (Set(participant2.adminParty, consortiumPartyId), 20.0),
      (Set(participant2.adminParty, consortiumPartyId), 21.0),
      (Set(participant2.adminParty, consortiumPartyId), 22.0),
      (Set(participant2.adminParty, consortiumPartyId), 31.0),
      // P3
      (Set(participant3.adminParty, consortiumPartyId), 30.0),
      (Set(participant3.adminParty, consortiumPartyId), 32.0),
      (Set(participant3.adminParty, consortiumPartyId), 12.0),
    )

    /*
   Coins:
    P1: 10, 11
    P2: 20, 21, 22, 31
    P3: 30, 32, 12
     */

    CoinFactoryHelpers.getCoins(participant1, as = participant1.adminParty) shouldBe coinsFor(
      currentCoins,
      participant1.adminParty,
    )
    CoinFactoryHelpers.getCoins(participant1, as = consortiumPartyId) shouldBe currentCoins

    CoinFactoryHelpers.getCoins(participant2, as = participant2.adminParty) shouldBe coinsFor(
      currentCoins,
      participant2.adminParty,
    )
    CoinFactoryHelpers.getCoins(participant2, as = consortiumPartyId) shouldBe currentCoins

    CoinFactoryHelpers.getCoins(participant3, as = participant3.adminParty) shouldBe coinsFor(
      currentCoins,
      participant3.adminParty,
    )

    {
      val transferredCoin: (Set[PartyId], Double) =
        (
          Set(consortiumPartyId, participant1.adminParty),
          12.0,
        ) // original coin (before transfer to P3)
      val expectedCoins = currentCoins + transferredCoin
      /*
        There are two coins with value 12.0 in the ACS of P3 for consortiumPartyId:
        - The original one (P3 does not learn about the archival since it does not host a stakeholder)
        - The new/transferred one (P3 learns about the create since a signatory is its admin party)
       */

      CoinFactoryHelpers.getCoins(participant3, as = consortiumPartyId) shouldBe expectedCoins
    }
  }
}

final class OffboardingConsortiumPartyIntegrationTestPostgres
    extends OffboardingConsortiumPartyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
