// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.LoggerSuppressionHelpers
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  ParticipantPermission,
}
import com.digitalasset.canton.topology.{Namespace, PartyId, UniqueIdentifier}
import monocle.Monocle.toAppliedFocusOps

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/*
The goal of this test is to check the behaviour around onboarding a consortium party (svc) to a new, empty participant.
We use a `Coin` contract with two signatories: the svc and an owner.

The suite contains the following steps:
- Create a consortium party (svc) hosted on three participants (P1, P2, P3) + authorized parties on a single BFT synchronizer
- Create a few coins and transfer some coins
- Add a new, empty participant (P4)
- Onboard the consortium party (svc) on P4 with a coin transfer during party migration
- Set party confirmation threshold from 3 to 4 and transfer a coin
- Check that the distributed namespace owning the svc can be extended to include P4
- Check that P4 can create, receive and transfer coins
 */
sealed trait OnboardingConsortiumPartyIntegrationTest extends ConsortiumPartyIntegrationTest {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .updateTestingConfig(
        // do not delay sending commitments
        _.focus(_.maxCommitmentSendDelayMillis).replace(Some(NonNegativeInt.zero))
      )
      .withSetup { implicit env =>
        import env.*

        //  Reduce the epsilon (topology change delay) to 5ms so that topology transaction(s)
        //  become effective sooner; and an outdated topology state and test flakiness is avoided.
        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.update(topologyChangeDelay =
            config.NonNegativeFiniteDuration(FiniteDuration(5, TimeUnit.MILLISECONDS))
          ),
        )

        hostingParticipants = Seq(participant1, participant2, participant3)
        owningParticipants = Seq(participant1, participant2, participant3)

        hostingParticipants.foreach(_.synchronizers.connect_local(sequencer1, daName))
        hostingParticipants.foreach(_.dars.upload(CantonLfV21))

        val decentralizedNamespace = owningParticipants
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

  private val acsFilename = s"${getClass.getSimpleName}.gz"

  override def afterAll(): Unit = {
    Try(File(acsFilename).delete())
    super.afterAll()
  }

  private var hostingParticipants: Seq[LocalParticipantReference] = _
  private var owningParticipants: Seq[LocalParticipantReference] = _
  private var initialDecentralizedNamespace: Namespace = _

  /*
    Coins before this test:
      P1: 10, 11, 12
      P2: 20, 21, 22
      P3: 30, 31, 32
      -
   */
  "Onboard the consortium party on empty participant 4 (P4) with transfer during migration" in {
    implicit env =>
      import env.*

      participant4.synchronizers.connect_local(sequencer1, daName)
      participant4.dars.upload(CantonLfV21)

      // During the time we onboard the svc to P4 and the time we import the ACS to P4, we can have commitment
      // mismatches if the commitment processor ticks; therefore we silence mismatches and no shared contracts warnings
      loggerFactory.assertLogsUnorderedOptional(
        {
          logger.debug(
            "Onboarding: Update party-to-participant mapping: Sequencer 1 (FROM) and P4 (TO) propose svc -> P4"
          )
          val ledgerEndP1 = participant1.ledger_api.state.end()

          val newHostingParticipants = hostingParticipants :+ participant4
          newHostingParticipants.foreach(
            _.topology.party_to_participant_mappings.propose(
              party = consortiumPartyId,
              newParticipants =
                newHostingParticipants.map(_.id -> ParticipantPermission.Confirmation),
              threshold = threshold,
              store = daId,
            )
          )

          logger.debug(
            "Onboarding: Assert consortium party is hosted on P4 and all other participants know about it"
          )
          eventually() {
            // If the topology change is only checked on P4, then due message interleaving other participants may
            // not know about the change yet (resulting in a flaky test).
            newHostingParticipants.foreach(
              _.topology.party_to_participant_mappings.is_known(
                daId,
                consortiumPartyId,
                newHostingParticipants,
              ) shouldBe true
            )
          }

          hostingParticipants = newHostingParticipants

          val partyAddedOnP4Offset = participant1.parties.find_party_max_activation_offset(
            partyId = consortiumPartyId,
            participantId = participant4.id,
            synchronizerId = daId,
            beginOffsetExclusive = ledgerEndP1,
            completeAfter = PositiveInt.one,
          )

          logger.debug("Onboarding: Export ACS from P1")
          participant1.parties.export_acs(
            Set(consortiumPartyId),
            exportFilePath = acsFilename,
            ledgerOffset = partyAddedOnP4Offset,
          )

          participant4.synchronizers.disconnect(daName)

          // Requires the previous topology change to be effective (topology change delay),
          // otherwise this coin transfer fails (resulting in a flaky test)
          logger.debug("Onboarding: Transfer coin from P1 to P2 (activity during migration)")

          // Since P4 is offline, we need to disable the synchronization, as it will not observe the transaction
          CoinFactoryHelpers.transferCoin(
            consortiumPartyId,
            participant1.adminParty,
            participant1,
            participant2.adminParty,
            participant2,
            11.0,
            sync = false,
          )

          currentCoins = Set(
            // P1
            (Set(participant1.adminParty, consortiumPartyId), 10.0),
            (Set(participant1.adminParty, consortiumPartyId), 12.0),
            // P2
            (Set(participant2.adminParty, consortiumPartyId), 20.0),
            (Set(participant2.adminParty, consortiumPartyId), 21.0),
            (Set(participant2.adminParty, consortiumPartyId), 22.0),
            (Set(participant2.adminParty, consortiumPartyId), 11.0),
            // P3
            (Set(participant3.adminParty, consortiumPartyId), 31.0),
            (Set(participant3.adminParty, consortiumPartyId), 32.0),
            (Set(participant3.adminParty, consortiumPartyId), 30.0),
          )

          logger.debug("Onboarding: Import ACS to P4 (an empty participant)")
          participant4.ledger_api.state.acs.of_all() shouldBe empty
          participant4.repair.import_acs(acsFilename)

          logger.debug(s"Onboarding: Connect P4 to the synchronizer $daName")
          participant4.synchronizers.connect_local(sequencer1, daName)
        },
        // for each counter-participant of P4, there can be a commitment mismatch and a no shared contracts error, which we suppress
        // in total we suppress six warnings
        Seq(participant1, participant2, participant3).flatMap(_ =>
          LoggerSuppressionHelpers.suppressOptionalAcsCmtMismatchAndNoSharedContracts()
        )*
      )

      // Need to wait until P4 caught up with the transaction(s) that happened while it was disconnected;
      // the coin transfer P1 -> P2 may not yet be known to P4.
      eventually() {
        CoinFactoryHelpers.getCoins(
          participant4,
          as = consortiumPartyId,
        ) shouldBe currentCoins
      }
  }

  /*
  Coins before this test:
    P1: 10, 12
    P2: 11, 20, 21, 22
    P3: 30, 31, 32
    P4: -
   */
  "Assert P4 also confirms a transaction on behalf of the consortium party" in { implicit env =>
    import env.*

    val newThreshold = PositiveInt.tryCreate(hostingParticipants.size)

    logger.debug(s"Onboarding: Update party confirmation threshold to $newThreshold")
    owningParticipants.foreach(
      _.topology.party_to_participant_mappings.propose(
        party = consortiumPartyId,
        newParticipants = hostingParticipants.map(_.id -> ParticipantPermission.Confirmation),
        threshold = newThreshold,
        store = daId,
      )
    )

    logger.debug(
      s"Onboarding: Check party confirmation threshold is updated to $newThreshold on all participants"
    )
    eventually() {
      hostingParticipants.foreach(
        _.topology.party_to_participant_mappings.is_known(
          daId,
          consortiumPartyId,
          hostingParticipants,
        ) shouldBe true
      )
    }

    hostingParticipants = Seq(participant1, participant2, participant3, participant4)

    CoinFactoryHelpers.transferCoin(
      consortiumPartyId,
      participant3.adminParty,
      participant3,
      participant2.adminParty,
      participant2,
      30.0,
    )

    currentCoins = Set(
      // P1
      (Set(participant1.adminParty, consortiumPartyId), 10.0),
      (Set(participant1.adminParty, consortiumPartyId), 12.0),
      // P2
      (Set(participant2.adminParty, consortiumPartyId), 20.0),
      (Set(participant2.adminParty, consortiumPartyId), 21.0),
      (Set(participant2.adminParty, consortiumPartyId), 22.0),
      (Set(participant2.adminParty, consortiumPartyId), 11.0),
      (Set(participant2.adminParty, consortiumPartyId), 30.0),
      // P3
      (Set(participant3.adminParty, consortiumPartyId), 31.0),
      (Set(participant3.adminParty, consortiumPartyId), 32.0),
    )
  }

  /*
  Coins before this test:
    P1: 10, 12
    P2: 11, 20, 21, 22, 30
    P3: 31, 32
    P4: -
   */
  "Create new coins for P4" in { implicit env =>
    import env.*

    val newCoinsAmount = Seq(40.0, 41.0, 42.0)
    val newCoins =
      newCoinsAmount.map(amount => (Set(consortiumPartyId, participant4.adminParty), amount)).toSet

    CoinFactoryHelpers.createCoins(participant1.adminParty, participant1, newCoinsAmount)
    newCoinsAmount.foreach(
      CoinFactoryHelpers.transferCoin(
        consortiumPartyId,
        participant1.adminParty,
        participant1,
        participant4.adminParty,
        participant4,
        _,
      )
    )
    checkCoins(participant4, newCoinsAmount)

    currentCoins = currentCoins ++ newCoins

    CoinFactoryHelpers.getCoins(participant4, as = participant4.adminParty) shouldBe newCoins
    CoinFactoryHelpers.getCoins(participant4, as = consortiumPartyId) shouldBe currentCoins
  }

  "Extend the owners of the namespace to include P4" in { implicit env =>
    import env.*

    val updatedNamespace = DecentralizedNamespaceDefinition
      .create(
        decentralizedNamespace = initialDecentralizedNamespace,
        threshold = PositiveInt.tryCreate(hostingParticipants.size),
        owners = NonEmpty(
          Set,
          participant4.namespace,
          owningParticipants.map(_.namespace)*
        ),
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
      participant4.topology.decentralized_namespaces
        .list(filterNamespace = initialDecentralizedNamespace.filterString, store = daId)
        .map(_.item)
        .loneElement shouldBe updatedNamespace
    }
  }

  /*
  Coins before this test:
    P1: 10, 12
    P2: 11, 20, 21, 22, 30
    P3: 31, 32
    P4: 40, 41, 42
   */
  "P4 can receive coins and transfer coins" in { implicit env =>
    import env.*

    CoinFactoryHelpers.transferCoin(
      consortiumPartyId,
      participant1.adminParty,
      participant1,
      participant4.adminParty,
      participant4,
      12.0,
    )
    CoinFactoryHelpers.transferCoin(
      consortiumPartyId,
      participant4.adminParty,
      participant4,
      participant2.adminParty,
      participant2,
      41.0,
    )
  }
}

final class OnboardingConsortiumPartyIntegrationTestPostgres
    extends OnboardingConsortiumPartyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
