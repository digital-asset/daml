// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedContractEntry
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{
  FeatureFlag,
  LocalMediatorReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.logging.{LogEntry, SuppressingLogger, SuppressionRule}
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription
import org.slf4j.event.Level

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.*

@nowarn("msg=match may not be exhaustive")
sealed abstract class SynchronizerRepairIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasExecutionContext
    with RepairTestUtil {

  // two participant, two synchronizer environment to use first synchronizer to source some "real" contracts, then to be
  // considered "broken", and a second initially "empty" synchronizer to use as replacement synchronizer
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransform(ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair))
      .withSetup { implicit env =>
        import env.*
        Seq(participant1, participant2).foreach { p =>
          p.synchronizers.connect_local(lostSynchronizerSequencer, alias = lostSynchronizerAlias)
          p.dars.upload(CantonExamplesPath)
        }

        participant1.parties.enable(
          aliceS,
          synchronizeParticipants = Seq(participant2),
        )
        participant2.parties.enable(
          bobS,
          synchronizeParticipants = Seq(participant1),
        )
      }

  "Able to set up participants with data and connect to replacement synchronizer" in {
    implicit env =>
      import env.*

      val Alice = aliceS.toPartyId(participant1)
      val Bob = bobS.toPartyId(participant2)

      Range.apply(0, 3).foreach { _ =>
        createContract(participant1, Alice, Bob)
        createContract(participant2, Bob, Alice)
      }
      exerciseContract(participant1, Alice, createContract(participant1, Alice, Alice))
      exerciseContract(participant2, Bob, createContract(participant2, Bob, Bob))

      logger.debug("Checking that participants see contracts in ACS")

      assertAcsCounts(
        (participant1, Map(Alice -> 7, Bob -> 6)),
        (participant2, Map(Alice -> 6, Bob -> 7)),
      )

      val lostSubscriptionMessage = ResilientSequencerSubscription.LostSequencerSubscription
        .Warn(lostSynchronizerSequencer.id, _logOnCreation = false)
        .cause

      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          // From here on consider the lost synchronizer broken.

          // Stop mediator first for less log noise / test flakiness. Doing do outside of
          // user manual entry as taking down the mediator is not necessary for the tutorial.
          lostSynchronizerMediator.stop()

          // user-manual-entry-begin: ConsiderSynchronizerBroken
          lostSynchronizerSequencer.stop()
          Seq(participant1, participant2).foreach { p =>
            p.synchronizers.disconnect(lostSynchronizerAlias)
            // Let the participant know not to attempt to reconnect to the lost synchronizer
            p.synchronizers.modify(
              lostSynchronizerAlias,
              _.copy(manualConnect = true),
              validation = SequencerConnectionValidation.Disabled,
            )
          }
          // user-manual-entry-end: ConsiderSynchronizerBroken
        },
        (logs: Seq[LogEntry]) => {
          val expectedMessages = List(
            "Request failed for sequencer. Is the server running?",
            lostSubscriptionMessage,
          )

          SuppressingLogger.assertThatLogDoesntContainUnexpected(expectedMessages)(logs)
        },
      )

      logger.debug(
        "Participants disconnected from lost synchronizer will now attempt to connect to new synchronizer"
      )

      Seq(participant1, participant2).foreach(
        _.synchronizers.connect_local(newSynchronizerSequencer, alias = newSynchronizerAlias)
      )

      // Wait for topology state to appear before disconnecting again.
      clue("newSynchronizer initialization timed out") {
        eventually()(
          (
            participant1.synchronizers.active(newSynchronizerAlias),
            participant2.synchronizers.active(newSynchronizerAlias),
          ) shouldBe (true, true)
        )
      }

      // Note that merely registering the synchronizer is not good enough as we need the topology state to be built.
      // For that we connect to replacement synchronizer and disconnect once the participants have the new synchronizer topology state.

      // Note also that the synchronizers.connect_local is repeated below so that we don't need to include the above
      // clue/eventually block in the user manual entry:

      // user-manual-entry-begin: InitializeIdentityStateAndDisconnect
      Seq(participant1, participant2).foreach(
        _.synchronizers.connect_local(newSynchronizerSequencer, alias = newSynchronizerAlias)
      )

      // Run a few transactions on the new synchronizer so that the topology state chosen by the repair commands
      // really is the active one that we've seen
      participant1.health.ping(participant2, synchronizerId = Some(newSynchronizerId))

      Seq(participant1, participant2).foreach(_.synchronizers.disconnect(newSynchronizerAlias))
      // user-manual-entry-end: InitializeIdentityStateAndDisconnect

      logger.debug("Participants disconnected from new synchronizer")

      // At this point both synchronizers are still registered.
      Seq(participant1, participant2).foreach(
        _.synchronizers.list_registered().map(_._1.synchronizerAlias).toSet shouldBe Set(
          lostSynchronizerAlias,
          newSynchronizerAlias,
        )
      )

      // And neither synchronizer is connected.
      Seq(participant1, participant2).foreach(_.synchronizers.list_connected() shouldBe Nil)
  }

  "Move contract instances from lost synchronizer to new synchronizer invoking repair.change_synchronizer" in {
    implicit env =>
      import env.*

      val Alice = aliceS.toPartyId(participant1)
      val Bob = bobS.toPartyId(participant2)

      // user-manual-entry-begin: ChangeContractsSynchronizer
      // Extract participant contracts from the lost synchronizer.
      val contracts1 =
        participant1.testing.pcs_search(
          lostSynchronizerAlias,
          filterTemplate = "^Iou",
          activeSet = true,
        )
      val contracts2 =
        participant2.testing.pcs_search(
          lostSynchronizerAlias,
          filterTemplate = "^Iou",
          activeSet = true,
        )

      // Finally change the contracts from the lost synchronizer to the new synchronizer.
      participant1.repair.change_assignation(
        contracts1.map(_._2.contractId),
        lostSynchronizerAlias,
        newSynchronizerAlias,
      )
      participant2.repair.change_assignation(
        contracts2.map(_._2.contractId),
        lostSynchronizerAlias,
        newSynchronizerAlias,
        skipInactive = false,
      )
      // user-manual-entry-end: ChangeContractsSynchronizer

      // Ensure that shared contracts match.
      val Seq(sharedContracts1, sharedContracts2) = Seq(contracts1, contracts2).map(
        _.filter { case (_isActive, contract) =>
          contract.metadata.stakeholders.contains(Alice.toLf) &&
          contract.metadata.stakeholders.contains(Bob.toLf)
        }.toSet
      )

      // This sanity-test happens outside of user manual entry, so scala-test primitive don't show
      // up in the above user manual entry.
      clue("checking if contracts match") {
        sharedContracts1 shouldBe sharedContracts2
      }
  }

  "Able to exercise contracts moved to replacement synchronizer" in { implicit env =>
    import env.*

    val Alice = aliceS.toPartyId(participant1)
    val Bob = bobS.toPartyId(participant2)

    // user-manual-entry-begin: VerifyNewSynchronizerWorks
    Seq(participant1, participant2).foreach(_.synchronizers.reconnect(newSynchronizerAlias))

    // Look up a couple of contracts moved from the lost synchronizer.
    val Seq(iouAlice, iouBob) = Seq(participant1 -> Alice, participant2 -> Bob).map {
      case (participant, party) =>
        participant.ledger_api.javaapi.state.acs
          .await[iou.Iou.Contract, iou.Iou.ContractId, iou.Iou](iou.Iou.COMPANION)(
            party,
            _.data.owner == party.toProtoPrimitive,
          )
    }

    // Ensure that we can create new contracts.
    Seq(participant1 -> ((Alice, Bob)), participant2 -> ((Bob, Alice))).foreach {
      case (participant, (payer, owner)) =>
        participant.ledger_api.javaapi.commands.submit_flat(
          Seq(payer),
          new iou.Iou(
            payer.toProtoPrimitive,
            owner.toProtoPrimitive,
            new iou.Amount(200.toBigDecimal, "USD"),
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
    }

    // Even better: Confirm that we can exercise choices on the moved contracts.
    Seq(participant2 -> ((Bob, iouBob)), participant1 -> ((Alice, iouAlice))).foreach {
      case (participant, (owner, iou)) =>
        participant.ledger_api.javaapi.commands
          .submit_flat(Seq(owner), iou.id.exerciseCall().commands.asScala.toSeq)
    }
    // user-manual-entry-end: VerifyNewSynchronizerWorks

    // should observe two more contracts
    assertAcsCountsWithFilter(
      {
        case WrappedContractEntry(ContractEntry.ActiveContract(value)) =>
          value.synchronizerId == newSynchronizerId.toProtoPrimitive
        case _ => false
      },
      (participant1, Map(Alice -> 9, Bob -> 8)),
      (participant2, Map(Alice -> 8, Bob -> 9)),
    )
    // after the exercise alice and bob each have three GetCash contracts (one from first test step only visible locally)
    assertAcsCountsWithFilter(
      {
        case entry @ WrappedContractEntry(ContractEntry.ActiveContract(active)) =>
          active.synchronizerId == newSynchronizerId.toProtoPrimitive && entry.templateId
            .isModuleEntity("Iou", "GetCash")
        case _ => false
      },
      (participant1, Map(Alice -> 3, Bob -> 2)),
      (participant2, Map(Alice -> 2, Bob -> 3)),
    )

  }

  // various "aliases" to make the user manual entries more intuitive
  private def lostSynchronizerSequencer(implicit
      env: TestConsoleEnvironment
  ): LocalSequencerReference =
    env.sequencer1

  private def lostSynchronizerMediator(implicit
      env: TestConsoleEnvironment
  ): LocalMediatorReference =
    env.mediator1

  private def newSynchronizerSequencer(implicit
      env: TestConsoleEnvironment
  ): LocalSequencerReference =
    env.sequencer2

  private def lostSynchronizerAlias(implicit env: TestConsoleEnvironment) = env.daName

  private def newSynchronizerAlias(implicit env: TestConsoleEnvironment) = env.acmeName

  private def newSynchronizerId(implicit env: TestConsoleEnvironment) = env.acmeId
}

final class SynchronizerRepairReferenceIntegrationTestPostgres
    extends SynchronizerRepairIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}

final class SynchronizerRepairBftOrderingIntegrationTestPostgres
    extends SynchronizerRepairIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}
