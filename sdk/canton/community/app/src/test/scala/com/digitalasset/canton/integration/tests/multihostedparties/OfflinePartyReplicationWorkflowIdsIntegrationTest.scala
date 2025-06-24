// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP

import java.time.Instant
import java.util.Collections

sealed trait OfflinePartyReplicationWorkflowIdsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val acsFilename = s"${getClass.getSimpleName}.gz"

  private val mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofSeconds(1)
  private val confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(1)
  private val waitTimeMs =
    (mediatorReactionTimeout + confirmationResponseTimeout + config.NonNegativeFiniteDuration
      .ofMillis(500)).duration.toMillis
  private val reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer2, alias = daName)
      participant3.synchronizers.connect_local(sequencer2, alias = daName)

      participants.all.dars.upload(CantonTestsPath)

      sequencers.all.foreach(
        _.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            mediatorReactionTimeout = mediatorReactionTimeout,
            confirmationResponseTimeout = confirmationResponseTimeout,
            reconciliationInterval = reconciliationInterval,
          ),
        )
      )
    }

  "Migrations are grouped by ledger time and can be correlated through the workflow ID" in {
    implicit env =>
      import env.*

      val alice = participant1.parties.enable(
        "Alice",
        synchronizeParticipants = Seq(participant2),
      )

      // create some IOUs, we'll expect the migration to group together those sharing the
      // ledger time (i.e. they have been created in the same transaction)
      for (commands <- Seq(ious(alice, 3), ious(alice, 4), ious(alice, 1), ious(alice, 2))) {
        participant1.ledger_api.javaapi.commands
          .submit(actAs = Seq(alice), commands = commands)
      }

      PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
        participant1,
        alice,
        PositiveInt.one,
        Set(
          (participant1, PP.Submission),
          (participant2, PP.Submission),
        ),
      )

      val onboardingTx = participant1.topology.party_to_participant_mappings
        .list(
          synchronizerId = daId,
          filterParty = alice.filterString,
          filterParticipant = participant2.filterString,
        )
        .loneElement
        .context

      sequencer1.topology.synchronizer_parameters.propose_update(
        sequencer1.synchronizer_id,
        _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(0)),
      )
      sequencer2.topology.synchronizer_parameters.propose_update(
        sequencer2.synchronizer_id,
        _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(0)),
      )

      eventually() {
        val ints = participant1.topology.synchronizer_parameters
          .list(store = daId)
          .map { change =>
            change.item.participantSynchronizerLimits.confirmationRequestsMaxRate
          }
        ints.head.unwrap shouldBe 0
      }

      Threading.sleep(waitTimeMs)

      replicate(
        party = alice,
        source = participant1,
        target = participant2,
        onboardingTx.validFrom,
      )

      // Check that the transactions generated for the migration are actually grouped as
      // expected and that their workflow IDs can be used to correlate those transactions
      val txs = participant2.ledger_api.javaapi.updates.transactions(Set(alice), completeAfter = 4)
      withClue("Transactions should be grouped by ledger time") {
        txs.map(_.getTransaction.get().getEffectiveAt.toEpochMilli).distinct should have size 4
      }
      withClue("Transaction should share the same workflow ID prefix") {
        txs.map(_.getTransaction.get().getWorkflowId.dropRight(4)).distinct should have size 1
      }
      inside(txs) { case Seq(tx1, tx2, tx3, tx4) =>
        import scala.jdk.CollectionConverters.ListHasAsScala

        tx1.getTransaction.get().getEvents.asScala should have size 3
        tx1.getTransaction.get().getWorkflowId should endWith("-1-4")

        tx2.getTransaction.get().getEvents.asScala should have size 4
        tx2.getTransaction.get().getWorkflowId should endWith("-2-4")

        tx3.getTransaction.get().getEvents.asScala should have size 1
        tx3.getTransaction.get().getWorkflowId should endWith("-3-4")

        tx4.getTransaction.get().getEvents.asScala should have size 2
        tx4.getTransaction.get().getWorkflowId should endWith("-4-4")

      }

      sequencer1.topology.synchronizer_parameters.propose_update(
        sequencer1.synchronizer_id,
        _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(10000)),
      )
      sequencer2.topology.synchronizer_parameters.propose_update(
        sequencer2.synchronizer_id,
        _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(10000)),
      )

  }

  "The workflow ID prefix must be configurable" in { implicit env =>
    import env.*

    val workflowIdPrefix = "SOME_WORKFLOW_ID_123"

    val bob = participant1.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant3),
    )

    for (commands <- Seq(ious(bob, 1), ious(bob, 1))) {
      participant1.ledger_api.javaapi.commands
        .submit(actAs = Seq(bob), commands = commands)
    }

    PartyToParticipantDeclarative.forParty(Set(participant1, participant3), daId)(
      participant1,
      bob,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant3, PP.Submission),
      ),
    )

    val onboardingTx = participant1.topology.party_to_participant_mappings
      .list(
        synchronizerId = daId,
        filterParty = bob.filterString,
        filterParticipant = participant3.filterString,
      )
      .loneElement
      .context

    sequencer1.topology.synchronizer_parameters.propose_update(
      sequencer1.synchronizer_id,
      _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(0)),
    )
    sequencer2.topology.synchronizer_parameters.propose_update(
      sequencer2.synchronizer_id,
      _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(0)),
    )

    eventually() {
      val ints = participant1.topology.synchronizer_parameters
        .list(store = daId)
        .map { change =>
          change.item.participantSynchronizerLimits.confirmationRequestsMaxRate
        }
      ints.head.unwrap shouldBe 0
    }

    Threading.sleep(2500)

    replicate(
      party = bob,
      source = participant1,
      target = participant3,
      onboardingTx.validFrom,
      workflowIdPrefix = workflowIdPrefix,
    )

    // Check that the workflow ID prefix is set as specified
    val txs = participant3.ledger_api.javaapi.updates.transactions(Set(bob), completeAfter = 2)
    inside(txs) { case Seq(tx1, tx2) =>
      tx1.getTransaction.get().getWorkflowId shouldBe s"$workflowIdPrefix-1-2"
      tx2.getTransaction.get().getWorkflowId shouldBe s"$workflowIdPrefix-2-2"
    }

  }

  private def replicate(
      party: PartyId,
      source: ParticipantReference,
      target: ParticipantReference,
      topologyTransactionEffectiveTime: Instant,
      workflowIdPrefix: String = "",
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    try {
      repair.party_replication.step1_hold_and_store_acs(
        party,
        daId,
        source,
        target.id,
        acsFilename,
        topologyTransactionEffectiveTime,
      )
      repair.party_replication.step2_import_acs(party, daId, target, acsFilename, workflowIdPrefix)
    } finally {
      val acsExport = File(acsFilename)
      if (acsExport.exists) {
        acsExport.delete()
      }
    }
  }

  private def ious(party: PartyId, n: Int): Seq[Command] = {
    import scala.jdk.CollectionConverters.IteratorHasAsScala
    def iou =
      new Iou(
        party.toProtoPrimitive,
        party.toProtoPrimitive,
        new Amount(java.math.BigDecimal.ONE, "USD"),
        Collections.emptyList,
      )
    Seq.fill(n)(iou).flatMap(_.create.commands.iterator.asScala)
  }

}

final class OfflinePartyReplicationWorkflowIdsIntegrationTestPostgres
    extends OfflinePartyReplicationWorkflowIdsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
