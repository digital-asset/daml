// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.multihostedparties.PartyActivationFlow.authorizeOnly
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId

import java.util.Collections

sealed trait OfflinePartyReplicationWorkflowIdsIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase
    with UseSilentSynchronizerInTest {

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // Party replication to the target participant may trigger ACS commitment mismatch warnings.
  // This is expected behavior. To reduce the frequency of these warnings and avoid associated
  // test flakes, `reconciliationInterval` is set to one year.
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer2, alias = daName)
      participant3.synchronizers.connect_local(sequencer2, alias = daName)

      participants.all.dars.upload(CantonTestsPath)

      sequencers.all.foreach { s =>
        adjustTimeouts(s)
        s.topology.synchronizer_parameters
          .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))
      }
    }

  "Migrations are grouped by ledger time and can be correlated through the workflow ID" in {
    implicit env =>
      import env.*

      alice = participant1.parties.enable("Alice", synchronizeParticipants = Seq(participant2))

      // create some IOUs, we'll expect the migration to group together those sharing the
      // ledger time (i.e. they have been created in the same transaction)
      for (commands <- Seq(ious(alice, 3), ious(alice, 4), ious(alice, 1), ious(alice, 2))) {
        participant1.ledger_api.javaapi.commands
          .submit(actAs = Seq(alice), commands = commands)
      }

      val beforeActivationOffset =
        authorizeOnly(alice, daId, source = participant1, target = participant2)

      silenceSynchronizerAndAwaitEffectiveness(daId, Seq(sequencer1, sequencer2), participant1)

      replicate(
        party = alice,
        source = participant1,
        target = participant2,
        beforeActivationOffset,
      )

      resumeSynchronizerAndAwaitEffectiveness(daId, Seq(sequencer1, sequencer2), participant1)

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

  }

  "The workflow ID prefix must be configurable" in { implicit env =>
    import env.*

    val workflowIdPrefix = "SOME_WORKFLOW_ID_123"

    bob = participant1.parties.enable("Bob", synchronizeParticipants = Seq(participant3))

    for (commands <- Seq(ious(bob, 1), ious(bob, 1))) {
      participant1.ledger_api.javaapi.commands.submit(actAs = Seq(bob), commands = commands)
    }

    val beforeActivationOffset =
      authorizeOnly(bob, daId, participant1, participant3)

    silenceSynchronizerAndAwaitEffectiveness(daId, Seq(sequencer1, sequencer2), participant1)

    replicate(
      party = bob,
      source = participant1,
      target = participant3,
      beforeActivationOffset,
      workflowIdPrefix = workflowIdPrefix,
    )

    resumeSynchronizerAndAwaitEffectiveness(daId, Seq(sequencer1, sequencer2), participant1)

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
      beforeActivationOffset: Long,
      workflowIdPrefix: String = "",
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    repair.party_replication.step1_hold_and_store_acs(
      party,
      daId,
      source,
      target.id,
      acsSnapshotPath,
      beforeActivationOffset,
    )
    repair.party_replication.step2_import_acs(
      party,
      daId,
      target,
      acsSnapshotPath,
      workflowIdPrefix,
    )
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
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
