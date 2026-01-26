// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.ledger.api.v2.reassignment.ReassignmentEvent
import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.BaseTest.UnsupportedExternalPartyTest.MultiRootNodeSubmission
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.multihostedparties.PartyActivationFlow.authorizeOnly
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.Party
import monocle.syntax.all.*

import java.time.Instant
import java.util.Collections

abstract class OfflinePartyReplicationWorkflowIdsIntegrationTestBase(
    alphaMultiSynchronizerSupport: Boolean = false
) extends OfflinePartyReplicationIntegrationTestBase
    with UseSilentSynchronizerInTest {

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // Party replication to the target participant may trigger ACS commitment mismatch warnings.
  // This is expected behavior. To reduce the frequency of these warnings and avoid associated
  // test flakes, `reconciliationInterval` is set to one year.
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.alphaMultiSynchronizerSupport)
            .replace(alphaMultiSynchronizerSupport)
        )
      )
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer2, alias = daName)
        participant3.synchronizers.connect_local(sequencer2, alias = daName)

        participants.all.dars.upload(CantonTestsPath)

        sequencers.all.foreach { s =>
          adjustTimeouts(s)
          s.topology.synchronizer_parameters
            .propose_update(
              daId,
              _.update(reconciliationInterval = reconciliationInterval.toConfig),
            )
        }
      }

  // Normalized data structure to handle both Transactions (Standard) and Reassignments (Alpha Multi-Synchronizer Support)
  private case class NormalizedEvent(timestamp: Instant, workflowId: String, eventCount: Int)

  "Migrations are grouped by ledger time and can be correlated through the workflow ID" onlyRunWithLocalParty (MultiRootNodeSubmission) in {
    implicit env =>
      import env.*

      alice =
        participant1.parties.testing.enable("Alice", synchronizeParticipants = Seq(participant2))

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
      val events = fetchEvents(participant2, alice, expectedCount = 4)

      withClue("Events should be grouped by ledger time") {
        events.map(_.timestamp).distinct should have size 4
      }
      withClue("Events should share the same workflow ID prefix") {
        events.map(_.workflowId.dropRight(4)).distinct should have size 1
      }

      inside(events) { case Seq(e1, e2, e3, e4) =>
        e1.eventCount shouldBe 3
        e1.workflowId should endWith("-1")

        e2.eventCount shouldBe 4
        e2.workflowId should endWith("-2")

        e3.eventCount shouldBe 1
        e3.workflowId should endWith("-3")

        e4.eventCount shouldBe 2
        e4.workflowId should endWith("-4")
      }

  }

  "The workflow ID prefix must be configurable" in { implicit env =>
    import env.*

    val workflowIdPrefix = "SOME_WORKFLOW_ID_123"

    bob = participant1.parties.enable("Bob", synchronizeParticipants = Seq(participant3))

    for (commands <- Seq(ious(bob, 1), ious(bob, 1))) {
      participant1.ledger_api.javaapi.commands.submit(actAs = Seq(bob), commands = commands)
    }

    val beforeActivationOffset = authorizeOnly(bob, daId, participant1, participant3)

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
    val events = fetchEvents(participant3, bob, expectedCount = 2)

    inside(events) { case Seq(e1, e2) =>
      e1.workflowId shouldBe s"$workflowIdPrefix-1"
      e2.workflowId shouldBe s"$workflowIdPrefix-2"
    }
  }

  // Fetches either Transactions or Reassignments based on configuration and normalizes them
  private def fetchEvents(
      target: ParticipantReference,
      party: Party,
      expectedCount: Int,
  ): Seq[NormalizedEvent] =
    if (alphaMultiSynchronizerSupport) {
      val reassignments = target.ledger_api.updates
        .reassignments(Set(party), completeAfter = PositiveInt.tryCreate(expectedCount))
      reassignments.map { r =>
        // Extract timestamp from the first 'Assigned' event in the reassignment
        val timestamp = r.reassignment.events
          .collectFirst { case ReassignmentEvent(assigned: ReassignmentEvent.Event.Assigned) =>
            assigned.assigned.value.createdEvent.value.createdAt.value
          }
          .getOrElse(
            fail(s"Reassignment ${r.reassignment.updateId} had no Assigned event with timestamp")
          )

        NormalizedEvent(
          timestamp.asJavaInstant,
          r.reassignment.workflowId,
          r.reassignment.events.size,
        )
      }
    } else {
      val txs = target.ledger_api.javaapi.updates
        .transactions(Set(party), completeAfter = PositiveInt.tryCreate(expectedCount))
      import scala.jdk.CollectionConverters.ListHasAsScala
      txs.map { tx =>
        val t = tx.getTransaction.get
        NormalizedEvent(t.getEffectiveAt, t.getWorkflowId, t.getEvents.asScala.size)
      }
    }

  private def replicate(
      party: Party,
      source: ParticipantReference,
      target: ParticipantReference,
      beforeActivationOffset: Long,
      workflowIdPrefix: String = "",
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    source.parties.export_party_acs(
      party,
      daId,
      target,
      beforeActivationOffset,
      exportFilePath = acsSnapshotPath,
    )

    target.synchronizers.disconnect_all()
    target.repair.import_acsV2(acsSnapshotPath, daId, workflowIdPrefix)
    target.synchronizers.reconnect_all()
  }

  private def ious(party: Party, n: Int): Seq[Command] = {
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

final class OfflinePartyReplicationWorkflowIdsIntegrationTest
    extends OfflinePartyReplicationWorkflowIdsIntegrationTestBase {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

final class OfflinePartyReplicationWorkflowIdsReassignmentIntegrationTest
    extends OfflinePartyReplicationWorkflowIdsIntegrationTestBase(
      alphaMultiSynchronizerSupport = true
    ) {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
