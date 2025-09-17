// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.config.{DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1_S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LSUBase.Fixture
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.sequencing.SequencerConnections

/** The goal of this test is to ensure that LSU can happen between unassign and assign:
  *   - Two IOUs are created: one on da and the other on acme
  *   - Unassignments are submitted (to the other synchronizer)
  *   - Synchronizer da is upgraded
  *   - Assignments are done
  *
  * Nodes:
  *   - Old da: sequencer1, mediator1
  *   - acme: sequencer2, mediator2
  *   - New da: sequencer3, mediator3
  */
abstract class LSUReassignmentsIntegrationTest extends LSUBase {

  override protected def testName: String = "lsu-reassignments"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator3" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S3M3_Config
      .withNetworkBootstrap { implicit env =>
        NetworkBootstrapper(S1M1_S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        val daSequencerConnection =
          SequencerConnections.single(sequencer1.sequencerConnection.withAlias(daName.toString))
        participants.all.synchronizers.connect(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = daSequencerConnection,
            timeTracker = SynchronizerTimeTrackerConfig(observationLatency =
              config.NonNegativeFiniteDuration.Zero
            ),
          )
        )
        participants.all.synchronizers.connect_local(sequencer2, acmeName)

        participants.all.dars.upload(CantonExamplesPath)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )

        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1))
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer3), Seq(mediator3))
      }

  "Logical synchronizer upgrade" should {
    "work with reassignments" in { implicit env =>
      import env.*

      val fixture = Fixture(daId, upgradeTime)

      val alice = participant1.parties.enable("Alice", synchronizer = Some(daName))
      participant1.parties.enable("Alice", synchronizer = Some(acmeName))

      val bank = participant1.parties.enable("Bank", synchronizer = Some(daName))
      participant1.parties.enable("Bank", synchronizer = Some(acmeName))

      val iou1 = IouSyntax.createIou(participant1, Some(acmeId))(bank, alice, amount = 1.0)
      val iou1Cid = LfContractId.assertFromString(iou1.id.contractId)
      val iou2 = IouSyntax.createIou(participant1, Some(daId))(bank, alice, amount = 2.0)
      val iou2Cid = LfContractId.assertFromString(iou2.id.contractId)

      val assignationsInitial = participant1.ledger_api.state.acs
        .of_party(alice)
        .map(c => c.contractId -> c.synchronizerId.value)
        .toMap

      assignationsInitial.get(iou1.id.contractId).value shouldBe acmeId.logical
      assignationsInitial.get(iou2.id.contractId).value shouldBe daId.logical

      performSynchronizerNodesLSU(fixture)

      val reassignment1Id = participant1.ledger_api.commands
        .submit_unassign(alice, Seq(iou1Cid), acmeId, daId)
        .reassignmentId

      val reassignment2Id = participant1.ledger_api.commands
        .submit_unassign(alice, Seq(iou2Cid), daId, acmeId)
        .reassignmentId

      val reassignmentStore = participant1.underlying.value.sync.syncPersistentStateManager
        .get(daId)
        .value
        .reassignmentStore

      val iou1ReassignmentTargetPSId = {
        // Only reassignment of iou1 is in this store
        val (_, reassignmentId, _) = reassignmentStore
          .findEarliestIncomplete()
          .futureValueUS
          .value

        reassignmentStore
          .findReassignmentEntry(reassignmentId)
          .futureValueUS
          .value
          .unassignmentData
          .value
          .targetPSId
          .unwrap
      }

      // Initial target should be the old synchronizer
      iou1ReassignmentTargetPSId shouldBe fixture.currentPSId

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }

      participant1.ledger_api.commands
        .submit_assign(alice, reassignment1Id, acmeId, daId)
        .reassignmentId

      participant1.ledger_api.commands
        .submit_assign(alice, reassignment2Id, daId, acmeId)
        .reassignmentId

      val assignationsFinal = participant1.ledger_api.state.acs
        .of_party(alice)
        .map(c => c.contractId -> c.synchronizerId.value)
        .toMap

      assignationsFinal.get(iou1.id.contractId).value shouldBe daId.logical
      assignationsFinal.get(iou2.id.contractId).value shouldBe acmeId.logical

      oldSynchronizerNodes.all.stop()
    }
  }
}

final class LSUReassignmentsReferenceIntegrationTest extends LSUReassignmentsIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3")),
    )
  )
}
