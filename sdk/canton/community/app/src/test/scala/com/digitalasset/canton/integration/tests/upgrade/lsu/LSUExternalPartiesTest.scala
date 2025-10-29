// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.ledger.javaapi.data.DisclosedContract
import com.digitalasset.canton.config
import com.digitalasset.canton.config.{DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.SequencerConnections

import java.util.Optional
import scala.jdk.CollectionConverters.*

abstract class LSUExternalPartiesIntegrationTest extends LSUBase {

  override protected def testName: String = "lsu-external-parties"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
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

        participants.all.dars.upload(CantonExamplesPath)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )

        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1))
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2))
      }

  "Logical synchronizer upgrade" should {
    "work with external parties" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      val alice = participant1.parties.external.enable("AliceE")
      val bob = participant2.parties.enable("Bob")
      val charlie = participant2.parties.external.enable("CharlieE")

      // Submission is done on P2 not hosting alice
      val txIouAlice = participant2.ledger_api.javaapi.commands
        .submit(
          Seq(alice),
          IouSyntax.testIou(alice.partyId, bob).create().commands().asScala.toSeq,
          includeCreatedEventBlob = true,
        )

      // Submission is done on P1 not hosting charlie
      participant1.ledger_api.javaapi.commands
        .submit(
          Seq(charlie),
          IouSyntax.testIou(charlie.partyId, charlie.partyId).create().commands().asScala.toSeq,
        )

      performSynchronizerNodesLSU(fixture)
      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }
      oldSynchronizerNodes.all.stop()

      val iou = JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(txIouAlice).loneElement
      val iouCreated = txIouAlice.getEvents.asScalaProtoCreatedContracts.loneElement

      val disclosedIou = new DisclosedContract(
        iouCreated.createdEventBlob,
        daId.logical.toProtoPrimitive,
        Optional.of(Iou.TEMPLATE_ID_WITH_PACKAGE_ID),
        Optional.of(iou.id.contractId),
      )

      participant3.ledger_api.state.acs.of_all() shouldBe empty
      // Submission is done on P3, not hosting anybody
      participant3.ledger_api.javaapi.commands
        .submit(
          actAs = Seq(alice),
          commands = iou.id.exerciseArchive().commands().asScala.toSeq,
          // As P3 does not know about the iou so it is disclosed to prepare the archive
          disclosedContracts = Seq(disclosedIou),
        )

      participant1.ledger_api.state.acs.of_party(alice.partyId) should have size 0
    }
  }
}

final class LSUExternalPartiesReferenceIntegrationTest extends LSUExternalPartiesIntegrationTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}
