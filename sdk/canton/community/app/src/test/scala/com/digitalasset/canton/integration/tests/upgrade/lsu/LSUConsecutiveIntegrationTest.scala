// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LSUBase.Fixture
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.annotation.nowarn

/*
 * This test is used to test the consecutive LSUs.
 * The consecutive PSIds are:
 *
 * - (lsid, 0, tested protocol version)
 * - (lsid, 1, pv=dev)
 * - (lsid, 2, tested protocol version)
 */
@nowarn("msg=dead code")
final class LSUConsecutiveIntegrationTest extends LSUBase {
  override protected def testName: String = "lsu-consecutive"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val newOldMediators: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")

  private lazy val upgradeTime1: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)
  private lazy val upgradeTime2: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(90)

  override protected lazy val upgradeTime: CantonTimestamp = throw new IllegalAccessException(
    "Use upgradeTime1 and upgradeTime2 instead"
  )

  override protected def configTransforms: List[ConfigTransform] = {
    val lowerBound1 = ConfigTransforms
      .updateSequencerConfig("sequencer2")(
        _.focus(_.parameters.sequencingTimeLowerBoundExclusive).replace(Some(upgradeTime1))
      )

    val lowerBound2 = ConfigTransforms
      .updateSequencerConfig("sequencer3")(
        _.focus(_.parameters.sequencingTimeLowerBoundExclusive).replace(Some(upgradeTime2))
      )

    val allNewNodes = Set("sequencer2", "sequencer3", "mediator2", "mediator3")

    List(
      lowerBound1,
      lowerBound2,
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
    ) ++ ConfigTransforms.enableAlphaVersionSupport
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 3,
        numMediators = 3,
      )
      /*
      The test is made slightly more robust by controlling explicitly which nodes are running.
      This allows to ensure that correct synchronizer nodes are used for each LSU.
       */
      .withManualStart
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        val daSequencerConnection =
          SequencerConnections.single(sequencer1.sequencerConnection.withAlias(daName.toString))

        participants.local.start()

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
        participant1.health.ping(participant2)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )
      }

  "LSU" should {
    "be done after another LSU" in { implicit env =>
      import env.*

      val fixture1 = Fixture(
        currentPSId = daId,
        upgradeTime = upgradeTime1,
        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1)),
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2)),
        newOldNodesResolution = Map("sequencer2" -> "sequencer1", "mediator2" -> "mediator1"),
        oldSynchronizerOwners = Set[InstanceReference](sequencer1, mediator1),
        newPV = ProtocolVersion.dev,
        newSerial = daId.serial.increment.toNonNegative,
      )

      val fixture2 = Fixture(
        currentPSId = daId,
        upgradeTime = upgradeTime2,
        oldSynchronizerNodes = fixture1.newSynchronizerNodes,
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer3), Seq(mediator3)),
        newOldNodesResolution = Map("sequencer3" -> "sequencer2", "mediator3" -> "mediator2"),
        oldSynchronizerOwners = Set[InstanceReference](sequencer2, mediator2),
        newPV = testedProtocolVersion,
        newSerial = fixture1.newSerial.increment.toNonNegative,
      )

      val alice = participant1.parties.enable("Alice")
      val bank = participant2.parties.enable("Bank")

      // Perform the two LSUs
      Seq(fixture1, fixture2).foreach { fixture =>
        logger.info(s"Preparing to do LSU from ${fixture.currentPSId} to ${fixture.newPSId}")

        IouSyntax.createIou(participant2)(bank, alice).discard

        fixture.newSynchronizerNodes.all.start()

        performSynchronizerNodesLSU(fixture)

        environment.simClock.value.advanceTo(fixture.upgradeTime.immediateSuccessor)

        eventually() {
          participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
        }

        fixture.oldSynchronizerNodes.all.stop()

        environment.simClock.value.advance(Duration.ofSeconds(1))

        waitForTargetTimeOnSequencer(
          fixture.newSynchronizerNodes.sequencers.loneElement,
          environment.clock.now,
        )

        IouSyntax.createIou(participant2)(bank, alice).discard
      }

      // 2 LSUs, one create before and after each LSU
      participant1.ledger_api.state.acs.of_party(alice).size shouldBe 4
      participant2.ledger_api.state.acs.of_party(bank).size shouldBe 4
    }
  }
}
