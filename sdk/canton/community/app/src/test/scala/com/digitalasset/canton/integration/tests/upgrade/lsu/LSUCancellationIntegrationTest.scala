// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.DynamicSynchronizerParameters as ConsoleDynamicSynchronizerParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.{DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LSUBase.Fixture
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.{PartyId, TopologyManagerError}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import scala.annotation.nowarn

/** The goal is to ensure that an LSU can be cancelled and that another LSU can be done
  * subsequently.
  *
  * Test setup:
  *
  *   - LSU is announced
  *   - Before the upgrade time, it is cancelled
  *   - Another LSU is announced
  *   - Second LSU is performed
  */
@nowarn("msg=dead code")
abstract class LSUCancellationIntegrationTest extends LSUBase {

  override protected def testName: String = "logical-synchronizer-upgrade"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val newOldMediators: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")

  private lazy val upgradeTime1: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)
  private lazy val upgradeTime2: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(90)

  private var fixture1: Fixture = _
  private var fixture2: Fixture = _

  private var bob: PartyId = _

  private var dynamicSynchronizerParameters: ConsoleDynamicSynchronizerParameters = _

  override protected lazy val upgradeTime: CantonTimestamp = throw new IllegalAccessException(
    "Use upgradeTime1 and upgradeTime2 instead"
  )

  override protected def configTransforms: List[ConfigTransform] = {
    val lowerBound1 = List("sequencer2") // successor of sequencer1 for the first upgrade
      .map(sequencerName =>
        ConfigTransforms
          .updateSequencerConfig(sequencerName)(
            _.focus(_.parameters.sequencingTimeLowerBoundExclusive).replace(Some(upgradeTime1))
          )
      )

    val lowerBound2 = List("sequencer3") // successor of sequencer1 for the second upgrade
      .map(sequencerName =>
        ConfigTransforms
          .updateSequencerConfig(sequencerName)(
            _.focus(_.parameters.sequencingTimeLowerBoundExclusive).replace(Some(upgradeTime2))
          )
      )

    val allNewNodes = Set("sequencer2", "sequencer3", "mediator2", "mediator3")

    lowerBound1 ++ lowerBound2 ++ List(
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
    ) ++ ConfigTransforms.enableAlphaVersionSupport
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 1,
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
        participant1.health.ping(participant1)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )
      }

  /** Check whether an LSU is ongoing
    * @param successor
    *   Defined iff an upgrade is ongoing
    */
  private def checkLSUOngoing(
      successor: Option[SynchronizerSuccessor]
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    val connectedSynchronizer = participant1.underlying.value.sync
      .connectedSynchronizerForAlias(daName)
      .value

    connectedSynchronizer.ephemeral.recordOrderPublisher.getSynchronizerSuccessor shouldBe successor

    connectedSynchronizer.synchronizerCrypto.currentSnapshotApproximation.ipsSnapshot
      .synchronizerUpgradeOngoing()
      .futureValueUS
      .map { case (successor, _) => successor } shouldBe successor
  }

  "Logical synchronizer upgrade should be cancellable and re-announced" should {
    "initial setup" in { implicit env =>
      import env.*

      fixture1 = Fixture(
        currentPSId = daId,
        upgradeTime = upgradeTime1,
        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1)),
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2)),
        newOldNodesResolution = Map("sequencer2" -> "sequencer1", "mediator2" -> "mediator1"),
        oldSynchronizerOwners = synchronizerOwners1,
        newPV = ProtocolVersion.dev,
        newSerial = daId.serial.increment.toNonNegative,
      )

      fixture2 = Fixture(
        currentPSId = daId,
        upgradeTime = upgradeTime2,
        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1)),
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer3), Seq(mediator3)),
        newOldNodesResolution = Map("sequencer3" -> "sequencer1", "mediator3" -> "mediator1"),
        oldSynchronizerOwners = synchronizerOwners1,
        newPV = ProtocolVersion.dev,
        newSerial = fixture1.newSerial.increment.toNonNegative,
      )

      dynamicSynchronizerParameters = participant1.topology.synchronizer_parameters.latest(daId)

      // Some assertions below don't make sense if the value is too low
      dynamicSynchronizerParameters.decisionTimeout.asJava.getSeconds should be > 10L

      daId should not be fixture1.newPSId
      fixture1.newPSId should not be fixture2.newPSId

      val alice = participant1.parties.enable("Alice")
      val bank = participant1.parties.enable("Bank")
      IouSyntax.createIou(participant1)(bank, alice).discard
    }

    "first LSU and cancellation" in { implicit env =>
      import env.*

      val clock = environment.simClock.value

      sequencer2.start()
      mediator2.start()

      performSynchronizerNodesLSU(fixture1)

      eventually()(checkLSUOngoing(Some(fixture1.synchronizerSuccessor)))

      // Fails because the upgrade is ongoing
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.parties.enable("Bob"),
        _.shouldBeCantonErrorCode(TopologyManagerError.OngoingSynchronizerUpgrade),
      )

      clock.advanceTo(upgradeTime1.minusSeconds(5))

      fixture1.oldSynchronizerOwners.foreach(
        _.topology.synchronizer_upgrade.announcement.revoke(fixture1.newPSId, fixture1.upgradeTime)
      )

      eventually()(checkLSUOngoing(None))

      sequencer2.stop()
      mediator2.stop()

      clock.advanceTo(upgradeTime1.immediateSuccessor)

      // Time offset on the old sequencer is not applied
      sequencer1.underlying.value.sequencer.timeTracker
        .fetchTime()
        .futureValueUS should be < upgradeTime1.plus(
        dynamicSynchronizerParameters.decisionTimeout.asJava
      )

      // Call should fail if no upgrade is ongoing
      eventually() {
        participant1.underlying.value.sync
          .upgradeSynchronizerTo(daId, fixture1.synchronizerSuccessor)
          .value
          .futureValueUS
          .left
          .value shouldBe "No synchronizer upgrade ongoing"
      }

      bob = participant1.parties.enable("Bob")
    }

    "second LSU" in { implicit env =>
      import env.*

      val clock = environment.simClock.value

      sequencer3.start()
      mediator3.start()

      performSynchronizerNodesLSU(fixture2)

      clock.advanceTo(upgradeTime2.immediateSuccessor)

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture2.newPSId)) shouldBe true
      }

      // Time offset is applied on the old sequencer
      sequencer1.underlying.value.sequencer.timeTracker
        .fetchTime()
        .futureValueUS should be >= upgradeTime2.plus(
        dynamicSynchronizerParameters.decisionTimeout.asJava
      )

      // Bob is known
      participant1.topology.party_to_participant_mappings
        .list(fixture2.newPSId, filterParty = bob.filterString)
        .loneElement
    }
  }
}

final class LSUCancellationReferenceIntegrationTest extends LSUCancellationIntegrationTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3")),
    )
  )
}

final class LSUCancellationBftOrderingIntegrationTest extends LSUCancellationIntegrationTest {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3")),
    )
  )
}
