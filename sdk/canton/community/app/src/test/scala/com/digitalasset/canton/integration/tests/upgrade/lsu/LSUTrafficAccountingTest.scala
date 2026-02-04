// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
  PositiveLong,
}
import com.digitalasset.canton.console.{
  CommandFailure,
  InstanceReference,
  LocalInstanceReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality.Optional
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencing.service.GrpcSequencerService
import com.digitalasset.canton.topology.{Member, Party, SynchronizerId}
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.time.Duration
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.Random

/*
 * This test:
 * - Generates purchased/consumed traffic by performing some activity on the predecessor
 * - Performs an LSU (S1->S2, S3->S4)
 * - Runs parts of `TrafficControlTest` to ensure traffic control is working as expected after an LSU
 *
 * Test uses 2 participants (connect to both old/new PS), 4 sequencers and 2 mediators,
 * Participants connect via different sequencers to simulate real-world usage.
 * representing old(sequencer1, sequencer3, mediator1) and new(sequencer2, sequencer4, mediator2) synchronizer nodes
 */
abstract class LSUTrafficAccountingTest extends LSUBase with TrafficBalanceSupport {

  override protected def testName: String = "lsu-traffic-accounting"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1", "sequencer4" -> "sequencer3")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private val baseEventCost = 500L
  private val maxBaseTrafficAmount = 20_000L
  private val trafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(maxBaseTrafficAmount),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    // Enough to bootstrap the synchronizer and connect the participant after 1 second
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.tryCreate(baseEventCost),
  )

  private def updateBalanceForMember(
      instance: LocalInstanceReference,
      newBalance: PositiveLong,
      sequencer: LocalSequencerReference,
  )(implicit env: TestConsoleEnvironment): Assertion =
    updateBalanceForMember(
      instance,
      newBalance,
      () => {
        // Advance the clock just slightly so we can observe the new balance be effective
        env.environment.simClock.value.advance(Duration.ofMillis(1))
      },
      sequencer = sequencer,
    )

  private def getExerciseCommand(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
      party: Party,
      pkg: String,
  )(implicit
      env: TestConsoleEnvironment
  ): Command = {
    import env.*

    val createCommand = ledger_api_utils.create(
      pkg,
      "Test",
      "DummyWithParam",
      Map("operator" -> party),
    )

    val created = participant.ledger_api.commands.submit(
      Seq(party),
      Seq(createCommand),
      synchronizerId = Some(synchronizerId),
      readAs = Seq(party),
    )

    val contractId = created.events
      .map(_.getCreated.contractId)
      .headOption
      .value

    // Ensure we get the same input every test to get the same event cost
    val random = new Random(50L)
    val input = random.alphanumeric
      .take(trafficControlParameters.maxBaseTrafficAmount.value.toInt * 3)
      .mkString

    ledger_api_utils.exercise(
      pkg,
      "Test",
      "DummyWithParam",
      "ChoiceWithParam",
      // This is too big for the max burst base rate
      Map("paramString" -> input),
      contractId,
    )
  }

  private val topUpAmount = 250_000L

  private def custom_S2M1(implicit env: TestConsoleEnvironment): NetworkTopologyDescription = {
    import env.*

    NetworkTopologyDescription(
      daName,
      synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
      synchronizerThreshold = PositiveInt.one,
      sequencers = Seq(sequencer1, sequencer3),
      mediators = Seq(mediator1),
    )
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(custom_S2M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()

        import env.*

        participant1.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1), threshold = 1)
        )
        participant2.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer2), threshold = 1)
        )

        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1, sequencer3), Seq(mediator1))
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2, sequencer4), Seq(mediator2))

        // Enable traffic control
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            trafficControl = Some(trafficControlParameters),
            // "Deactivate" ACS commitments to not consume traffic in the background
            reconciliationInterval = config.PositiveDurationSeconds.ofDays(365),
          ),
        )

        participant1.ledger_api.packages.upload_dar(CantonTestsPath, synchronizerId = daId)

        // Fill up base rate
        environment.simClock.value.advance(Duration.ofSeconds(1))
      }

  "Traffic transfer during an LSU" should {

    "create and assert non-empty traffic state on the sequencer" in { implicit env =>
      import env.*

      val participantTopUpAmount = PositiveLong.tryCreate(500_000L)
      val mediatorTopUpAmount = PositiveLong.tryCreate(250_000L)

      updateBalanceForMember(participant1, participantTopUpAmount, sequencer1)
      updateBalanceForMember(mediator1, mediatorTopUpAmount, sequencer1)

      clue("check participant1' traffic state on participant1") {
        eventually() {
          participant1.traffic_control
            .traffic_state(daId)
            .extraTrafficPurchased
            .value shouldBe participantTopUpAmount.value
        }
      }

      // the top-up only becomes active on the next sequencing timestamp.
      // since we use simclock and have no other traffic going on, we need to ping here
      (1 to 20).foreach(_ => participant1.health.ping(participant1.id))

      // Sequencer should show the top-up
      clue(s"check participant1' traffic state on sequencer1") {
        eventually() {
          val sequencerTrafficStatus =
            sequencer1.traffic_control.traffic_state_of_members_approximate(
              Seq(participant1.id, mediator1.id)
            )
          def memberTraffic(member: Member): Option[TrafficState] =
            sequencerTrafficStatus.trafficStates.collectFirst {
              case (m, traffic) if m == member => traffic
            }
          memberTraffic(participant1.id).map(_.extraTrafficPurchased.value) shouldBe Some(
            participantTopUpAmount.value
          )
          memberTraffic(mediator1.id).map(_.extraTrafficPurchased.value) shouldBe Some(
            mediatorTopUpAmount.value
          )
        }
      }

      val trafficState = participant1.traffic_control
        .traffic_state(daId)
      trafficState.extraTrafficPurchased.value shouldBe 500000L
      trafficState.extraTrafficRemainder should be < 500000L
      trafficState.baseTrafficRemainder.value should be < (maxBaseTrafficAmount)
      trafficState.serial shouldBe Some(PositiveInt.tryCreate(1))
    }

    "perform an LSU" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)

      val alice = participant1.parties.enable("Alice")
      val bank = participant2.parties.enable("Bank")
      IouSyntax.createIou(participant2)(bank, alice).discard

      // Before upgrade time has been reached on the old sequencer we expect an error
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        sequencer1.traffic_control.get_lsu_state(),
        _.shouldBeCantonErrorCode(SequencerError.NoOngoingLSU),
      )

      performSynchronizerNodesLSU(fixture)

      // Generate some non-trivial base traffic remainder state
      environment.simClock.value.advanceTo(upgradeTime.minusMillis(20L))
      participant1.health.ping(participant2)

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      val oldTrafficStateSeq1 = eventually(retryOnTestFailuresOnly = false) {
        loggerFactory.assertLogsUnorderedOptional(
          sequencer1.traffic_control.get_lsu_state(),
          (Optional, _.shouldBeCantonErrorCode(SequencerError.NotAtUpgradeTimeOrBeyond)),
        )
      }

      // Sanity check: setting LSU traffic on a sequencer1 without a lower bound set produces an error
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        sequencer1.traffic_control.set_lsu_state(oldTrafficStateSeq1),
        _.shouldBeCantonErrorCode(SequencerError.MissingSynchronizerPredecessor),
      )

      val oldTrafficStateSeq3 = eventually(retryOnTestFailuresOnly = false) {
        loggerFactory.assertLogsUnorderedOptional(
          sequencer3.traffic_control.get_lsu_state(),
          (Optional, _.shouldBeCantonErrorCode(SequencerError.NotAtUpgradeTimeOrBeyond)),
        )
      }

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }

      environment.simClock.value.advance(Duration.ofSeconds(1))

      // we start the ping before setting the traffic state to check that sequencer doesn't drop requests
      // while traffic is still being initialized
      val pingSucceeded = Promise[Unit]()
      loggerFactory.assertEventuallyLogsSeq(
        SuppressionRule.forLogger[GrpcSequencerService] && SuppressionRule.Level(Level.INFO)
      )(
        Future(participant1.health.ping(participant2))
          .map(_ => pingSucceeded.success(()))
          .discard // Note: we cannot return the Future via the suppression, it will put it on directExecutionContext
        ,
        logs =>
          if (logs.exists(_.message.contains("sends request with id"))) {
            logger.debug("Ping request is at the sequencer")
            succeed
          } else {
            logger.debug("Ping request has not reached the sequencer yet...")
            fail("Ping request not sent yet")
          },
      )

      // Ping should not have completed
      pingSucceeded.isCompleted shouldBe false

      logger.debug("Setting traffic state on new sequencers")
      sequencer2.traffic_control.set_lsu_state(oldTrafficStateSeq1)
      sequencer4.traffic_control.set_lsu_state(oldTrafficStateSeq3)

      // Command is expected to return an error if called again
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        sequencer2.traffic_control.set_lsu_state(oldTrafficStateSeq1),
        _.shouldBeCantonErrorCode(SequencerError.LSUTrafficAlreadyInitialized),
      )

      // ping should succeed now
      logger.debug("Waiting for ping to succeed")
      clue("request blocked during LSU traffic state initialization should succeed") {
        pingSucceeded.future.futureValue
      }

      logger.debug("Comparing traffic states on old and new sequencers")
      val members = Seq(
        participant1.id.member,
        participant2.id.member,
        mediator1.id.member,
      )

      def getTraffic(sequencer: LocalSequencerReference) = members
        .map(m =>
          m -> sequencer.underlying.value.sequencer.sequencer
            .getTrafficStateAt(m, upgradeTime)
            .futureValueUS
            .value
        )
        .toMap

      val oldSNTrafficStateSeq1 = getTraffic(sequencer1)
      val oldSNTrafficStateSeq3 = getTraffic(sequencer3)

      // check that we have non-trivial base traffic remainder case in the test
      oldSNTrafficStateSeq1.exists { case (_, trafficState) =>
        trafficState.exists(_.baseTrafficRemainder.value < maxBaseTrafficAmount)
      } shouldBe true

      val newSNTrafficStateSeq2 = getTraffic(sequencer2)
      val newSNTrafficStateSeq4 = getTraffic(sequencer4)

      // check that traffic didn't fork during the LSU before and after the upgrade
      newSNTrafficStateSeq2 shouldEqual oldSNTrafficStateSeq1
      newSNTrafficStateSeq4 shouldEqual oldSNTrafficStateSeq3
      // sanity checks
      oldSNTrafficStateSeq1 shouldEqual oldSNTrafficStateSeq3
      newSNTrafficStateSeq2 shouldEqual newSNTrafficStateSeq4

      oldSynchronizerNodes.all.stop()

      environment.simClock.value.advance(Duration.ofSeconds(1))

      waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)
      waitForTargetTimeOnSequencer(sequencer4, environment.clock.now)

      val aliceIou =
        participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(alice)
      val bob = participant1.parties.enable("Bob")

      participant1.ledger_api.javaapi.commands
        .submit(Seq(alice), aliceIou.id.exerciseTransfer(bob.toLf).commands().asScala.toSeq)

      val bobIou = participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(bob)

      participant2.ledger_api.javaapi.commands
        .submit(Seq(bank), bobIou.id.exerciseArchive().commands().asScala.toSeq)

      // Subsequent call should be successful
      participant1.underlying.value.sync
        .upgradeSynchronizerTo(daId, fixture.synchronizerSuccessor)
        .futureValueUS
        .value shouldBe ()
    }

    "fail to exercise a large command with not enough traffic (after LSU)" in { implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      val trafficBefore = participant2.traffic_control.traffic_state(daId)
      logger.debug(s"Traffic before adv time: $trafficBefore")

      // Re-fill the base rate to give some credit to the mediator, still won't be enough for the submission request though
      clock.advance(trafficControlParameters.maxBaseTrafficAccumulationDuration.asJava)
      participant2.health.ping(participant2)
      val trafficAfter = participant2.traffic_control.traffic_state(daId)
      logger.debug(s"Traffic after adv time: $trafficAfter")

      participant2.ledger_api.packages.upload_dar(CantonTestsPath, synchronizerId = daId)

      logger.debug("Uploaded DAR to participant2")
      clock.advance(trafficControlParameters.maxBaseTrafficAccumulationDuration.asJava)

      val claire = participant2.parties.enable("Claire")

      val pkg = participant2.packages.find_by_module("Test").headOption.map(_.packageId).value

      val exerciseCommand =
        getExerciseCommand(participant2, sequencer2.synchronizer_id, claire, pkg)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant2.ledger_api.commands.submit(
          Seq(claire),
          Seq(exerciseCommand),
          workflowId = sequencer2.name,
          readAs = Seq(claire),
        ),
        _.warningMessage should include("AboveTrafficLimit"),
        _.warningMessage should include("AboveTrafficLimit"),
        _.errorMessage should include("Request failed for participant2"),
      )

      val trafficState1 = participant2.traffic_control.traffic_state(daId)
      trafficState1.extraTrafficPurchased shouldBe NonNegativeLong.zero
      trafficState1.extraTrafficRemainder shouldBe 0L
    }

    "allow participants to top up (after LSU)" in { implicit env =>
      import env.*
      updateBalanceForMember(participant1, PositiveLong.tryCreate(topUpAmount), sequencer2)
      updateBalanceForMember(participant2, PositiveLong.tryCreate(topUpAmount), sequencer2)

      eventually() {
        val trafficState1 = participant1.traffic_control.traffic_state(daId)
        val trafficState2 = participant2.traffic_control.traffic_state(daId)

        trafficState1.extraTrafficPurchased.value shouldBe (topUpAmount)
        trafficState1.serial shouldBe Some(PositiveInt.tryCreate(2))
        trafficState2.extraTrafficPurchased.value shouldBe topUpAmount
        trafficState2.serial shouldBe Some(PositiveInt.tryCreate(1))
      }
    }

    "charge for time proofs (after LSU)" in { implicit env =>
      import env.*
      val clock = env.environment.simClock.value
      clock.advance(Duration.ofSeconds(1))
      val trafficBeforeCommand = getTrafficForMember(participant1, sequencer2).value
      participant1.runningNode.value.getNode.value.sync
        .lookupSynchronizerTimeTracker(sequencer2.physical_synchronizer_id)
        .value
        .fetchTimeProof()
        .futureValueUS
        .discard
      val trafficAfterCommand = getTrafficForMember(participant1, sequencer2).value
      (trafficBeforeCommand.baseTrafficRemainder.value - trafficAfterCommand.baseTrafficRemainder.value) shouldBe baseEventCost
    }

    "succeed to run a big transaction with enough credit (after LSU)" in { implicit env =>
      import env.*

      val clock = env.environment.simClock.value

      val claire = participant2.parties.testing.find("Claire")
      val pkg = participant2.packages.find_by_module("Test").headOption.map(_.packageId).value

      val exerciseCommand =
        getExerciseCommand(participant2, sequencer2.synchronizer_id, claire, pkg)
      val trafficBeforeCommand = participant2.traffic_control.traffic_state(daId)

      // Fill in base rate
      clock.advance(Duration.ofSeconds(1))

      // Now it should succeed
      participant2.ledger_api.commands.submit(
        Seq(claire),
        Seq(exerciseCommand),
        synchronizerId = Some(sequencer2.synchronizer_id),
        readAs = Seq(claire),
      )

      val trafficAfterCommand = participant2.traffic_control.traffic_state(daId)
      // Make sure we used extra traffic
      trafficAfterCommand.extraTrafficRemainder should be < trafficBeforeCommand.extraTrafficRemainder
      // Total limit stays the same
      trafficAfterCommand.extraTrafficPurchased.value shouldBe topUpAmount

      // Make sure base rate is full
      clock.advance(Duration.ofSeconds(1))

      // Run it again to compare the 2 traffic consumptions
      val exerciseCommandRerun =
        getExerciseCommand(participant2, sequencer2.synchronizer_id, claire, pkg)

      val trafficBeforeRerun = participant2.traffic_control.traffic_state(daId)

      // Fill in base rate
      clock.advance(Duration.ofSeconds(1))

      participant2.ledger_api.commands.submit(
        Seq(claire),
        Seq(exerciseCommandRerun),
        synchronizerId = Some(sequencer2.synchronizer_id),
        readAs = Seq(claire),
      )

      val trafficAfterRerun = participant2.traffic_control.traffic_state(daId)

      // Make sure we used extra traffic
      trafficAfterRerun.extraTrafficRemainder should be < trafficBeforeRerun.extraTrafficRemainder
      // Total limit stays the same
      trafficAfterRerun.extraTrafficPurchased.value shouldBe topUpAmount

      // Because there's _some_ differences between the 2 runs (contract ID is different, timestamps are different),
      // and views are gzipped, the cost can be slightly different, so allow for some thin margin of error
      val consumptionRound1 =
        trafficBeforeCommand.extraTrafficRemainder - trafficAfterCommand.extraTrafficRemainder
      val consumptionRound2 =
        trafficBeforeRerun.extraTrafficRemainder - trafficAfterRerun.extraTrafficRemainder

      (consumptionRound1 - consumptionRound2) should equal(0L +- 100L)
    }

    "update traffic state of sequencer and participant on top ups (after LSU)" in { implicit env =>
      import env.*

      val topUpAmount = PositiveLong.tryCreate(500000L)

      updateBalanceForMember(participant1, topUpAmount, sequencer2)

      clue("check participant1' traffic state on participant1") {
        eventually() {
          participant1.traffic_control
            .traffic_state(daId)
            .extraTrafficPurchased
            .value shouldBe 500000L
        }
      }

      // Sequencer should show the top-up
      clue(s"check participant1' traffic state on sequencer2") {
        eventually() {
          val seqStatus = sequencer2.traffic_control.traffic_state_of_members_approximate(
            Seq(participant1.id)
          )
          val p1s = seqStatus.trafficStates.find(_._1 == participant1.id)
          p1s.isDefined shouldBe true
          p1s.value._2.extraTrafficPurchased.value shouldBe topUpAmount.value
        }
      }

      // the top-up only becomes active on the next sequencing timestamp.
      // since we use simclock and have no other traffic going on, we need to ping here
      participant1.health.ping(participant1.id)

      participant1.traffic_control
        .traffic_state(daId)
        .extraTrafficPurchased
        .value shouldBe 500000L
    }
  }
}

final class LSUReferenceTrafficAccountingTest extends LSUTrafficAccountingTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer3"), Set("sequencer2", "sequencer4")),
    )
  )
}

// TODO(#16789) Re-enable test once dynamic onboarding (traffic control) is supported for DA BFT
//final class LSUBftOrderingTrafficAccountingTest
//  extends LSUTrafficAccountingTest {
//    registerPlugin(
//      new UseBftSequencer(
//        loggerFactory,
//        MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
//      )
//    )
//}
