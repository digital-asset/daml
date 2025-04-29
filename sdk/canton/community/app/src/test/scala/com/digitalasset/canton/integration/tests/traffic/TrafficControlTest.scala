// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import cats.syntax.functor.*
import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.admin.api.client.data.ParticipantStatus.SubmissionReady
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
  PositiveLong,
}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalInstanceReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseH2,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.OutdatedTrafficCost
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.{
  TrafficControlSerialTooLow,
  TrafficPurchasedRequestAsyncSendFailed,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerTrafficStatus,
  TimestampSelector,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Authorized
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.{Member, PartyId}
import com.digitalasset.canton.{ProtocolVersionChecksFixtureAnyWordSpec, config}
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.util.Random

trait TrafficControlTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OnboardsNewSequencerNode
    with ProtocolVersionChecksFixtureAnyWordSpec {

  protected val enableSequencerRestart: Boolean = true

  private val baseEventCost = 500L
  private val trafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(20 * 1000L),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    // Enough to bootstrap the synchronizer and connect the participant after 1 second
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.tryCreate(baseEventCost),
  )

  protected val pruningWindow = config.NonNegativeFiniteDuration.ofSeconds(5)

  private def getTrafficForMember(
      member: Member
  )(implicit env: TestConsoleEnvironment): Option[TrafficState] = {
    import env.*

    sequencer1.traffic_control
      .traffic_state_of_members_approximate(Seq(member))
      .trafficStates
      .get(member)
  }

  private def updateBalanceForMember(
      instance: LocalInstanceReference,
      newBalance: PositiveLong,
  )(implicit env: TestConsoleEnvironment) = {
    val member = instance.id.member

    sendTopUp(member, newBalance.toNonNegative)

    eventually() {
      // Advance the clock just slightly so we can observe the new balance be effective
      env.environment.simClock.value.advance(Duration.ofMillis(1))
      getTrafficForMember(member).value.extraTrafficPurchased.value shouldBe newBalance.value
    }
  }

  private def sendTopUp(
      member: Member,
      newBalance: NonNegativeLong,
      serialO: Option[PositiveInt] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): PositiveInt = {
    import env.*

    val serial = serialO
      .orElse(getTrafficForMember(member).flatMap(_.serial).map(_.increment))
      .getOrElse(PositiveInt.one)

    sequencer1.traffic_control.set_traffic_balance(member, serial, newBalance)

    serial
  }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 3,
        numSequencers = 2,
        numMediators = 1,
      )
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerClientConfigs_(
          // Force the participant to notice quickly that the synchronizer is down
          _.focus(_.warnDisconnectDelay).replace(config.NonNegativeFiniteDuration.ofMillis(1))
        ),
        ConfigTransforms.useStaticTime,
      )
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.trafficConfig.pruningRetentionWindow)
            .replace(pruningWindow)
            .focus(_.trafficConfig.trafficPurchasedCacheSizePerMember)
            .replace(PositiveInt.one)
        )
      )
      .withSetup { implicit env =>
        import env.*

        // Get the status from the sequencer before anything happens to check we don't get warnings when getting topology
        // and traffic purchase value
        getStatusFromSequencer(sequencer1).trafficStates should contain theSameElementsAs Map(
          mediator1.id.member -> TrafficState.empty(CantonTimestamp.MinValue)
        )

        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(trafficControl = Some(trafficControlParameters)),
        )

        // "Deactivate" ACS commitments to not consume traffic in the background
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
        )
        sequencer1.topology.synchronisation.await_idle()
      }

  private val topUpAmount = 100 * 1000L

  "start participants and connect them" in { implicit env =>
    import env.*

    participant1.start()
    participant2.start()

    participant1.health.wait_for_running()
    participant2.health.wait_for_running()

    // Fill up base rate
    environment.simClock.value.advance(Duration.ofSeconds(1))

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant2.synchronizers.connect_local(sequencer1, daName)
  }

  "have default traffic state only for registered members" in { implicit env =>
    import env.*

    val statuses = sequencer1.traffic_control
      .traffic_state_of_members(
        Seq(participant1.member, participant2.member, participant3.member, mediator1.id.member)
      )
      .trafficStates

    val expectedMembers = List[LocalInstanceReference](participant1, participant2, mediator1)

    statuses.keySet should contain theSameElementsAs expectedMembers.map(_.id.member).toSet[Member]

    expectedMembers.foreach { node =>
      clue(s"default state for $node") {
        val memberTrafficStatus = statuses.collectFirst {
          case (member, trafficState) if node.id.member == member => trafficState
        }
        memberTrafficStatus should not be empty
      }
    }

    val unregisteredStatuses = sequencer1.traffic_control
      .traffic_state_of_members(
        Seq(participant3.member)
      )
      .trafficStates
    unregisteredStatuses shouldBe empty
  }

  "send an error if topping up for an unknown member" in { implicit env =>
    import env.*
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      updateBalanceForMember(participant3, PositiveLong.one),
      _.errorMessage should include(TrafficPurchasedRequestAsyncSendFailed.id),
    )
  }

  "fail to exercise a large command with not enough traffic" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value
    // Re-fill the base rate to give some credit to the mediator, still won't be enough for the submission request though
    clock.advance(trafficControlParameters.maxBaseTrafficAccumulationDuration.asJava)
    participant1.ledger_api.packages.upload_dar(CantonTestsPath)

    val alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant1),
    )

    val pkg = participant1.packages.find_by_module("Test").headOption.map(_.packageId).value

    val exerciseCommand = getExerciseCommand(alice, pkg)

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.ledger_api.commands.submit(
        Seq(alice),
        Seq(exerciseCommand),
        workflowId = sequencer1.name,
        readAs = Seq(alice),
      ),
      _.warningMessage should include("AboveTrafficLimit"),
      _.warningMessage should include("AboveTrafficLimit"),
      _.errorMessage should include("Request failed for participant1"),
    )

    val trafficState1 = participant1.traffic_control.traffic_state(daId)
    trafficState1.extraTrafficPurchased shouldBe NonNegativeLong.zero
    trafficState1.extraTrafficRemainder shouldBe 0L
  }

  "allow participants to top up" in { implicit env =>
    import env.*
    updateBalanceForMember(participant1, PositiveLong.tryCreate(topUpAmount))

    eventually() {
      val trafficState2 = participant1.traffic_control.traffic_state(daId)
      trafficState2.extraTrafficPurchased.value shouldBe topUpAmount
    }
  }

  "charge for time proofs" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value
    clock.advance(Duration.ofSeconds(1))
    val trafficBeforeCommand = getTrafficForMember(participant1).value
    participant1.runningNode.value.getNode.value.sync
      .lookupSynchronizerTimeTracker(daId)
      .value
      .fetchTimeProof()
      .futureValueUS
      .discard
    val trafficAfterCommand = getTrafficForMember(participant1).value
    (trafficBeforeCommand.baseTrafficRemainder.value - trafficAfterCommand.baseTrafficRemainder.value) shouldBe baseEventCost
  }

  "succeed to run a big transaction with enough credit" in { implicit env =>
    import env.*

    val clock = env.environment.simClock.value

    val alice = participant1.parties.find("Alice")
    val pkg = participant1.packages.find_by_module("Test").headOption.map(_.packageId).value

    val exerciseCommand = getExerciseCommand(alice, pkg)
    val trafficBeforeCommand = participant1.traffic_control.traffic_state(daId)

    // Fill in base rate
    clock.advance(Duration.ofSeconds(1))

    // Now it should succeed
    participant1.ledger_api.commands.submit(
      Seq(alice),
      Seq(exerciseCommand),
      synchronizerId = Some(sequencer1.synchronizer_id),
      readAs = Seq(alice),
    )

    val trafficAfterCommand = participant1.traffic_control.traffic_state(daId)
    // Make sure we used extra traffic
    trafficAfterCommand.extraTrafficRemainder should be < trafficBeforeCommand.extraTrafficRemainder
    // Total limit stays the same
    trafficAfterCommand.extraTrafficPurchased.value shouldBe topUpAmount

    // Make sure base rate is full
    clock.advance(Duration.ofSeconds(1))

    // Run it again to compare the 2 traffic consumptions
    val exerciseCommandRerun = getExerciseCommand(alice, pkg)

    val trafficBeforeRerun = participant1.traffic_control.traffic_state(daId)

    // Fill in base rate
    clock.advance(Duration.ofSeconds(1))

    participant1.ledger_api.commands.submit(
      Seq(alice),
      Seq(exerciseCommandRerun),
      synchronizerId = Some(sequencer1.synchronizer_id),
      readAs = Seq(alice),
    )

    val trafficAfterRerun = participant1.traffic_control.traffic_state(daId)

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

  "support restarting of sequencers" onlyRunWhen (enableSequencerRestart) in { implicit env =>
    import env.*

    // sanity check, and also makes the latest block only contain events from/to p1
    // When we restart we'll check that p2 recovered its state as well even though it is not in the latest block
    participant1.health.ping(participant1.id)

    val trafficStateBeforeRestart =
      sequencer1.traffic_control.traffic_state_of_members(
        Seq(participant1.id, participant2.id, mediator1.id, sequencer1.id)
      )

    // Both P1 and P2 should have a status
    trafficStateBeforeRestart.trafficStates.keys should contain(participant1.id)
    trafficStateBeforeRestart.trafficStates.keys should contain(participant2.id)

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        sequencer1.stop()
        sequencer1.start()
        sequencer1.health.wait_for_running()
      },
      LogEntry.assertLogSeq(
        Seq.empty,
        Seq(
          _.warningMessage should include(LostSequencerSubscription.id),
          // We may get some failed gRPC calls to the sequencer while it's down (e.g auth token refresh)
          _.warningMessage should include("Connection refused"),
        ),
      ),
    )

    val trafficStateAfterRestart = sequencer1.traffic_control.traffic_state_of_members(
      Seq(participant1.id, participant2.id, mediator1.id, sequencer1.id)
    )

    trafficStateBeforeRestart.trafficStates should
      contain theSameElementsAs trafficStateAfterRestart.trafficStates

    eventually() {
      participant1.health.status.trySuccess.connectedSynchronizers
        .get(daId) should contain(SubmissionReady(true))
      participant1.health.ping(participant1.id)
    }

  }

  "update traffic state of sequencer and participant on top ups" in { implicit env =>
    import env.*

    val topUpAmount = PositiveLong.tryCreate(500000L)

    updateBalanceForMember(participant1, topUpAmount)

    clue("check participant1' traffic state on participant1") {
      eventually() {
        participant1.traffic_control
          .traffic_state(daId)
          .extraTrafficPurchased
          .value shouldBe 500000L
      }
    }

    // Sequencer should show the top-up
    clue(s"check participant1' traffic state on sequencer1") {
      eventually() {
        val seqStatus = sequencer1.traffic_control.traffic_state_of_members_approximate(
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

  "onboard a new sequencer" in { implicit env =>
    import env.*

    clue("Onboarding new sequencer") {
      sequencer2.start()
      sequencer2.health.wait_for_running()

      onboardNewSequencer(
        daId,
        newSequencer = sequencer2,
        existingSequencer = sequencer1,
        synchronizerOwners = initializedSynchronizers(daName).synchronizerOwners,
      )

      // Both sequencers should have the same traffic status
      eventually() {
        getStatusFromSequencer(
          sequencer2
        ).trafficStates should contain theSameElementsAs getStatusFromSequencer(
          sequencer1
        ).trafficStates
      }
    }
  }

  "restart newly onboarded sequencer immediately after onboarding" in { implicit env =>
    import env.*

    sequencer2.stop()
    sequencer2.start()
    sequencer2.health.wait_for_running()

    // Both sequencers should have the same traffic status
    getStatusFromSequencer(
      sequencer1
    ).trafficStates.view.toMap should
      contain theSameElementsAs getStatusFromSequencer(sequencer2).trafficStates.view.toMap
  }

  "get the same traffic state across sequencers" in { implicit env =>
    import env.*

    val seq1StatusBefore = getStatusFromSequencer(sequencer1)
    val seq2StatusBefore = getStatusFromSequencer(sequencer2)
    seq1StatusBefore.trafficStates.fmap(dropTimestampAndLastCost) should
      contain theSameElementsAs seq2StatusBefore.trafficStates.fmap(dropTimestampAndLastCost)

    participant1.health.ping(participant2.id)

    // Status should have changed for both participants on both sequencers
    def seq1StatusAfter = sequencer1.traffic_control
      .last_traffic_state_update_of_members(Seq(participant1, participant2))
    def seq2StatusAfter = sequencer2.traffic_control
      .last_traffic_state_update_of_members(Seq(participant1, participant2))
    List(participant1, participant2).foreach { participant =>
      val trafficFromSeq1BeforePing =
        seq1StatusBefore.trafficStates.find(_._1 == participant.id).value
      def trafficFromSeq1AfterPing =
        seq1StatusAfter.trafficStates.find(_._1 == participant.id).value._2

      clue(s"Traffic state for ${participant.id} has changed on sequencer1") {
        trafficFromSeq1BeforePing should not be trafficFromSeq1AfterPing
      }

      val trafficFromSeq2BeforePing =
        seq2StatusBefore.trafficStates.find(_._1 == participant.id).value
      def trafficFromSeq2AfterPing =
        seq2StatusAfter.trafficStates.find(_._1 == participant.id).value._2

      clue(s"Traffic state for ${participant.id} has changed on sequencer2") {
        trafficFromSeq2BeforePing should not be trafficFromSeq2AfterPing
      }

      // And the participants should report the same traffic as the sequencers
      def trafficFromNode = participant.traffic_control.traffic_state(daId)
      clue(s"Traffic state for ${participant.id} is equal to state reported by sequencer1") {
        eventually() {
          dropTimestampAndLastCost(trafficFromNode) shouldBe dropTimestampAndLastCost(
            trafficFromSeq1AfterPing
          )
        }
      }
      clue(s"Traffic state for ${participant.id} is equal to state reported by sequencer2") {
        eventually() {
          dropTimestampAndLastCost(trafficFromNode) shouldBe dropTimestampAndLastCost(
            trafficFromSeq2AfterPing
          )
        }
      }
    }
    clue(s"Sequencers report the same state") {
      // And both sequencers should have the same overall statuses
      eventually() {
        seq1StatusAfter.trafficStates should contain theSameElementsAs seq2StatusAfter.trafficStates
      }
    }
  }

  "crashed sequencer should catch up after being restarted" in { implicit env =>
    import env.*

    sequencer2.stop()

    participant1.health.ping(participant2.id)

    sequencer2.start()
    sequencer2.health.wait_for_running()

    eventually() {
      // Both sequencers should have the same traffic status
      getStatusFromSequencer(
        sequencer1
      ).trafficStates.view.toMap should contain theSameElementsAs getStatusFromSequencer(
        sequencer2
      ).trafficStates.view.toMap
    }
  }

  "restarted participant should initialize their traffic state correctly" in { implicit env =>
    import env.*

    val trafficFromP1Before = participant1.traffic_control.traffic_state(daId)
    val trafficFromP2Before = participant2.traffic_control.traffic_state(daId)

    participant1.stop()
    participant2.stop()
    participant1.start()
    participant2.start()

    participant1.health.wait_for_running()
    participant2.health.wait_for_running()

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant2.synchronizers.connect_local(sequencer1, daName)

    def checkTrafficAfterRestart(participant: LocalParticipantReference, before: TrafficState) = {
      participant.synchronizers.active(daName) shouldBe true
      val trafficAfter = participant.traffic_control.traffic_state(daId)
      // It's possible we get a state at a more recent time
      // if the last sequenced event is more recent than the traffic state before we shutdown
      // Similarly the lastConsumedCost will be 0 because the traffic state timestamp does not correspond to an event
      // for which the sender has consumed traffic
      trafficAfter.extraTrafficPurchased shouldBe before.extraTrafficPurchased
      trafficAfter.extraTrafficConsumed shouldBe before.extraTrafficConsumed
      // Base traffic remainder may have increased while we restarted
      trafficAfter.baseTrafficRemainder.value should be >= before.baseTrafficRemainder.value
      trafficAfter.timestamp.isAfter(before.timestamp) shouldBe true
    }

    eventually() {
      checkTrafficAfterRestart(participant1, trafficFromP1Before)
      checkTrafficAfterRestart(participant2, trafficFromP2Before)
    }

  }

  "disconnecting from the synchronizer just after a traffic purchase should not lose the update" in {
    implicit env =>
      import env.*

      val trafficFromP1Before = participant1.traffic_control.traffic_state(daId)
      val newTopUp = trafficFromP1Before.extraTrafficPurchased.value + 1000
      updateBalanceForMember(participant1, PositiveLong.tryCreate(newTopUp))

      participant1.synchronizers.disconnect(daName)
      participant1.synchronizers.connect_local(sequencer1, daName)

      eventually() {
        participant1.synchronizers.active(daName) shouldBe true
        val trafficFromP1After = participant1.traffic_control.traffic_state(daId)

        trafficFromP1After.extraTrafficPurchased.value shouldBe newTopUp
      }

  }

  "return only participant and mediators traffic state" in { implicit env =>
    import env.*
    sequencer1.traffic_control
      .traffic_state_of_all_members()
      .trafficStates
      .keys should contain theSameElementsAs List(
      participant1.id,
      participant2.id,
      mediator1.id,
    )
  }

  "update traffic control config via dynamic synchronizer parameters" in { implicit env =>
    import env.*

    // We'll use p3 to ensure it can re-connect to the synchronizer even if the traffic parameters change
    // while it's disconnected, and the tolerance window has elapsed
    participant3.start()
    participant3.health.wait_for_running()
    participant3.synchronizers.connect_local(sequencer1, daName)
    // Ping itself to make sure everything is fine
    participant3.health.ping(participant3)

    // Disconnect p3
    participant3.synchronizers.disconnect(daName)

    val clock = env.environment.simClock.value

    // Advance the clock way in the future to get outside the tolerance window for outdated traffic costs
    clock.advance(Duration.ofHours(1))

    // Update traffic parameters
    val baseTrafficAmount: Long = 50 * 1000
    val updatedConfig = trafficControlParameters
      .copy(
        // Change the read / write factor to force the cost computation to change after the params are updated
        readVsWriteScalingFactor =
          trafficControlParameters.readVsWriteScalingFactor.*(PositiveInt.two),
        maxBaseTrafficAmount = NonNegativeLong.tryCreate(baseTrafficAmount),
        maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(10),
      )
    sequencer1.topology.synchronizer_parameters.propose_update(
      synchronizerId = daId,
      _.update(trafficControl = Some(updatedConfig)),
    )

    sequencer1.topology.synchronisation.await_idle()
    sequencer2.topology.synchronisation.await_idle()

    // Run a ping such that the topology gets updated and the new config comes in effect
    participant1.health.ping(participant2.id)

    // Create a new namespace delegation. This adds a topology transaction to its local store,
    // that will be broadcast when it reconnects to the synchronizer
    val newSigningKey =
      participant3.keys.secret.generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)
    participant3.topology.namespace_delegations.propose_delegation(
      participant3.namespace,
      newSigningKey,
      CanSignAllMappings,
      store = Authorized,
    )

    // Ensure that P3 can re-connect properly
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        participant3.synchronizers.reconnect_local(daName)
        eventually() {
          // Make sure the transaction goes through eventually and the synchronizer sees that p3 hosts Bob
          participant3.topology.namespace_delegations
            .list(
              daId,
              filterTargetKey = Some(newSigningKey.fingerprint.toProtoPrimitive),
            ) should not be empty
        }
      },
      LogEntry.assertLogSeq(
        Seq.empty,
        // Upon connection to the synchronizer, there's a race between the participant reading events from the sequencer,
        // receiving the topology update with the traffic parameter changes, and the flushing of its synchronizer outbox.
        // Very likely, flushing of the synchronizer outbox happens before the participant had time to process the traffic
        // parameters change, which fails the first attempt to flush because the computed cost is incorrect, which
        // outputs the logs below. Synchronizer connection should still go through and the flushed transaction be sent
        // successfully eventually, as asserted above.
        // However it's theoretically possible that the participant updates its traffic params before flushing,
        // in which case flushing would work on the first attempt, and no errors would be logged. To avoid creating a
        // flake if that ever happens, we don't require those errors to be suppressed and have them as optional instead.
        Seq(
          _.warningMessage should include(OutdatedTrafficCost.id),
          _.warningMessage should include("synchronizer outbox flusher"),
        ),
      ),
    )

    // Ping to make sure everything still works
    participant3.health.ping(participant3)

    // Advance the clock for more than the full max burst duration...
    clock.advance(Duration.ofSeconds(15))

    // ... and verify that we now have "max burst duration" seconds of base traffic amount at 50 000 traffic / second = 500 000
    for {
      seq <- List(sequencer1, sequencer2)
      part <- List(participant1, participant2, participant3)
    } yield {
      clue(s"check ${part.id} traffic state on ${seq.id}") {
        val trafficStatus = seq.underlying.value.sequencer.sequencer
          .trafficStatus(Seq(part.id), TimestampSelector.LatestApproximate)
          .failOnShutdown
          .futureValue
        trafficStatus.trafficStates.get(part.id).value.baseTrafficRemainder.value should
          equal(baseTrafficAmount)
      }
    }
  }

  // Tests only for the new balance update mechanism
  "balance serial should be per member" in { implicit env =>
    import env.*

    val balanceP1 = getTrafficForMember(participant1.id).value
    val balanceP2 = getTrafficForMember(participant2.id).value
    balanceP1.serial.map(_.value) shouldBe Some(3)
    balanceP2.serial shouldBe None
  }

  "reject top-up requests with a non-increasing serial" in { implicit env =>
    import cats.syntax.either.*
    import env.*

    val topUpBalance = NonNegativeLong.tryCreate(100000)
    val serial = sendTopUp(participant1, topUpBalance)

    // Ensure the balance update is seen by sequencer1 by checking the serial
    eventually() {
      // Advance the clock just slightly so we can observe the new balance be effective
      env.environment.simClock.value.advance(Duration.ofMillis(1))
      val trafficState =
        sequencer1.traffic_control.traffic_state_of_members_approximate(Seq(participant1))
      trafficState.trafficStates.loneElement._2.serial shouldBe Some(serial)
    }

    // Check that serial values <= the latest used are rejected
    (serial.value - 1 to serial.value).foreach { newSerial =>
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        Either
          .catchOnly[CommandFailure] {
            sendTopUp(participant1, topUpBalance.tryAdd(1000), Some(newSerial))
          }
          .left
          .value
          .getMessage should include("Command execution failed"),
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.errorMessage should (include(TrafficControlSerialTooLow.id) and include(
                s"Latest serial used by this member is Some($serial)"
              )),
              "Top-up with serial too low",
            )
          )
        ),
      )
    }
  }

  private def getStatusFromSequencer(
      sequencer: LocalSequencerReference
  )(implicit env: TestConsoleEnvironment): SequencerTrafficStatus = {
    import env.*

    sequencer.traffic_control.traffic_state_of_members(
      Seq(
        participant1.id,
        participant2.id,
        mediator1.id,
        sequencer1.id,
        sequencer2.id,
      )
    )
  }

  private def getExerciseCommand(alice: PartyId, pkg: String)(implicit
      env: TestConsoleEnvironment
  ): Command = {
    import env.*

    val createCommand = ledger_api_utils.create(
      pkg,
      "Test",
      "DummyWithParam",
      Map("operator" -> alice),
    )

    val created = participant1.ledger_api.commands.submit(
      Seq(alice),
      Seq(createCommand),
      synchronizerId = Some(sequencer1.synchronizer_id),
      readAs = Seq(alice),
    )

    val contractId = created.eventsById.values
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

  // Used to compare MemberTrafficStatuses without taking into account the timestamp, which varies between nodes based
  // on their current sim clock. We can still meaningfully compare the rest of the data though and assert they are equal
  // even with unaligned clocks. Last consumed cost is only present for a specific timestamp, when it was consumed,
  // so we ignore it as well.
  private def dropTimestampAndLastCost(status: TrafficState) =
    status.copy(
      timestamp = CantonTimestamp.MinValue,
      lastConsumedCost = NonNegativeLong.zero,
    )
}

class TrafficControlTestH2 extends TrafficControlTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

class TrafficControlTestPostgres extends TrafficControlTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

// TODO(#16789) Re-enable test once dynamic onboarding is supported for BFT Orderer
class TrafficControlTestBftOrderingPostgres
//  extends TrafficControlTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseBftOrderingBlockSequencer(loggerFactory))
//  override protected val enableSequencerRestart: Boolean = false
//}

// TODO(#16789) Re-enable test once dynamic onboarding is supported for BFT Orderer
class TrafficControlTestBftOrderingH2
//  extends TrafficControlTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(new UseBftOrderingBlockSequencer(loggerFactory))
//  override protected val enableSequencerRestart: Boolean = false
//}
