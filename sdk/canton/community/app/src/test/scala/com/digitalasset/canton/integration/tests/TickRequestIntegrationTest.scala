// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
}
import com.digitalasset.canton.config.{StorageConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.sequencing.{
  SequencerConnections,
  TrafficControlParameters as InternalTrafficControlParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.Member

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

sealed trait TickRequestIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  // Effectively disable ACS commitment exchanges
  private val reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)
  // Effectively disable time proof requests on inactive synchronizers
  private val minObservationDuration = config.NonNegativeFiniteDuration.ofDays(365)
  private val observationLatency = config.NonNegativeFiniteDuration.ofSeconds(1)
  private val patienceDuration = config.NonNegativeFiniteDuration.ofSeconds(2)
  private val ledgerTimeRecordTimeTolerance = config.NonNegativeFiniteDuration.ofMinutes(1)
  private val participantResponseTimeout = config.NonNegativeFiniteDuration.ofMinutes(2)
  private val mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofMinutes(2)
  private val synchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(
    observationLatency = observationLatency,
    patienceDuration = patienceDuration,
    minObservationDuration = minObservationDuration,
  )
  private val topologyTransactionRegistrationTimeout = config.NonNegativeFiniteDuration.ofMinutes(1)

  private val setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L)
  private lazy val trafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(20 * 1000L),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    // Enough to bootstrap the synchronizer and connect the participant after 1 second
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = setBalanceRequestSubmissionWindowSize,
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.tryCreate(500L),
  )

  private def advanceTimeBeyondTimeouts(simClock: SimClock): Unit = {
    logger.info("advancing the clock beyond all timeouts")
    simClock.advance(
      ledgerTimeRecordTimeTolerance.asJava
        .plus(participantResponseTimeout.asJava)
        .plus(mediatorReactionTimeout.asJava)
        .plus(patienceDuration.asJava)
        .plus(observationLatency.asJava)
        .plus(setBalanceRequestSubmissionWindowSize.asJava.multipliedBy(2L))
        .plus(topologyTransactionRegistrationTimeout.asJavaApproximation)
        .plus(java.time.Duration.ofMillis(1))
    )
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateSynchronizerTimeTrackerConfigs_(_ => synchronizerTimeTrackerConfig),
        ConfigTransforms.updateTargetTimestampForwardTolerance(Duration.ofHours(1)),
      )
      .addConfigTransforms(
        ConfigTransforms.setTopologyTransactionRegistrationTimeout(
          topologyTransactionRegistrationTimeout
        )*
      )
      .withSetup { implicit env =>
        import env.*

        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(
                confirmationResponseTimeout = participantResponseTimeout,
                mediatorReactionTimeout = mediatorReactionTimeout,
                reconciliationInterval = reconciliationInterval,
                ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
                // Disable automatic assignments
                assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero,
                trafficControl =
                  Option.when(synchronizer.synchronizerId == daId.logical)(trafficControlParameters),
              ),
            )
        )
        // Fill up base rate on DA synchronizer to allow the participants to connect
        environment.simClock.value.advance(Duration.ofSeconds(1))

        val daSequencerConnection =
          SequencerConnections.single(sequencer1.sequencerConnection.withAlias(daName.toString))
        participants.all.synchronizers.connect(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = daSequencerConnection,
            timeTracker = synchronizerTimeTrackerConfig,
          )
        )

        val acmeSequencerConnection =
          SequencerConnections.single(sequencer2.sequencerConnection.withAlias(acmeName.toString))
        participants.all.synchronizers.connect(
          SynchronizerConnectionConfig(
            synchronizerAlias = acmeName,
            sequencerConnections = acmeSequencerConnection,
            timeTracker = synchronizerTimeTrackerConfig,
          )
        )

        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = acmeId)
        participant1.parties.enable("Alice", synchronizer = daName)
        participant2.parties.enable("Bob", synchronizer = daName)
        participant1.parties.enable("Alice", synchronizer = acmeName)
        participant2.parties.enable("Bob", synchronizer = acmeName)
      }

  "no time proof request after a successful traffic top-up" in { implicit env =>
    import env.*

    // top up all participants so that the subsequent tests can run
    val serials = sequencer1.traffic_control
      .traffic_state_of_members_approximate(participants.all.map(_.member))
      .trafficStates

    val collector1 = new TickRequestCollector(sequencer1)
    collector1.startCollecting()

    val newBalance = NonNegativeLong.tryCreate(Long.MaxValue / 2)
    serials.foreach { case (participant, trafficState) =>
      sequencer1.traffic_control.set_traffic_balance(
        participant,
        trafficState.serial.fold(PositiveInt.one)(_.increment),
        newBalance,
      )
    }

    eventually() {
      // Advance the clock just slightly so we can observe the new balance be effective
      env.environment.simClock.value.advance(Duration.ofMillis(1))
      val states = sequencer1.traffic_control
        .traffic_state_of_members_approximate(participants.all.map(_.member))
        .trafficStates
      states.foreach { case (participant, trafficState) =>
        withClue(s"For participant $participant") {
          trafficState.extraTrafficPurchased shouldBe newBalance
        }
      }
    }

    advanceTimeBeyondTimeouts(env.environment.simClock.value)

    always() {
      collector1.collected shouldBe Seq.empty
    }

    collector1.stopCollecting()
  }

  "no time proof requests after a successful confirmation request" in { implicit env =>
    import env.*
    val alice = participant1.parties.list(filterParty = "Alice", limit = 1).loneElement.party
    val bob = participant2.parties.list(filterParty = "Bob", limit = 1).loneElement.party
    val simClock = env.environment.simClock.value

    val collector1 = new TickRequestCollector(sequencer1)

    logger.info("Running a successful transaction")
    collector1.startCollecting()

    val iou = IouSyntax.createIou(participant1, Some(daId.logical))(alice, bob)

    advanceTimeBeyondTimeouts(simClock)

    always() {
      collector1.collected shouldBe Seq.empty
    }

    logger.info("Running a successful unassignment")

    val collector2 = new TickRequestCollector(sequencer2)
    collector2.startCollecting()

    val iouId = LfContractId.assertFromString(iou.id.contractId)

    val unassignment = participant2.ledger_api.javaapi.commands
      .submit_unassign(
        bob,
        Seq(iouId),
        daId.logical,
        acmeId.logical,
      )
      .getEvents
      .loneElement

    advanceTimeBeyondTimeouts(simClock)

    val expectedRequests = Seq(
      // The submitting participant of the unassignment needs to get a time proof for the target timestamp
      participant2,
      // Each reassigning participant needs to validate the target topology at the target timestamp
      // and therefore has to observe the target timestamp. Since the submitting participant already requested
      // a time proof during submission, it should not request another one.
      participant1,
    ).map(_.member)
    always() {
      collector1.collected shouldBe Seq.empty
      // We can test for equality here because those tick requests must have happened during phases 1 and 3.
      collector2.collected shouldBe expectedRequests
    }
    collector2.clear()

    logger.info("Running a successful assignment")

    val reassignmentId = unassignment match {
      case unassign: com.daml.ledger.javaapi.data.UnassignedEvent => unassign.getReassignmentId
      case _ => fail(s"Expected an unassignment event, but got $unassignment")
    }
    participant1.ledger_api.javaapi.commands.submit_assign(
      alice,
      reassignmentId,
      daId.logical,
      acmeId.logical,
    )

    advanceTimeBeyondTimeouts(simClock)
    always() {
      collector1.collected shouldBe Seq.empty
      collector2.collected shouldBe Seq.empty
    }

    collector1.stopCollecting()
    collector2.stopCollecting()
  }

  "no time proof requests after a successful topology submission" in { implicit env =>
    import env.*

    val collector1 = new TickRequestCollector(sequencer1)
    collector1.startCollecting()

    participant1.parties.enable("Charlie", synchronizer = daName)

    advanceTimeBeyondTimeouts(env.environment.simClock.value)
    always() {
      collector1.collected shouldBe Seq.empty
    }
    collector1.stopCollecting()
  }

  private class TickRequestCollector(sequencer: LocalSequencerReference) {
    private val timeProofRequesters: AtomicReference[Seq[Member]] =
      new AtomicReference[Seq[Member]](Vector.empty)

    def startCollecting(): Unit = {
      val progSequencer1 = getProgrammableSequencer(sequencer.name)
      progSequencer1.setPolicy_(s"record all time proofs on sequencer ${sequencer.name}") {
        submissionRequest =>
          if (TimeProof.isTimeProofSubmission(submissionRequest)) {
            timeProofRequesters.getAndUpdate(_ :+ submissionRequest.sender)
          }
          SendDecision.Process
      }
    }

    def stopCollecting(): Unit =
      getProgrammableSequencer(sequencer.name).resetPolicy()

    def collected: Seq[Member] =
      timeProofRequesters.get()

    def clear(): Unit = timeProofRequesters.set(Vector.empty)
  }
}

class TickRequestIntegrationTestMemory extends TickRequestIntegrationTest {
  registerPlugin(
    new UseReferenceBlockSequencer[StorageConfig.Memory](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
