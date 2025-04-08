// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.metrics.api.testing.MetricValues.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
}
import com.digitalasset.canton.config.{DbConfig, NonNegativeFiniteDuration}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.{MessageId, TrafficState}
import com.digitalasset.canton.sequencing.{SequencerConnections, SubmissionRequestAmplification}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.{SequencerAlias, config}
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.DurationInt

abstract class SubmissionRequestAmplificationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val trafficControlParameters = TrafficControlParameters(
    // No base traffic, so we can precisely measure using extra traffic only
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(0L),
    readVsWriteScalingFactor = PositiveInt.tryCreate(200),
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config.withNetworkBootstrap { implicit env =>
      import env.*
      new NetworkBootstrapper(
        S2M2.copy(overrideMediatorToSequencers =
          Some(
            Map(
              // A threshold of two ensures that the mediators connect to both sequencers.
              // TODO(#19911) Make this properly configurable
              mediator1 -> (Seq(sequencer1, sequencer2), PositiveInt.two),
              mediator2 -> (Seq(sequencer1, sequencer2), PositiveInt.two),
            )
          )
        )
      )
    }

  "reconfigure mediators to use amplification" in { implicit env =>
    import env.*

    mediators.local.foreach(
      _.sequencer_connection.modify_connections { old =>
        SequencerConnections.tryMany(
          old.connections,
          old.sequencerTrustThreshold,
          SubmissionRequestAmplification(
            PositiveInt.tryCreate(2),
            config.NonNegativeFiniteDuration.Zero,
          ),
        )
      }
    )
  }

  "connect participants with amplification" in { implicit env =>
    import env.*

    participants.local.foreach(
      _.synchronizers.connect_local_bft(
        alias = daName,
        synchronizer = NonEmpty(
          Map,
          SequencerAlias.tryCreate("seq1x") -> sequencer1,
          SequencerAlias.tryCreate("seq2x") -> sequencer2,
        ),
        // A threshold of two ensures that the participants connect to both sequencers.
        // TODO(#19911) Make this properly configurable
        sequencerTrustThreshold = PositiveInt.two,
        submissionRequestAmplification = SubmissionRequestAmplification(
          PositiveInt.tryCreate(2),
          config.NonNegativeFiniteDuration.Zero,
        ),
      )
    )
  }

  def getProcessedMessages(sequencer: LocalSequencerReference, markBefore: Boolean): Long = {
    val metric = sequencer.underlying.value.sequencer.metrics.publicApi.messagesProcessed
    // Explicitly mark the metric so that it exists even if the sequencer has not yet seen any submission requests.
    if (markBefore) metric.mark()
    metric.value
  }

  "run a ping with amplification enabled" in { implicit env =>
    import env.*

    val before = sequencers.local.map(getProcessedMessages(_, true))
    participant1.health.ping(participant2.id)

    eventually() {
      val after = sequencers.local.map(getProcessedMessages(_, false))

      // Check that each sequencer gets submission requests from both participants and the mediators
      // The ping transaction creates the following submission requests:
      // - 1 confirmation request
      // - 1 confirmation response from participant1
      // - 2 confirmation result messages (one from each mediator)
      // The pong transaction creates the following submission requests:
      // - 1 confirmation request
      // - 2 confirmation responses from the two participants
      // - 2 confirmation result messages (one from each mediator)
      // In total 9 submission requests.
      // Additionally, we may see ACS commitments and time proof requests that go only to a single sequencer
      forEvery(sequencers.local.zip(before.zip(after))) { case (sequencer, (bef, aft)) =>
        withClue(s"sequencer ${sequencer.id}, before: $bef, after: $aft.") {
          (aft - bef) should be >= 9L
        }
      }
    }
  }

  "scale traffic consumed with amplification" in { implicit env =>
    import env.*

    val currentAmplificationConfigs = participants.local.map(
      _.synchronizers.config(daName).value.sequencerConnections.submissionRequestAmplification
    )

    def trafficStateOf(member: Member): TrafficState = {
      def getState(sequencer: LocalSequencerReference) =
        sequencer.traffic_control
          .traffic_state_of_members(Seq(member))
          .trafficStates
          .find(_._1 == member)
          .value
          ._2

      // Make sure both sequencers see the same state
      eventually() {
        val seq1State = getState(sequencer1)
        seq1State shouldBe getState(sequencer2)
        seq1State
      }
    }

    val topUpAmount = 100000L
    val topUpAmountNNL = NonNegativeLong.tryCreate(topUpAmount)
    // Disconnect and re-connect participants without amplification
    participants.local.foreach { participant =>
      participant.synchronizers.disconnect(daName)
      participant.synchronizers.modify(
        daName,
        _.focus(_.sequencerConnections).modify(old =>
          old.withSubmissionRequestAmplification(
            // Make sure to set patience to 0 to force double sending of submission requests
            old.submissionRequestAmplification
              .copy(factor = PositiveInt.one, patience = NonNegativeFiniteDuration.Zero)
          )
        ),
      )
      participant.synchronizers.reconnect(daName)
    }

    // Enable traffic control so we can assert how amplification affects traffic
    sequencer1.topology.synchronizer_parameters.propose_update(
      synchronizerId = daId,
      _.update(trafficControl = Some(trafficControlParameters)),
    )
    sequencer1.topology.synchronisation.await_idle()

    val members = List(participant1.id, participant2.id, mediator1.id, mediator2.id)
    // Give credits to everyone
    members.foreach { n =>
      sequencer1.traffic_control.set_traffic_balance(n, PositiveInt.one, topUpAmountNNL)
    }

    eventually() {
      members.foreach { member =>
        // Need to approximate the traffic state to the latest available here, otherwise we won't observe the change
        // since no event is sequenced after the top up
        // Check that both sequencers see the top up
        sequencer1.traffic_control
          .traffic_state_of_members_approximate(Seq(member))
          .trafficStates(member)
          .extraTrafficPurchased
          .value shouldBe topUpAmount
        sequencer2.traffic_control
          .traffic_state_of_members_approximate(Seq(member))
          .trafficStates(member)
          .extraTrafficPurchased
          .value shouldBe topUpAmount
      }
    }

    // run a ping without amplification
    participant1.health.ping(participant2.id, timeout = timeouts.unbounded)
    val trafficAfter = trafficStateOf(participant1.member)
    val trafficConsumedWithoutAmplification = trafficAfter.extraTrafficConsumed.value

    // reconnect with amplification
    participants.local.foreach { participant =>
      participant.synchronizers.disconnect(daName)
      participant.synchronizers.modify(
        daName,
        _.focus(_.sequencerConnections).modify(old =>
          old.withSubmissionRequestAmplification(
            old.submissionRequestAmplification.copy(factor = PositiveInt.tryCreate(2))
          )
        ),
      )
      participant.synchronizers.reconnect(daName)
    }

    // Get the traffic before and after the ping
    val trafficBeforePingWithAmplification = trafficStateOf(participant1.member)
    participant1.health.ping(participant2.id)
    val trafficAfterPingWithAmplification = trafficStateOf(participant1.member)
    val trafficConsumedAfterPingWithAmplification =
      trafficAfterPingWithAmplification.extraTrafficConsumed
    // Diff the consumption to get cost of ping
    val effectiveTrafficConsumedWithAmplification =
      trafficConsumedAfterPingWithAmplification.value - trafficBeforePingWithAmplification.extraTrafficConsumed.value

    // Cost with amplification should be twice without it, with some wiggle room because exact payload size varies slightly because of different timestamps and compression
    effectiveTrafficConsumedWithAmplification should equal(
      2 * trafficConsumedWithoutAmplification +- (effectiveTrafficConsumedWithAmplification.toDouble * 0.1d).toLong
    )

    // Disable traffic control to not mess with the rest of the test suite
    sequencer1.topology.synchronizer_parameters.propose_update(
      synchronizerId = daId,
      _.update(trafficControl = None),
    )

    // restore the previous amplification configs
    participants.local.zipWithIndex.foreach { case (participant, i) =>
      val previousConfig = currentAmplificationConfigs(i)
      participant.synchronizers.disconnect(daName)
      participant.synchronizers.modify(
        daName,
        _.focus(_.sequencerConnections).modify(old =>
          old.withSubmissionRequestAmplification(submissionRequestAmplification = previousConfig)
        ),
      )
      participant.synchronizers.reconnect(daName)

      // Last thing we do above is reconnect the participants to the synchronizer
      // To make sure everything is processed and the synchronizer is idle to avoid polluting the rest of the test suite
      // make sure both the participants and the sequencers agree that all participants are now connected to the synchronizer
      eventually() {
        participant.synchronizers.active(daName) shouldBe true
        sequencers.local.foreach(
          _.topology.participant_synchronizer_states.active(daId, participant.id) shouldBe true
        )
      }
    }

  }

  "ping even if one sequencer refuses or drops" in { implicit env =>
    import env.*

    val sequencer = getProgrammableSequencer(sequencer1.name)

    val dropped = new AtomicInteger(0)
    sequencer.setPolicy_("drop everything but time proofs and ACS commitments")(
      SendPolicy.processTimeProofs_ { submissionRequest =>
        if (ProgrammableSequencerPolicies.isAcsCommitment(submissionRequest)) {
          SendDecision.Process
        } else {
          dropped.getAndIncrement()
          SendDecision.Drop
        }
      }
    )
    participant1.health.ping(participant2.id)

    eventually() {
      dropped.get() should be >= 9
    }

    sequencer.resetPolicy()
  }

  def reconfigurePatience(
      newPatience: config.NonNegativeFiniteDuration
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // TODO(#18817) We must wait here until the mediator becomes idle.
    //  Otherwise, the connection modification runs concurrently with the mediator's application handler from the previous tests,
    //  which can trigger a race condition in the resilient sequencer subscription logic.
    //  We do so by running a topology transaction and waiting until the mediator sees it.
    //  Additionally, we modify the participants first to give the mediators' application handlers
    //  some more time to finish processing the topology transaction.
    val dummyParty = participant1.parties.enable(java.util.UUID.randomUUID().toString)
    eventually() {
      mediators.local.foreach { mediator =>
        mediator.topology.party_to_participant_mappings.is_known(
          daId,
          dummyParty,
          Seq(participant1),
        ) shouldBe true
      }
    }

    participants.local.foreach { participant =>
      participant.synchronizers.disconnect(daName)
      participant.synchronizers.modify(
        daName,
        _.focus(_.sequencerConnections).modify(old =>
          old.withSubmissionRequestAmplification(
            old.submissionRequestAmplification.copy(patience = newPatience)
          )
        ),
      )
      participant.synchronizers.reconnect(daName)
    }

    mediators.local.foreach(
      _.sequencer_connection.modify_connections(old =>
        old.withSubmissionRequestAmplification(
          old.submissionRequestAmplification.copy(patience = newPatience)
        )
      )
    )
  }

  "ping without amplification in the patient case" in { implicit env =>
    import env.*

    val patience = config.NonNegativeFiniteDuration.fromDuration(20.second).value
    reconfigurePatience(patience)

    // We don't reuse message IDs across submission requests.
    // So to check that amplification does not kick in, we simply record the message IDs across both sequencers
    // and check that they are unique.
    val messageIds = new AtomicReference[Seq[MessageId]](Seq.empty)

    sequencers.local.foreach { sequencerRef =>
      val sequencer = getProgrammableSequencer(sequencerRef.name)
      sequencer.setPolicy_("record message ids") { submissionRequest =>
        messageIds.updateAndGet(_ :+ submissionRequest.messageId)
        SendDecision.Process
      }
    }

    participant1.health.ping(participant2.id)

    val recordedMessageIds = messageIds.get()
    recordedMessageIds.size shouldBe >=(9)
    recordedMessageIds.distinct shouldBe recordedMessageIds

    sequencers.local.foreach(sequencerRef =>
      getProgrammableSequencer(sequencerRef.name).resetPolicy()
    )
  }

  "retry upon timeout" in { implicit env =>
    import env.*
    val patience = config.NonNegativeFiniteDuration.fromDuration(1.second).value
    reconfigurePatience(patience)

    // One of the sequencers drops all messages.
    // Check that some requests are amplified by finding duplicate message IDs recorded
    val messageIds1 = new AtomicReference[Seq[MessageId]](Seq.empty)
    val messageIds2 = new AtomicReference[Seq[MessageId]](Seq.empty)

    val seq1 = getProgrammableSequencer(sequencer1.name)
    seq1.setPolicy_("record message ids and drop") {
      SendPolicy.processTimeProofs_ { submissionRequest =>
        if (ProgrammableSequencerPolicies.isAcsCommitment(submissionRequest)) {
          SendDecision.Process
        } else {
          messageIds1.updateAndGet(_ :+ submissionRequest.messageId)
          SendDecision.Drop
        }
      }
    }

    val seq2 = getProgrammableSequencer(sequencer2.name)
    seq2.setPolicy_("record message ids") { submissionRequest =>
      messageIds2.updateAndGet(_ :+ submissionRequest.messageId)
      SendDecision.Process
    }

    // Ping three times to get 27 submission requests in total
    participant1.health.ping(participant2.id)
    participant1.health.ping(participant2.id)
    participant1.health.ping(participant2.id)

    eventually() {
      val droppedIds = messageIds1.get()
      val sequencedIds = messageIds2.get()
      // The probability of none of the submission requests targeting sequencer1 is (1/2)^27.
      // This should be small enough that this does not flake.
      droppedIds should not be empty
      sequencedIds should contain allElementsOf droppedIds
    }

    seq1.resetPolicy()
    seq2.resetPolicy()

  }

}

class SubmissionRequestAmplificationReferenceIntegrationTestPostgres
    extends SubmissionRequestAmplificationIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
