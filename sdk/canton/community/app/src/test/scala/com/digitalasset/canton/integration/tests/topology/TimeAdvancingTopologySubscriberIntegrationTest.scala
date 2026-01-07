// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{SynchronizerTimeTrackerConfig, TestSequencerClientFor}
import com.digitalasset.canton.console.{ParticipantReference, SequencerReference}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.SequencedSerializedEvent
import com.digitalasset.canton.sequencing.client.DelayedSequencerClient
import com.digitalasset.canton.sequencing.client.DelayedSequencerClient.{
  DelaySequencerClient,
  SequencedEventDelayPolicy,
}
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  ClosedEnvelope,
  Deliver,
  SequencedEvent,
  TimeProof,
}
import com.digitalasset.canton.synchronizer.sequencer.time.TimeAdvancingTopologySubscriber.TimeAdvanceBroadcastMessageIdPrefix
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.Promise

trait TimeAdvancingTopologySubscriberIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private lazy val observationLatency: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(10)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S2M1
      .updateTestingConfig(
        _.focus(_.testSequencerClientFor).replace(
          Set(
            TestSequencerClientFor(this.getClass.getSimpleName, "participant1", "synchronizer1")
          )
        )
      )
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.timeTracker.observationLatency).replace(observationLatency.toConfig)
        ),
        ConfigTransforms.updateAllMediatorConfigs_(
          _.focus(_.timeTracker.observationLatency).replace(observationLatency.toConfig)
        ),
      )
      // Do not use a static time because this test requires a non-zero topology change delay
      .withSetup { env =>
        import env.*

        def connect(
            participant: ParticipantReference,
            sequencer: SequencerReference,
        ): Unit = {
          val daSequencerConnection =
            SequencerConnections.single(sequencer.sequencerConnection.withAlias(daName.toString))
          participant.synchronizers.connect_by_config(
            SynchronizerConnectionConfig(
              synchronizerAlias = daName,
              sequencerConnections = daSequencerConnection,
              timeTracker =
                SynchronizerTimeTrackerConfig(observationLatency = observationLatency.toConfig),
            )
          )
        }

        connect(participant1, sequencer1)
        connect(participant2, sequencer2)
      }

  "TimeAdvancingTopologySubscriber" should {
    "prevent time proof requests after topology transactions" in { implicit env =>
      import env.*

      val timeProofRequestCounter = new AtomicInteger(0)
      val timeAdvancingCounter = new AtomicInteger(0)
      val timeAdvancingDelay = Promise[Unit]()

      val progSeqs =
        Seq(sequencer1, sequencer2).map(sequencer => getProgrammableSequencer(sequencer.name))

      progSeqs.foreach(_.setPolicy_("count time proof requests and delay time advancing requests") {
        submissionRequest =>
          if (TimeProof.isTimeProofSubmission(submissionRequest)) {
            timeProofRequestCounter.getAndIncrement().discard
            SendDecision.Process
          } else if (
            submissionRequest.messageId.unwrap.startsWith(TimeAdvanceBroadcastMessageIdPrefix)
          ) {
            // Make sure that we actually have both sequencers submit time advancements.
            // Without holding them back, there is the chance that one sequencer is slow and observes the other's
            // advancement, so it could then decide to not attempt to send anything at all.
            val count = timeAdvancingCounter.incrementAndGet()
            if (count >= 2) timeAdvancingDelay.trySuccess(()).discard
            SendDecision.HoldBack(timeAdvancingDelay.future)
          } else {
            SendDecision.Process
          }
      })

      // Look at all messages received by participant 1 and count those that are addressed to AllMembersOfSynchronizer
      val broadcastsObservedByP1 = new AtomicReference[Seq[SequencedSerializedEvent]](Vector.empty)
      val p1SequencerClientInterceptor = DelayedSequencerClient
        .delayedSequencerClient(
          this.getClass.getSimpleName,
          daId,
          participant1.id.uid.toString,
        )
        .value
      p1SequencerClientInterceptor.setDelayPolicy(new SequencedEventDelayPolicy {
        private def isBroadcastEvent(event: SequencedEvent[ClosedEnvelope]): Boolean = event match {
          case deliver: Deliver[ClosedEnvelope] =>
            deliver.envelopes.exists(_.recipients.allRecipients.contains(AllMembersOfSynchronizer))
          case _ => false
        }

        override def apply(event: SequencedSerializedEvent): DelaySequencerClient = {
          if (isBroadcastEvent(event.underlying.value.content))
            broadcastsObservedByP1.getAndUpdate(_ :+ event).discard
          DelayedSequencerClient.Immediate
        }
      })

      // Send a topology transaction
      val start = System.nanoTime()
      participant1.parties.enable("test-party")
      val end = System.nanoTime()

      // Let's wait until the observation latency really has elapsed to make sure that we'll catch late time proof requests.
      val wait = Math.max(observationLatency.duration.toMillis - (end - start) / 1000000, 0)
      Threading.sleep(wait + 100)

      timeProofRequestCounter.get() shouldBe 0

      val broadcasts = broadcastsObservedByP1.get().map(_.underlying.value.content)
      // We expect at least two broadcasts: One for the topology transaction and one for the aggregated notification message
      // If the sequencing of the notification message is slow, we may see more of them.
      broadcasts.size shouldBe >=(2)

      progSeqs.foreach(_.resetPolicy())
    }
  }
}

class TimeAdvancingTopologySubscriberBftOrderingIntegrationTestPostgres
    extends TimeAdvancingTopologySubscriberIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
