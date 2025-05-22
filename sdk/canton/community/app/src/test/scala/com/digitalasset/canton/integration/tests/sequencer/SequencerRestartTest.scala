// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.error.MediatorError.InvalidMessage
import com.digitalasset.canton.integration.IntegrationTestUtilities.*
import com.digitalasset.canton.integration.{CommunityIntegrationTest, TestConsoleEnvironment}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.messages.{EmptyRootHashMessagePayload, RootHashMessage}
import com.digitalasset.canton.protocol.{LocalRejectError, RootHash}
import com.digitalasset.canton.sequencing.client.SendCallback
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  MediatorGroupRecipient,
  MemberRecipient,
  Recipients,
}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import org.scalactic.source.Position
import org.scalatest.Assertion
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Minutes, Span}

import scala.concurrent.Future
import scala.concurrent.duration.*

trait SequencerRestartTest { self: CommunityIntegrationTest =>

  private val ExpectedLogs: Seq[(LogEntryOptionality, LogEntry => Assertion)] = Seq(
    (
      LogEntryOptionality.OptionalMany,
      // Those test cases may interfere with each other by sending submissions in the background.
      // Some of the submissions, which started as part of one test case, may time out as part of the next one.
      _.warningMessage should include("Submission timed out at"),
    )
  )

  protected def name: String

  protected def restartSequencers()(implicit env: TestConsoleEnvironment): Unit

  s"environment using a restartable $name sequencer" should {
    val timeout = NonNegativeDuration.tryFromDuration(4.minutes)
    "be able to restart sequencer between different activities" in { implicit env =>
      import env.*

      val time = participant1.health.ping(participant2, timeout = timeout)
      logger.info(s"P1 pings p2 in $time. Restarting.")

      loggerFactory.suppressWarningsAndErrors {
        restartSequencers()

        logger.info(s"Restarted. Performing a second ping.")
        val time2 = participant1.health.ping(participant2, timeout = timeout)
        logger.info(s"P1 pings p2 in $time2.")
      }
    }

    "be able to restart sequencer during activity" in { implicit env =>
      import env.*
      val time = participant1.health.ping(env.participant2, timeout = timeout)
      logger.info(s"P1 pings p2 in $time.")

      val p1Count = grabCounts(daName, participant1, limit = 250)

      val levels: Int = 6

      loggerFactory.suppressWarningsAndErrors {
        val restartF = Future {
          val before = poll(40.seconds, 10.milliseconds) {
            val p1Count2 = grabCounts(daName, participant1, limit = 250)
            // wait until a quarter of the levels have been dealt with
            assert(
              p1Count2.acceptedTransactionCount - p1Count.acceptedTransactionCount >= math
                .pow(2, (levels + 1d) / 4),
              s"A quarter of levels have not yet been dealt with",
            )
            // make sure the bong hasn't finished
            assert(
              p1Count2.acceptedTransactionCount - p1Count.acceptedTransactionCount <= 3 * math
                .pow(2, (levels + 1d) / 4),
              s"Bong almost finished",
            )
            p1Count2.pcsCount
          }

          logger.info(s"Restarting sequencers at pcs count $before")
          restartSequencers()
          logger.info("Successfully restarted sequencers")

          val after = grabCounts(daName, participant1, limit = 250).pcsCount
          logger.info(s"After restart, $participant1 has pcs count $after")
        }

        val bongF = Future {
          import env.*
          participant2.testing.bong(
            targets = Set(participant1.id, participant2.id),
            validators = Set(participant1.id),
            levels = levels,
            timeout = 5.minutes,
          )
        }

        val patience = defaultPatience.copy(timeout = 3.minutes)
        restartF.futureValue(patience, Position.here)

        logger.info(s"Performing a second ping, after restart")
        val time2 = participant1.health.ping(participant2, timeout = timeout)
        logger.info(s"P1 pings p2 in $time2, after restart")

        val bongDuration = bongF.futureValue(Timeout(Span(5, Minutes)))
        logger.info(s"Bong completed after $bongDuration")

        // The bong above might lead to some ongoing activity -- if the test suite finishes and
        // Canton is torn down, this might lead to the slower bong responses to still be ongoing
        // but not being able to complete cleanly. This is fine but running a ping here between
        // the participants involved in the previous bong ensures that we block here until all
        // transactions are processed and we don't cause unwanted noise during shutdown. This
        // works under the assumption that at this point all the bong responses are being
        // processed by the sequencer.
        logger.info(s"Performing a third ping, after bong")
        val time3 = participant1.health.ping(participant2, timeout = timeout)
        logger.info(s"P1 pings p2 in $time3, after bong")
      }
    }

    "restart while aggregating submissions" in { implicit env =>
      import env.*

      // We want to test that the sequencer keeps the aggregation state across restarts.
      // To that end, we generate a plain root hash message and send it as part of aggregated submissions to the mediator and one participant.
      // One part is sent before the restart, the other one after.
      // We check the delivery by looking for the alarms in the logs.
      val rhm = RootHashMessage(
        RootHash(TestHash.digest(1)),
        daId.toPhysical,
        testedProtocolVersion,
        TransactionViewType,
        CantonTimestamp.Epoch,
        EmptyRootHashMessagePayload,
      )
      val batch = Batch.of(
        testedProtocolVersion,
        rhm -> Recipients.cc(
          MediatorGroupRecipient(MediatorGroupIndex.zero),
          MemberRecipient(participant1.id),
        ),
      )
      val ts = env.environment.clock.now
      val maxTs = ts.plusSeconds(300)
      val aggregationRule = AggregationRule(
        NonEmpty(Seq, participant1.id, participant2.id),
        PositiveInt.tryCreate(2),
        testedProtocolVersion,
      )

      def sendAndWait(participant: LocalParticipantReference): Unit = {
        implicit val metricsContext: MetricsContext = MetricsContext.Empty
        val client = participant.underlying.value.sync
          .readyConnectedSynchronizerById(daId)
          .value
          .sequencerClient
        val callback = SendCallback.future
        val sendAsync = client.sendAsync(
          batch,
          topologyTimestamp = Some(ts),
          maxSequencingTime = maxTs,
          aggregationRule = Some(aggregationRule),
          callback = callback,
        )
        sendAsync.valueOrFailShutdown("Participant send").futureValue
        callback.future.onShutdown(fail("shutdown")).futureValue
      }

      clue("Sending first part of the aggregatable submission from participant 1") {
        sendAndWait(participant1)
      }

      loggerFactory.suppressWarningsAndErrors {
        clue("Restart sequencers") {
          restartSequencers()
          // Send a ping to make sure that both participants are connected again
          participant1.health.ping(participant2, timeout = timeout)
        }
      }

      val expectedLogs: Seq[(LogEntryOptionality, LogEntry => Assertion)] = ExpectedLogs ++ Seq(
        (
          LogEntryOptionality.Required,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ should include("Received no encrypted view message of type"),
          ),
        ),
        (
          LogEntryOptionality.Required,
          _.shouldBeCantonError(
            LocalRejectError.MalformedRejects.BadRootHashMessages,
            _ should include("Received no encrypted view message of type TransactionViewType"),
          ),
        ),
        (
          LogEntryOptionality.Required,
          _.shouldBeCantonError(
            InvalidMessage,
            _ should (include("Received a confirmation response") and include(
              "with an unknown request id"
            )),
          ),
        ),
      )

      loggerFactory.assertLogsUnorderedOptional(
        clue("Sending second part of the aggregatable submission from participant2") {
          sendAndWait(participant2)
        },
        expectedLogs *,
      )
    }
  }
}
