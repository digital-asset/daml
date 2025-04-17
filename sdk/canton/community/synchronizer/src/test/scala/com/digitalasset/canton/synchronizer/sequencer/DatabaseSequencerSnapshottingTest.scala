// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.{HashPurpose, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.client.RequestSigner
import com.digitalasset.canton.sequencing.protocol.{Recipients, SubmissionRequest}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.synchronizer.sequencer.store.{DbSequencerStoreTest, SequencerStore}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{MediatorId, TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.Materializer

import java.time.Duration

trait DatabaseSequencerSnapshottingTest extends SequencerApiTest with DbTest {

  def createSequencer(
      crypto: SynchronizerCryptoClient
  )(implicit materializer: Materializer): CantonSequencer =
    createSequencerWithSnapshot(None)

  val crypto = TestingIdentityFactory(
    TestingTopology(),
    loggerFactory,
    DynamicSynchronizerParameters.initialValues(clock, testedProtocolVersion),
  ).forOwnerAndSynchronizer(owner = mediatorId, synchronizerId)

  val requestSigner = RequestSigner(crypto, testedProtocolVersion, loggerFactory)

  def createSequencerWithSnapshot(
      initialState: Option[SequencerInitialState]
  )(implicit materializer: Materializer): DatabaseSequencer = {
    if (clock == null)
      clock = createClock()
    val metrics = SequencerMetrics.noop("database-sequencer-test")

    val dbConfig = TestDatabaseSequencerConfig()
    val sequencerStore = SequencerStore(
      storage,
      testedProtocolVersion,
      bufferedEventsMaxMemory = SequencerWriterConfig.DefaultBufferedEventsMaxMemory,
      bufferedEventsPreloadBatchSize = SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      sequencerMember = sequencerId,
      blockSequencerMode = false,
      cachingConfigs = CachingConfigs(),
      batchingConfig = BatchingConfig(),
    )

    DatabaseSequencer.single(
      dbConfig,
      initialState,
      DefaultProcessingTimeouts.testing,
      storage,
      sequencerStore,
      clock,
      synchronizerId,
      sequencerId,
      testedProtocolVersion,
      crypto,
      metrics,
      loggerFactory,
    )(executorService, tracer, materializer)
  }

  override protected def supportAggregation: Boolean = false

  override protected def defaultExpectedTrafficReceipt: Option[TrafficReceipt] = None

  "Database snapshotting" should {

    "allow a new separate database to be created" in { env =>
      import env.*

      val messageContent = "hello"
      val messageContent2 = "hello2"
      val sender: MediatorId = mediatorId
      val recipients = Recipients.cc(sender)

      val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)
      val request2: SubmissionRequest = createSendRequest(sender, messageContent2, recipients)

      val testSequencerWrapper =
        TestDatabaseSequencerWrapper(sequencer.asInstanceOf[DatabaseSequencer])

      for {
        signedRequest <- valueOrFail(
          requestSigner.signRequest(request, HashPurpose.SubmissionRequestSignature).failOnShutdown
        )(s"Sign request")
        _ <- valueOrFail(
          testSequencerWrapper.registerMemberInternal(sender, CantonTimestamp.Epoch).failOnShutdown
        )(
          "Register mediator"
        )
        _ <- valueOrFail(
          testSequencerWrapper
            .registerMemberInternal(sequencerId, CantonTimestamp.Epoch)
            .failOnShutdown
        )(
          "Register sequencer"
        )

        _ <- sequencer.sendAsyncSigned(signedRequest).valueOrFailShutdown("Sent async")
        messages <- readForMembers(List(sender), sequencer).failOnShutdown("readForMembers")
        _ = {
          val details = EventDetails(
            previousTimestamp = None,
            to = sender,
            messageId = Some(request.messageId),
            trafficReceipt = None,
            EnvelopeDetails(messageContent, recipients),
          )
          checkMessages(List(details), messages)
        }

        error <- sequencer
          .snapshot(CantonTimestamp.MaxValue)
          .failOnShutdown
          .leftOrFail("snapshotting after the watermark is expected to fail")
        _ <- error.cause should include(" is after the safe watermark")

        // Note: below we use the timestamp that is currently the safe watermark in the sequencer
        snapshot <- valueOrFail(sequencer.snapshot(CantonTimestamp.Epoch.immediateSuccessor))(
          "get snapshot"
        )

        _ = {
          // This makes DBS start with the clock.now timestamp being set to watermark
          // and allow taking snapshot2 without triggering the check of the watermark
          clock.asInstanceOf[SimClock].advanceTo(CantonTimestamp.Epoch.immediateSuccessor)
        }

        _ <- cleanDb(storage) // we clean the DB to simulate a new separate database

        // create a second separate sequencer from the snapshot
        secondSequencer = createSequencerWithSnapshot(
          Some(
            SequencerInitialState(
              synchronizerId,
              snapshot,
              latestSequencerEventTimestamp = None,
              initialTopologyEffectiveTimestamp = None,
            )
          )
        )

        // the snapshot from the second sequencer should look the same except that the lastTs will become the lower bound
        snapshot2 <- valueOrFail(
          secondSequencer.snapshot(CantonTimestamp.Epoch.immediateSuccessor)
        )("get snapshot")
        _ = {
          snapshot2 shouldBe (snapshot.copy(status =
            snapshot.status.copy(lowerBound = snapshot.lastTs)
          )(snapshot.representativeProtocolVersion))
        }

        signedRequest2 <- valueOrFail(
          requestSigner.signRequest(request2, HashPurpose.SubmissionRequestSignature)
        )(s"Sign request")
        _ <- {
          // need to advance clock so that the new event doesn't get the same timestamp as the previous one,
          // which would then cause it to be ignored on the read path
          simClockOrFail(clock).advance(Duration.ofSeconds(1))
          secondSequencer.sendAsyncSigned(signedRequest2).value
        }

        messages2 <- readForMembers(
          List(sender),
          secondSequencer,
          startTimestamp = firstEventTimestamp(sender)(messages).map(_.immediateSuccessor),
        )

      } yield {
        // the second sequencer (started from snapshot) is able to continue operating and create new messages
        val details2 = EventDetails(
          previousTimestamp = messages.headOption.map(_._2.timestamp),
          to = sender,
          messageId = Some(request2.messageId),
          trafficReceipt = None,
          EnvelopeDetails(messageContent2, recipients),
        )
        checkMessages(List(details2), messages2)

        secondSequencer.close()

        succeed
      }
    }
  }

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    DbSequencerStoreTest.cleanSequencerTables(storage)
}

class DatabaseSequencerSnapshottingTestPostgres
    extends DatabaseSequencerSnapshottingTest
    with PostgresTest

class DatabaseSequencerSnapshottingTestH2 extends DatabaseSequencerSnapshottingTest with H2Test
