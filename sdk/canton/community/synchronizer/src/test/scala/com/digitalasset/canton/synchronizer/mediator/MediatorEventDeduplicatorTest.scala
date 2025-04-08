// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.syntax.alternative.*
import cats.syntax.foldable.*
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.mediator.store.MediatorDeduplicationStore.DeduplicationData
import com.digitalasset.canton.synchronizer.mediator.store.{
  FinalizedResponseStore,
  InMemoryFinalizedResponseStore,
  InMemoryMediatorDeduplicationStore,
  MediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorTestMetrics
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{DelayUtil, MonadUtil}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.scalatest.Assertion

import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random

import TestVerdictSender.Result

class MediatorEventDeduplicatorTest
    extends BaseTestWordSpec
    with HasExecutionContext
    with HasTestCloseContext {

  private val deduplicationTimeout: Duration = Duration.ofSeconds(10)
  private val decisionTime: CantonTimestamp = CantonTimestamp.ofEpochSecond(100)

  private val maxDelayMillis: Int = 10

  private def mkDeduplicator()
      : (MediatorEventDeduplicator, TestVerdictSender, MediatorDeduplicationStore) = {
    val store: MediatorDeduplicationStore =
      new InMemoryMediatorDeduplicationStore(loggerFactory, timeouts)
    store.initialize(CantonTimestamp.MinValue).futureValueUS

    val finalizedResponseStore: FinalizedResponseStore =
      new InMemoryFinalizedResponseStore(loggerFactory)

    val verdictSender =
      new TestVerdictSender(null, daMediator, null, testedProtocolVersion, loggerFactory)

    val state = new MediatorState(
      finalizedResponseStore,
      store,
      wallClock,
      MediatorTestMetrics,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )
    state.initialize(CantonTimestamp.MinValue).futureValueUS

    val deduplicator = new DefaultMediatorEventDeduplicator(
      state,
      verdictSender,
      _ => FutureUnlessShutdown.outcomeF(delayed(deduplicationTimeout)),
      _ => FutureUnlessShutdown.outcomeF(delayed(decisionTime)),
      testedProtocolVersion,
      loggerFactory,
    )
    (deduplicator, verdictSender, store)
  }

  def delayed[A](value: A): Future[A] = {
    val duration = Random.nextInt(maxDelayMillis + 1)
    DelayUtil.delay(duration.millis).map(_ => value)
  }

  private def ts(i: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(i.toLong)

  private lazy val uuids: Seq[UUID] = List(
    "51f3ffff-9248-453b-807b-91dd7ed23298",
    "c0175d4a-def2-481e-a979-ae9d335b5d35",
    "b9f66e2a-4867-465e-b51f-c727f2d0a18f",
  ).map(UUID.fromString)

  private lazy val request: Seq[OpenEnvelope[MediatorConfirmationRequest]] =
    uuids.map(mkMediatorRequest)

  private def deduplicateRequests(
      deduplicator: MediatorEventDeduplicator,
      startingTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
  )(is: Int*): (Seq[Boolean], FutureUnlessShutdown[Unit]) = {
    val (isUnique, storeFs) =
      MonadUtil
        .sequentialTraverse(requests(startingTimestamp, is*))(
          (deduplicator.rejectDuplicate _).tupled
        )
        .futureValueUS
        .separate
    (isUnique, storeFs.sequence_)
  }

  private def requests(
      startingTime: CantonTimestamp,
      is: Int*
  ): Seq[(CantonTimestamp, MediatorConfirmationRequest, Seq[DefaultOpenEnvelope])] =
    is.map(idx =>
      (startingTime.plusSeconds(idx.toLong), request(idx).protocolMessage, Seq(request(idx)))
    )

  private def deduplicationDataWithTs(iAndTime: (Int, CantonTimestamp)*): Set[DeduplicationData] =
    iAndTime.map { case (i, requestTime) =>
      DeduplicationData(uuids(i), requestTime, requestTime plus deduplicationTimeout)
    }.toSet

  private def deduplicationData(is: Int*): Set[DeduplicationData] =
    deduplicationDataWithTs(is.map(i => i -> ts(i))*)

  private def mkMediatorRequest(uuid: UUID): OpenEnvelope[MediatorConfirmationRequest] = {
    import Pretty.*

    val mediatorRequest = mock[MediatorConfirmationRequest]
    when(mediatorRequest.requestUuid).thenReturn(uuid)
    when(mediatorRequest.pretty).thenReturn(
      prettyOfClass[MediatorConfirmationRequest](param("uuid", _.requestUuid))
    )

    mkDefaultOpenEnvelope(mediatorRequest)
  }

  private def mkDefaultOpenEnvelope[A <: ProtocolMessage](protocolMessage: A): OpenEnvelope[A] =
    OpenEnvelope(protocolMessage, Recipients.cc(daMediator))(testedProtocolVersion)

  private def assertNextSentVerdict(
      verdictSender: TestVerdictSender,
      requestTime: CantonTimestamp = ts(0),
      expireAfter: CantonTimestamp = ts(0).plus(deduplicationTimeout),
  )(envelopes: OpenEnvelope[MediatorConfirmationRequest]*): Assertion = {
    val rejects = envelopes.map { envelope =>
      val request = envelope.protocolMessage
      val reject = MediatorVerdict.MediatorReject(
        MediatorError.MalformedMessage.Reject(
          s"The request uuid (${request.requestUuid}) must not be used until $expireAfter."
        )
      )
      Result(
        RequestId(requestTime),
        decisionTime,
        Some(request),
        Some(reject.toVerdict(testedProtocolVersion)),
      )

    }

    val results =
      List.fill(envelopes.size)(verdictSender.sentResultsQueue.poll(0, TimeUnit.SECONDS))
    results should contain theSameElementsAs rejects

  }

  "The event deduplicator" should {
    "accept events with unique uuids" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (isUnique, storeF) = deduplicateRequests(deduplicator)(0, 1, 2)
      isUnique shouldBe Seq(true, true, true)

      store.allData() shouldBe deduplicationData(0, 1, 2)

      storeF.futureValueUS
      verdictSender.sentResults shouldBe empty
    }

    "reject duplicates across submissions" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      // populate the store
      val (isUnique, storeF1) = deduplicateRequests(deduplicator)(0, 1)
      isUnique shouldBe Seq(true, true)
      store.allData() shouldBe deduplicationData(0, 1)

      storeF1.futureValueUS
      verdictSender.sentResults shouldBe empty

      // submit same event with same requestTime
      // This should not occur in production, as the sequencer creates unique timestamps and
      // the deduplication state is cleaned up during initialization.
      val (isUnique2, storeF2) = loggerFactory.assertLogs(
        deduplicateRequests(deduplicator)(0),
        _.shouldBeCantonErrorCode(MediatorError.MalformedMessage),
      )
      isUnique2 shouldBe Seq(false)

      store.allData() shouldBe deduplicationData(0, 1)

      storeF2.futureValueUS
      assertNextSentVerdict(verdictSender)(request(0))
      verdictSender.sentResults shouldBe empty

      // submit same event with increased requestTime
      val (isUnique3, storeF3) = loggerFactory.assertLogs(
        deduplicateRequests(deduplicator, ts(2))(0),
        _.shouldBeCantonErrorCode(MediatorError.MalformedMessage),
      )
      isUnique3 shouldBe Seq(false)

      store.allData() shouldBe deduplicationData(0, 1)

      storeF3.futureValueUS
      assertNextSentVerdict(verdictSender, ts(2))(request(0))
      verdictSender.sentResults shouldBe empty
    }

    "allow for reusing uuids after expiration time" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (uniqueEvents, storeF1) = deduplicateRequests(deduplicator)(0, 1)
      uniqueEvents shouldBe Seq(true, true)
      store.allData() shouldBe deduplicationData(0, 1)

      storeF1.futureValueUS
      verdictSender.sentResults shouldBe empty

      val expireAfter = ts(0).plus(deduplicationTimeout).immediateSuccessor
      val (isUnique2, storeF2) = deduplicateRequests(deduplicator, expireAfter)(0)
      isUnique2 shouldBe Seq(true)

      store.allData() shouldBe deduplicationDataWithTs(
        0 -> ts(0),
        1 -> ts(1),
        0 -> expireAfter,
      )

      storeF2.futureValueUS
      verdictSender.sentResults shouldBe empty
    }

    "handle concurrent requests in the right order" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      forAll(request.indices) { i =>
        val (isUnique1, storeF1) = deduplicateRequests(deduplicator)(i)
        val requestTime = ts(request.size + 1)
        val (isUnique2, storeF2) = loggerFactory.suppressWarningsAndErrors(
          deduplicateRequests(deduplicator, requestTime)(i)
        )

        isUnique1 shouldBe Seq(true)
        isUnique2 shouldBe Seq(false)

        store.findUuid(uuids(i), ts(i)) shouldBe deduplicationData(i)

        storeF1.futureValueUS
        storeF2.futureValueUS
        assertNextSentVerdict(verdictSender, requestTime = requestTime.plusSeconds(i.toLong))(
          request(i)
        )
      }
    }

    "correctly propagate completion of asynchronous actions" in {
      val deduplicator = mkHangingDeduplicator()

      val (isUnique1, storeF1) = deduplicateRequests(deduplicator)(0)

      val (isUnique2, storeF2) = loggerFactory.suppressWarningsAndErrors(
        deduplicateRequests(deduplicator)(0)
      )

      isUnique1 shouldBe Seq(true)
      isUnique2 shouldBe Seq(false)

      always(durationOfSuccess = 1.second) {
        storeF1 should not be Symbol("completed")
        storeF2 should not be Symbol("completed")
      }
    }
  }

  def mkHangingDeduplicator(): MediatorEventDeduplicator = {
    val store = new MediatorDeduplicationStore {
      override protected def loggerFactory: NamedLoggerFactory =
        MediatorEventDeduplicatorTest.this.loggerFactory

      override protected def doInitialize(deleteFromInclusive: CantonTimestamp)(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

      override protected def persist(data: DeduplicationData)(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.never

      override protected def prunePersistentData(
          upToInclusive: CantonTimestamp
      )(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

      override protected val timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    }
    store.initialize(CantonTimestamp.MinValue).futureValueUS

    val finalizedResponseStore: FinalizedResponseStore =
      new InMemoryFinalizedResponseStore(loggerFactory)

    val state = new MediatorState(
      finalizedResponseStore,
      store,
      wallClock,
      MediatorTestMetrics,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )

    state.initialize(CantonTimestamp.MinValue).futureValueUS

    val verdictSender = new VerdictSender {
      override def sendResult(
          requestId: RequestId,
          request: MediatorConfirmationRequest,
          verdict: Verdict,
          decisionTime: CantonTimestamp,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        FutureUnlessShutdown.never

      override def sendResultBatch(
          requestId: RequestId,
          batch: Batch[DefaultOpenEnvelope],
          decisionTime: CantonTimestamp,
          aggregationRule: Option[AggregationRule],
          sendVerdict: Boolean,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        FutureUnlessShutdown.never

      override def sendReject(
          requestId: RequestId,
          requestO: Option[MediatorConfirmationRequest],
          rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
          rejectionReason: Verdict.MediatorReject,
          decisionTime: CantonTimestamp,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        FutureUnlessShutdown.never
    }

    new DefaultMediatorEventDeduplicator(
      state,
      verdictSender,
      _ => FutureUnlessShutdown.pure(deduplicationTimeout),
      _ => FutureUnlessShutdown.pure(decisionTime),
      testedProtocolVersion,
      loggerFactory,
    )
  }
}
