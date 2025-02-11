// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
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
}
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
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

  private val requestTime: CantonTimestamp = CantonTimestamp.Epoch
  private val requestTime2: CantonTimestamp = requestTime.plusSeconds(1)
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

    val deduplicator = new DefaultMediatorEventDeduplicator(
      store,
      finalizedResponseStore,
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

  private lazy val uuids: Seq[UUID] = List(
    "51f3ffff-9248-453b-807b-91dd7ed23298",
    "c0175d4a-def2-481e-a979-ae9d335b5d35",
    "b9f66e2a-4867-465e-b51f-c727f2d0a18f",
  ).map(UUID.fromString)

  private lazy val request: Seq[OpenEnvelope[MediatorConfirmationRequest]] =
    uuids.map(mkMediatorRequest)

  private def requests(is: Int*): Seq[OpenEnvelope[MediatorConfirmationRequest]] = is.map(request)

  private def deduplicationData(iAndTime: (Int, CantonTimestamp)*): Set[DeduplicationData] =
    iAndTime.map { case (i, requestTime) =>
      DeduplicationData(uuids(i), requestTime, requestTime plus deduplicationTimeout)
    }.toSet

  private def deduplicationData(requestTime: CantonTimestamp, is: Int*): Set[DeduplicationData] =
    deduplicationData(is.map(_ -> requestTime)*)

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

  private lazy val response: DefaultOpenEnvelope = {
    val message = SignedProtocolMessage(
      mock[TypedSignedProtocolMessageContent[ConfirmationResponse]],
      NonEmpty(Seq, SymbolicCrypto.emptySignature),
      testedProtocolVersion,
    )
    mkDefaultOpenEnvelope(message)
  }

  private def assertNextSentVerdict(
      verdictSender: TestVerdictSender,
      requestTime: CantonTimestamp = this.requestTime,
      expireAfter: CantonTimestamp = this.requestTime.plus(deduplicationTimeout),
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

      val (uniqueEvents, storeF) =
        deduplicator.rejectDuplicates(requestTime, requests(0, 1, 2)).futureValueUS
      uniqueEvents shouldBe requests(0, 1, 2)

      store.allData() shouldBe deduplicationData(requestTime, 0, 1, 2)

      storeF.futureValueUS
      verdictSender.sentResults shouldBe empty
    }

    "accept non-requests" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val envelopes = Seq(response)

      val (uniqueEvents, storeF) =
        deduplicator.rejectDuplicates(requestTime, envelopes).futureValueUS
      uniqueEvents shouldBe envelopes

      store.allData() shouldBe empty

      storeF.futureValueUS
      verdictSender.sentResults shouldBe empty
    }

    "reject duplicates in same batch" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (uniqueEvents, storeF) = loggerFactory.assertLogs(
        deduplicator.rejectDuplicates(requestTime, requests(0, 1, 0)).futureValueUS,
        entry => {
          entry.shouldBeCantonErrorCode(MediatorError.MalformedMessage)
          entry.warningMessage should include(
            s"The request uuid (${uuids(0)}) must not be used until ${requestTime.plus(deduplicationTimeout)}."
          )
        },
      )
      uniqueEvents shouldBe requests(0, 1)

      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF.futureValueUS
      assertNextSentVerdict(verdictSender)(request(0))
      verdictSender.sentResults shouldBe empty
    }

    "reject duplicates across batches" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      // populate the store
      val (uniqueEvents, storeF1) =
        deduplicator.rejectDuplicates(requestTime, requests(0, 1)).futureValueUS
      uniqueEvents shouldBe requests(0, 1)
      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF1.futureValueUS
      verdictSender.sentResults shouldBe empty

      // submit same event with same requestTime
      // This should not occur in production, as the sequencer creates unique timestamps and
      // the deduplication state is cleaned up during initialization.
      val (uniqueEvents2, storeF2) = loggerFactory.assertLogs(
        deduplicator.rejectDuplicates(requestTime, requests(0)).futureValueUS,
        _.shouldBeCantonErrorCode(MediatorError.MalformedMessage),
      )
      uniqueEvents2 shouldBe Seq.empty

      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF2.futureValueUS
      assertNextSentVerdict(verdictSender)(request(0))
      verdictSender.sentResults shouldBe empty

      // submit same event with increased requestTime
      val (uniqueEvents3, storeF3) = loggerFactory.assertLogs(
        deduplicator.rejectDuplicates(requestTime2, requests(0)).futureValueUS,
        _.shouldBeCantonErrorCode(MediatorError.MalformedMessage),
      )
      uniqueEvents3 shouldBe Seq.empty

      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF3.futureValueUS
      assertNextSentVerdict(verdictSender, requestTime2)(request(0))
      verdictSender.sentResults shouldBe empty
    }

    "filter out duplicate requests" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (uniqueEvents, storeF1) =
        deduplicator.rejectDuplicates(requestTime, requests(0, 1)).futureValueUS
      uniqueEvents shouldBe requests(0, 1)
      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF1.futureValueUS
      verdictSender.sentResults shouldBe empty

      val (uniqueEvents2, storeF2) = loggerFactory.assertLogs(
        deduplicator
          .rejectDuplicates(
            requestTime2,
            Seq(
              response,
              request(0),
              request(2),
              request(0),
              response,
              request(1),
            ),
          )
          .futureValueUS,
        _.shouldBeCantonErrorCode(MediatorError.MalformedMessage),
        _.shouldBeCantonErrorCode(MediatorError.MalformedMessage),
        _.shouldBeCantonErrorCode(MediatorError.MalformedMessage),
      )
      uniqueEvents2 shouldBe Seq(response, request(2), response)
      store
        .allData() shouldBe deduplicationData(0 -> requestTime, 1 -> requestTime, 2 -> requestTime2)

      storeF2.futureValueUS

      assertNextSentVerdict(verdictSender, requestTime2)(request(0), request(0), request(1))
      verdictSender.sentResults shouldBe empty
    }

    "allow for reusing uuids after expiration time" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (uniqueEvents, storeF1) =
        deduplicator.rejectDuplicates(requestTime, requests(0, 1)).futureValueUS
      uniqueEvents shouldBe requests(0, 1)
      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF1.futureValueUS
      verdictSender.sentResults shouldBe empty

      val expireAfter = requestTime.plus(deduplicationTimeout).immediateSuccessor
      val (uniqueEvents2, storeF2) = deduplicator
        .rejectDuplicates(
          expireAfter,
          requests(0),
        )
        .futureValueUS
      uniqueEvents2 shouldBe requests(0)
      store.allData() shouldBe deduplicationData(
        0 -> requestTime,
        1 -> requestTime,
        0 -> expireAfter,
      )

      storeF2.futureValueUS
      verdictSender.sentResults shouldBe empty
    }

    "handle concurrent requests in the right order" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      forAll(request.indices) { i =>
        val (uniqueEvents1, storeF1) =
          deduplicator.rejectDuplicates(requestTime, requests(i)).futureValueUS
        val (uniqueEvents2, storeF2) = loggerFactory.suppressWarningsAndErrors(
          deduplicator.rejectDuplicates(requestTime2, requests(i)).futureValueUS
        )

        uniqueEvents1 shouldBe requests(i)
        uniqueEvents2 shouldBe empty

        store.findUuid(uuids(i), requestTime) shouldBe deduplicationData(requestTime, i)

        storeF1.futureValueUS
        storeF2.futureValueUS
        assertNextSentVerdict(verdictSender, requestTime = requestTime2)(request(i))
      }
    }

    "correctly propagate completion of asynchronous actions" in {
      val deduplicator = mkHangingDeduplicator()

      val (uniqueEvents1, storeF1) =
        deduplicator.rejectDuplicates(requestTime, requests(0)).futureValueUS

      val (uniqueEvents2, storeF2) = loggerFactory.suppressWarningsAndErrors(
        deduplicator.rejectDuplicates(requestTime, requests(0)).futureValueUS
      )

      uniqueEvents1 shouldBe requests(0)
      uniqueEvents2 shouldBe empty

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
      store,
      finalizedResponseStore,
      verdictSender,
      _ => FutureUnlessShutdown.pure(deduplicationTimeout),
      _ => FutureUnlessShutdown.pure(decisionTime),
      testedProtocolVersion,
      loggerFactory,
    )
  }
}
