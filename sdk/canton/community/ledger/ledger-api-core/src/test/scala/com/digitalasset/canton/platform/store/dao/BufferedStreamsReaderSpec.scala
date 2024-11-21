// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.data.AbsoluteOffset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReaderSpec.*
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, HasExecutorServiceGeneric}
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.{Done, NotUsed}
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success
import scala.util.chaining.*

class BufferedStreamsReaderSpec
    extends AnyWordSpec
    with Matchers
    with PekkoBeforeAndAfterAll
    with TestFixtures
    with HasExecutionContext {

  "stream (static)" when {
    "buffer filter" should {
      "return filtered elements (Inclusive slice)" in new StaticTestScope {
        run(
          transactionsBuffer = inMemoryFanoutBuffer,
          startInclusive = offset2,
          endInclusive = offset3,
          bufferSliceFilter = noFilterBufferSlice(_).filterNot(_.updateId == "tx-3"),
        )
        streamElements should contain theSameElementsInOrderAs Seq(
          offset2 -> "tx-2"
        )
      }
    }

    "request within buffer range inclusive" should {
      "fetch from buffer" in new StaticTestScope {
        run(
          transactionsBuffer = inMemoryFanoutBuffer,
          startInclusive = offset2,
          endInclusive = offset3,
        )
        streamElements should contain theSameElementsInOrderAs Seq(
          offset2 -> "tx-2",
          offset3 -> "tx-3",
        )
      }
    }

    "request within buffer range inclusive (multiple chunks)" should {
      "correctly fetch from buffer" in new StaticTestScope {
        run(
          transactionsBuffer = inMemoryFanoutBufferWithSmallChunkSize,
          startInclusive = offset2,
          endInclusive = offset3,
        )

        streamElements should contain theSameElementsInOrderAs Seq(
          offset2 -> "tx-2",
          offset3 -> "tx-3",
        )
      }
    }

    "request before buffer start" should {
      "fetch from buffer and storage" in new StaticTestScope {
        val filterMock = new Object

        val anotherResponseForOffset1 = "(1) Response fetched from storage"
        val anotherResponseForOffset2 = "(2) Response fetched from storage"

        val fetchFromPersistence = buildFetchFromPersistence(
          expectedStartInclusive = offset0,
          expectedEndInclusive = offset2,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(
            Seq(offset1 -> anotherResponseForOffset1, offset2 -> anotherResponseForOffset2)
          ),
        )

        run(
          transactionsBuffer = inMemoryFanoutBufferWithSmallChunkSize,
          startInclusive = offset0,
          endInclusive = offset3,
          fetchFromPersistence = fetchFromPersistence,
          persistenceFetchArgs = filterMock,
          bufferSliceFilter = noFilterBufferSlice,
        )

        streamElements should contain theSameElementsInOrderAs Seq(
          offset1 -> anotherResponseForOffset1,
          offset2 -> anotherResponseForOffset2,
          offset3 -> "tx-3",
        )
      }

      "fetch from buffer and storage chunked with buffer filter" in new StaticTestScope {
        val filterMock = new Object

        val anotherResponseForOffset1 = "(1) Response fetched from storage"

        val fetchFromPersistence = buildFetchFromPersistence(
          expectedStartInclusive = offset0,
          expectedEndInclusive = offset1,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(Seq(offset1 -> anotherResponseForOffset1)),
        )

        run(
          startInclusive = offset0,
          endInclusive = offset3,
          fetchFromPersistence = fetchFromPersistence,
          persistenceFetchArgs = filterMock,
          bufferSliceFilter = noFilterBufferSlice(_).filterNot(_.updateId == "tx-3"),
        )

        streamElements should contain theSameElementsInOrderAs Seq(
          offset1 -> anotherResponseForOffset1,
          offset2 -> "tx-2",
        )
      }
    }

    "request before buffer bounds" should {
      "fetch only from storage" in new StaticTestScope {
        val filterMock = new Object

        val fetchedElements = Vector(
          offset1 -> "Some API response from persistence",
          offset2 -> "Another API response from persistence",
        )

        val fetchFromPersistence = buildFetchFromPersistence(
          expectedStartInclusive = offset1,
          expectedEndInclusive = offset2,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(fetchedElements),
        )

        run(
          transactionsBuffer = smallInMemoryFanoutBuffer,
          startInclusive = offset1,
          endInclusive = offset2,
          fetchFromPersistence = fetchFromPersistence,
          persistenceFetchArgs = filterMock,
        )

        streamElements should contain theSameElementsInOrderAs fetchedElements
      }
    }
  }

  "stream (dynamic)" when {
    "catching up from buffer begin (exclusive)" should {
      "return the correct ranges" in new DynamicTestScope() {
        runF(
          for {
            // Prepopulate stores
            _ <- updateStores(maxBufferSize)
            // Stream from the beginning and assert
            _ <- stream(1, maxBufferSize)
          } yield succeed
        )
      }
    }

    "catching up from buffer (inclusive)" should {
      "return the correct ranges" in new DynamicTestScope() {
        runF(
          for {
            // Prepopulate stores
            _ <- updateStores(maxBufferSize)
            // Stream from the middle and assert
            _ <- stream(maxBufferSize / 2 + 1, maxBufferSize)
          } yield succeed
        )
      }
    }

    def testConsumerFallingBehind(
        bufferSize: Int,
        bufferChunkSize: Int,
        consumerSubscriptionFrom: Int,
        updateAgainWithCount: Int,
    ) = new DynamicTestScope(maxBufferSize = bufferSize, maxBufferChunkSize = bufferChunkSize) {
      runF(
        for {
          // Prepopulate stores
          _ <- updateStores(count = bufferSize)
          // Start stream subscription
          (assertFirst1000, unblockConsumer) = streamWithHandle(
            startInclusiveIdx = consumerSubscriptionFrom,
            endInclusiveIdx = bufferSize,
          )
          // Feed the buffer and effectively force the consumer to fall behind
          _ <- updateStores(count = updateAgainWithCount)
          _ = unblockConsumer()
          _ <- assertFirst1000
        } yield succeed
      )
    }

    val bufferSize = 100
    val bufferChunkSize = 10

    "falling completely behind" should {
      "return the correct ranges when starting from the beginning" in {
        testConsumerFallingBehind(
          bufferSize = bufferSize,
          bufferChunkSize = bufferChunkSize,
          consumerSubscriptionFrom = 1,
          updateAgainWithCount = bufferSize,
        )
      }

      "return the correct ranges when starting from an offset originally in the buffer at subscription time" in {
        testConsumerFallingBehind(
          bufferSize = bufferSize,
          bufferChunkSize = bufferChunkSize,
          consumerSubscriptionFrom = bufferSize / 2 + 1,
          updateAgainWithCount = bufferSize,
        )
      }
    }
  }
}

object BufferedStreamsReaderSpec {

  trait TestFixtures
      extends Matchers
      with ScalaFutures
      with BaseTest
      with HasExecutorServiceGeneric { self: PekkoBeforeAndAfterAll =>

    implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

    implicit val ec: ExecutionContext = executorService

    val metrics = LedgerApiServerMetrics.ForTesting
    val Seq(offset0, offset1, offset2, offset3) =
      (0 to 3) map { idx => absoluteOffset(idx.toLong) }: @nowarn("msg=match may not be exhaustive")
    val offsetUpdates: Seq[TransactionLogUpdate.TransactionAccepted] =
      (1L to 3L).map(transaction)

    val noFilterBufferSlice
        : TransactionLogUpdate => Option[TransactionLogUpdate.TransactionAccepted] =
      tracedUpdate =>
        tracedUpdate match {
          case update: TransactionLogUpdate.TransactionAccepted => Some(update)
          case _ => None
        }

    val inMemoryFanoutBuffer: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 3,
      metrics = metrics,
      maxBufferedChunkSize = 3,
      loggerFactory = loggerFactory,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(inMemoryFanoutBuffer.push))

    val inMemoryFanoutBufferWithSmallChunkSize: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 3,
      metrics = metrics,
      maxBufferedChunkSize = 1,
      loggerFactory = loggerFactory,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(inMemoryFanoutBuffer.push))

    val smallInMemoryFanoutBuffer: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 1,
      metrics = metrics,
      maxBufferedChunkSize = 1,
      loggerFactory = loggerFactory,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(inMemoryFanoutBuffer.push))

    trait StaticTestScope {

      val streamElements: ArrayBuffer[(AbsoluteOffset, String)] =
        ArrayBuffer.empty[(AbsoluteOffset, String)]

      private val failingPersistenceFetch = new FetchFromPersistence[Object, String] {
        override def apply(
            startInclusive: AbsoluteOffset,
            endInclusive: AbsoluteOffset,
            filter: Object,
        )(implicit
            loggingContext: LoggingContextWithTrace
        ): Source[(AbsoluteOffset, String), NotUsed] = fail(
          "Unexpected call to fetch from persistence"
        )
      }

      def run(
          startInclusive: AbsoluteOffset,
          endInclusive: AbsoluteOffset,
          transactionsBuffer: InMemoryFanoutBuffer = inMemoryFanoutBufferWithSmallChunkSize,
          fetchFromPersistence: FetchFromPersistence[Object, String] = failingPersistenceFetch,
          persistenceFetchArgs: Object = new Object,
          bufferSliceFilter: TransactionLogUpdate => Option[
            TransactionLogUpdate.TransactionAccepted
          ] = noFilterBufferSlice,
      ): Done =
        new BufferedStreamsReader[Object, String](
          inMemoryFanoutBuffer = transactionsBuffer,
          fetchFromPersistence = fetchFromPersistence,
          bufferedStreamEventsProcessingParallelism = 2,
          metrics = metrics,
          streamName = "some_tx_stream",
          loggerFactory,
        )(executorService)
          .stream[TransactionLogUpdate.TransactionAccepted](
            startInclusive = startInclusive,
            endInclusive = endInclusive,
            persistenceFetchArgs = persistenceFetchArgs,
            bufferFilter = bufferSliceFilter,
            toApiResponse = tx => Future.successful(tx.updateId),
          )
          .runWith(Sink.foreach(streamElements.addOne))
          .futureValue

      def buildFetchFromPersistence(
          expectedStartInclusive: AbsoluteOffset,
          expectedEndInclusive: AbsoluteOffset,
          expectedFilter: Object,
          thenReturnStream: Source[(AbsoluteOffset, String), NotUsed],
      ): FetchFromPersistence[Object, String] =
        new FetchFromPersistence[Object, String] {
          override def apply(
              startInclusive: AbsoluteOffset,
              endInclusive: AbsoluteOffset,
              filter: Object,
          )(implicit
              loggingContext: LoggingContextWithTrace
          ): Source[(AbsoluteOffset, String), NotUsed] =
            (startInclusive, endInclusive, filter) match {
              case (`expectedStartInclusive`, `expectedEndInclusive`, `expectedFilter`) =>
                thenReturnStream
              case unexpected =>
                fail(s"Unexpected fetch transactions subscription start: $unexpected")
            }
        }
    }

    class DynamicTestScope(val maxBufferSize: Int = 100, val maxBufferChunkSize: Int = 10) {
      @volatile private var persistenceStore =
        Vector.empty[(AbsoluteOffset, TransactionLogUpdate.TransactionAccepted)]
      @volatile private var ledgerEndIndex = 0L
      private val inMemoryFanoutBuffer =
        new InMemoryFanoutBuffer(maxBufferSize, metrics, maxBufferChunkSize, loggerFactory)

      private val fetchFromPersistence = new FetchFromPersistence[Object, String] {
        override def apply(
            startInclusive: AbsoluteOffset,
            endInclusive: AbsoluteOffset,
            filter: Object,
        )(implicit
            loggingContext: LoggingContextWithTrace
        ): Source[(AbsoluteOffset, String), NotUsed] =
          if (startInclusive > endInclusive) fail("startExclusive after endInclusive")
          else if (endInclusive > absoluteOffset(ledgerEndIndex))
            fail("endInclusive after ledgerEnd")
          else
            persistenceStore
              .dropWhile(_._1 < startInclusive)
              .takeWhile(_._1 <= endInclusive)
              .map { case (o, tx) => o -> tx.updateId }
              .pipe(Source(_))
      }

      private val streamReader = new BufferedStreamsReader[Object, String](
        inMemoryFanoutBuffer = inMemoryFanoutBuffer,
        fetchFromPersistence = fetchFromPersistence,
        bufferedStreamEventsProcessingParallelism = 2,
        metrics = metrics,
        streamName = "some_tx_stream",
        loggerFactory,
      )

      def updateStores(count: Int): Future[Done] = {
        val (done, handle) = {
          val blockingPromise = Promise[Unit]()
          val unblockHandle: () => Unit = () => blockingPromise.complete(Success(()))

          val done = Source
            .fromIterator(() => (ledgerEndIndex + 1L to count + ledgerEndIndex).iterator)
            .async
            .mapAsync(1) { idx =>
              blockingPromise.future.map(_ => idx)
            }
            .async
            .runForeach(updateFixtures)

          done -> unblockHandle
        }
        handle()
        done
      }

      def stream(
          startInclusiveIdx: Int,
          endInclusiveIdx: Int,
      ): Future[Assertion] = {
        val (done, handle) = streamWithHandle(startInclusiveIdx, endInclusiveIdx)
        handle()
        done
      }

      def streamWithHandle(
          startInclusiveIdx: Int,
          endInclusiveIdx: Int,
      ): (Future[Assertion], () => Unit) = {
        val blockingPromise = Promise[Unit]()
        val unblockHandle: () => Unit = () => blockingPromise.complete(Success(()))

        val assertReadStream = streamReader
          .stream[TransactionLogUpdate.TransactionAccepted](
            startInclusive = absoluteOffset(startInclusiveIdx.toLong),
            endInclusive = absoluteOffset(endInclusiveIdx.toLong),
            persistenceFetchArgs = new Object, // Not used
            bufferFilter = noFilterBufferSlice, // Do not filter
            toApiResponse = tx => Future.successful(tx.updateId),
          )
          .async
          .mapAsync(1) { idx =>
            blockingPromise.future.map(_ => idx)
          }
          .async
          .runWith(Sink.seq)
          .map { result =>
            withClue(s"[$startInclusiveIdx, $endInclusiveIdx]") {
              result.size shouldBe endInclusiveIdx - startInclusiveIdx + 1
            }
            val expectedElements = (startInclusiveIdx.toLong to endInclusiveIdx.toLong) map { idx =>
              absoluteOffset(idx) -> s"tx-$idx"
            }
            result should contain theSameElementsInOrderAs expectedElements
          }

        assertReadStream -> unblockHandle
      }

      def runF(f: => Future[Assertion]): Assertion =
        f.futureValue

      private def updateFixtures(idx: Long): Unit = {
        val offsetAt = absoluteOffset(idx)
        val tx = transaction(idx)
        persistenceStore = persistenceStore.appended(offsetAt -> tx)
        inMemoryFanoutBuffer.push(tx)
        ledgerEndIndex = idx
      }
    }
  }

  private val someDomainId = DomainId.tryFromString("some::domain-id")

  private def transaction(i: Long) =
    TransactionLogUpdate.TransactionAccepted(
      updateId = s"tx-$i",
      commandId = "",
      workflowId = "",
      effectiveAt = Timestamp.Epoch,
      offset = absoluteOffset(i),
      events = Vector(null),
      completionStreamResponse = None,
      domainId = someDomainId.toProtoPrimitive,
      recordTime = Timestamp.Epoch,
    )(TraceContext.empty)

  private def absoluteOffset(idx: Long): AbsoluteOffset = {
    val base = 1000000000L
    AbsoluteOffset.tryFromLong(base + idx)
  }
}
