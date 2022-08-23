// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.InMemoryFanoutBuffer
import com.daml.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.daml.platform.store.dao.BufferedStreamsReaderSpec._
import com.daml.platform.store.interfaces.TransactionLogUpdate
import org.scalatest.Assertion
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.Success
import scala.util.chaining._

class BufferedStreamsReaderSpec
    extends AnyWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with TestFixtures {

  "stream (static)" when {
    "buffer filter" should {
      "return filtered elements (Inclusive slice)" in new StaticTestScope {
        run(
          transactionsBuffer = inMemoryFanoutBuffer,
          startExclusive = offset1,
          endInclusive = offset3,
          bufferSliceFilter = noFilterBufferSlice(_).filterNot(_.transactionId == "tx-3"),
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
          startExclusive = offset1,
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
          startExclusive = offset1,
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
          expectedStartExclusive = offset0,
          expectedEndInclusive = offset2,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(
            Seq(offset1 -> anotherResponseForOffset1, offset2 -> anotherResponseForOffset2)
          ),
        )

        run(
          transactionsBuffer = inMemoryFanoutBufferWithSmallChunkSize,
          startExclusive = offset0,
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
          expectedStartExclusive = offset0,
          expectedEndInclusive = offset1,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(Seq(offset1 -> anotherResponseForOffset1)),
        )

        run(
          startExclusive = offset0,
          endInclusive = offset3,
          fetchFromPersistence = fetchFromPersistence,
          persistenceFetchArgs = filterMock,
          bufferSliceFilter = noFilterBufferSlice(_).filterNot(_.transactionId == "tx-3"),
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
          expectedStartExclusive = offset0,
          expectedEndInclusive = offset2,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(fetchedElements),
        )

        run(
          transactionsBuffer = smallInMemoryFanoutBuffer,
          startExclusive = offset0,
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
            _ <- stream(0, maxBufferSize)
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
            _ <- stream(maxBufferSize / 2, maxBufferSize)
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
            startExclusiveIdx = consumerSubscriptionFrom,
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
          consumerSubscriptionFrom = 0,
          updateAgainWithCount = bufferSize,
        )
      }

      "return the correct ranges when starting from an offset originally in the buffer at subscription time" in {
        testConsumerFallingBehind(
          bufferSize = bufferSize,
          bufferChunkSize = bufferChunkSize,
          consumerSubscriptionFrom = bufferSize / 2,
          updateAgainWithCount = bufferSize,
        )
      }
    }
  }
}

object BufferedStreamsReaderSpec {
  trait TestFixtures extends Matchers with ScalaFutures with IntegrationPatience {
    self: AkkaBeforeAndAfterAll =>

    implicit val lc: LoggingContext = LoggingContext.ForTesting
    val metrics = new Metrics(new MetricRegistry())
    val Seq(offset0, offset1, offset2, offset3) = (0 to 3) map { idx => offset(idx.toLong) }
    val offsetUpdates: Seq[(Offset, TransactionLogUpdate.TransactionAccepted)] =
      Seq(offset1, offset2, offset3).zip((1 to 3).map(idx => transaction(s"tx-$idx")))

    val noFilterBufferSlice
        : TransactionLogUpdate => Option[TransactionLogUpdate.TransactionAccepted] = {
      case update: TransactionLogUpdate.TransactionAccepted => Some(update)
      case _: TransactionLogUpdate.TransactionRejected => None
    }

    val inMemoryFanoutBuffer: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 3,
      metrics = metrics,
      maxBufferedChunkSize = 3,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(Function.tupled(inMemoryFanoutBuffer.push)))

    val inMemoryFanoutBufferWithSmallChunkSize: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 3,
      metrics = metrics,
      maxBufferedChunkSize = 1,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(Function.tupled(inMemoryFanoutBuffer.push)))

    val smallInMemoryFanoutBuffer: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 1,
      metrics = metrics,
      maxBufferedChunkSize = 1,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(Function.tupled(inMemoryFanoutBuffer.push)))

    trait StaticTestScope {
      val streamElements: ArrayBuffer[(Offset, String)] = ArrayBuffer.empty[(Offset, String)]

      private val failingPersistenceFetch = new FetchFromPersistence[Object, String] {
        override def apply(startExclusive: Offset, endInclusive: Offset, filter: Object)(implicit
            loggingContext: LoggingContext
        ): Source[(Offset, String), NotUsed] = fail("Unexpected call to fetch from persistence")
      }

      def run(
          startExclusive: Offset,
          endInclusive: Offset,
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
        )
          .stream[TransactionLogUpdate.TransactionAccepted](
            startExclusive = startExclusive,
            endInclusive = endInclusive,
            persistenceFetchArgs = persistenceFetchArgs,
            bufferFilter = bufferSliceFilter,
            toApiResponse = tx => Future.successful(tx.transactionId),
          )
          .runWith(Sink.foreach(streamElements.addOne))
          .futureValue

      def buildFetchFromPersistence(
          expectedStartExclusive: Offset,
          expectedEndInclusive: Offset,
          expectedFilter: Object,
          thenReturnStream: Source[(Offset, String), NotUsed],
      ): FetchFromPersistence[Object, String] =
        new FetchFromPersistence[Object, String] {
          override def apply(startExclusive: Offset, endInclusive: Offset, filter: Object)(implicit
              loggingContext: LoggingContext
          ): Source[(Offset, String), NotUsed] =
            (startExclusive, endInclusive, filter) match {
              case (`expectedStartExclusive`, `expectedEndInclusive`, `expectedFilter`) =>
                thenReturnStream
              case unexpected =>
                fail(s"Unexpected fetch transactions subscription start: $unexpected")
            }
        }
    }

    class DynamicTestScope(val maxBufferSize: Int = 100, val maxBufferChunkSize: Int = 10) {
      @volatile private var persistenceStore =
        Vector.empty[(Offset, TransactionLogUpdate.TransactionAccepted)]
      @volatile private var ledgerEndIndex = 0L
      private val inMemoryFanoutBuffer =
        new InMemoryFanoutBuffer(maxBufferSize, metrics, maxBufferChunkSize)

      private val fetchFromPersistence = new FetchFromPersistence[Object, String] {
        override def apply(startExclusive: Offset, endInclusive: Offset, filter: Object)(implicit
            loggingContext: LoggingContext
        ): Source[(Offset, String), NotUsed] = {
          if (startExclusive > endInclusive) fail("startExclusive after endInclusive")
          else if (endInclusive > offset(ledgerEndIndex)) fail("endInclusive after ledgerEnd")
          else
            persistenceStore
              .dropWhile(_._1 <= startExclusive)
              .takeWhile(_._1 <= endInclusive)
              .map { case (o, tx) => o -> tx.transactionId }
              .pipe(Source(_))
        }
      }

      private val streamReader = new BufferedStreamsReader[Object, String](
        inMemoryFanoutBuffer = inMemoryFanoutBuffer,
        fetchFromPersistence = fetchFromPersistence,
        bufferedStreamEventsProcessingParallelism = 2,
        metrics = metrics,
        streamName = "some_tx_stream",
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
          startExclusiveIdx: Int,
          endInclusiveIdx: Int,
      ): Future[Assertion] = {
        val (done, handle) = streamWithHandle(startExclusiveIdx, endInclusiveIdx)
        handle()
        done
      }

      def streamWithHandle(
          startExclusiveIdx: Int,
          endInclusiveIdx: Int,
      ): (Future[Assertion], () => Unit) = {
        val blockingPromise = Promise[Unit]()
        val unblockHandle: () => Unit = () => blockingPromise.complete(Success(()))

        val assertReadStream = streamReader
          .stream[TransactionLogUpdate.TransactionAccepted](
            startExclusive = offset(startExclusiveIdx.toLong),
            endInclusive = offset(endInclusiveIdx.toLong),
            persistenceFetchArgs = new Object, // Not used
            bufferFilter = noFilterBufferSlice, // Do not filter
            toApiResponse = tx => Future.successful(tx.transactionId),
          )
          .async
          .mapAsync(1) { idx =>
            blockingPromise.future.map(_ => idx)
          }
          .async
          .runWith(Sink.seq)
          .map { result =>
            withClue(s"($startExclusiveIdx, $endInclusiveIdx]") {
              result.size shouldBe endInclusiveIdx - startExclusiveIdx
            }
            val expectedElements = ((startExclusiveIdx.toLong + 1L) to endInclusiveIdx.toLong) map {
              idx =>
                offset(idx) -> s"tx-$idx"
            }
            result should contain theSameElementsInOrderAs expectedElements
          }

        assertReadStream -> unblockHandle
      }

      def runF(f: => Future[Assertion]): Assertion =
        f.futureValue

      private def updateFixtures(idx: Long): Unit = {
        val offsetAt = offset(idx)
        val tx = transaction(s"tx-$idx")
        persistenceStore = persistenceStore.appended(offsetAt -> tx)
        inMemoryFanoutBuffer.push(offsetAt, tx)
        ledgerEndIndex = idx
      }
    }
  }

  private def transaction(discriminator: String) =
    TransactionLogUpdate.TransactionAccepted(
      transactionId = discriminator,
      commandId = "",
      workflowId = "",
      effectiveAt = Timestamp.Epoch,
      offset = Offset.beforeBegin,
      events = Vector(null),
      completionDetails = None,
    )

  private def offset(idx: Long): Offset = {
    val base = BigInt(1L) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}
