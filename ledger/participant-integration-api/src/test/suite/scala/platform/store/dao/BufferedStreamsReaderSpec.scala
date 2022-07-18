// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.EventsBuffer
import com.daml.platform.store.dao.BufferedStreamsReader.PersistenceFetch
import com.daml.platform.store.dao.BufferedStreamsReaderSpec.{offset, transaction}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.CompletionDetails
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class BufferedStreamsReaderSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with AkkaBeforeAndAfterAll {
  private implicit val lc: LoggingContext = LoggingContext.ForTesting

  "getEvents" when {
    val metrics = new Metrics(new MetricRegistry())
    val Seq(offset1, offset2, offset3, offset4) = (1 to 4).map(id => offset((id * 2).toLong))
    val offsetBetween_offset2_and_offset3 = offset(5L)
    val rejection = TransactionLogUpdate.TransactionRejected(
      offset1,
      completionDetails = CompletionDetails(
        completionStreamResponse = CompletionStreamResponse(),
        submitters = Set("some-submitter"),
      ),
    )
    val Seq(txAccepted1, txAccepted2, txAccepted3) = (1 to 3).map { idx =>
      transaction(s"tx-$idx")
    }
    val offsetUpdates = Seq(
      offset1 -> rejection,
      offset2 -> txAccepted1,
      offset3 -> txAccepted2,
      offset4 -> txAccepted3,
    )

    // Dummy filter. We are only interested that this reference
    // is passed downstream to the persistence fetch caller.
    val persistenceFetchFilter = new Object

    val bufferSliceFilter
        : TransactionLogUpdate => Option[TransactionLogUpdate.TransactionAccepted] = {
      case update: TransactionLogUpdate.TransactionAccepted => Some(update)
      case _: TransactionLogUpdate.TransactionRejected => None
    }

    val toApiResponse: TransactionLogUpdate.TransactionAccepted => Future[String] = {
      case `txAccepted1` => Future.successful("tx2")
      case `txAccepted2` => Future.successful("tx3")
      case `txAccepted3` => Future.successful("tx4")
      case other => fail(s"Unexpected $other")
    }

    val transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
      maxBufferSize = 3,
      metrics = metrics,
      bufferQualifier = "test",
      maxBufferedChunkSize = 100,
    )

    offsetUpdates.foreach(Function.tupled(transactionsBuffer.push))

    def readerGetEventsGeneric(
        transactionsBuffer: EventsBuffer[TransactionLogUpdate],
        startExclusive: Offset,
        endInclusive: Offset,
        persistenceFetch: PersistenceFetch[Object, String],
    ): Future[Seq[(Offset, String)]] =
      new BufferedStreamsReader[Object, String](
        inMemoryFanoutBuffer = transactionsBuffer,
        persistenceFetch = persistenceFetch,
        eventProcessingParallelism = 2,
        metrics = metrics,
        name = "some_tx_stream",
      )
        .streamUsingBuffered[TransactionLogUpdate.TransactionAccepted](
          startExclusive = startExclusive,
          endInclusive = endInclusive,
          persistenceFetchFilter = persistenceFetchFilter,
          bufferSliceFilter = bufferSliceFilter,
          toApiResponse = toApiResponse,
        )
        .runWith(Sink.seq)

    "request within buffer range inclusive but with start exclusive not matching an offset in the buffer" should {
      "fetch from buffer" in {
        readerGetEventsGeneric(
          transactionsBuffer = transactionsBuffer,
          startExclusive = offsetBetween_offset2_and_offset3,
          endInclusive = offset4,
          persistenceFetch = (_, _, _) => fail("Should not fetch"),
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset3 -> "tx3",
            offset4 -> "tx4",
          )
        )
      }
    }

    "request within buffer range inclusive but with start exclusive matching an offset in the buffer" should {
      "fetch from buffer" in {
        readerGetEventsGeneric(
          transactionsBuffer = transactionsBuffer,
          startExclusive = offset2,
          endInclusive = offset4,
          persistenceFetch = (_, _, _) => fail("Should not fetch"),
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset3 -> "tx3",
            offset4 -> "tx4",
          )
        )
      }
    }

    "request withing buffer range inclusive (multiple chunks)" should {
      "correctly fetch from buffer" in {
        val transactionsBufferWithSmallChunkSize = new EventsBuffer[TransactionLogUpdate](
          maxBufferSize = 3,
          metrics = metrics,
          bufferQualifier = "test",
          maxBufferedChunkSize = 1,
        )

        offsetUpdates.foreach(Function.tupled(transactionsBufferWithSmallChunkSize.push))

        readerGetEventsGeneric(
          transactionsBuffer = transactionsBufferWithSmallChunkSize,
          startExclusive = offset2,
          endInclusive = offset4,
          persistenceFetch = (_, _, _) => fail("Should not fetch"),
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset3 -> "tx3",
            offset4 -> "tx4",
          )
        )
      }
    }

    "request before buffer start" should {
      "fetch from buffer and storage" in {
        val anotherResponseForOffset2 = "Response fetched from storage"
        readerGetEventsGeneric(
          transactionsBuffer = transactionsBuffer,
          startExclusive = offset1,
          endInclusive = offset3,
          persistenceFetch = {
            case (`offset1`, `offset2`, `persistenceFetchFilter`) =>
              _ => Source.single(offset2 -> anotherResponseForOffset2)
            case unexpected =>
              fail(s"Unexpected fetch transactions subscription start: $unexpected")
          },
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset2 -> anotherResponseForOffset2,
            offset3 -> "tx3",
          )
        )
      }

      "fetch from buffer and storage chunked" in {
        val transactionsBufferWithSmallChunkSize = new EventsBuffer[TransactionLogUpdate](
          maxBufferSize = 3,
          metrics = metrics,
          bufferQualifier = "test",
          maxBufferedChunkSize = 1,
        )

        offsetUpdates.foreach(Function.tupled(transactionsBufferWithSmallChunkSize.push))
        val anotherResponseForOffset2 = "(2) Response fetched from storage"
        val anotherResponseForOffset3 = "(3) Response fetched from storage"
        readerGetEventsGeneric(
          transactionsBuffer = transactionsBufferWithSmallChunkSize,
          startExclusive = offset1,
          endInclusive = offset4,
          persistenceFetch = {
            case (`offset1`, `offset3`, `persistenceFetchFilter`) =>
              _ =>
                Source(
                  Seq(offset2 -> anotherResponseForOffset2, offset3 -> anotherResponseForOffset3)
                )
            case unexpected =>
              fail(s"Unexpected fetch transactions subscription start: $unexpected")
          },
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset2 -> anotherResponseForOffset2,
            offset3 -> anotherResponseForOffset3,
            offset4 -> "tx4",
          )
        )
      }
    }

    "request before buffer bounds" should {
      "fetch only from storage" in {
        val transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
          maxBufferSize = 1,
          metrics = metrics,
          bufferQualifier = "test",
          maxBufferedChunkSize = 100,
        )

        offsetUpdates.foreach(Function.tupled(transactionsBuffer.push))
        val fetchedElements = Vector(
          offset2 -> "Some API response from persistence",
          offset3 -> "Another API response from persistence",
        )

        readerGetEventsGeneric(
          transactionsBuffer = transactionsBuffer,
          startExclusive = offset1,
          endInclusive = offset3,
          persistenceFetch = {
            case (`offset1`, `offset3`, `persistenceFetchFilter`) =>
              _ => Source.fromIterator(() => fetchedElements.iterator)
            case unexpected => fail(s"Unexpected $unexpected")
          },
        ).map(_ should contain theSameElementsInOrderAs fetchedElements)
      }
    }
  }
}

object BufferedStreamsReaderSpec {
  private def transaction(discriminator: String) =
    TransactionLogUpdate.TransactionAccepted(
      transactionId = discriminator,
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
