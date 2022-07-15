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
import com.daml.platform.store.dao.BufferedStreamsReaderSpec.{offset, predecessor, transaction}
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
    val Seq(offset1, offset2, offset3, offset4) = (1 to 4).map(id => offset(id.toLong))
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

    // Dummy filters. We're not interested in concrete details
    // since we are asserting only the generic getEvents method
    val persistenceFetchFilter = new Object

    val bufferSliceFilter
        : TransactionLogUpdate => Option[TransactionLogUpdate.TransactionAccepted] = {
      case update: TransactionLogUpdate.TransactionAccepted => Some(update)
      case _: TransactionLogUpdate.TransactionRejected => None
    }

    val toApiResponse: TransactionLogUpdate.TransactionAccepted => Future[String] = {
      case `txAccepted1` => Future.successful("tx1")
      case `txAccepted2` => Future.successful("tx2")
      case `txAccepted3` => Future.successful("tx3")
      case other => fail(s"Unexpected $other")
    }

    val apiResponseFromDB = "Some API response from storage"

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
        transactionsBuffer = transactionsBuffer,
        persistenceFetch = persistenceFetch,
        eventProcessingParallelism = 2,
        metrics = metrics,
        name = "some_tx_stream",
      )
        .getEvents[TransactionLogUpdate.TransactionAccepted](
          startExclusive = startExclusive,
          endInclusive = endInclusive,
          persistenceFetchFilter = persistenceFetchFilter,
          bufferSliceFilter = bufferSliceFilter,
          toApiResponse = toApiResponse,
        )
        .runWith(Sink.seq)

    "request within buffer range inclusive on offset gap as start exclusive" should {
      "fetch from buffer" in {
        readerGetEventsGeneric(
          transactionsBuffer = transactionsBuffer,
          startExclusive = predecessor(offset3),
          endInclusive = offset4,
          persistenceFetch = (_, _, _) => fail("Should not fetch"),
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset3 -> "tx2",
            offset4 -> "tx3",
          )
        )
      }
    }

    "request within buffer range inclusive on existing offset as start inclusive" should {
      "fetch from buffer" in {
        readerGetEventsGeneric(
          transactionsBuffer = transactionsBuffer,
          startExclusive = offset2,
          endInclusive = offset4,
          persistenceFetch = (_, _, _) => fail("Should not fetch"),
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset3 -> "tx2",
            offset4 -> "tx3",
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
            offset3 -> "tx2",
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
            offset4 -> "tx3",
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
        val fetchedElements = Vector(offset2 -> apiResponseFromDB, offset3 -> "tx1")

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

  private def predecessor(offset: Offset): Offset =
    Offset.fromByteArray((BigInt(offset.toByteArray) - 1).toByteArray)

  private def offset(idx: Long): Offset = {
    val base = BigInt(1L) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}
