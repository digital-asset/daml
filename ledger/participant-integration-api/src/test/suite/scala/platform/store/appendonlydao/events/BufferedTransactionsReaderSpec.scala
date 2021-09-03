// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.metrics.Metrics
import com.daml.platform.store.appendonlydao.events.BufferedTransactionsReader.FetchTransactions
import com.daml.platform.store.appendonlydao.events.BufferedTransactionsReaderSpec.{
  predecessor,
  transactionLogUpdate,
}
import com.daml.platform.store.cache.EventsBuffer
import com.daml.platform.store.interfaces.TransactionLogUpdate
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class BufferedTransactionsReaderSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with BeforeAndAfterAll {
  private val actorSystem = ActorSystem()
  private implicit val materializer: Materializer = Materializer(actorSystem)

  "getTransactions" when {
    val metrics = new Metrics(new MetricRegistry())
    val offsetUpdates @ Seq(
      (offset1, update1),
      (offset2, update2),
      (offset3, update3),
      (offset4, update4),
    ) =
      (1 to 4).map { idx =>
        Offset.fromByteArray(BigInt(idx * 2 + 1234).toByteArray) -> transactionLogUpdate(s"tx-$idx")
      }

    // Dummy filters. We're not interested in concrete details
    // since we are asserting only the generic getTransactions method
    val filter, otherFilter = new Object

    val txApiMap @ Seq(
      (apiTx1, apiResponse1),
      (apiTx2, apiResponse2),
      (apiTx3, apiResponse3),
      (apiTx4, apiResponse4),
    ) = (1 to 4).map(idx => s"Some API TX $idx" -> s"Some API response $idx from buffer")

    val toApiTx: (TransactionLogUpdate, Object, Boolean) => Future[Option[String]] = {
      case (`update1`, `otherFilter`, false) => Future.successful(None)
      case (`update1`, `filter`, false) => Future.successful(Some(apiTx1))
      case (`update2`, `filter`, false) => Future.successful(Some(apiTx2))
      case (`update3`, `filter`, false) => Future.successful(Some(apiTx3))
      case (`update4`, `filter`, false) => Future.successful(Some(apiTx4))
      case unexpected => fail(s"Unexpected $unexpected")
    }

    val apiResponseCtor = txApiMap.map { case (apiTx, apiResponse) =>
      Seq(apiTx) -> apiResponse
    }.toMap

    val transactionsBuffer = new EventsBuffer[Offset, TransactionLogUpdate](
      maxBufferSize = 3L,
      metrics = metrics,
      bufferQualifier = "test",
      isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
    )

    offsetUpdates.foreach { case (offset, update) =>
      transactionsBuffer.push(offset, update)
    }

    def readerGetTransactionsGeneric(
        eventsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
        startExclusive: Offset,
        endInclusive: Offset,
        fetchTransactions: FetchTransactions[Object, String],
    ) =
      BufferedTransactionsReader
        .getTransactions(eventsBuffer)(
          startExclusive = startExclusive,
          endInclusive = endInclusive,
          filter = filter,
          verbose = false,
        )(
          toApiTx = toApiTx,
          apiResponseCtor = apiResponseCtor,
          fetchTransactions = fetchTransactions,
          toApiTxTimer = metrics.daml.services.index.streamsBuffer.toTransactionTrees,
          sourceTimer = metrics.daml.services.index.streamsBuffer.getTransactionTrees,
          resolvedFromBufferCounter =
            metrics.daml.services.index.streamsBuffer.transactionTreesBuffered,
          totalRetrievedCounter = metrics.daml.services.index.streamsBuffer.transactionTreesTotal,
          bufferSizeCounter = metrics.daml.services.index.streamsBuffer.transactionTreesBufferSize,
          outputStreamBufferSize = 128,
        )
        .runWith(Sink.seq)

    "request within buffer range inclusive on offset gap as start exclusive" should {
      "fetch from buffer" in {
        readerGetTransactionsGeneric(
          eventsBuffer = transactionsBuffer,
          startExclusive = predecessor(offset3),
          endInclusive = offset4,
          fetchTransactions = (_, _, _, _) => fail("Should not fetch"),
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset3 -> apiResponse3,
            offset4 -> apiResponse4,
          )
        )
      }
    }

    "request within buffer range inclusive on existing offset as start inclusive" should {
      "fetch from buffer" in {
        readerGetTransactionsGeneric(
          eventsBuffer = transactionsBuffer,
          startExclusive = offset2,
          endInclusive = offset4,
          fetchTransactions = (_, _, _, _) => fail("Should not fetch"),
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset3 -> apiResponse3,
            offset4 -> apiResponse4,
          )
        )
      }
    }

    "request before buffer start" should {
      "fetch from buffer and storage" in {
        val anotherResponseForOffset2 = "Response fetched from storage"
        readerGetTransactionsGeneric(
          eventsBuffer = transactionsBuffer,
          startExclusive = offset1,
          endInclusive = offset3,
          fetchTransactions = {
            case (`offset1`, `offset2`, `filter`, false) =>
              Source.single(offset2 -> anotherResponseForOffset2)
            case unexpected =>
              fail(s"Unexpected fetch transactions subscription start: $unexpected")
          },
        ).map(
          _ should contain theSameElementsInOrderAs Seq(
            offset2 -> anotherResponseForOffset2,
            offset3 -> apiResponse3,
          )
        )
      }
    }

    "request before buffer bounds" should {
      "fetch only from storage" in {
        val transactionsBuffer = new EventsBuffer[Offset, TransactionLogUpdate](
          maxBufferSize = 1L,
          metrics = metrics,
          bufferQualifier = "test",
          isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
        )

        offsetUpdates.foreach { case (offset, update) =>
          transactionsBuffer.push(offset, update)
        }
        val fetchedElements = Vector(offset2 -> apiResponse1, offset3 -> apiResponse2)

        readerGetTransactionsGeneric(
          eventsBuffer = transactionsBuffer,
          startExclusive = offset1,
          endInclusive = offset3,
          fetchTransactions = {
            case (`offset1`, `offset3`, `filter`, false) =>
              Source.fromIterator(() => fetchedElements.iterator)
            case unexpected => fail(s"Unexpected $unexpected")
          },
        ).map(_ should contain theSameElementsInOrderAs fetchedElements)
      }
    }
  }

  override def afterAll(): Unit = {
    val _ = actorSystem.terminate()
  }
}

object BufferedTransactionsReaderSpec {
  private def transactionLogUpdate(discriminator: String) =
    TransactionLogUpdate.Transaction(
      transactionId = discriminator,
      workflowId = "",
      effectiveAt = Instant.EPOCH,
      offset = Offset.beforeBegin,
      events = Vector(null),
    )

  private def predecessor(offset: Offset): Offset =
    Offset.fromByteArray((BigInt(offset.toByteArray) - 1).toByteArray)
}
