// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref.{IdString, Identifier, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import com.daml.platform.store.dao.events.BufferedTransactionsReader.{
  FetchTransactions,
  invertMapping,
}
import com.daml.platform.store.dao.events.BufferedTransactionsReaderSpec.{
  offset,
  predecessor,
  transaction,
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
        offset(idx.toLong) -> transaction(s"tx-$idx")
      }

    // Dummy filters. We're not interested in concrete details
    // since we are asserting only the generic getTransactions method
    val filter = new Object

    val filterEvents: TransactionLogUpdate => Option[TransactionLogUpdate.Transaction] = {
      case `update1` => None
      case `update2` => Some(update2)
      case `update3` => Some(update3)
      case `update4` => Some(update4)
      case unexpected => fail(s"Unexpected $unexpected")
    }

    val apiResponseFromDB = "Some API response from storage"
    val apiResponses @ Seq(apiResponse2, apiResponse3, apiResponse4) =
      (2 to 4).map(idx => s"Some API response $idx from buffer")

    val toApiTx = Seq(update2, update3, update4)
      .zip(apiResponses)
      .map { case (u, r) =>
        u -> Future.successful(r)
      }
      .toMap

    val transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
      maxBufferSize = 3,
      metrics = metrics,
      bufferQualifier = "test",
      isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
      maxBufferedChunkSize = 100,
    )

    offsetUpdates.foreach(Function.tupled(transactionsBuffer.push))

    def readerGetTransactionsGeneric(
        eventsBuffer: EventsBuffer[TransactionLogUpdate],
        startExclusive: Offset,
        endInclusive: Offset,
        fetchTransactions: FetchTransactions[Object, String],
    ) =
      BufferedTransactionsReader
        .getTransactions[Object, String](eventsBuffer)(
          startExclusive = startExclusive,
          endInclusive = endInclusive,
          filter = filter,
          verbose = false,
          metrics,
          eventProcessingParallelism = 2,
        )(
          filterEvents = filterEvents,
          toApiTx = toApiTx,
          fetchTransactions = fetchTransactions,
          bufferReaderMetrics = metrics.daml.services.index.BufferedReader("some_tx_stream"),
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

      "fetch from buffer and storage chunked" in {
        val transactionsBufferWithSmallChunkSize = new EventsBuffer[TransactionLogUpdate](
          maxBufferSize = 3,
          metrics = metrics,
          bufferQualifier = "test",
          isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
          maxBufferedChunkSize = 1,
        )

        offsetUpdates.foreach(Function.tupled(transactionsBufferWithSmallChunkSize.push))
        val anotherResponseForOffset2 = "(2) Response fetched from storage"
        val anotherResponseForOffset3 = "(3) Response fetched from storage"
        readerGetTransactionsGeneric(
          eventsBuffer = transactionsBufferWithSmallChunkSize,
          startExclusive = offset1,
          endInclusive = offset4,
          fetchTransactions = {
            case (`offset1`, `offset3`, `filter`, false) =>
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
            offset4 -> apiResponse4,
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
          isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
          maxBufferedChunkSize = 100,
        )

        offsetUpdates.foreach(Function.tupled(transactionsBuffer.push))
        val fetchedElements = Vector(offset2 -> apiResponseFromDB, offset3 -> apiResponse2)

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

  "invertMapping" should {
    "invert the mapping of Map[Party, Set[TemplateId]] into a Map[TemplateId, Set[Party]]" in {
      def party: String => IdString.Party = Party.assertFromString
      def templateId: String => Identifier = Identifier.assertFromString

      val partiesToTemplates =
        Map(
          party("p11") -> Set(templateId("a:b:t1")),
          party("p12") -> Set(templateId("a:b:t1"), templateId("a:b:t2")),
          party("p21") -> Set(templateId("a:b:t2")),
        )

      val expectedTemplatesToParties =
        Map(
          templateId("a:b:t1") -> Set(party("p11"), party("p12")),
          templateId("a:b:t2") -> Set(party("p21"), party("p12")),
        )

      invertMapping(partiesToTemplates) shouldBe expectedTemplatesToParties
    }
  }

  override def afterAll(): Unit = {
    val _ = actorSystem.terminate()
  }
}

object BufferedTransactionsReaderSpec {
  private def transaction(discriminator: String) =
    TransactionLogUpdate.Transaction(
      transactionId = discriminator,
      workflowId = "",
      effectiveAt = Timestamp.Epoch,
      offset = Offset.beforeBegin,
      events = Vector(null),
    )

  private def predecessor(offset: Offset): Offset =
    Offset.fromByteArray((BigInt(offset.toByteArray) - 1).toByteArray)

  private def offset(idx: Long): Offset = {
    val base = BigInt(1L) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}
