// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.api.v1.transaction.{TransactionTree, Transaction => FlatTransaction}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.EventsBuffer
import com.daml.platform.store.dao.LedgerDaoTransactionsReader
import com.daml.platform.store.interfaces.TransactionLogUpdate
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class MemorySafeBufferedTransactionsReaderSpec
    extends AsyncFlatSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with ArgumentMatchersSugar
    with MockitoSugar {
  private val metrics = new Metrics(new MetricRegistry())
  private implicit val lc: LoggingContext = LoggingContext.ForTesting

  behavior of classOf[BufferedTransactionsReader].getSimpleName

  it should "control/pass" in {
    val bufferSize = 10L
    val payloadSize = 10000000
    val epochs = 10

    test(
      bufferSize = bufferSize,
      maxFetchSize = 3,
      payloadSize = payloadSize,
      epochs = epochs,
      load = (_, _) => Future.unit,
    )
  }

  it should "operate within bounded memory" in {
    val bufferSize = 10L
    val payloadSize = 10000000
    val epochs = 10

    test(
      bufferSize = bufferSize,
      maxFetchSize = 3,
      payloadSize = payloadSize,
      epochs = epochs,
      load = { case (epoch, transactionsReader) =>
        transactionsReader
          .getTransactionTrees(
            offset(epoch.toLong * bufferSize + 1L),
            offset((epoch + 1).toLong * bufferSize),
            Set.empty,
            verbose = false,
          )
          .map { el =>
            Thread.sleep(1000L)
            el
          }
          .runForeach(e => println(e._1))
      },
    )
  }

  private def test(
      bufferSize: Long,
      maxFetchSize: Int,
      payloadSize: Int,
      epochs: Int,
      load: (Int, BufferedTransactionsReader) => Future[_],
  ): Future[Assertion] = {
    val buffer = new EventsBuffer[TransactionLogUpdate](
      maxBufferSize = bufferSize,
      metrics = metrics,
      bufferQualifier = "test",
      isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
      maxBufferedChunkSize = maxFetchSize,
    )

    val mockReader = mock[LedgerDaoTransactionsReader]
    when(
      mockReader.getTransactionTrees(any[Offset], any[Offset], any[Set[Party]], any[Boolean])(
        any[LoggingContext]
      )
    )
      .thenReturn(Source.empty)

    val bufferedReader = new BufferedTransactionsReader(
      delegate = mockReader,
      transactionsBuffer = buffer,
      filterFlatTransactions = (_, _) => tx => Some(tx),
      filterTransactionTrees = _ => tx => Some(tx),
      flatToApiTransactions = (_, _, _) =>
        tx =>
          Future.successful(
            GetTransactionsResponse(Seq(FlatTransaction(transactionId = tx.transactionId)))
          ),
      treesToApiTransactions = (_, _, _) =>
        tx =>
          Future.successful(
            GetTransactionTreesResponse(Seq(TransactionTree(transactionId = tx.transactionId)))
          ),
      metrics = metrics,
    )

    Future
      .sequence(
        (0 until epochs).map { epoch =>
          (1L to bufferSize).foreach { lsb =>
            val idx = epoch.toLong * bufferSize + lsb
            val newOffset = offset(idx)
            buffer.push(newOffset, tx(newOffset, payloadSize))
          }
          load(epoch, bufferedReader)
        }
      )
      .map(_ => succeed)
  }

  private def tx(offset: Offset, payloadSize: Int): TransactionLogUpdate.Transaction =
    TransactionLogUpdate.Transaction(
      // Just encode the massive payload in the transaction id
      transactionId = scala.util.Random.nextString(payloadSize),
      workflowId = "",
      effectiveAt = Time.Timestamp.Epoch,
      offset = offset,
      events = Vector(null),
    )

  private def offset(idx: Long): Offset = {
    val base = BigInt(1) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}
