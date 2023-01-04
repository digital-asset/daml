// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.platform.store.cache.InMemoryFanoutBuffer
import com.daml.platform.store.dao.BufferedTransactionByIdReader.{
  FetchTransactionByIdFromPersistence,
  ToApiResponse,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class BufferedTransactionByIdReaderSpec extends AsyncFlatSpec with MockitoSugar with Matchers {
  private val className = classOf[BufferedTransactionByIdReader[_]].getSimpleName

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val requestingParties = Set("p1", "p2").map(Ref.Party.assertFromString)

  private val bufferedTransactionId1 = "bufferedTid_1"
  private val bufferedTransactionId2 = "bufferedTid_2"
  private val notBufferedTransactionId = "notBufferedTid"
  private val unknownTransactionId = "unknownTransactionId"

  private val bufferedTransaction1 = tx(bufferedTransactionId1)
  private val bufferedTransaction2 = tx(bufferedTransactionId2)

  private val inMemoryFanout = mock[InMemoryFanoutBuffer]
  when(inMemoryFanout.lookup(bufferedTransactionId1)).thenReturn(Some(bufferedTransaction1))
  when(inMemoryFanout.lookup(bufferedTransactionId2)).thenReturn(Some(bufferedTransaction2))
  when(inMemoryFanout.lookup(notBufferedTransactionId)).thenReturn(None)
  when(inMemoryFanout.lookup(unknownTransactionId)).thenReturn(None)

  private val toApiResponse = mock[ToApiResponse[String]]
  when(toApiResponse.apply(bufferedTransaction1, requestingParties, loggingContext))
    .thenReturn(Future.successful(Some(bufferedTransactionId1)))
  when(toApiResponse.apply(bufferedTransaction2, requestingParties, loggingContext))
    .thenReturn(Future.successful(None))

  private val fetchFromPersistence = new FetchTransactionByIdFromPersistence[String] {
    override def apply(
        transactionId: String,
        requestingParties: Set[Party],
        loggingContext: LoggingContext,
    ): Future[Option[String]] =
      transactionId match {
        case `notBufferedTransactionId` => Future.successful(Some(notBufferedTransactionId))
        case `unknownTransactionId` => Future.successful(None)
        case other => fail(s"Unexpected $other transactionId")
      }
  }

  private val bufferedTransactionByIdReader = new BufferedTransactionByIdReader[String](
    inMemoryFanoutBuffer = inMemoryFanout,
    fetchFromPersistence = fetchFromPersistence,
    toApiResponse = toApiResponse,
  )

  s"$className.fetch" should "convert to API response and return if transaction buffered" in {
    for {
      response1 <- bufferedTransactionByIdReader.fetch(bufferedTransactionId1, requestingParties)
      response2 <- bufferedTransactionByIdReader.fetch(bufferedTransactionId2, requestingParties)
    } yield {
      response1 shouldBe Some(bufferedTransactionId1)
      response2 shouldBe None
      verify(toApiResponse).apply(bufferedTransaction1, requestingParties, loggingContext)
      verify(toApiResponse).apply(bufferedTransaction2, requestingParties, loggingContext)
      succeed
    }
  }

  s"$className.fetch" should "delegate to persistence fetch if transaction not buffered" in {
    for {
      response1 <- bufferedTransactionByIdReader.fetch(notBufferedTransactionId, requestingParties)
      response2 <- bufferedTransactionByIdReader.fetch(unknownTransactionId, requestingParties)
    } yield {
      response1 shouldBe Some(notBufferedTransactionId)
      response2 shouldBe None
      verifyZeroInteractions(toApiResponse)
      succeed
    }
  }

  private def tx(discriminator: String) =
    TransactionLogUpdate.TransactionAccepted(
      transactionId = discriminator,
      workflowId = "",
      commandId = "",
      effectiveAt = Timestamp.Epoch,
      offset = Offset.beforeBegin,
      events = Vector(null),
      completionDetails = None,
    )
}
