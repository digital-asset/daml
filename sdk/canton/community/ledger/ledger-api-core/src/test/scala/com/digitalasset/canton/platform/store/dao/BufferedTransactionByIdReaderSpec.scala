// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedTransactionByIdReader.{
  FetchTransactionByIdFromPersistence,
  ToApiResponse,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AsyncFlatSpec

import scala.concurrent.Future

class BufferedTransactionByIdReaderSpec extends AsyncFlatSpec with MockitoSugar with BaseTest {
  private val className = classOf[BufferedTransactionByIdReader[_]].getSimpleName

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace(
    loggerFactory
  )

  private val requestingParties = Set("p1", "p2").map(Ref.Party.assertFromString)
  private val someDomainId = DomainId.tryFromString("some::domain-id")

  private val bufferedUpdateId1 = "bufferedTid_1"
  private val bufferedUpdateId2 = "bufferedTid_2"
  private val notBufferedUpdateId = "notBufferedTid"
  private val unknownUpdateId = "unknownUpdateId"

  private val bufferedTransaction1 = Traced(tx(bufferedUpdateId1))
  private val bufferedTransaction2 = Traced(tx(bufferedUpdateId2))

  private val inMemoryFanout = mock[InMemoryFanoutBuffer]
  when(inMemoryFanout.lookup(bufferedUpdateId1)).thenReturn(Some(bufferedTransaction1))
  when(inMemoryFanout.lookup(bufferedUpdateId2)).thenReturn(Some(bufferedTransaction2))
  when(inMemoryFanout.lookup(notBufferedUpdateId)).thenReturn(None)
  when(inMemoryFanout.lookup(unknownUpdateId)).thenReturn(None)

  private val toApiResponse = mock[ToApiResponse[String]]
  when(toApiResponse.apply(bufferedTransaction1, requestingParties, loggingContext))
    .thenReturn(Future.successful(Some(bufferedUpdateId1)))
  when(toApiResponse.apply(bufferedTransaction2, requestingParties, loggingContext))
    .thenReturn(Future.successful(None))

  private val fetchFromPersistence = new FetchTransactionByIdFromPersistence[String] {
    override def apply(
        transactionId: String,
        requestingParties: Set[Party],
        loggingContext: LoggingContextWithTrace,
    ): Future[Option[String]] =
      transactionId match {
        case `notBufferedUpdateId` => Future.successful(Some(notBufferedUpdateId))
        case `unknownUpdateId` => Future.successful(None)
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
      response1 <- bufferedTransactionByIdReader.fetch(bufferedUpdateId1, requestingParties)
      response2 <- bufferedTransactionByIdReader.fetch(bufferedUpdateId2, requestingParties)
    } yield {
      response1 shouldBe Some(bufferedUpdateId1)
      response2 shouldBe None
      verify(toApiResponse).apply(bufferedTransaction1, requestingParties, loggingContext)
      verify(toApiResponse).apply(bufferedTransaction2, requestingParties, loggingContext)
      succeed
    }
  }

  s"$className.fetch" should "delegate to persistence fetch if transaction not buffered" in {
    for {
      response1 <- bufferedTransactionByIdReader.fetch(notBufferedUpdateId, requestingParties)
      response2 <- bufferedTransactionByIdReader.fetch(unknownUpdateId, requestingParties)
    } yield {
      response1 shouldBe Some(notBufferedUpdateId)
      response2 shouldBe None
      verifyZeroInteractions(toApiResponse)
      succeed
    }
  }

  private def tx(discriminator: String) =
    TransactionLogUpdate.TransactionAccepted(
      updateId = discriminator,
      workflowId = "",
      commandId = "",
      effectiveAt = Timestamp.Epoch,
      offset = Offset.beforeBegin,
      events = Vector(null),
      completionStreamResponse = None,
      domainId = someDomainId.toProtoPrimitive,
      recordTime = Timestamp.Epoch,
    )
}
