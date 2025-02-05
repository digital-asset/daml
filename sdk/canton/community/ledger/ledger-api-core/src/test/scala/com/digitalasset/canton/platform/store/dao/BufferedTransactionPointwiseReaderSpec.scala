// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedTransactionByIdReader.{
  FetchTransactionPointwiseFromPersistence,
  ToApiResponse,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AsyncFlatSpec

import scala.concurrent.Future

class BufferedTransactionPointwiseReaderSpec extends AsyncFlatSpec with MockitoSugar with BaseTest {
  private val className = classOf[BufferedTransactionPointwiseReader[_, _]].getSimpleName

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace(
    loggerFactory
  )

  private val requestingParties = Set("p1", "p2").map(Ref.Party.assertFromString)
  private val someSynchronizerId = SynchronizerId.tryFromString("some::synchronizer id")

  private val bufferedUpdateId1 = "bufferedTid_1"
  private val bufferedUpdateId2 = "bufferedTid_2"
  private val notBufferedUpdateId = "notBufferedTid"
  private val unknownUpdateId = "unknownUpdateId"

  private val bufferedOffset1 = Offset.firstOffset
  private val bufferedOffset2 = bufferedOffset1.increment
  private val notBufferedOffset = bufferedOffset2.increment
  private val unknownOffset = notBufferedOffset.increment

  private val bufferedTransaction1 = tx(bufferedUpdateId1, bufferedOffset1)
  private val bufferedTransaction2 = tx(bufferedUpdateId2, bufferedOffset2)

  private val inMemoryFanout = mock[InMemoryFanoutBuffer]
  when(inMemoryFanout.lookup(bufferedUpdateId1)).thenReturn(Some(bufferedTransaction1))
  when(inMemoryFanout.lookup(bufferedUpdateId2)).thenReturn(Some(bufferedTransaction2))
  when(inMemoryFanout.lookup(notBufferedUpdateId)).thenReturn(None)
  when(inMemoryFanout.lookup(unknownUpdateId)).thenReturn(None)

  when(inMemoryFanout.lookup(bufferedOffset1)).thenReturn(Some(bufferedTransaction1))
  when(inMemoryFanout.lookup(bufferedOffset2)).thenReturn(Some(bufferedTransaction2))
  when(inMemoryFanout.lookup(notBufferedOffset)).thenReturn(None)
  when(inMemoryFanout.lookup(unknownOffset)).thenReturn(None)

  private val toApiResponse = mock[ToApiResponse[Set[Party], String]]
  when(toApiResponse.apply(bufferedTransaction1, requestingParties, loggingContext))
    .thenReturn(Future.successful(Some(bufferedUpdateId1)))
  when(toApiResponse.apply(bufferedTransaction2, requestingParties, loggingContext))
    .thenReturn(Future.successful(None))

  private val fetchFromPersistenceById =
    new FetchTransactionPointwiseFromPersistence[(String, Set[Party]), String] {
      override def apply(
          queryParam: (String, Set[Party]),
          loggingContext: LoggingContextWithTrace,
      ): Future[Option[String]] =
        queryParam._1 match {
          case `notBufferedUpdateId` => Future.successful(Some(notBufferedUpdateId))
          case `unknownUpdateId` => Future.successful(None)
          case other => fail(s"Unexpected $other transactionId")
        }
    }

  private val bufferedTransactionByIdReader =
    new BufferedTransactionPointwiseReader[(String, Set[Party]), String](
      fetchFromPersistence = fetchFromPersistenceById,
      fetchFromBuffer = queryParam => inMemoryFanout.lookup(queryParam._1),
      toApiResponse = (tx, queryParam, lc) => toApiResponse(tx, queryParam._2, lc),
    )

  private val fetchFromPersistenceByOffset =
    new FetchTransactionPointwiseFromPersistence[(Offset, Set[Party]), String] {
      override def apply(
          queryParam: (Offset, Set[Party]),
          loggingContext: LoggingContextWithTrace,
      ): Future[Option[String]] =
        queryParam._1 match {
          case `notBufferedOffset` => Future.successful(Some(notBufferedUpdateId))
          case `unknownOffset` => Future.successful(None)
          case other => fail(s"Unexpected offset $other")
        }
    }

  private val bufferedTransactionByOffsetReader =
    new BufferedTransactionPointwiseReader[(Offset, Set[Party]), String](
      fetchFromPersistence = fetchFromPersistenceByOffset,
      fetchFromBuffer = queryParam => inMemoryFanout.lookup(queryParam._1),
      toApiResponse = (tx, queryParam, lc) => toApiResponse(tx, queryParam._2, lc),
    )

  s"$className.fetch" should "convert to API response and return if transaction buffered" in {
    for {
      response1 <- bufferedTransactionByIdReader.fetch(bufferedUpdateId1 -> requestingParties)
      response2 <- bufferedTransactionByIdReader.fetch(bufferedUpdateId2 -> requestingParties)
      response3 <- bufferedTransactionByOffsetReader.fetch(bufferedOffset1 -> requestingParties)
      response4 <- bufferedTransactionByOffsetReader.fetch(bufferedOffset2 -> requestingParties)
    } yield {
      response1 shouldBe Some(bufferedUpdateId1)
      response2 shouldBe None
      response3 shouldBe response1
      response4 shouldBe response2
      verify(toApiResponse, times(2)).apply(bufferedTransaction1, requestingParties, loggingContext)
      verify(toApiResponse, times(2)).apply(bufferedTransaction2, requestingParties, loggingContext)
      succeed
    }
  }

  s"$className.fetch" should "delegate to persistence fetch if transaction not buffered" in {
    for {
      response1 <- bufferedTransactionByIdReader.fetch(notBufferedUpdateId -> requestingParties)
      response2 <- bufferedTransactionByIdReader.fetch(unknownUpdateId -> requestingParties)
      response3 <- bufferedTransactionByOffsetReader.fetch(notBufferedOffset -> requestingParties)
      response4 <- bufferedTransactionByOffsetReader.fetch(unknownOffset -> requestingParties)
    } yield {
      response1 shouldBe Some(notBufferedUpdateId)
      response2 shouldBe None
      response3 shouldBe response1
      response4 shouldBe response2
      verifyZeroInteractions(toApiResponse)
      succeed
    }
  }

  private def tx(discriminator: String, offset: Offset) =
    TransactionLogUpdate.TransactionAccepted(
      updateId = discriminator,
      workflowId = "",
      commandId = "",
      effectiveAt = Timestamp.Epoch,
      offset = offset,
      events = Vector(null),
      completionStreamResponse = None,
      synchronizerId = someSynchronizerId.toProtoPrimitive,
      recordTime = Timestamp.Epoch,
    )
}
