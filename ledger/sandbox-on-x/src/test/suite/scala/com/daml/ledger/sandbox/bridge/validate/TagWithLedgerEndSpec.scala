// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.api.domain
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.domain.{Rejection, Submission}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

class TagWithLedgerEndSpec extends AnyFlatSpec with Matchers with MockitoSugar {
  behavior of classOf[TagWithLedgerEndImpl].getSimpleName

  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val indexServiceMock = mock[IndexService]
  private val tagWithLedgerEnd = new TagWithLedgerEndImpl(
    indexService = indexServiceMock,
    bridgeMetrics = new BridgeMetrics(Metrics.ForTesting),
  )

  "tagWithLedgerEnd" should "tag the incoming submissions with the index service ledger end" in {
    val offsetString = Ref.HexString.assertFromString("ab")
    val expectedOffset = Offset.fromHexString(offsetString)
    val indexServiceProvidedOffset = domain.LedgerOffset.Absolute(offsetString)

    val somePreparedSubmission =
      mock[PreparedSubmission]
        .tap { preparedSubmission =>
          val someSubmission = mock[Submission].tap { submission =>
            when(submission.loggingContext).thenReturn(loggingContext)
          }
          when(preparedSubmission.submission).thenReturn(someSubmission)
        }

    when(indexServiceMock.currentLedgerEnd())
      .thenReturn(Future.successful(indexServiceProvidedOffset))

    tagWithLedgerEnd(Right(somePreparedSubmission))
      .map(_ shouldBe Right(expectedOffset -> somePreparedSubmission))
  }

  "tagWithLedgerEnd" should "propagate a rejection" in {
    val rejectionIn = Left(mock[Rejection])
    tagWithLedgerEnd(rejectionIn).map(_ shouldBe rejectionIn)
  }
}
