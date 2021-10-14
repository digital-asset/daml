// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.transaction

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.DomainMocks
import com.daml.ledger.participant.state.index.v2.IndexTransactionsService
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.ErrorCodesVersionSwitcher
import io.grpc.StatusRuntimeException
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class ApiTransactionServiceSpec extends AsyncFlatSpec with Matchers with MockitoSugar {

  it should "return correct exception when transaction is not found (V1)" in {
    testLookUpFlatByTransactionId(
      enableSelfServiceErrorCodes = false,
      expectedErrorMessage = "NOT_FOUND: Transaction not found, or not visible.",
    )
  }

  it should "return correct exception when transaction is not found (V2)" in {
    testLookUpFlatByTransactionId(
      enableSelfServiceErrorCodes = true,
      expectedErrorMessage =
        "NOT_FOUND: TRANSACTION_NOT_FOUND(11,0): Transaction not found, or not visible.",
    )
  }

  private def testLookUpFlatByTransactionId(
      enableSelfServiceErrorCodes: Boolean,
      expectedErrorMessage: String,
  ): Future[Assertion] = {
    // given
    val transactionsServiceMock = mock[IndexTransactionsService]
    val setOfParties = Set(DomainMocks.party)
    when(
      transactionsServiceMock.getTransactionById(
        DomainMocks.transactionId,
        setOfParties,
      )(LoggingContext.ForTesting)
    ).thenReturn(
      Future.successful(None)
    )

    val tested = new ApiTransactionService(
      transactionsService = transactionsServiceMock,
      metrics = new Metrics(new MetricRegistry),
      errorsVersionsSwitcher =
        new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = enableSelfServiceErrorCodes),
    )(
      executionContext = executionContext,
      loggingContext = LoggingContext.ForTesting,
    )

    // when
    val actual = recoverToExceptionIf[StatusRuntimeException] {
      tested.lookUpFlatByTransactionId(
        transactionId = DomainMocks.transactionId,
        requestingParties = setOfParties,
      )
    }

    // then
    actual.map { ex =>
      assert(ex.getMessage == expectedErrorMessage)
    }
  }
}
