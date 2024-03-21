// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.UUID

import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionByEventIdRequest,
  TransactionServiceGrpc,
}
import io.grpc.Status
import org.scalatest.Assertion

import scala.concurrent.Future

final class GetTransactionByEventIdAuthIT extends ReadOnlyServiceCallAuthTests {

  override def serviceCallName: String = "TransactionService#GetTransactionByEventId"

  override def successfulBehavior: Future[Any] => Future[Assertion] =
    expectFailure(_: Future[Any], Status.Code.INVALID_ARGUMENT)

  private lazy val request =
    new GetTransactionByEventIdRequest(unwrappedLedgerId, UUID.randomUUID.toString, List(mainActor))

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(TransactionServiceGrpc.stub(channel), token).getTransactionByEventId(request)

}
