// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.UUID

import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionByIdRequest,
  TransactionServiceGrpc,
}
import io.grpc.Status
import org.scalatest.Assertion

import scala.concurrent.Future

final class GetFlatTransactionByIdAuthIT extends ReadOnlyServiceCallAuthTests {

  override def serviceCallName: String = "TransactionService#GetFlatTransactionById"

  override def successfulBehavior: Future[Any] => Future[Assertion] =
    expectFailure(_: Future[Any], Status.Code.NOT_FOUND)

  private lazy val request =
    new GetTransactionByIdRequest(unwrappedLedgerId, UUID.randomUUID.toString, List(mainActor))

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(TransactionServiceGrpc.stub(channel), context.token).getFlatTransactionById(request)

}
