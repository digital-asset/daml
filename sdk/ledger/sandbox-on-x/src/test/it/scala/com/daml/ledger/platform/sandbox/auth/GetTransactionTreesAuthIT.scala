// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  TransactionServiceGrpc,
}
import com.daml.platform.sandbox.services.SubmitAndWaitDummyCommand
import io.grpc.stub.StreamObserver

final class GetTransactionTreesAuthIT
    extends ExpiringStreamServiceCallAuthTests[GetTransactionTreesResponse]
    with SubmitAndWaitDummyCommand {

  override def serviceCallName: String = "TransactionService#GetTransactionTrees"

  private lazy val request =
    new GetTransactionsRequest(unwrappedLedgerId, Option(ledgerBegin), None, txFilterFor(mainActor))

  override protected def stream
      : Option[String] => StreamObserver[GetTransactionTreesResponse] => Unit =
    token =>
      observer =>
        stub(TransactionServiceGrpc.stub(channel), token).getTransactionTrees(request, observer)

}
