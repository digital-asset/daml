// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  override protected def stream(
      context: ServiceCallContext
  ): StreamObserver[GetTransactionTreesResponse] => Unit =
    observer =>
      stub(TransactionServiceGrpc.stub(channel), context.token)
        .getTransactionTrees(request, observer)

}
