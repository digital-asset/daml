// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.transaction_service.{GetLedgerEndRequest, TransactionServiceGrpc}

import scala.concurrent.Future

final class GetLedgerEndAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "TransactionService#GetLedgerEnd"

  private lazy val request = new GetLedgerEndRequest(unwrappedLedgerId)

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(TransactionServiceGrpc.stub(channel), context.token).getLedgerEnd(request)

}
