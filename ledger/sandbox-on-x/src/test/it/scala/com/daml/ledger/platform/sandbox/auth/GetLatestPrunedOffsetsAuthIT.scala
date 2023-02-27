// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.transaction_service.{
  GetLatestPrunedOffsetsRequest,
  TransactionServiceGrpc,
}

import scala.concurrent.Future

class GetLatestPrunedOffsetsAuthIT extends PublicServiceCallAuthTests {
  override def serviceCallName: String = "TransactionService#GetLatestPrunedOffsets"

  private lazy val request = new GetLatestPrunedOffsetsRequest()

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(TransactionServiceGrpc.stub(channel), context.token).getLatestPrunedOffsets(request)

}
