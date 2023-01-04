// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse,
}
import com.daml.platform.testing.StreamConsumer

import scala.concurrent.Future

final class GetActiveContractsAuthIT extends ReadOnlyServiceCallAuthTests {

  override def serviceCallName: String = "ActiveContractsService#GetActiveContracts"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    new StreamConsumer[GetActiveContractsResponse](
      stub(ActiveContractsServiceGrpc.stub(channel), context.token)
        .getActiveContracts(
          new GetActiveContractsRequest(unwrappedLedgerId, txFilterFor(mainActor)),
          _,
        )
    ).first()

}
