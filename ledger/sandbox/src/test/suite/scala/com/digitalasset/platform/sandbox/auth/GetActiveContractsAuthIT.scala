// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import com.digitalasset.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.platform.testing.StreamConsumer

import scala.concurrent.Future

final class GetActiveContractsAuthIT extends ReadOnlyServiceCallAuthTests {

  override def serviceCallName: String = "ActiveContractsService#GetActiveContracts"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    new StreamConsumer[GetActiveContractsResponse](
      stub(ActiveContractsServiceGrpc.stub(channel), token)
        .getActiveContracts(
          new GetActiveContractsRequest(unwrappedLedgerId, txFilterFor(mainActor)),
          _)).first()

}
