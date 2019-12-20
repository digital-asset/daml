// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import com.digitalasset.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfigurationServiceGrpc
}
import com.digitalasset.platform.testing.StreamConsumer

import scala.concurrent.Future

final class GetLedgerConfigurationAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "LedgerConfigurationService#GetLedgerConfiguration"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    new StreamConsumer[GetLedgerConfigurationResponse](
      stub(LedgerConfigurationServiceGrpc.stub(channel), token)
        .getLedgerConfiguration(new GetLedgerConfigurationRequest(unwrappedLedgerId), _)).first()

}
