// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfigurationServiceGrpc
}
import com.daml.platform.testing.StreamConsumer

import scala.concurrent.Future

final class GetLedgerConfigurationAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "LedgerConfigurationService#GetLedgerConfiguration"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    new StreamConsumer[GetLedgerConfigurationResponse](
      stub(LedgerConfigurationServiceGrpc.stub(channel), token)
        .getLedgerConfiguration(new GetLedgerConfigurationRequest(unwrappedLedgerId), _)).first()

}
