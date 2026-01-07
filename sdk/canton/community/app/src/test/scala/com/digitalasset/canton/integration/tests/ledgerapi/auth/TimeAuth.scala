// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  TimeServiceGrpc,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment

import scala.concurrent.Future

trait TimeAuth {

  this: ServiceCallAuthTests =>

  def loadTimeNow(
      token: Option[String]
  )(implicit env: TestConsoleEnvironment): Future[Option[GetTimeResponse]] = {
    import env.*
    stub(TimeServiceGrpc.stub(channel), token)
      .getTime(new GetTimeRequest())
      .map(Some(_))

  }
}
