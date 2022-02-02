// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  TimeServiceGrpc,
}
import com.daml.platform.testing.StreamConsumer

import scala.concurrent.Future

final class GetTimeAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "TimeService#GetTime"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    new StreamConsumer[GetTimeResponse](
      stub(TimeServiceGrpc.stub(channel), token)
        .getTime(new GetTimeRequest(unwrappedLedgerId), _)
    ).first()

}
