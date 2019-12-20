// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import com.digitalasset.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  TimeServiceGrpc
}
import com.digitalasset.platform.testing.StreamConsumer

import scala.concurrent.Future

final class GetTimeAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "TimeService#GetTime"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    new StreamConsumer[GetTimeResponse](
      stub(TimeServiceGrpc.stub(channel), token)
        .getTime(new GetTimeRequest(unwrappedLedgerId), _)).first()

}
