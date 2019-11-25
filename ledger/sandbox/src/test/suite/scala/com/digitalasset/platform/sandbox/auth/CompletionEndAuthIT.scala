// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import com.digitalasset.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest
}

import scala.concurrent.Future

final class CompletionEndAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "CommandCompletionService#CompletionEnd"

  private lazy val request = new CompletionEndRequest(unwrappedLedgerId)

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(CommandCompletionServiceGrpc.stub(channel), token).completionEnd(request)

}
