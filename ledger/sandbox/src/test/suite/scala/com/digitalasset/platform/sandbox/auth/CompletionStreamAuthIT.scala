// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import io.grpc.stub.StreamObserver

final class CompletionStreamAuthIT
    extends ExpiringStreamServiceCallAuthTests[CompletionStreamResponse] {

  override def serviceCallName: String = "CommandCompletionService#CompletionStream"

  private lazy val request =
    new CompletionStreamRequest(
      unwrappedLedgerId,
      serviceCallName,
      List(mainActor),
      Some(ledgerBegin))

  override protected def stream
    : Option[String] => StreamObserver[CompletionStreamResponse] => Unit =
    token =>
      observer =>
        stub(CommandCompletionServiceGrpc.stub(channel), token).completionStream(request, observer)

}
