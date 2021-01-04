// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  // The completion stream is the one read-only endpoint where the application
  // identifier is part of the request. Hence, we didn't put this test in a shared
  // trait to no have to have to override the result for the transaction service
  // authorization tests.

  it should "allow calls with the correct application ID" in {
    expectSuccess(serviceCallWithToken(canReadAsMainActorActualApplicationId))
  }

  it should "deny calls with a random application ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsMainActorRandomApplicationId))
  }

}
