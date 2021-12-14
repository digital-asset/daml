// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.platform.testing.StreamConsumer
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class CompletionStreamAuthIT
    extends ExpiringStreamServiceCallAuthTests[CompletionStreamResponse] {

  override def serviceCallName: String = "CommandCompletionService#CompletionStream"

  override protected def stream
      : Option[String] => StreamObserver[CompletionStreamResponse] => Unit = streamFor(
    serviceCallName
  )

  private def mkRequest(applicationId: String) =
    new CompletionStreamRequest(
      unwrappedLedgerId,
      applicationId,
      List(mainActor),
      Some(ledgerBegin),
    )

  private def streamFor(
      applicationId: String
  ): Option[String] => StreamObserver[CompletionStreamResponse] => Unit =
    token =>
      observer =>
        stub(CommandCompletionServiceGrpc.stub(channel), token)
          .completionStream(mkRequest(applicationId), observer)

  private def serviceCallWithoutApplicationId(token: Option[String]): Future[Any] =
    submitAndWait().flatMap(_ =>
      new StreamConsumer[CompletionStreamResponse](streamFor("")(token)).first()
    )

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

  it should "allow calls with an empty application ID for a token with an application id" in {
    expectSuccess(serviceCallWithoutApplicationId(canReadAsMainActorActualApplicationId))
  }

  it should "deny calls with an empty application ID for a token without an application id" in {
    expectInvalidArgument(serviceCallWithoutApplicationId(canReadAsMainActor))
  }
}
