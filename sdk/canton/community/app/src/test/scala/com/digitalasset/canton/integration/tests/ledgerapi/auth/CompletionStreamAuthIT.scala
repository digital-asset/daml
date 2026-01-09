// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.test.StreamConsumer
import com.daml.jwt.StandardJWTPayload
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class CompletionStreamAuthIT
    extends ExpiringStreamServiceCallAuthTests[CompletionStreamResponse] {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "CommandCompletionService#CompletionStream"

  override val testCanReadAsMainActor: Boolean = false

  override protected def stream(
      context: ServiceCallContext,
      env: TestConsoleEnvironment,
  ): StreamObserver[CompletionStreamResponse] => Unit =
    streamFor(context)

  private def mkRequest(userId: String, mainActorId: String) =
    new CompletionStreamRequest(
      userId,
      List(mainActorId),
      0,
    )
  private def streamFor(
      context: ServiceCallContext
  ): StreamObserver[CompletionStreamResponse] => Unit =
    observer =>
      stub(CommandCompletionServiceGrpc.stub(channel), context.token)
        .completionStream(mkRequest(context.userId, context.mainActorId), observer)

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    val mainActorId = getMainActorId
    submitAndWaitAsMainActor(mainActorId).flatMap(_ =>
      new StreamConsumer[CompletionStreamResponse](
        stream(context.copy(mainActorId = mainActorId), env)
      ).first()
    )
  }

  override def serviceCallWithMainActorUser(
      userPrefix: String,
      rights: Vector[proto.Right.Kind],
      tokenModifier: StandardJWTPayload => StandardJWTPayload = identity,
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    for {
      (_, context) <- createUserByAdmin(
        userPrefix + mainActor,
        rights = rights.map(proto.Right(_)),
        tokenModifier = tokenModifier,
      )
      mainActorId = getMainActorId
      _ <- submitAndWait(context.token, "", party = mainActorId)
      _ <- new StreamConsumer[CompletionStreamResponse](
        streamFor(context.copy(mainActorId = mainActorId))
      ).first()
    } yield ()
  }

  // The completion stream is the one read-only endpoint where the application
  // identifier is part of the request. Hence, we didn't put this test in a shared
  // trait to no have to have to override the result for the transaction service
  // authorization tests.

  serviceCallName should {
    "allow calls with user ID matching the the token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with user ID matching the token"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(canActAsMainActor.copy(userIdOverride = Some(mainActorActUser)))
      )
    }

    "deny calls with an user ID not matching the token" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Present a JWT with an user ID not matching the token")
    ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(canActAsMainActor.copy(userIdOverride = Some(mainActorReadUser)))
      )
    }

    "allow calls with an empty user ID and a token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with an empty user ID and a token"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canActAsMainActor.copy(userIdOverride = Some(""))
        )
      )
    }
  }
}
