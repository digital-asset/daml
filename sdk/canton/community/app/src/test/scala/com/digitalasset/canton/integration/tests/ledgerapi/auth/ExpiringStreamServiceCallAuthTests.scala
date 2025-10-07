// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.timer.Delayed
import com.digitalasset.canton.auth.AuthorizationChecksErrors.AccessTokenExpired
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommand
import io.grpc.Status
import io.grpc.stub.StreamObserver

import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

trait ExpiringStreamServiceCallAuthTests[T]
    extends ReadOnlyServiceCallAuthTests
    with SubmitAndWaitDummyCommand {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  protected def stream(
      context: ServiceCallContext,
      env: TestConsoleEnvironment,
  ): StreamObserver[T] => Unit

  private def expectExpiration(context: ServiceCallContext, mainActorId: String)(implicit
      env: TestConsoleEnvironment
  ): Future[Unit] = {
    val promise = Promise[Unit]()
    stream(
      context.copy(updateFormat = updateFormat(Some(mainActorId)), mainActorId = mainActorId),
      env,
    )(
      new StreamObserver[T] {
        @volatile private[this] var gotSomething = false
        def onNext(value: T): Unit =
          gotSomething = true
        def onError(t: Throwable): Unit =
          t match {
            case GrpcException(GrpcStatus(Status.Code.ABORTED, Some(description)), _)
                if gotSomething && description.contains(AccessTokenExpired.id) =>
              val _ = promise.trySuccess(())
            case _ =>
              val _ = promise.tryFailure(t)
          }
        def onCompleted(): Unit = {
          val _ =
            promise.tryFailure(new RuntimeException("stream completed before token expiration"))
        }
      }
    )
    promise.future
  }

  private def canActAsMainActorExpiresInFiveSeconds: ServiceCallContext =
    ServiceCallContext(
      Some(toHeader(expiringIn(Duration.ofSeconds(5), standardToken(mainActorActUser))))
    )

  // TODO(#23986)
//  private def canReadAsMainActorExpiresInFiveSeconds: ServiceCallContext =
//    ServiceCallContext(
//      Some(toHeader(expiringIn(Duration.ofSeconds(5), standardToken(mainActorReadUser))))
//    )

  serviceCallName should {
    // TODO(#23986)
//    "break a stream in-flight upon read-only token expiration" taggedAs securityAsset
//      .setAttack(
//        streamAttack(threat = "Present a read-only JWT upon expiration")
//      ) in { implicit env =>
//      import env.*
//      val mainActorId = getMainActorId
//      val _ = submitAndWaitAsMainActor(mainActorId)
//      val _ = Delayed.Future.by(10.seconds)(submitAndWaitAsMainActor(mainActorId))
//      expectExpiration(canReadAsMainActorExpiresInFiveSeconds, mainActorId)
//        .map(_ => succeed)
//        .futureValue
//    }

    "break a stream in-flight upon read/write token expiration" taggedAs securityAsset
      .setAttack(
        streamAttack(threat = "Present a read/write JWT upon expiration")
      ) in { implicit env =>
      import env.*
      val mainActorId = getMainActorId
      val _ = submitAndWaitAsMainActor(mainActorId)
      val _ = Delayed.Future.by(10.seconds)(submitAndWaitAsMainActor(mainActorId))
      expectExpiration(canActAsMainActorExpiresInFiveSeconds, mainActorId)
        .map(_ => succeed)
        .futureValue
    }
  }

}
