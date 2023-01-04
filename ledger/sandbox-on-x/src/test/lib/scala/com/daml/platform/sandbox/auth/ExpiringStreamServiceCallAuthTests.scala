// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.platform.sandbox.services.SubmitAndWaitDummyCommand
import com.daml.platform.testing.StreamConsumer
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import com.daml.timer.Delayed
import io.grpc.Status
import io.grpc.stub.StreamObserver

import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

trait ExpiringStreamServiceCallAuthTests[T]
    extends ReadOnlyServiceCallAuthTests
    with SubmitAndWaitDummyCommand {

  protected def stream: Option[String] => StreamObserver[T] => Unit

  private def expectExpiration(token: String): Future[Unit] = {
    val promise = Promise[Unit]()
    stream(Option(token))(new StreamObserver[T] {
      @volatile private[this] var gotSomething = false
      def onNext(value: T): Unit = {
        gotSomething = true
      }
      def onError(t: Throwable): Unit = {
        t match {
          case GrpcException(GrpcStatus(Status.Code.PERMISSION_DENIED, _), _) if gotSomething =>
            val _ = promise.trySuccess(())
          case NonFatal(e) =>
            val _ = promise.tryFailure(e)
        }
      }
      def onCompleted(): Unit = {
        val _ = promise.tryFailure(new RuntimeException("stream completed before token expiration"))
      }
    })
    promise.future
  }

  private def canActAsMainActorExpiresInFiveSeconds =
    toHeader(expiringIn(Duration.ofSeconds(5), readWriteToken(mainActor)))

  private def canReadAsMainActorExpiresInFiveSeconds =
    toHeader(expiringIn(Duration.ofSeconds(5), readOnlyToken(mainActor)))

  it should "break a stream in flight upon read-only token expiration" taggedAs securityAsset
    .setAttack(
      streamAttack(threat = "Present a read-only JWT upon expiration")
    ) in {
    val _ = Delayed.Future.by(10.seconds)(submitAndWaitAsMainActor())
    expectExpiration(canReadAsMainActorExpiresInFiveSeconds).map(_ => succeed)
  }

  it should "break a stream in flight upon read/write token expiration" taggedAs securityAsset
    .setAttack(
      streamAttack(threat = "Present a read/write JWT upon expiration")
    ) in {
    val _ = Delayed.Future.by(10.seconds)(submitAndWaitAsMainActor())
    expectExpiration(canActAsMainActorExpiresInFiveSeconds).map(_ => succeed)
  }

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    submitAndWaitAsMainActor().flatMap(_ => new StreamConsumer[T](stream(token)).first())

}
