// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.time.Duration

import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import com.digitalasset.platform.sandbox.services.SubmitAndWaitDummyCommand
import com.digitalasset.platform.testing.StreamConsumer
import com.digitalasset.timer.Delayed
import io.grpc.Status
import io.grpc.stub.StreamObserver

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

  it should "break a stream in flight upon read-only token expiration" in {
    val _ = Delayed.Future.by(10.seconds)(submitAndWait())
    expectExpiration(canReadAsMainActorExpiresInFiveSeconds).map(_ => succeed)
  }

  it should "break a stream in flight upon read/write token expiration" in {
    val _ = Delayed.Future.by(10.seconds)(submitAndWait())
    expectExpiration(canActAsMainActorExpiresInFiveSeconds).map(_ => succeed)
  }

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    submitAndWait().flatMap(_ => new StreamConsumer[T](stream(token)).first())

}
