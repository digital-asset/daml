// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.util

import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import io.grpc.stub.{ClientCallStreamObserver, ClientResponseObserver}
import org.slf4j.Logger

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}

class ClientCancelled extends Exception

abstract class ObserverWithResult[RespT, Result](logger: Logger)
    extends ClientResponseObserver[Any, RespT] {

  private val requestStream: AtomicReference[ClientCallStreamObserver[?]] = new AtomicReference

  def streamName: String

  def result: Future[Result] = promise.future

  def completeWith(): Future[Result]

  override def onNext(value: RespT): Unit = ()

  override def onError(t: Throwable): Unit = {
    logger.info(withStreamName(s"Received error: $t"))
    t match {
      case ex: io.grpc.StatusRuntimeException if isServerShuttingDownError(ex) =>
        logger.info(s"Stopping reading the stream due to the server being shut down.")
        promise.completeWith(completeWith())
      case ex =>
        ex.getCause match {
          case _: ClientCancelled =>
            logger.info(s"Stopping reading the stream due to a client cancellation.")
            promise.completeWith(completeWith())
          case ex =>
            promise.failure(ex)
        }
    }
  }

  override def beforeStart(requestStream: ClientCallStreamObserver[Any]): Unit =
    this.requestStream set requestStream

  def cancel(): Unit =
    requestStream.get.cancel("", new ClientCancelled)

  private def isServerShuttingDownError(ex: io.grpc.StatusRuntimeException): Boolean =
    ErrorDetails.matches(ex, GrpcErrors.AbortedDueToShutdown)

  override def onCompleted(): Unit = {
    logger.info(withStreamName(s"Stream completed."))
    promise.completeWith(completeWith())
  }

  private val promise: Promise[Result] = Promise[Result]()

  protected def withStreamName(message: String) = s"[$streamName] $message"

}
