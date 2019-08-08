// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import io.grpc.Context
import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

object HeadObserver {

  def apply[A](attach: StreamObserver[A] => Unit): Future[Option[A]] = {
    val observer = new HeadObserver[A]
    attach(observer)
    observer.result
  }

}

final class HeadObserver[A] extends StreamObserver[A] {

  private[this] val promise = Promise[Option[A]]()

  val result: Future[Option[A]] = promise.future

  override def onNext(value: A): Unit = promise.synchronized {
    promise.trySuccess(Some(value))
    val _ = Context
      .current()
      .withCancellation()
      .cancel(new RuntimeException("Interested only in the first item"))
  }

  override def onError(t: Throwable): Unit = promise.synchronized {
    val _ = promise.tryFailure(t)
  }

  override def onCompleted(): Unit = promise.synchronized {
    val _ = promise.trySuccess(None)
  }
}
