// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.utils

import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

object FirstElementObserver {

  def apply[T]: (StreamObserver[T], Future[Option[T]]) = {
    val promise = Promise[Option[T]]

    val streamObserver = new StreamObserver[T] {
      override def onNext(value: T): Unit = {
        val _ = promise.trySuccess(Some(value))
      }

      override def onError(t: Throwable): Unit = {
        val _ = promise.tryFailure(t)
      }

      override def onCompleted(): Unit = {
        val _ = promise.trySuccess(None)
      }
    }
    streamObserver -> promise.future
  }
}
