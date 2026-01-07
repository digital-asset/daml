// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service.channel

import com.digitalasset.canton.util.Mutex
import io.grpc.stub.ServerCallStreamObserver

import java.util.concurrent.atomic.AtomicBoolean

/** Helper trait to ensure that a GRPC response StreamObserver is only completed or terminated with
  * an error once and to notify the owner upon completion.
  */
private[channel] trait CompletesGrpcResponseObserver[T] {
  this: AutoCloseable =>

  private[channel] def responseObserver: ServerCallStreamObserver[T]

  private lazy val responseObserverClosed = new AtomicBoolean(false)
  private val lock = new Mutex()

  /** Notify the owner that this instance has completed, no longer needs to be tracked, and is ready
    * for closing.
    */
  protected def notifyOnComplete(): Unit

  /** Completes the response observer in various ways. By default, calls onCompleted, but the caller
    * can also specify onError or a no-op to only flag completion.
    */
  final protected def complete(
      onComplete: ServerCallStreamObserver[T] => Unit = _.onCompleted()
  ): Unit =
    lock.exclusive {
      if (!responseObserverClosed.get()) {
        onComplete(responseObserver)
        responseObserverClosed.set(true)
        notifyOnComplete()
      }
    }
}
