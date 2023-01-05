// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.rs

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import io.grpc.stub.ServerCallStreamObserver

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}

class MockServerCallStreamObserver[T] extends ServerCallStreamObserver[T] {

  private val completionPromise = Promise[Unit]()
  val demandForRequests = new AtomicInteger(0)
  private val onReadyHandler = new AtomicReference[Runnable](() => ())
  private val onCancelHandler = new AtomicReference[Runnable](() => ())
  private val responseBuffer = new ArrayBuffer[T]()
  private val cancelled = new AtomicBoolean(false)
  private val unsatisfiedResponseDemand = new AtomicInteger(0)

  def completionFuture: Future[Unit] = completionPromise.future

  def elements: Vector[T] = responseBuffer.toVector

  def elementsWhenCompleted(implicit ec: ExecutionContext): Future[Vector[T]] =
    completionFuture.map(_ => responseBuffer.toVector)

  def demandResponse(count: Int = 1): Unit = {
    require(count > 0)
    if (unsatisfiedResponseDemand.getAndAdd(count) == 0) {
      onReadyHandler.get().run()
    }

  }

  def signalCancellation(): Unit = {
    cancelled.set(true)
    onCancelHandler.get().run()
  }

  override def isCancelled: Boolean = cancelled.get()

  override def setOnCancelHandler(onCancelHandler: Runnable): Unit = {
    this.onCancelHandler.set(onCancelHandler)
  }

  override def setCompression(compression: String): Unit = ???

  override def isReady: Boolean = unsatisfiedResponseDemand.get() > 0

  override def setOnReadyHandler(onReadyHandler: Runnable): Unit = {
    this.onReadyHandler.set(onReadyHandler)
  }

  override def disableAutoInboundFlowControl(): Unit = ()

  override def request(count: Int): Unit = {
    demandForRequests.addAndGet(count)
    ()
  }

  override def setMessageCompression(enable: Boolean): Unit = ???

  // This is not thread safe on purpose since we expect CallStreamObserver method calls
  // to be either synchronized or executed from a single thread.
  override def onNext(value: T): Unit = {
    unsatisfiedResponseDemand.decrementAndGet()
    responseBuffer.append(value)
  }

  override def onError(t: Throwable): Unit = completionPromise.failure(t)

  override def onCompleted(): Unit = completionPromise.success(())

}
