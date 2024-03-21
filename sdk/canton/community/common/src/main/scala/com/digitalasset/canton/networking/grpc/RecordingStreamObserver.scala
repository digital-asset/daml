// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import io.grpc.stub.StreamObserver

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

/** Stream observer that records all incoming events.
  */
class RecordingStreamObserver[RespT](
    completeAfter: Int = Int.MaxValue,
    filter: RespT => Boolean = (_: RespT) => true,
) extends StreamObserver[RespT] {

  val responseQueue: java.util.concurrent.BlockingQueue[RespT] =
    new java.util.concurrent.LinkedBlockingDeque[RespT](completeAfter)

  def responses: Seq[RespT] = responseQueue.asScala.toSeq

  private val completionP: Promise[Unit] = Promise[Unit]()
  val completion: Future[Unit] = completionP.future
  def result(implicit executionContext: ExecutionContext): Future[Seq[RespT]] =
    completionP.future.map(_ => responses)

  val responseCount: AtomicInteger = new AtomicInteger()

  override def onNext(value: RespT): Unit = if (filter(value)) {
    responseQueue.offer(value)
    if (responseCount.incrementAndGet() >= completeAfter) {
      val _ = completionP.trySuccess(())
    }
  }

  override def onError(t: Throwable): Unit = {
    val _ = completionP.tryFailure(t)
  }

  override def onCompleted(): Unit = {
    val _ = completionP.trySuccess(())
  }
}
