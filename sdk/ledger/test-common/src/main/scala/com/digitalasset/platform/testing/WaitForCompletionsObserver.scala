// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import java.util.concurrent.atomic.AtomicInteger

import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import io.grpc.Context
import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

object WaitForCompletionsObserver {

  def apply(n: Int)(attach: StreamObserver[CompletionStreamResponse] => Unit): Future[Unit] = {
    if (n < 1) {
      Future.failed(
        new IllegalArgumentException(
          s"Invalid argument $n, `WaitForCompletionsObserver` requires a strictly positive integer as an argument"
        )
      )
    } else {
      val observer = new WaitForCompletionsObserver(n)
      attach(observer)
      observer.result
    }
  }

}

final class WaitForCompletionsObserver private (expectedCompletions: Int)
    extends StreamObserver[CompletionStreamResponse] {

  private val promise = Promise[Unit]()
  private val counter = new AtomicInteger(0)

  val result: Future[Unit] = promise.future

  override def onNext(v: CompletionStreamResponse): Unit = {
    val total = counter.addAndGet(v.completions.size)
    if (total >= expectedCompletions) {
      promise.trySuccess(())
      Context.current().withCancellation().cancel(null)
      ()
    }
  }

  override def onError(throwable: Throwable): Unit = {
    val _ = promise.tryFailure(throwable)
  }

  override def onCompleted(): Unit = {
    val _ = promise.tryFailure(new RuntimeException("no more completions"))
  }

}
