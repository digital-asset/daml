// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.rs

import java.util.concurrent.atomic.AtomicReference

import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.{Future, Promise}

class BufferingSubscriber[T] extends Subscriber[T] {

  private val subscription = Promise[Subscription]()
  private val elementsReceived = new AtomicReference[List[T]](Nil)
  private val completion = Promise[Unit]()

  def getElements: List[T] = elementsReceived.get().reverse
  def getCompletion: Future[Unit] = completion.future
  def getSubscription: Future[Subscription] = subscription.future

  override def onSubscribe(s: Subscription): Unit = subscription.success(s)

  override def onNext(t: T): Unit = {
    elementsReceived.updateAndGet(t :: _)
    ()
  }

  override def onError(t: Throwable): Unit = completion.failure(t)

  override def onComplete(): Unit = completion.success(())
}
