// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs

import com.daml.grpc.adapter.SingleThreadExecutionSequencerPool
import com.daml.grpc.adapter.client.rs.BufferingSubscriptionTest.MockClientCallStreamObserver
import io.grpc.stub.ClientCallStreamObserver
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BufferingSubscriptionTest extends AnyWordSpecLike with Matchers {

  private val sequencePool = new SingleThreadExecutionSequencerPool("test")

  "Buffering subscription" should {

    "propagate cancellation when no elements were sent" in {
      val observer = new MockClientCallStreamObserver()
      val subscription = new BufferingSubscription[String](
        (_: Subscriber[_ >: String]) => (),
        () => observer,
        sequencePool.getExecutionSequencer,
        new Subscriber[String] {
          override def onSubscribe(s: Subscription): Unit = ???
          override def onNext(t: String): Unit = ???
          override def onError(t: Throwable): Unit = ???
          override def onComplete(): Unit = ???
        },
        "test",
      )
      subscription.cancel()
      eventually {
        subscription.isCancelled should be(true)
        observer.cancelled should be(true)
      }
    }
  }
}
object BufferingSubscriptionTest {
  class MockClientCallStreamObserver extends ClientCallStreamObserver[String] {
    var cancelled = false
    override def cancel(message: String, cause: Throwable): Unit = cancelled = true
    override def isReady: Boolean = ???
    override def setOnReadyHandler(onReadyHandler: Runnable): Unit = ???
    override def request(count: Int): Unit = ???
    override def setMessageCompression(enable: Boolean): Unit = ???
    override def disableAutoInboundFlowControl(): Unit = ???
    override def onNext(value: String): Unit = ???
    override def onError(t: Throwable): Unit = ???
    override def onCompleted(): Unit = ???
  }
}
