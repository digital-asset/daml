// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs

import com.daml.grpc.adapter.SingleThreadExecutionSequencerPool
import com.daml.grpc.adapter.client.rs.BufferingSubscriptionTest.{
  MockClientCallStreamObserver,
  NoOpSubscriber,
}
import io.grpc.stub.ClientCallStreamObserver
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BufferingSubscriptionTest extends AnyWordSpecLike with Matchers {

  private val sequencePool = new SingleThreadExecutionSequencerPool("test")

  "Buffering subscription" should {
    "propagate cancellation when elements are sent" in {
      val observer = new MockClientCallStreamObserver()
      val subscription = newBufferingSubscription(observer)
      subscription.onNextElement(observer)
      subscription.cancel()
      eventually {
        subscription.isCancelled should be(true)
        observer.cancelled should be(true)
      }
    }

    "propagate cancellation when no elements were sent" in {
      val observer = new MockClientCallStreamObserver()
      val subscription = newBufferingSubscription(observer)
      subscription.cancel()
      eventually {
        subscription.isCancelled should be(true)
        observer.cancelled should be(true)
      }
    }
  }
  private def newBufferingSubscription(
      observer: MockClientCallStreamObserver
  ) = {
    new BufferingSubscription[String](
      (_: Subscriber[_ >: String]) => (),
      () => observer,
      sequencePool.getExecutionSequencer,
      NoOpSubscriber,
      "test",
    )
  }
}

object BufferingSubscriptionTest {
  object NoOpSubscriber extends Subscriber[String] {
    override def onSubscribe(s: Subscription): Unit = ???
    override def onNext(t: String): Unit = ???
    override def onError(t: Throwable): Unit = ???
    override def onComplete(): Unit = ???
  }
  class MockClientCallStreamObserver extends ClientCallStreamObserver[String] {
    @volatile
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
