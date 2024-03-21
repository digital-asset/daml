// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.rs

import com.daml.grpc.adapter.TestExecutionSequencerFactory
import org.reactivestreams.tck.SubscriberWhiteboxVerification.SubscriberPuppet
import org.reactivestreams.tck.{SubscriberWhiteboxVerification, TestEnvironment}
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatestplus.testng.TestNGSuiteLike

// This suite uses Integer instead of Int because if we'd use Int, scala would interpret the `null` element from
// test required_spec213_onNext_mustThrowNullPointerExceptionWhenParametersAreNull as numerical 0.
class ServerSubscriberWhiteboxTest
    extends SubscriberWhiteboxVerification[Integer](new TestEnvironment(500, 500, false))
    with TestNGSuiteLike {

  override def createSubscriber(
      probe: SubscriberWhiteboxVerification.WhiteboxSubscriberProbe[Integer]
  ): Subscriber[Integer] = {
    val so = new MockServerCallStreamObserver[Integer]
    val sub = new ServerSubscriber[Integer](
      so,
      TestExecutionSequencerFactory.instance.getExecutionSequencer,
    ) {
      override def onSubscribe(subscription: Subscription): Unit = {
        super.onSubscribe(subscription)
        probe.registerOnSubscribe(new SubscriberPuppet {
          override def triggerRequest(elements: Long): Unit = {
            for (_ <- 1L to elements) so.demandResponse()
          }

          override def signalCancel(): Unit = {
            so.signalCancellation()
          }
        })
      }

      override def onNext(response: Integer): Unit = {
        super.onNext(response)
        probe.registerOnNext(response)
      }

      override def onError(throwable: Throwable): Unit = {
        super.onError(throwable)
        probe.registerOnError(throwable)
      }

      override def onComplete(): Unit = {
        super.onComplete()
        probe.registerOnComplete()
      }
    }
    so.demandResponse()
    sub
  }

  override def createElement(element: Int): Integer = element
}
