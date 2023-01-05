// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.rs

import com.daml.grpc.adapter.TestExecutionSequencerFactory
import org.reactivestreams.Subscriber
import org.reactivestreams.tck.{SubscriberBlackboxVerification, TestEnvironment}
import org.scalatestplus.testng.TestNGSuiteLike

class ServerSubscriberBlackboxTest
    extends SubscriberBlackboxVerification[Integer](new TestEnvironment(500, 500, false))
    with TestNGSuiteLike {
  override def createSubscriber(): Subscriber[Integer] = {
    val so = new MockServerCallStreamObserver[Integer]
    val sub = new ServerSubscriber[Integer](
      so,
      TestExecutionSequencerFactory.instance.getExecutionSequencer,
    )
    so.demandResponse()
    sub
  }

  override def createElement(i: Int): Integer = i
}
