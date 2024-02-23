// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client

import com.daml.grpc.adapter.utils.BufferingObserver
import com.daml.platform.hello.HelloServiceGrpc.HelloServiceStub
import com.daml.platform.hello.{HelloRequest, HelloResponse}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

trait ReferenceClientCompatibilityCheck extends ResultAssertions with ScalaFutures with Matchers {
  self: AnyWordSpec =>

  def referenceClientCompatible(helloStub: => HelloServiceStub) = {

    "respond with the correct number of elements and correct content in 1-* setup" in {
      val observer = new BufferingObserver[HelloResponse]

      helloStub.serverStreaming(HelloRequest(elemCount), observer)

      whenReady(observer.resultsF)(assertElementsAreInOrder(elemCount.toLong))
    }

    "handle cancellation in 1-* setup" in {
      val observer = new BufferingObserver[HelloResponse](Some(halfCount))
      helloStub
        .serverStreaming(HelloRequest(elemCount), observer)

      whenReady(observer.resultsF)(assertElementsAreInOrder(halfCount.toLong))
    }
  }
}
