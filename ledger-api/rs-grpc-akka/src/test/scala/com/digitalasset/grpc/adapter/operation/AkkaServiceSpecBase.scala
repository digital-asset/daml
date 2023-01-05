// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import java.util.concurrent.TimeUnit._
import java.net.SocketAddress

import com.daml.grpc.adapter.{ExecutionSequencerFactory, TestExecutionSequencerFactory}
import com.daml.grpc.adapter.client.ReferenceClientCompatibilityCheck
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.awaitility.Awaitility._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

abstract class AkkaServiceSpecBase(override protected val socketAddress: Option[SocketAddress])
    extends AnyWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with ScalaFutures
    with ReferenceClientCompatibilityCheck
    with AkkaClientCompatibilityCheck
    with AkkaServiceFixture {

  "Akka service" when {

    "testing with reference client" should {
      behave like referenceClientCompatible(clientStub)
    }

    "testing with akka client" should {
      behave like akkaClientCompatible(clientStub)
    }

    "asked for server call count" should {

      "return the correct number" in {
        await()
          .atMost(5, SECONDS)
          .until(() =>
            service.getServerStreamingCalls == 7
          ) // The number of calls in the previous tests
      }
    }
  }

  override implicit protected def esf: ExecutionSequencerFactory =
    TestExecutionSequencerFactory.instance
}
