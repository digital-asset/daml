// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import org.apache.pekko.stream.scaladsl.Sink
import com.daml.grpc.adapter.client.ReferenceClientCompatibilityCheck
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.grpc.adapter.{ExecutionSequencerFactory, TestExecutionSequencerFactory}
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.platform.hello.HelloRequest
import io.grpc.StatusRuntimeException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.net.SocketAddress

abstract class PekkoClientWithReferenceServiceSpecBase(
    override protected val socketAddress: Option[SocketAddress]
) extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with PekkoBeforeAndAfterAll
    with ScalaFutures
    with ReferenceClientCompatibilityCheck
    with PekkoClientCompatibilityCheck
    with ReferenceServiceFixture {

  protected implicit val esf: ExecutionSequencerFactory = TestExecutionSequencerFactory.instance

  "Pekko client" when {

    "testing with reference service" should {
      behave like pekkoClientCompatible(clientStub)
    }

    "handle request errors when server streaming" in {
      val elemsF = ClientAdapter
        .serverStreaming(HelloRequest(-1), clientStub.serverStreaming)
        .runWith(Sink.ignore)

      whenReady(elemsF.failed)(_ shouldBe a[StatusRuntimeException])
    }

  }
}
