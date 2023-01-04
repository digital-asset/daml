// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.operation

import akka.stream.scaladsl.Sink
import com.daml.grpc.adapter.client.ReferenceClientCompatibilityCheck
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.daml.grpc.adapter.{ExecutionSequencerFactory, TestExecutionSequencerFactory}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.platform.hello.HelloRequest
import io.grpc.StatusRuntimeException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.net.SocketAddress

abstract class AkkaClientWithReferenceServiceSpecBase(
    override protected val socketAddress: Option[SocketAddress]
) extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with AkkaBeforeAndAfterAll
    with ScalaFutures
    with ReferenceClientCompatibilityCheck
    with AkkaClientCompatibilityCheck
    with ReferenceServiceFixture {

  protected implicit val esf: ExecutionSequencerFactory = TestExecutionSequencerFactory.instance

  "Akka client" when {

    "testing with reference service" should {
      behave like akkaClientCompatible(clientStub)
    }

    "handle request errors when server streaming" in {
      val elemsF = ClientAdapter
        .serverStreaming(HelloRequest(-1), clientStub.serverStreaming)
        .runWith(Sink.ignore)

      whenReady(elemsF.failed)(_ shouldBe a[StatusRuntimeException])
    }

  }
}
