// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.grpc.sampleservice.implementations.ReferenceImplementation
import com.daml.ledger.client.GrpcChannel
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.GrpcServerSpec._
import com.daml.platform.hello.{HelloRequest, HelloServiceGrpc}
import com.daml.ports.Port
import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class GrpcServerSpec extends AsyncWordSpec with Matchers with TestResourceContext {
  "a GRPC server" should {
    "handle a request to a valid service" in {
      resources().use { channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          response <- helloService.single(HelloRequest(7))
        } yield {
          response.respInt shouldBe 14
        }
      }
    }

    "fail with a nice exception" in {
      resources().use { channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .fails(HelloRequest(7, ByteString.copyFromUtf8("This is some text.")))
            .failed
        } yield {
          exception.getMessage shouldBe "INTERNAL: This is some text."
        }
      }
    }

    "fail with a nice exception, even when the text is quite long" in {
      val length = 2 * 1024
      val exceptionMessage = "There was an error. " + Stream.continually("x").take(length).mkString

      resources().use { channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .fails(HelloRequest(7, ByteString.copyFromUtf8(exceptionMessage)))
            .failed
        } yield {
          exception.getMessage shouldBe s"INTERNAL: $exceptionMessage"
        }
      }
    }

    "fail with a nice exception, even when the text is too long for the client to process" in {
      val length = 1024 * 1024
      val exceptionMessage =
        "There was an error. " +
          Stream.continually("x").take(length).mkString +
          " And then some extra text that won't be sent."

      resources().use { channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .fails(HelloRequest(7, ByteString.copyFromUtf8(exceptionMessage)))
            .failed
        } yield {
          // We don't want to test the exact message content, just that it does indeed contain a
          // large chunk of the response error message, followed by "...".
          exception.getMessage should fullyMatch regex "INTERNAL: There was an error. x{1024,}\\.\\.\\.".r
        }
      }
    }
  }
}

object GrpcServerSpec {

  private val maxInboundMessageSize = 4 * 1024 * 1024 /* copied from the Sandbox configuration */

  private val clientConfiguration = LedgerClientConfiguration(
    applicationId = classOf[GrpcServerSpec].getSimpleName,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
  )

  private def resources(): ResourceOwner[ManagedChannel] =
    for {
      eventLoopGroups <- new ServerEventLoopGroups.Owner(
        classOf[GrpcServerSpec].getSimpleName,
        workerParallelism = sys.runtime.availableProcessors(),
        bossParallelism = 1,
      )
      server <- new GrpcServer.Owner(
        address = None,
        desiredPort = Port.Dynamic,
        maxInboundMessageSize = maxInboundMessageSize,
        metrics = new Metrics(new MetricRegistry),
        eventLoopGroups = eventLoopGroups,
        services = Seq(new ReferenceImplementation),
      )
      channel <- new GrpcChannel.Owner(Port(server.getPort), clientConfiguration)
    } yield channel

}
