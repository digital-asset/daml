// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.util.concurrent.Executors

import com.codahale.metrics.MetricRegistry
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.grpc.sampleservice.implementations.HelloServiceReferenceImplementation
import com.daml.ledger.client.GrpcChannel
import com.daml.ledger.client.configuration.LedgerClientChannelConfiguration
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.GrpcServerSpec._
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.daml.ports.Port
import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

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
          exception.getMessage shouldBe "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: This is some text."
        }
      }
    }

    "fail with a nice exception, even when the text is quite long" in {
      val errorMessage = "There was an error. " + "x" * 2048
      val returnedMessage = "There was an error. " + "x" * 447 + "..."
      resources().use { channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .fails(HelloRequest(7, ByteString.copyFromUtf8(errorMessage)))
            .failed
        } yield {
          exception.getMessage shouldBe s"INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: $returnedMessage"
        }
      }
    }

    "fail with a nice exception, even when the text is too long for the client to process" in {
      val length = 1024 * 1024
      val exceptionMessage =
        "There was an error. " +
          LazyList.continually("x").take(length).mkString +
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
          exception.getMessage should fullyMatch regex "INVALID_ARGUMENT: INVALID_ARGUMENT\\(8,0\\): The submitted command has invalid arguments: There was an error. x{400,}\\.\\.\\.".r
        }
      }
    }
  }
}

object GrpcServerSpec {

  private val maxInboundMessageSize = 4 * 1024 * 1024 /* copied from the Sandbox configuration */

  class TestedHelloService extends HelloServiceReferenceImplementation {
    override def fails(request: HelloRequest): Future[HelloResponse] = {
      val errorLogger =
        DamlContextualizedErrorLogger.forTesting(getClass)
      Future.failed(
        LedgerApiErrors.RequestValidation.InvalidArgument
          .Reject(request.payload.toStringUtf8)(errorLogger)
          .asGrpcError
      )
    }
  }

  private def resources(): ResourceOwner[ManagedChannel] =
    for {
      executor <- ResourceOwner.forExecutorService(() => Executors.newSingleThreadExecutor())
      server <- GrpcServer.owner(
        address = None,
        desiredPort = Port.Dynamic,
        maxInboundMessageSize = maxInboundMessageSize,
        metrics = new Metrics(new MetricRegistry),
        servicesExecutor = executor,
        services = Seq(new TestedHelloService),
      )
      channel <- new GrpcChannel.Owner(
        Port(server.getPort),
        LedgerClientChannelConfiguration.InsecureDefaults,
      )
    } yield channel

}
