// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.api.v0.HelloServiceGrpc.HelloService
import com.digitalasset.canton.grpc.sampleservice.HelloServiceReferenceImplementation
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  SuppressingLogger,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.GrpcServerSpec.*
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.apiserver.ratelimiting.{
  LimitResult,
  RateLimitingInterceptor,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.{BindableService, ManagedChannel, ServerInterceptor, StatusRuntimeException}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

final class GrpcServerSpec
    extends AsyncWordSpec
    with BaseTest
    with TestResourceContext
    with HasExecutionContext
    with MetricValues {
  "a GRPC server" should {
    "handle a request to a valid service" in {
      resources(loggerFactory).use { channel =>
        val helloService = v0.HelloServiceGrpc.stub(channel)
        for {
          response <- helloService.hello(v0.Hello.Request("foo"))
        } yield {
          response.msg shouldBe "foofoo"
        }
      }
    }

    "fail with a nice exception" in {
      resources(loggerFactory, helloService = new FailingHelloService()(_)).use { channel =>
        val helloService = v0.HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .hello(v0.Hello.Request("This is some text."))
            .failed
        } yield {
          exception.getMessage shouldBe "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: This is some text."
        }
      }
    }

    "fail with a nice exception, even when the text is quite long" in {
      val errorMessage = "There was an error. " + "x" * 2048
      val returnedMessage = "There was an error. " + "x" * 447 + "..."
      resources(loggerFactory, helloService = new FailingHelloService()(_)).use { channel =>
        val helloService = v0.HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .hello(v0.Hello.Request(errorMessage))
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

      resources(loggerFactory, helloService = new FailingHelloService()(_)).use { channel =>
        val helloService = v0.HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .hello(v0.Hello.Request(exceptionMessage))
            .failed
        } yield {
          // We don't want to test the exact message content, just that it does indeed contain a
          // large chunk of the response error message, followed by "...".
          exception.getMessage should fullyMatch regex "INVALID_ARGUMENT: INVALID_ARGUMENT\\(8,0\\): The submitted command has invalid arguments: There was an error. x{400,}\\.\\.\\.".r
        }
      }
    }

    "install rate limit interceptor" in {
      val metricsFactory = new InMemoryMetricsFactory
      val metrics = new Metrics(metricsFactory, metricsFactory, new MetricRegistry, true)
      val overLimitRejection = LedgerApiErrors.ThreadpoolOverloaded.Rejection(
        "test",
        "test",
        100,
        59,
        "test",
      )
      val rateLimitingInterceptor = RateLimitingInterceptor(
        loggerFactory,
        metrics,
        rateLimitingConfig,
        additionalChecks = List((_, _) => {
          LimitResult.OverLimit(
            overLimitRejection
          )
        }),
      )
      resources(loggerFactory, metrics, List(rateLimitingInterceptor)).use { channel =>
        val helloService = v0.HelloServiceGrpc.stub(channel)
        helloService.hello(v0.Hello.Request("foo")).failed.map {
          case s: StatusRuntimeException =>
            s.getStatus.getDescription shouldBe overLimitRejection.asGrpcStatus.getMessage
          case o => fail(s"Expected StatusRuntimeException, not $o")
        }
      }
    }

  }
}

object GrpcServerSpec {

  private val maxInboundMessageSize = 4 * 1024 * 1024 /* copied from the Sandbox configuration */

  private val rateLimitingConfig = RateLimitingConfig.Default

  class FailingHelloService(implicit ec: ExecutionContext)
      extends HelloServiceReferenceImplementation {
    override def hello(request: v0.Hello.Request): Future[v0.Hello.Response] = {
      val loggerFactory = SuppressingLogger(getClass)
      val logger = loggerFactory.getTracedLogger(getClass)
      val errorLogger = ErrorLoggingContext(logger, LoggingContextWithTrace.ForTesting)

      Future.failed(
        RequestValidationErrors.InvalidArgument
          .Reject(request.msg)(errorLogger)
          .asGrpcError
      )
    }
  }

  private def resources(
      loggerFactory: NamedLoggerFactory,
      metrics: Metrics = Metrics.ForTesting,
      interceptors: List[ServerInterceptor] = List.empty,
      helloService: ExecutionContext => BindableService with HelloService =
        new HelloServiceReferenceImplementation()(_),
  )(implicit ec: ExecutionContext): ResourceOwner[ManagedChannel] =
    for {
      executor <- ResourceOwner.forExecutorService(() => Executors.newSingleThreadExecutor())
      server <- GrpcServer.owner(
        address = None,
        desiredPort = Port.Dynamic,
        maxInboundMessageSize = maxInboundMessageSize,
        metrics = metrics,
        servicesExecutor = executor,
        services = Seq(helloService(ec)),
        interceptors = interceptors,
        loggerFactory = loggerFactory,
      )
      channel <- new GrpcChannel.Owner(
        Port.tryCreate(server.getPort),
        LedgerClientChannelConfiguration.InsecureDefaults,
      )
    } yield channel

}
