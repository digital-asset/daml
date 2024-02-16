// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.error.{DamlError, ErrorGenerator}
import com.daml.grpc.sampleservice.implementations.HelloServiceReferenceImplementation
import com.daml.ledger.resources.{ResourceOwner, TestResourceContext}
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.digitalasset.canton.config.RequireTypes.Port
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
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ServerInterceptor, StatusRuntimeException}
import org.scalacheck.Gen
import org.scalatest.compatible.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.Executors
import scala.concurrent.Future

final class GrpcServerSpec
    extends AsyncWordSpec
    with BaseTest
    with TestResourceContext
    with MetricValues
    with HasExecutionContext {

  "a GRPC server" should {
    "handle a request to a valid service" in {
      resources(loggerFactory).use { channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          response <- helloService.single(HelloRequest(7))
        } yield {
          response.respInt shouldBe 14
        }
      }
    }

    "fail with a nice exception" in {
      resources(loggerFactory).use { channel =>
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
      resources(loggerFactory).use { channel =>
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

      resources(loggerFactory).use { channel =>
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

    "fuzzy ensure non-security sensitive errors are forwarded gracefully" in {
      val checkerValue = "Sentinel error"
      val nonSecuritySensitiveErrorGen =
        ErrorGenerator
          .errorGenerator(
            securitySensitive = Some(false),
            // Only generate errors that have a grpc code / meant to be sent over the wire
            additionalErrorCategoryFilter = _.grpcCode.isDefined,
          )
          .map(err => err.copy(cause = s"$checkerValue - ${err.cause}"))

      fuzzTestErrorCodePropagation(
        errorCodeGen = nonSecuritySensitiveErrorGen,
        expectedIncludedMessage = checkerValue,
      )
    }

    "fuzzy ensure security sensitive errors are forwarded gracefully" in {
      val securitySensitiveErrorGen = ErrorGenerator.errorGenerator(securitySensitive = Some(true))

      fuzzTestErrorCodePropagation(
        errorCodeGen = securitySensitiveErrorGen,
        expectedIncludedMessage =
          "An error occurred. Please contact the operator and inquire about the request",
      )
    }

    "install rate limit interceptor" in {
      val metricsFactory = new InMemoryMetricsFactory
      val metrics = new Metrics(metricsFactory, metricsFactory, new MetricRegistry)
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
        val helloService = HelloServiceGrpc.stub(channel)
        helloService.single(HelloRequest(7)).failed.map {
          case s: StatusRuntimeException =>
            s.getStatus.getDescription shouldBe overLimitRejection.asGrpcStatus.getMessage
          case o => fail(s"Expected StatusRuntimeException, not $o")
        }
      }
    }

  }

  private def fuzzTestErrorCodePropagation(
      errorCodeGen: Gen[DamlError],
      expectedIncludedMessage: String,
  ): Future[Assertion] = {
    val numberOfIterations = 100

    val randomExceptionGeneratingService = new HelloServiceReferenceImplementation {
      override def fails(request: HelloRequest): Future[HelloResponse] =
        Future.failed(errorCodeGen.sample.value.asGrpcError)
    }

    resources(loggerFactory, service = randomExceptionGeneratingService).use { channel =>
      val helloService = HelloServiceGrpc.stub(channel)
      for (_ <- 1 to numberOfIterations) {
        val f = for {
          exception <- helloService
            .fails(HelloRequest(0, ByteString.empty()))
            .failed
        } yield {
          exception.getMessage should include(expectedIncludedMessage)
        }
        f.futureValue
      }
      succeed
    }
  }
}

object GrpcServerSpec {

  private val maxInboundMessageSize = 4 * 1024 * 1024 /* copied from the Sandbox configuration */

  private val rateLimitingConfig = RateLimitingConfig.Default

  class TestedHelloService extends HelloServiceReferenceImplementation {
    override def fails(request: HelloRequest): Future[HelloResponse] = {
      val loggerFactory = SuppressingLogger(getClass)
      val logger = loggerFactory.getTracedLogger(getClass)
      val errorLogger = ErrorLoggingContext(logger, LoggingContextWithTrace.ForTesting)

      Future.failed(
        RequestValidationErrors.InvalidArgument
          .Reject(request.payload.toStringUtf8)(errorLogger)
          .asGrpcError
      )
    }
  }

  private def resources(
      loggerFactory: NamedLoggerFactory,
      metrics: Metrics = Metrics.ForTesting,
      interceptors: List[ServerInterceptor] = List.empty,
      service: HelloServiceReferenceImplementation = new TestedHelloService,
  ): ResourceOwner[ManagedChannel] =
    for {
      executor <- ResourceOwner.forExecutorService(() => Executors.newSingleThreadExecutor())
      server <- GrpcServer.owner(
        address = None,
        desiredPort = Port.Dynamic,
        maxInboundMessageSize = maxInboundMessageSize,
        metrics = metrics,
        servicesExecutor = executor,
        services = Seq(service),
        interceptors = interceptors,
        loggerFactory = loggerFactory,
      )
      channel <- new GrpcChannel.Owner(
        Port.tryCreate(server.getPort),
        LedgerClientChannelConfiguration.InsecureDefaults,
      )
    } yield channel

}
