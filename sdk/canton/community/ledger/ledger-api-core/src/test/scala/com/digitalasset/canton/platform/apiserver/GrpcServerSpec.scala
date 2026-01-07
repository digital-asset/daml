// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.base.error.{ErrorGenerator, RpcError}
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.grpc.sampleservice.HelloServiceReferenceImplementation
import com.digitalasset.canton.ledger.client.GrpcChannel
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.resources.TestResourceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  SuppressingLogger,
}
import com.digitalasset.canton.metrics.{LedgerApiServerHistograms, LedgerApiServerMetrics}
import com.digitalasset.canton.networking.grpc.ratelimiting.LimitResult
import com.digitalasset.canton.platform.apiserver.GrpcServerSpec.*
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.apiserver.ratelimiting.RateLimitingInterceptorFactory
import com.digitalasset.canton.protobuf.Hello
import com.digitalasset.canton.protobuf.HelloServiceGrpc.HelloService
import com.digitalasset.canton.{BaseTest, HasExecutionContext, protobuf}
import io.grpc.*
import io.grpc.ClientCall.Listener
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener
import org.scalacheck.Gen
import org.scalatest.Assertion
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
        val helloService = protobuf.HelloServiceGrpc.stub(channel)
        for {
          response <- helloService.hello(protobuf.Hello.Request("foo"))
        } yield {
          response.msg shouldBe "foofoo"
        }
      }
    }

    "fail with a nice exception" in {
      resources(loggerFactory, helloService = new FailingHelloService()(_)).use { channel =>
        val helloService = protobuf.HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .hello(protobuf.Hello.Request("This is some text."))
            .failed
        } yield {
          exception.getMessage shouldBe "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: This is some text."
        }
      }
    }

    "fail with a nice exception, even when the text is quite long" in {
      val errorMessage = "There was an error. " + "x" * 2048
      val returnedMessage = "There was an error. " + "x" * 447 + "..."
      resources(loggerFactory, helloService = new FailingHelloService()(_)).use { channel =>
        val helloService = protobuf.HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .hello(protobuf.Hello.Request(errorMessage))
            .failed
        } yield {
          exception.getMessage shouldBe s"INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: $returnedMessage"
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
        val helloService = protobuf.HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .hello(protobuf.Hello.Request(exceptionMessage))
            .failed
        } yield {
          // We don't want to test the exact message content, just that it does indeed contain a
          // large chunk of the response error message, followed by "...".
          exception.getMessage should fullyMatch regex "INVALID_ARGUMENT: INVALID_ARGUMENT\\(8,0\\): The submitted request has invalid arguments: There was an error. x{400,}\\.\\.\\.".r
        }
      }
    }

    "fuzzy ensure non-security sensitive errors are forwarded gracefully" in {
      val checkerValue = "Sentinel error"
      val nonSecuritySensitiveErrorGen =
        ErrorGenerator
          .errorGenerator(
            redactDetails = Some(false),
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
      val securitySensitiveErrorGen = ErrorGenerator.errorGenerator(redactDetails = Some(true))

      fuzzTestErrorCodePropagation(
        errorCodeGen = securitySensitiveErrorGen,
        expectedIncludedMessage =
          "An error occurred. Please contact the operator and inquire about the request",
      )
    }

    "install rate limit interceptor" in {
      val metricsFactory = new InMemoryMetricsFactory
      val inventory = new HistogramInventory
      val metrics = new LedgerApiServerMetrics(
        new LedgerApiServerHistograms(MetricName("test"))(inventory),
        metricsFactory,
      )
      val overLimitRejection = LedgerApiErrors.ThreadpoolOverloaded.Rejection(
        "test",
        "test",
        100,
        59,
        "test",
      )
      val rateLimitingInterceptor = RateLimitingInterceptorFactory.create(
        loggerFactory,
        rateLimitingConfig,
        additionalChecks = List { (_, _) =>
          LimitResult.OverLimit(
            overLimitRejection
          )
        },
      )
      resources(loggerFactory, metrics, List(rateLimitingInterceptor)).use { channel =>
        val helloService = protobuf.HelloServiceGrpc.stub(channel)
        helloService.hello(protobuf.Hello.Request("foo")).failed.map {
          case s: StatusRuntimeException =>
            s.getStatus.getDescription shouldBe overLimitRejection.asGrpcStatus.getMessage
          case o => fail(s"Expected StatusRuntimeException, not $o")
        }
      }
    }

    "handle a request with short header" in {
      resources(
        loggerFactory,
        clientInterceptors = List(new HeaderClientInterceptor("ShortHeader")),
      ).use { channel =>
        val helloService = protobuf.HelloServiceGrpc.stub(channel)
        for {
          response <- helloService.hello(protobuf.Hello.Request("foo"))
        } yield {
          response.msg shouldBe "foofoo"
        }
      }
    }

    "don't handle a request with long header" in {
      resources(loggerFactory, clientInterceptors = List(new HeaderClientInterceptor("A" * 10000)))
        .use { channel =>
          val helloService = protobuf.HelloServiceGrpc.stub(channel)
          helloService.hello(protobuf.Hello.Request("foo")).failed.map {
            case s: StatusRuntimeException =>
              s.getStatus.getCode shouldBe Status.Code.INTERNAL
              s.getStatus.getDescription shouldBe "http2 exception"
              s.getCause.getMessage should include("Header size exceeded max allowed size")
            case o => fail(s"Expected StatusRuntimeException, not $o")
          }
        }
    }

    "handle a request with long header when large metadata size permitted" in {
      resources(
        loggerFactory,
        clientInterceptors = List(new HeaderClientInterceptor("A" * 10000)),
        maxInboundMetadataSize = Some(20000),
      ).use { channel =>
        val helloService = protobuf.HelloServiceGrpc.stub(channel)
        for {
          response <- helloService.hello(protobuf.Hello.Request("foo"))
        } yield {
          response.msg shouldBe "foofoo"
        }
      }
    }

  }

  private def fuzzTestErrorCodePropagation(
      errorCodeGen: Gen[RpcError],
      expectedIncludedMessage: String,
  ): Future[Assertion] = {
    val numberOfIterations = 100

    val randomExceptionGeneratingService = new HelloServiceReferenceImplementation {
      override def hello(request: Hello.Request): Future[Hello.Response] =
        Future.failed(errorCodeGen.sample.value.asGrpcError)
    }

    resources(loggerFactory, helloService = _ => randomExceptionGeneratingService).use { channel =>
      val helloService = protobuf.HelloServiceGrpc.stub(channel)
      for (_ <- 1 to numberOfIterations) {
        val f = for {
          exception <- helloService.hello(Hello.Request("not relevant")).failed
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

  class FailingHelloService(implicit ec: ExecutionContext)
      extends HelloServiceReferenceImplementation {
    override def hello(request: protobuf.Hello.Request): Future[protobuf.Hello.Response] = {
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
      metrics: LedgerApiServerMetrics = LedgerApiServerMetrics.ForTesting,
      serverInterceptors: List[ServerInterceptor] = List.empty,
      clientInterceptors: List[ClientInterceptor] = List.empty,
      helloService: ExecutionContext => BindableService with HelloService =
        new HelloServiceReferenceImplementation()(_),
      maxInboundMetadataSize: Option[Int] = None,
  )(implicit ec: ExecutionContext): ResourceOwner[Channel] =
    for {
      executor <- ResourceOwner.forExecutorService(() => Executors.newSingleThreadExecutor())
      server <- GrpcServerOwner(
        address = None,
        desiredPort = Port.Dynamic,
        maxInboundMessageSize = maxInboundMessageSize,
        maxInboundMetadataSize = maxInboundMetadataSize.getOrElse(8 * 1024),
        metrics = metrics,
        servicesExecutor = executor,
        services = Seq(helloService(ec)),
        interceptors = serverInterceptors,
        loggerFactory = loggerFactory,
        keepAlive = None,
      )
      channel <- new GrpcChannel.Owner(
        Port.tryCreate(server.getPort).unwrap,
        LedgerClientChannelConfiguration.InsecureDefaults,
      )
    } yield clientInterceptors.foldLeft[Channel](channel) { case (channel, interceptor) =>
      ClientInterceptors.intercept(channel, interceptor)
    }

  val CUSTOM_HEADER_KEY: Metadata.Key[String] =
    Metadata.Key.of("custom_client_header_key", Metadata.ASCII_STRING_MARSHALLER)

  class HeaderClientInterceptor(customHeader: String) extends ClientInterceptor {
    override def interceptCall[ReqT, RespT](
        method: MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions,
        next: Channel,
    ): ClientCall[ReqT, RespT] =
      new SimpleForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions)) {
        override def start(responseListener: Listener[RespT], headers: Metadata): Unit = {
          /* put custom header */
          headers.put(CUSTOM_HEADER_KEY, customHeader)
          super.start(
            new SimpleForwardingClientCallListener[RespT](responseListener) {
              override def onHeaders(headers: Metadata): Unit =
                super.onHeaders(headers)
            },
            headers,
          )
        }
      }
  }
}
