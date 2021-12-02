// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.daml.error.{
  BaseError,
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCategory,
  ErrorClass,
  ErrorCode,
}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.akka.ServerAdapter
import com.daml.grpc.sampleservice.HelloService_Responding
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.daml.platform.testing.StreamConsumer
import com.daml.ports.Port
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition, _}
import org.scalatest.{Assertion, Checkpoints}
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

// TODO error codes: Assert also on what is logged?
final class ErrorInterceptorSpec
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with Eventually
    with TestResourceContext
    with Checkpoints {

  import ErrorInterceptorSpec._

  private val bypassMsg: String =
    "(should still intercept the error to bypass default gRPC error handling)"

  classOf[ErrorInterceptor].getSimpleName - {

    assume(FooMissingErrorCode.category.grpcCode.get != Status.Code.INTERNAL)

    "for a server unary future endpoint" - {
      "when signalling with a non-self-service error should SANITIZE the server response when arising " - {
        "inside a Future" in {
          exerciseUnaryFutureEndpoint(useSelfService = false, insideFuture = true)
            .map { t: StatusRuntimeException =>
              assertSecuritySanitizedError(t)
            }
        }

        s"outside a Future $bypassMsg" in {
          exerciseUnaryFutureEndpoint(useSelfService = false, insideFuture = false)
            .map { t: StatusRuntimeException =>
              assertSecuritySanitizedError(t)
            }
        }
      }

      "when signalling with a self-service error should NOT SANITIZE the server response when arising" - {
        "inside a Future" in {
          exerciseUnaryFutureEndpoint(useSelfService = true, insideFuture = true)
            .map { t: StatusRuntimeException =>
              assertFooMissingError(
                actualError = t,
                expectedMsg = "Non-Status.INTERNAL self-service error inside a Future",
              )
            }
        }

        s"outside a Future $bypassMsg" in {
          exerciseUnaryFutureEndpoint(useSelfService = true, insideFuture = false)
            .map { t: StatusRuntimeException =>
              assertFooMissingError(
                actualError = t,
                expectedMsg = "Non-Status.INTERNAL self-service error outside a Future",
              )
            }
        }
      }
    }

    "for an server streaming Akka endpoint" - {

      "when signalling with a non-self-service error should SANITIZE the server response when arising" - {
        "inside a Stream" in {
          exerciseStreamingAkkaEndpoint(useSelfService = false, insideStream = true)
            .map { t: StatusRuntimeException =>
              assertSecuritySanitizedError(t)
            }
        }

        s"outside a Stream $bypassMsg" in {
          exerciseStreamingAkkaEndpoint(useSelfService = false, insideStream = false)
            .map { t: StatusRuntimeException =>
              assertSecuritySanitizedError(t)
            }
        }
      }

      "when signalling with a self-service error should NOT SANITIZE the server response when arising" - {
        "inside a Stream" in {
          exerciseStreamingAkkaEndpoint(useSelfService = true, insideStream = true)
            .map { t: StatusRuntimeException =>
              assertFooMissingError(
                actualError = t,
                expectedMsg = "Non-Status.INTERNAL self-service error inside a Stream",
              )
            }
        }

        s"outside a Stream $bypassMsg" in {
          exerciseStreamingAkkaEndpoint(useSelfService = true, insideStream = false)
            .map { t: StatusRuntimeException =>
              assertFooMissingError(
                actualError = t,
                expectedMsg = "Non-Status.INTERNAL self-service error outside a Stream",
              )
            }
        }
      }

    }
  }

  private def exerciseUnaryFutureEndpoint(
      useSelfService: Boolean,
      insideFuture: Boolean,
  ): Future[StatusRuntimeException] = {
    val response: Future[HelloResponse] = server(
      tested = new ErrorInterceptor(),
      service = new HelloService_Failing(
        useSelfService = useSelfService,
        insideFutureOrStream = insideFuture,
      ),
    ).use { channel =>
      HelloServiceGrpc.stub(channel).single(HelloRequest(1))
    }
    recoverToExceptionIf[StatusRuntimeException] {
      response
    }
  }

  private def exerciseStreamingAkkaEndpoint(
      useSelfService: Boolean,
      insideStream: Boolean,
  ): Future[StatusRuntimeException] = {
    val response: Future[Vector[HelloResponse]] = server(
      tested = new ErrorInterceptor(),
      service = new HelloService_Failing(
        useSelfService = useSelfService,
        insideFutureOrStream = insideStream,
      ),
    ).use { channel =>
      val streamConsumer = new StreamConsumer[HelloResponse](observer =>
        HelloServiceGrpc.stub(channel).serverStreaming(HelloRequest(1), observer)
      )
      streamConsumer.all()
    }
    recoverToExceptionIf[StatusRuntimeException] {
      response
    }
  }

  private def assertSecuritySanitizedError(actualError: StatusRuntimeException): Assertion = {
    val actualStatus = actualError.getStatus
    val actual = (actualStatus.getCode, actualStatus.getDescription)
    val expected = (
      Status.Code.INTERNAL,
      "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>",
    )
    actual shouldBe expected
    // TODO error-codes: Assert also on error's metadata.
  }

  private def assertFooMissingError(
      actualError: StatusRuntimeException,
      expectedMsg: String,
  ): Assertion = {
    val actualStatus = actualError.getStatus
    val actual = (actualStatus.getCode, actualStatus.getDescription)
    val expectedDescription = s"FOO_MISSING_ERROR_CODE(11,0): Foo is missing: $expectedMsg"
    val expected = (FooMissingErrorCode.category.grpcCode.get, expectedDescription)
    actual shouldBe expected
  }

}

object ErrorInterceptorSpec {

  def server(tested: ErrorInterceptor, service: BindableService): ResourceOwner[Channel] = {
    for {
      server <- serverOwner(interceptor = tested, service = service)
      channel <- GrpcClientResource.owner(Port(server.getPort))
    } yield channel
  }

  private def serverOwner(
      interceptor: ServerInterceptor,
      service: BindableService,
  ): ResourceOwner[Server] =
    new ResourceOwner[Server] {
      def acquire()(implicit context: ResourceContext): Resource[Server] =
        Resource(Future {
          val server =
            NettyServerBuilder
              .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
              .directExecutor()
              .intercept(interceptor)
              .addService(service)
              .build()
          server.start()
          server
        })(server => Future(server.shutdown().awaitTermination(10, TimeUnit.SECONDS): Unit))
    }

  object FooMissingErrorCode
      extends ErrorCode(
        id = "FOO_MISSING_ERROR_CODE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      )(ErrorClass.root()) {

    case class Error(msg: String)(implicit
        override val loggingContext: ContextualizedErrorLogger
    ) extends BaseError.Impl(
          cause = s"Foo is missing: $msg"
        )

  }

  object FooUnknownErrorCode
      extends ErrorCode(
        id = "FOO_UNKNOWN_ERROR_CODE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      )(ErrorClass.root()) {

    case class Error(msg: String)(implicit
        override val loggingContext: ContextualizedErrorLogger
    ) extends BaseError.Impl(
          cause = s"Foo is unknown: $msg"
        )

  }

  trait HelloService_Base extends BindableService {
    self: HelloService =>

    private val logger = ContextualizedLogger.get(getClass)
    private val emptyLoggingContext = LoggingContext.newLoggingContext(identity)

    implicit protected val damlLogger: DamlContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, emptyLoggingContext, None)

    override def bindService(): ServerServiceDefinition =
      HelloServiceGrpc.bindService(this, scala.concurrent.ExecutionContext.Implicits.global)

    override def fails(request: HelloRequest): Future[HelloResponse] = ??? // not used in this test
  }

  /** @param useSelfService       - whether to use self service error codes or a "rouge" exception
    * @param insideFutureOrStream - whether to signal the exception inside a Future or a Stream, or outside to them
    */
  // TODO error codes: Extend a HelloService generated by our Akka streaming scalapb plugin. (~HelloServiceAkkaGrpc)
  class HelloService_Failing(useSelfService: Boolean, insideFutureOrStream: Boolean)(implicit
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
  ) extends HelloService
      with HelloService_Responding
      with HelloService_Base {

    override def serverStreaming(
        request: HelloRequest,
        responseObserver: StreamObserver[HelloResponse],
    ): Unit = {
      val where = if (insideFutureOrStream) "inside" else "outside"
      val t: Throwable = if (useSelfService) {
        FooMissingErrorCode
          .Error(s"Non-Status.INTERNAL self-service error $where a Stream")
          .asGrpcError
      } else {
        new IllegalArgumentException(s"Failure $where a Stream")
      }
      if (insideFutureOrStream) {
        val _ = Source
          .single(request)
          .via(Flow[HelloRequest].mapConcat(_ => throw t))
          .runWith(ServerAdapter.toSink(responseObserver))
      } else {
        throw t
      }
    }

    override def single(request: HelloRequest): Future[HelloResponse] = {
      val where = if (insideFutureOrStream) "inside" else "outside"
      val t: Throwable = if (useSelfService) {
        FooMissingErrorCode
          .Error(s"Non-Status.INTERNAL self-service error $where a Future")
          .asGrpcError
      } else {
        new IllegalArgumentException(s"Failure $where a Future")
      }
      if (insideFutureOrStream) {
        Future.failed(t)
      } else {
        throw t
      }
    }
  }

}
