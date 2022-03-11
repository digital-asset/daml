// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.daml.error.definitions.DamlError
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.utils.ErrorDetails
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCategory,
  ErrorClass,
  ErrorCode,
  ErrorsAssertions,
}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceAkkaGrpc, HelloServiceGrpc}
import com.daml.platform.testing.{LogCollectorAssertions, StreamConsumer}
import com.daml.ports.Port
import io.grpc.netty.NettyServerBuilder
import io.grpc.{BindableService, ServerServiceDefinition, _}
import org.scalatest.{Assertion, Assertions, Checkpoints}
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

final class ErrorInterceptorSpec
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with Eventually
    with TestResourceContext
    with Checkpoints
    with LogCollectorAssertions
    with ErrorsAssertions {

  import ErrorInterceptorSpec._

  private val bypassMsg: String =
    "(should still intercept the error to bypass default gRPC error handling)"

  classOf[ErrorInterceptor].getSimpleName - {

    assert(FooMissingErrorCode.category.grpcCode.get != Status.Code.INTERNAL)

    "for a server unary future endpoint" - {
      "when signalling with a non-self-service error should SANITIZE the server response when arising " - {
        "inside a Future" in {
          exerciseUnaryFutureEndpoint(
            new HelloServiceFailing(useSelfService = false, errorInsideFutureOrStream = true)
          )
            .map { t: StatusRuntimeException =>
              assertSecuritySanitizedError(t)
            }
        }

        s"outside a Future $bypassMsg" in {
          exerciseUnaryFutureEndpoint(
            new HelloServiceFailing(useSelfService = false, errorInsideFutureOrStream = false)
          )
            .map { t: StatusRuntimeException =>
              assertSecuritySanitizedError(t)
            }
        }
      }

      "when signalling with a self-service error should NOT SANITIZE the server response when arising" - {
        "inside a Future" in {
          exerciseUnaryFutureEndpoint(
            new HelloServiceFailing(useSelfService = true, errorInsideFutureOrStream = true)
          )
            .map { t: StatusRuntimeException =>
              assertFooMissingError(
                actual = t,
                expectedMsg = "Non-Status.INTERNAL self-service error inside a Future",
              )
            }
        }

        s"outside a Future $bypassMsg" in {
          exerciseUnaryFutureEndpoint(
            new HelloServiceFailing(useSelfService = true, errorInsideFutureOrStream = false)
          )
            .map { t: StatusRuntimeException =>
              assertFooMissingError(
                actual = t,
                expectedMsg = "Non-Status.INTERNAL self-service error outside a Future",
              )
            }
        }
      }
    }

    "for an server streaming Akka endpoint" - {

      "signal server shutting down" in {
        val service =
          new HelloServiceFailing(useSelfService = false, errorInsideFutureOrStream = true)
        service.close()
        exerciseStreamingAkkaEndpoint(service)
          .map { t: StatusRuntimeException =>
            assertMatchesErrorCode(t, LedgerApiErrors.ServerIsShuttingDown)
          }
      }

      "when signalling with a non-self-service error should SANITIZE the server response when arising" - {
        "inside a Stream" in {
          exerciseStreamingAkkaEndpoint(
            new HelloServiceFailing(useSelfService = false, errorInsideFutureOrStream = true)
          )
            .map { t: StatusRuntimeException =>
              assertSecuritySanitizedError(t)
            }
        }

        s"outside a Stream $bypassMsg" in {
          exerciseStreamingAkkaEndpoint(
            new HelloServiceFailing(useSelfService = false, errorInsideFutureOrStream = false)
          )
            .map { t: StatusRuntimeException =>
              assertSecuritySanitizedError(t)
            }
        }
      }

      "when signalling with a self-service error should NOT SANITIZE the server response when arising" - {
        "inside a Stream" in {
          exerciseStreamingAkkaEndpoint(
            new HelloServiceFailing(useSelfService = true, errorInsideFutureOrStream = true)
          )
            .map { t: StatusRuntimeException =>
              assertFooMissingError(
                actual = t,
                expectedMsg = "Non-Status.INTERNAL self-service error inside a Stream",
              )
            }
        }

        s"outside a Stream $bypassMsg" in {
          exerciseStreamingAkkaEndpoint(
            new HelloServiceFailing(useSelfService = true, errorInsideFutureOrStream = false)
          )
            .map { t: StatusRuntimeException =>
              assertFooMissingError(
                actual = t,
                expectedMsg = "Non-Status.INTERNAL self-service error outside a Stream",
              )
            }
        }
      }

    }
  }

  private def exerciseUnaryFutureEndpoint(
      helloService: BindableService
  ): Future[StatusRuntimeException] = {
    val response: Future[HelloResponse] = server(
      tested = new ErrorInterceptor(),
      service = helloService,
    ).use { channel =>
      HelloServiceGrpc.stub(channel).single(HelloRequest(1))
    }
    recoverToExceptionIf[StatusRuntimeException] {
      response
    }
  }

  private def exerciseStreamingAkkaEndpoint(
      helloService: BindableService
  ): Future[StatusRuntimeException] = {
    val response: Future[Vector[HelloResponse]] = server(
      tested = new ErrorInterceptor(),
      service = helloService,
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

  private def assertSecuritySanitizedError(actual: StatusRuntimeException): Assertion = {
    assertError(
      actual,
      expectedStatusCode = Status.Code.INTERNAL,
      expectedMessage =
        "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>",
      expectedDetails = Seq(),
    )
    Assertions.succeed
  }

  private def assertFooMissingError(
      actual: StatusRuntimeException,
      expectedMsg: String,
  ): Assertion = {
    assertError(
      actual,
      expectedStatusCode = FooMissingErrorCode.category.grpcCode.get,
      expectedMessage = s"FOO_MISSING_ERROR_CODE(11,0): Foo is missing: $expectedMsg",
      expectedDetails =
        Seq(ErrorDetails.ErrorInfoDetail("FOO_MISSING_ERROR_CODE", Map("category" -> "11"))),
    )
    Assertions.succeed
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

    case class Error(_msg: String)(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = s"Foo is missing: ${_msg}"
        )

  }

  trait HelloServiceBase extends BindableService {
    self: HelloService =>

    implicit protected val damlLogger: DamlContextualizedErrorLogger =
      DamlContextualizedErrorLogger.forTesting(getClass)

    override def bindService(): ServerServiceDefinition =
      HelloServiceGrpc.bindService(this, scala.concurrent.ExecutionContext.Implicits.global)

    override def fails(request: HelloRequest): Future[HelloResponse] = ??? // not used in this test
  }

  /** @param useSelfService - whether to use self service error codes or "rogue" exceptions
    * @param errorInsideFutureOrStream - whether to signal the exception inside a Future or a Stream, or outside to them
    */
  class HelloServiceFailing(useSelfService: Boolean, errorInsideFutureOrStream: Boolean)(implicit
      protected val esf: ExecutionSequencerFactory,
      protected val mat: Materializer,
  ) extends HelloServiceAkkaGrpc
      with HelloServiceResponding
      with HelloServiceBase {

    override protected def serverStreamingSource(
        request: HelloRequest
    ): Source[HelloResponse, NotUsed] = {
      val where = if (errorInsideFutureOrStream) "inside" else "outside"
      val t: Throwable = if (useSelfService) {
        FooMissingErrorCode
          .Error(s"Non-Status.INTERNAL self-service error $where a Stream")
          .asGrpcError
      } else {
        new IllegalArgumentException(s"Failure $where a Stream")
      }
      if (errorInsideFutureOrStream) {
        Source
          .single(request)
          .via(Flow[HelloRequest].mapConcat(_ => throw t))
      } else {
        throw t
      }
    }

    override def single(request: HelloRequest): Future[HelloResponse] = {
      val where = if (errorInsideFutureOrStream) "inside" else "outside"
      val t: Throwable = if (useSelfService) {
        FooMissingErrorCode
          .Error(s"Non-Status.INTERNAL self-service error $where a Future")
          .asGrpcError
      } else {
        new IllegalArgumentException(s"Failure $where a Future")
      }
      if (errorInsideFutureOrStream) {
        Future.failed(t)
      } else {
        throw t
      }
    }
  }

}
