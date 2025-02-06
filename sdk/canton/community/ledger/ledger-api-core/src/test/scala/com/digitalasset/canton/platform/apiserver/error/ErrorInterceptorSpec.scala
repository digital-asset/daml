// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.error

import com.daml.error.*
import com.daml.error.utils.ErrorDetails
import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.testing.utils.{PekkoBeforeAndAfterAll, TestingServerInterceptors}
import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.grpc.sampleservice.HelloServiceReferenceImplementation
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.resources.TestResourceContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, protobuf}
import io.grpc.*
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.scalatest.*
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

final class ErrorInterceptorSpec
    extends AsyncFreeSpec
    with PekkoBeforeAndAfterAll
    with OptionValues
    with Eventually
    with IntegrationPatience
    with TestResourceContext
    with Checkpoints
    with ErrorsAssertions
    with BaseTest
    with HasExecutionContext {

  import ErrorInterceptorSpec.*

  private val bypassMsg: String =
    "(should still intercept the error to bypass default gRPC error handling)"

  classOf[ErrorInterceptor].getSimpleName - {

    assert(FooMissingErrorCode.category.grpcCode.value != Status.Code.INTERNAL)

    "for a server unary future endpoint" - {
      "when signalling with a non-self-service error should SANITIZE the server response when arising " - {
        "inside a Future" in {
          exerciseUnaryFutureEndpoint(
            new HelloServiceFailing(
              useSelfService = false,
              errorInsideFutureOrStream = true,
              loggerFactory = loggerFactory,
            )
          ).map(assertRedactedError)
        }

        s"outside a Future $bypassMsg" in {
          exerciseUnaryFutureEndpoint(
            new HelloServiceFailing(
              useSelfService = false,
              errorInsideFutureOrStream = false,
              loggerFactory = loggerFactory,
            )
          ).map(assertRedactedError)
        }
      }

      "when signalling with a self-service error should NOT SANITIZE the server response when arising" - {
        "inside a Future" in {
          exerciseUnaryFutureEndpoint(
            new HelloServiceFailing(
              useSelfService = true,
              errorInsideFutureOrStream = true,
              loggerFactory = loggerFactory,
            )
          )
            .map { t =>
              assertFooMissingError(
                actual = t,
                expectedMsg = "Non-Status.INTERNAL self-service error inside a Future",
              )
            }
        }

        s"outside a Future $bypassMsg" in {
          exerciseUnaryFutureEndpoint(
            new HelloServiceFailing(
              useSelfService = true,
              errorInsideFutureOrStream = false,
              loggerFactory = loggerFactory,
            )
          )
            .map { t =>
              assertFooMissingError(
                actual = t,
                expectedMsg = "Non-Status.INTERNAL self-service error outside a Future",
              )
            }
        }
      }
    }

    "for a server streaming endpoint" - {

      "signal server shutting down" in {
        val service =
          new HelloServiceFailing(
            useSelfService = false,
            errorInsideFutureOrStream = true,
            loggerFactory = loggerFactory,
          )
        service.close()
        exerciseStreamingEndpoint(service)
          .map { t =>
            assertMatchesErrorCode(t, CommonErrors.ServerIsShuttingDown)
          }
      }

      "when signalling with a non-self-service error should SANITIZE the server response when arising" - {
        "inside a Stream" in {
          loggerFactory.assertLogs(
            within = {
              exerciseStreamingEndpoint(
                new HelloServiceFailing(
                  useSelfService = false,
                  errorInsideFutureOrStream = true,
                  loggerFactory = loggerFactory,
                )
              ).map(assertRedactedError)
            },
            assertions = _.errorMessage should include(
              "SERVICE_INTERNAL_ERROR(4,0): Unexpected or unknown exception occurred."
            ),
          )
        }

        s"outside a Stream $bypassMsg" in {
          exerciseStreamingEndpoint(
            new HelloServiceFailing(
              useSelfService = false,
              errorInsideFutureOrStream = false,
              loggerFactory = loggerFactory,
            )
          ).map(assertRedactedError)
        }

        "outside a Stream by directly calling stream-observer.onError" in {
          loggerFactory.assertLogs(
            exerciseStreamingEndpoint(
              new HelloServiceFailingDirectlyObserverOnError
            ).map(assertRedactedError)
            // the transformed error is expected to not be logged
            // so it is required that no entries will be found
          )
        }
      }

      "when signalling with a self-service error should NOT SANITIZE the server response when arising" - {
        "inside a Stream" in {
          exerciseStreamingEndpoint(
            new HelloServiceFailing(
              useSelfService = true,
              errorInsideFutureOrStream = true,
              loggerFactory = loggerFactory,
            )
          )
            .map { t =>
              assertFooMissingError(
                actual = t,
                expectedMsg = "Non-Status.INTERNAL self-service error inside a Stream",
              )
            }
        }

        s"outside a Stream $bypassMsg" in {
          exerciseStreamingEndpoint(
            new HelloServiceFailing(
              useSelfService = true,
              errorInsideFutureOrStream = false,
              loggerFactory = loggerFactory,
            )
          )
            .map { t =>
              assertFooMissingError(
                actual = t,
                expectedMsg = "Non-Status.INTERNAL self-service error outside a Stream",
              )
            }
        }
      }
    }
  }

  LogOnUnhandledFailureInClose.getClass.getSimpleName - {
    "is transparent when no exception is thrown" in {
      var idx = 0
      val call = () => {
        idx += 1
        idx
      }
      assert(LogOnUnhandledFailureInClose(logger, call()) === 1)
      assert(LogOnUnhandledFailureInClose(logger, call()) === 2)
    }

    "logs and re-throws an exception" in {
      val failure = new RuntimeException("some failure")
      val failingCall = () => throw failure

      loggerFactory
        .assertThrowsAndLogs[RuntimeException](
          within = LogOnUnhandledFailureInClose(logger, failingCall()),
          assertions = logEntry => {
            logEntry.errorMessage shouldBe "LEDGER_API_INTERNAL_ERROR(4,0): Unhandled error in ServerCall.close(). The gRPC client might have not been notified about the call/stream termination. Either notify clients to retry pending unary/streaming calls or restart the participant server."
            logEntry.mdc.keys should contain("err-context")
            logEntry.mdc
              .get("err-context")
              .value should fullyMatch regex """\{location=ErrorInterceptor.scala:\d+, throwableO=Some\(java.lang.RuntimeException: some failure\)\}"""
          },
        )
    }
  }

  private def exerciseUnaryFutureEndpoint(
      helloService: BindableService
  ): Future[StatusRuntimeException] = {
    val response: Future[protobuf.Hello.Response] = server(
      tested = new ErrorInterceptor(loggerFactory),
      service = helloService,
    ).use { channel =>
      protobuf.HelloServiceGrpc.stub(channel).hello(protobuf.Hello.Request("foo"))
    }
    recoverToExceptionIf[StatusRuntimeException] {
      response
    }
  }

  private def exerciseStreamingEndpoint(
      helloService: BindableService
  ): Future[StatusRuntimeException] = {
    val response: Future[Vector[protobuf.Hello.Response]] = server(
      tested = new ErrorInterceptor(loggerFactory),
      service = helloService,
    ).use { channel =>
      val streamConsumer = new StreamConsumer[protobuf.Hello.Response](observer =>
        protobuf.HelloServiceGrpc
          .stub(channel)
          .helloStreamed(protobuf.Hello.Request("foo"), observer)
      )
      streamConsumer.all()
    }
    recoverToExceptionIf[StatusRuntimeException] {
      response
    }
  }

  private def assertRedactedError(actual: StatusRuntimeException): Assertion = {
    assertError(
      actual,
      expectedStatusCode = Status.Code.INTERNAL,
      expectedMessage = BaseError.RedactedMessage(None),
      expectedDetails = Seq(),
      verifyEmptyStackTrace = false,
    )
    Assertions.succeed
  }

  private def assertFooMissingError(
      actual: StatusRuntimeException,
      expectedMsg: String,
  ): Assertion = {
    assertError(
      actual,
      expectedStatusCode = FooMissingErrorCode.category.grpcCode.value,
      expectedMessage = s"FOO_MISSING_ERROR_CODE(11,0): Foo is missing: $expectedMsg",
      expectedDetails = Seq(
        ErrorDetails.ErrorInfoDetail(
          "FOO_MISSING_ERROR_CODE",
          Map("category" -> "11", "test" -> getClass.getSimpleName),
        )
      ),
      verifyEmptyStackTrace = false,
    )
    Assertions.succeed
  }

  /** @param useSelfService            - whether to use self service error codes or "rogue" exceptions
    * @param errorInsideFutureOrStream - whether to signal the exception inside a Future or a Stream, or outside to them
    */
  class HelloServiceFailing(
      useSelfService: Boolean,
      errorInsideFutureOrStream: Boolean,
      val loggerFactory: NamedLoggerFactory,
  ) extends HelloServiceReferenceImplementation
      with StreamingServiceLifecycleManagement
      with NamedLogging {

    override def helloStreamed(
        request: protobuf.Hello.Request,
        responseObserver: StreamObserver[protobuf.Hello.Response],
    ): Unit = registerStream(responseObserver) {
      implicit val traceContext: TraceContext = TraceContext.empty
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
          .via(Flow[protobuf.Hello.Request].mapConcat(_ => throw t))
      } else {
        throw t
      }
    }

    override def hello(request: protobuf.Hello.Request): Future[protobuf.Hello.Response] = {
      implicit val traceContext: TraceContext = TraceContext.empty
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

  class HelloServiceFailingDirectlyObserverOnError extends HelloServiceReferenceImplementation {

    override def helloStreamed(
        request: protobuf.Hello.Request,
        responseObserver: StreamObserver[protobuf.Hello.Response],
    ): Unit =
      responseObserver.onError(
        new IllegalArgumentException(
          s"Failing the stream by passing a non error-code based error directly to observer.onError"
        )
      )

    override def hello(request: protobuf.Hello.Request): Future[protobuf.Hello.Response] =
      Assertions.fail("This class is not intended to test unary endpoints")
  }
}

object ErrorInterceptorSpec {

  def server(tested: ErrorInterceptor, service: BindableService): ResourceOwner[Channel] =
    TestingServerInterceptors.channelOwner(tested, service)

  object FooMissingErrorCode
      extends ErrorCode(
        id = "FOO_MISSING_ERROR_CODE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      )(ErrorClass.root()) {

    final case class Error(msg: String)(implicit
        val loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = s"Foo is missing: $msg"
        )

  }
}
