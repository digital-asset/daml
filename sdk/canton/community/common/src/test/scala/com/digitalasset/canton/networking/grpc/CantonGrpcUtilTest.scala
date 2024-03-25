// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import cats.data.EitherT
import com.digitalasset.canton.domain.api.v0.HelloServiceGrpc.{HelloService, HelloServiceStub}
import com.digitalasset.canton.domain.api.v0.{Hello, HelloServiceGrpc}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.networking.grpc.GrpcError.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.ServerInterceptors.intercept
import io.grpc.Status.Code.*
import io.grpc.*
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.util.MutableHandlerRegistry
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object CantonGrpcUtilTest {
  val request: Hello.Request = Hello.Request("Hello server")
  val response: Hello.Response = Hello.Response("Hello client")

  class Env(val service: HelloService, logger: TracedLogger)(ec: ExecutionContext) {
    val channelName: String = InProcessServerBuilder.generateName()

    val registry: MutableHandlerRegistry = new MutableHandlerRegistry

    val server: Server = InProcessServerBuilder
      .forName(channelName)
      .fallbackHandlerRegistry(registry)
      .build()

    val helloServiceDefinition: ServerServiceDefinition =
      intercept(HelloServiceGrpc.bindService(service, ec), TraceContextGrpc.serverInterceptor)

    registry.addService(helloServiceDefinition)

    val channel: ManagedChannel = InProcessChannelBuilder
      .forName(channelName)
      .intercept(TraceContextGrpc.clientInterceptor)
      .build()
    val client: HelloServiceGrpc.HelloServiceStub = HelloServiceGrpc.stub(channel)

    def sendRequest(
        timeoutMs: Long = 2000
    )(implicit traceContext: TraceContext): EitherT[Future, GrpcError, Hello.Response] =
      CantonGrpcUtil.sendGrpcRequest(client, "serverName")(
        _.hello(request),
        "command",
        Duration(timeoutMs, TimeUnit.MILLISECONDS),
        logger,
      )

    def close(): Unit = {
      channel.shutdown()
      channel.awaitTermination(2, TimeUnit.SECONDS)
      assert(channel.isTerminated)

      server.shutdown()
      server.awaitTermination()
    }
  }
}
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class CantonGrpcUtilTest extends FixtureAnyWordSpec with BaseTest with HasExecutionContext {
  import CantonGrpcUtilTest.*

  override type FixtureParam = Env

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new Env(mock[HelloService], logger)(parallelExecutionContext)
    try {
      withFixture(test.toNoArgTest(env))
    } finally {
      env.close()
    }
  }

  "A service" when {

    "the client sends a request" must {
      "send a response" in { env =>
        import env.*

        server.start()

        when(service.hello(request)).thenReturn(Future.successful(response))

        sendRequest().futureValue shouldBe response
      }
    }

    "the client cancels a request" must {
      "abort the request with CANCELLED and not log a warning" in { env =>
        import env.*

        server.start()

        val promise = Promise()
        when(service.hello(request)).thenReturn(promise.future)

        val context = Context.ROOT.withCancellation()
        context.run { () =>
          val requestF = sendRequest().value
          context.close()

          val err = requestF.futureValue.left.value
          err shouldBe a[GrpcClientGaveUp]
          err.status.getCode shouldBe CANCELLED
          Option(err.status.getCause) shouldBe None
        }
      }
    }

    "the client cancels a request with a specific exception" must {
      "abort the request with CANCELLED and log a warning" in { env =>
        import env.*

        val ex = new IllegalStateException("test description")

        server.start()

        val promise = Promise()
        when(service.hello(request)).thenReturn(promise.future)

        val context = Context.ROOT.withCancellation()
        context.run { () =>
          val requestF =
            loggerFactory.assertLogs(
              sendRequest().value,
              _.warningMessage shouldBe
                """Request failed for serverName.
                  |  GrpcClientGaveUp: CANCELLED/Context cancelled
                  |  Request: command
                  |  Causes: test description""".stripMargin,
            )
          context.cancel(ex)

          val err = requestF.futureValue.left.value
          err shouldBe a[GrpcClientGaveUp]
          err.status.getCode shouldBe CANCELLED
          err.status.getCause shouldBe ex
        }
      }
    }

    "the request deadline exceeds" must {
      "abort the request with DEADLINE_EXCEEDED" in { env =>
        import env.*

        server.start()

        val promise = Promise()
        when(service.hello(request)).thenReturn(promise.future)

        val requestF = loggerFactory.assertLogs(
          sendRequest(200).value,
          _.warningMessage should
            startWith("""Request failed for serverName.
                        |  GrpcClientGaveUp: DEADLINE_EXCEEDED/""".stripMargin),
        )

        val err = requestF.futureValue.left.value
        err shouldBe a[GrpcClientGaveUp]
        err.status.getCode shouldBe DEADLINE_EXCEEDED
      }
    }

    "the request timeout is negative" must {
      "abort the request with DEADLINE_EXCEEDED" in { env =>
        import env.*

        val requestF = loggerFactory.assertLogs(
          sendRequest(-100).value,
          _.warningMessage should
            startWith("""Request failed for serverName.
                        |  GrpcClientGaveUp: DEADLINE_EXCEEDED/""".stripMargin),
        )

        val err = requestF.futureValue.left.value
        err shouldBe a[GrpcClientGaveUp]
        err.status.getCode shouldBe DEADLINE_EXCEEDED
      }
    }

    "the server is permanently unavailable" must {
      "give up" in { env =>
        import env.*

        val requestF = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            sendRequest().value
          },
          logEntries => {
            logEntries should not be empty
            val (unavailableEntries, giveUpEntry) = logEntries.splitAt(logEntries.size - 1)
            forEvery(unavailableEntries) { logEntry =>
              logEntry.warningMessage shouldBe
                s"""Request failed for serverName. Is the server running? Did you configure the server address as 0.0.0.0? Are you using the right TLS settings? (details logged as DEBUG)
                   |  GrpcServiceUnavailable: UNAVAILABLE/Could not find server: $channelName
                   |  Request: command""".stripMargin
            }

            giveUpEntry.loneElement.warningMessage shouldBe "Retry timeout has elapsed, giving up."
          },
        )

        val err = Await.result(requestF, 5.seconds).left.value
        err shouldBe a[GrpcServiceUnavailable]
        err.status.getCode shouldBe UNAVAILABLE
        channel.getState(false) shouldBe ConnectivityState.TRANSIENT_FAILURE
      }
    }

    "the server is temporarily unavailable" must {
      "wait for the server" in { env =>
        import env.*

        when(service.hello(request)).thenReturn(Future.successful(response))

        val requestF = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            sendRequest(10000).value
          },
          logEntries => {
            logEntries should not be empty
            forEvery(logEntries) { logEntry =>
              logEntry.warningMessage shouldBe
                s"""Request failed for serverName. Is the server running? Did you configure the server address as 0.0.0.0? Are you using the right TLS settings? (details logged as DEBUG)
                   |  GrpcServiceUnavailable: UNAVAILABLE/Could not find server: $channelName
                   |  Request: command""".stripMargin
            }
          },
        )
        server.start()

        requestF.futureValue shouldBe Right(response)
      }
    }

    "the service is unavailable" must {
      "give up" in { env =>
        import env.*

        server.start()
        registry.removeService(helloServiceDefinition) shouldBe true

        val requestF = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            sendRequest().value
          },
          logEntries => {
            logEntries should not be empty
            val (unavailableEntries, giveUpEntry) = logEntries.splitAt(logEntries.size - 1)

            forEvery(unavailableEntries) { logEntry =>
              logEntry.warningMessage shouldBe
                s"""Request failed for serverName. Is the server initialized or is the server incompatible?
                   |  GrpcServiceUnavailable: UNIMPLEMENTED/Method not found: com.digitalasset.canton.domain.api.v0.HelloService/Hello
                   |  Request: command""".stripMargin
            }

            giveUpEntry.loneElement.warningMessage shouldBe "Retry timeout has elapsed, giving up."
          },
        )

        val err = Await.result(requestF, 5.seconds).left.value
        err shouldBe a[GrpcServiceUnavailable]
        err.status.getCode shouldBe UNIMPLEMENTED
      }
    }

    "the server fails with status INVALID_ARGUMENT" must {
      "report invalid argument" in { env =>
        import env.*

        val status = Status.INVALID_ARGUMENT
          .withCause(new NullPointerException)
          .withDescription("test description")

        server.start()
        when(service.hello(request)).thenReturn(Future.failed(status.asRuntimeException()))

        val requestF = loggerFactory.assertLogs(
          sendRequest().value,
          _.errorMessage shouldBe
            """Request failed for serverName.
              |  GrpcClientError: INVALID_ARGUMENT/test description
              |  Request: command""".stripMargin,
        )

        val err = Await.result(requestF, 2.seconds).left.value
        err shouldBe a[GrpcClientError]
        err.status.getCode shouldBe INVALID_ARGUMENT
        err.status.getDescription shouldBe "test description"
        err.status.getCause shouldBe null
      }
    }

    "the server fails with an async IllegalArgumentException" must {
      "report internal" in { env =>
        import env.*

        server.start()
        when(service.hello(request))
          .thenReturn(Future.failed(new IllegalArgumentException("test description")))

        val requestF = loggerFactory.assertLogs(
          sendRequest().value,
          _.errorMessage shouldBe
            """Request failed for serverName.
              |  GrpcServerError: INTERNAL/test description
              |  Request: command""".stripMargin,
        )

        val err = Await.result(requestF, 2.seconds).left.value
        err shouldBe a[GrpcServerError]
        err.status.getCode shouldBe INTERNAL
        err.status.getDescription shouldBe "test description"
        err.status.getCause shouldBe null
      }
    }

    "the server fails with a sync IllegalArgumentException" must {
      "report unknown" in { env =>
        import env.*

        server.start()
        when(service.hello(request)).thenThrow(new IllegalArgumentException("test exception"))

        val requestF = loggerFactory.assertLogs(
          sendRequest().value,
          err => {
            err.errorMessage shouldBe
              """Request failed for serverName.
                |  GrpcServerError: UNKNOWN/Application error processing RPC
                |  Request: command""".stripMargin
          },
        )

        val err = Await.result(requestF, 2.seconds).left.value
        err shouldBe a[GrpcServerError]

        // Note that reporting UNKNOWN to the client is not particularly helpful, but this is what GRPC does.
        // We usually have the ApiRequestLogger to ensure that this case is mapped to INTERNAL.

        err.status.getCode shouldBe UNKNOWN
        err.status.getDescription shouldBe "Application error processing RPC"
        err.status.getCause shouldBe null
      }
    }

    "the client is broken" must {
      "fail" in { env =>
        import env.*

        // Create a mocked client
        val brokenClient =
          mock[HelloServiceStub](withSettings.useConstructor(channel, CallOptions.DEFAULT))
        when(brokenClient.build(*[Channel], *[CallOptions])).thenReturn(brokenClient)

        // Make the client fail with an embedded cause
        val cause = new RuntimeException("test exception")
        val status = Status.INTERNAL.withDescription("test description").withCause(cause)
        when(brokenClient.hello(request)).thenReturn(Future.failed(status.asRuntimeException()))

        // Send the request
        val requestF = loggerFactory.assertLogs(
          {
            CantonGrpcUtil
              .sendGrpcRequest(brokenClient, "serverName")(
                _.hello(request),
                "command",
                Duration(2000, TimeUnit.MILLISECONDS),
                logger,
              )
              .value
          },
          logEntry => {
            logEntry.errorMessage shouldBe
              """Request failed for serverName.
              |  GrpcServerError: INTERNAL/test description
              |  Request: command
              |  Causes: test exception""".stripMargin
            logEntry.throwable shouldBe Some(cause)
          },
        )

        // Check the response
        val err = Await.result(requestF, 2.seconds).left.value
        err shouldBe a[GrpcServerError]

        err.status.getCode shouldBe INTERNAL
        err.status.getDescription shouldBe "test description"
        err.status.getCause shouldBe cause
      }
    }
  }
}
