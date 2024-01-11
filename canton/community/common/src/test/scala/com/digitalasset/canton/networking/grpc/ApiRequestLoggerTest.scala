// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.domain.api.v0.HelloServiceGrpc.HelloService
import com.digitalasset.canton.domain.api.v0.{Hello, HelloServiceGrpc}
import com.digitalasset.canton.logging.{NamedEventCapturingLogger, TracedLogger}
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.typesafe.scalalogging.Logger
import io.grpc.*
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.scalactic.Equality
import org.scalatest.Assertion
import org.scalatest.prop.{TableFor2, TableFor5}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level
import org.slf4j.event.Level.*

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

@SuppressWarnings(Array("org.wartremover.warts.Null"))
@nowarn("msg=match may not be exhaustive")
class ApiRequestLoggerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  val ChannelName: String = "testSender"

  val Request: Hello.Request = Hello.Request("Hello server")
  val Response: Hello.Response = Hello.Response("Hello client")

  // Exception messages are carefully chosen such that errors logged by SerializingExecutor will be suppressed.
  val Exception: RuntimeException = new RuntimeException("test exception (runtime exception)")
  val CheckedException: Exception = new Exception("test exception (checked exception)")
  val Error: UnknownError = new java.lang.UnknownError("test exception (error)")

  override protected def exitOnFatal =
    false // As we are testing with fatal errors, switch off call to system.exit

  val StatusDescription: String = "test status description"

  val Trailers: Metadata = {
    val m = new Metadata()
    m.put(Constant.MEMBER_ID_METADATA_KEY, "testValue")
    m
  }

  val InvalidArgumentStatus: Status = Status.INVALID_ARGUMENT.withDescription(StatusDescription)
  val AbortedStatus: Status = Status.ABORTED
  val InternalStatus: Status =
    Status.INTERNAL.withDescription(StatusDescription).withCause(Exception)
  val UnknownStatus: Status = Status.UNKNOWN.withCause(Error)
  val UnauthenticatedStatus: Status = Status.UNAUTHENTICATED.withDescription(StatusDescription)

  val failureCases: TableFor5[Status, Metadata, String, Level, String] = Table(
    ("Status", "Trailers", "Expected description", "Expected log level", "Expected log message"),
    (
      InvalidArgumentStatus,
      null,
      StatusDescription,
      INFO,
      s"failed with INVALID_ARGUMENT/$StatusDescription",
    ),
    (AbortedStatus, Trailers, null, INFO, s"failed with ABORTED\n  Trailers: $Trailers"),
    (InternalStatus, null, StatusDescription, ERROR, s"failed with INTERNAL/$StatusDescription"),
    (
      UnknownStatus,
      Trailers,
      Error.getMessage,
      ERROR,
      s"failed with UNKNOWN/${Error.getMessage}\n  Trailers: $Trailers",
    ),
    (
      UnauthenticatedStatus,
      null,
      StatusDescription,
      DEBUG,
      "failed with UNAUTHENTICATED/test status description",
    ),
  )

  val throwableCases: TableFor2[String, Throwable] = Table(
    ("Description", "Throwable"),
    ("RuntimeException", Exception),
    ("Exception", CheckedException),
    ("Error", Error),
  )

  val ClientCancelsStatus: Status = Status.CANCELLED.withDescription("Context cancelled")
  val ServerCancelsStatus: Status =
    Status.CANCELLED.withDescription("cancelling due to cancellation by client")

  val grpcClientCancelledStreamed: String = "failed with CANCELLED/call already cancelled"

  val cancelCases: TableFor5[String, Any, Level, String, Throwable] = Table(
    (
      "Description",
      "Action after cancellation",
      "Expected log level",
      "Expected log message",
      "Expected exception",
    ),
    ("server responding anyway", Response, INFO, grpcClientCancelledStreamed, null),
    ("server completing", (), INFO, null, null),
    (
      "server cancelling",
      ServerCancelsStatus,
      INFO,
      s"failed with CANCELLED/${ServerCancelsStatus.getDescription}",
      null,
    ),
    (
      "server failing",
      InternalStatus,
      ERROR,
      s"failed with INTERNAL/${InternalStatus.getDescription}",
      InternalStatus.getCause,
    ),
  )

  implicit val eqMetadata: Equality[Metadata] = (a: Metadata, b: Any) => {
    val first = Option(a).getOrElse(new Metadata())
    val secondAny = Option(b).getOrElse(new Metadata())

    secondAny match {
      case second: Metadata =>
        first.toString == second.toString
      case _ => false
    }
  }

  def assertClientFailure(
      clientCompletion: Future[_],
      serverStatus: Status,
      serverTrailers: Metadata = new Metadata(),
      clientCause: Throwable = null,
  ): Assertion = {
    inside(clientCompletion.failed.futureValue) { case sre: StatusRuntimeException =>
      sre.getStatus.getCode shouldBe serverStatus.getCode
      sre.getStatus.getDescription shouldBe serverStatus.getDescription
      sre.getCause shouldBe clientCause
      sre.getTrailers shouldEqual serverTrailers
    }
  }

  val requestTraceContext: TraceContext = TraceContext.withNewTraceContext(tc => tc)

  private val progressLogger = loggerFactory.getLogger(classOf[Env])

  val capturingLogger: NamedEventCapturingLogger =
    new NamedEventCapturingLogger(
      classOf[ApiRequestLoggerTest].getSimpleName,
      outputLogger = Some(progressLogger),
    )

  // need to override these loggers as we want the execution context to pick up the capturing logger
  override protected val noTracingLogger: Logger =
    capturingLogger.getLogger(classOf[ApiRequestLoggerTest])
  override protected val logger: TracedLogger = TracedLogger(noTracingLogger)

  class Env(logMessagePayloads: Boolean, maxStringLenth: Int, maxMetadataSize: Int) {
    val service: HelloService = mock[HelloService]

    val helloServiceDefinition: ServerServiceDefinition =
      HelloServiceGrpc.bindService(service, parallelExecutionContext)

    val apiRequestLogger: ApiRequestLogger =
      new ApiRequestLogger(
        capturingLogger,
        config = ApiLoggingConfig(
          messagePayloads = Some(logMessagePayloads),
          maxStringLength = maxStringLenth,
          maxMetadataSize = maxMetadataSize,
        ),
      )

    val server: Server = InProcessServerBuilder
      .forName(ChannelName)
      .executor(executorService)
      .addService(
        ServerInterceptors
          .intercept(helloServiceDefinition, apiRequestLogger, TraceContextGrpc.serverInterceptor)
      )
      .build()

    server.start()

    val channel: ManagedChannel =
      InProcessChannelBuilder
        .forName(ChannelName)
        .intercept(TraceContextGrpc.clientInterceptor)
        .build()

    val client: HelloServiceGrpc.HelloServiceStub = HelloServiceGrpc.stub(channel)

    // Remove events remaining from last test case.
    capturingLogger.eventQueue.clear()

    def close(): Unit = {
      channel.shutdown()
      channel.awaitTermination(1, TimeUnit.SECONDS)

      server.shutdown()
      server.awaitTermination(1, TimeUnit.SECONDS)
    }
  }

  private val testCounter = new AtomicInteger(0)

  def withEnv[T](
      logMessagePayloads: Boolean = true,
      maxStringLength: Int = 50,
      maxMetadataSize: Int = 200,
  )(test: Env => T): T = {
    val env = new Env(logMessagePayloads, maxStringLength, maxMetadataSize)
    val cnt = testCounter.incrementAndGet()
    try {
      progressLogger.debug(s"Starting api-request-logger-test $cnt")

      val result = TraceContextGrpc.withGrpcContext(requestTraceContext) { test(env) }

      // Check this, unless test is failing.
      capturingLogger.assertNoMoreEvents()
      progressLogger.debug(s"Finished api-request-logger-test $cnt with $result")
      result
    } catch {
      case e: Throwable =>
        progressLogger.info("Test failed with exception", e)
        env.close()
        throw e
    } finally {
      env.close()
    }
  }

  "On a unary call" when {

    def createExpectedLogMessage(
        content: String,
        includeRequestTraceContext: Boolean = false,
    ): String = {
      val mainMessage = s"Request c.d.c.d.a.v.HelloService/Hello by testSender: $content"
      val traceContextMessage = s"\n  Request ${requestTraceContext.showTraceId}"
      if (includeRequestTraceContext) mainMessage + traceContextMessage else mainMessage
    }

    def assertRequestLogged: Assertion = {
      capturingLogger.assertNextMessage(
        _ should startWith(
          "Request c.d.c.d.a.v.HelloService/Hello by testSender: " +
            s"received headers Metadata(" +
            s"traceparent=${requestTraceContext.asW3CTraceContext.value.parent}," +
            s"grpc-accept-encoding=gzip,user-agent=grpc-java-inprocess"
        ),
        TRACE,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage(
          "received a message Request(Hello server)",
          includeRequestTraceContext = true,
        ),
        DEBUG,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage("finished receiving messages"),
        TRACE,
      )
    }

    "intercepting a successful request" must {
      "log progress" in withEnv() { implicit env =>
        import env.*

        when(service.hello(Request)).thenReturn(Future.successful(Response))

        client.hello(Request).futureValue shouldBe Response

        assertRequestLogged
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage("sending response headers Metadata()"),
          TRACE,
        )
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage(
            "sending response Response(Hello client)",
            includeRequestTraceContext = true,
          ),
          DEBUG,
        )
        capturingLogger.assertNextMessageIs(createExpectedLogMessage("succeeded(OK)"), DEBUG)
        capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
      }
    }

    failureCases.forEvery {
      case (status, trailers, expectedDescription, expectedLogLevel, expectedMessage) =>
        s"intercepting a failed request (${status.getCode})" must {
          "log the failure" in withEnv() { implicit env =>
            import env.*

            when(service.hello(Request))
              .thenReturn(Future.failed(status.asRuntimeException(trailers)))

            assertClientFailure(
              client.hello(Request),
              status.withDescription(expectedDescription),
              trailers,
            )

            assertRequestLogged
            capturingLogger.assertNextMessageIs(
              createExpectedLogMessage(expectedMessage),
              expectedLogLevel,
              status.getCause,
            )
            capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
          }
        }
    }

    forEvery(
      throwableCases
        .collect {
          // Exclude non-exceptions, as they would fall to the underlying execution context and therefore need not be
          // handled by GRPC.
          case (description, exception: Exception) => (description, exception)
        }
    ) { case (description, exception) =>
      s"intercepting an unexpected async $description" must {
        "log progress and the throwable" in withEnv() { implicit env =>
          import env.*

          when(service.hello(Request)).thenAnswer(Future.failed(exception))

          assertClientFailure(
            client.hello(Request),
            Status.INTERNAL.withDescription(exception.getMessage),
          )

          assertRequestLogged
          capturingLogger.assertNextMessageIs(
            createExpectedLogMessage(s"failed with INTERNAL/${exception.getMessage}"),
            ERROR,
            exception,
          )
          capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
        }
      }
    }

    throwableCases.forEvery { case (description, throwable) =>
      s"intercepting an unexpected sync $description" must {
        "log progress and the error" in withEnv() { implicit env =>
          import env.*

          when(service.hello(Request)).thenThrow(throwable)

          assertClientFailure(client.hello(Request), Status.UNKNOWN)

          assertRequestLogged
          capturingLogger.assertNextMessageIs(
            createExpectedLogMessage("failed with an unexpected throwable"),
            ERROR,
            throwable,
          )

          throwable match {
            case NonFatal(_) =>
              capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
            case _: Throwable =>
              // since our latest gRPC upgrade (https://github.com/DACH-NY/canton/pull/15304),
              // the client might log one additional "completed" message before or after the
              // fatal error being logged by gRPC
              val capturedCompletedMessages = new AtomicInteger(0)
              if (capturingLogger.tryToPollMessage(createExpectedLogMessage("completed"), DEBUG)) {
                capturedCompletedMessages.getAndIncrement()
              }
              capturingLogger.assertNextMessageIs(
                s"A fatal error has occurred in $executionContextName. Terminating thread.",
                ERROR,
                throwable,
              )
              if (capturingLogger.tryToPollMessage(createExpectedLogMessage("completed"), DEBUG)) {
                capturedCompletedMessages.getAndIncrement()
              }
              withClue("the 'completed' message should appear at most once:") {
                capturedCompletedMessages.get() should be <= 1
              }
          }
        }
      }
    }

    forEvery(
      cancelCases
        .collect {
          case c @ (_, _: Status, _, _, _) => c
          case c @ (_, _: Hello.Response, _, err, _)
              // With grpc 1.35, io.grpc.stub.ServerCalls.ServerCallStreamObserverImpl.onNext only honors
              // client-cancellations of streaming calls, so don't expect the following error in unary calls:
              if err != grpcClientCancelledStreamed =>
            c
        }
    ) {
      case (
            description,
            afterCancelAction,
            expectedLogLevel,
            expectedLogMessage,
            expectedThrowable,
          ) =>
        s"intercepting a cancellation and $description" must {
          "log the cancellation" in withEnv() { implicit env =>
            import env.*

            val receivedRequestP = Promise[Unit]()
            val sendResponseP = Promise[Unit]()

            when(service.hello(Request)).thenAnswer[Hello.Request](_ => {
              receivedRequestP.success(())
              sendResponseP.future.map(_ =>
                afterCancelAction match {
                  case status: Status => throw status.asRuntimeException
                  case response: Hello.Response => response
                }
              )
            })

            val context = Context.current().withCancellation()
            context.run(() => {
              val requestF = client.hello(Request)

              receivedRequestP.future.futureValue
              context.cancel(Exception)

              assertClientFailure(requestF, ClientCancelsStatus, clientCause = Exception)
            })

            assertRequestLogged
            capturingLogger.assertNextMessageIs(createExpectedLogMessage("cancelled"), INFO)

            // Wait until the cancellation has arrived at the server.
            eventually() {
              apiRequestLogger.cancelled.get() shouldBe true
            }
            capturingLogger.assertNoMoreEvents()

            // Server still sends a response.
            sendResponseP.success(())

            capturingLogger.assertNextMessageIs(
              createExpectedLogMessage(expectedLogMessage),
              expectedLogLevel,
              expectedThrowable,
            )
          }
        }
    }
  }

  def setupStreamedService(
      action: StreamObserver[Hello.Response] => Unit
  )(implicit env: Env): Unit = {
    import env.*
    when(service.helloStreamed(refEq(Request), any[StreamObserver[Hello.Response]]))
      .thenAnswer[Hello.Request, StreamObserver[Hello.Response]] {
        case (_, observer: ServerCallStreamObserver[Hello.Response]) =>
          // Setting on cancel handler, because otherwise GRPC will throw if onNext/onComplete/onError is called after cancellation.
          // (In production, we do the same.)
          observer.setOnCancelHandler(() => ())

          observer.onNext(Response)
          observer.onNext(Response)
          action(observer)
        case (_, observer) =>
          logger.error(s"Invalid observer type: ${observer.getClass.getSimpleName}")
      }
  }

  def callStreamedServiceAndCheckClientFailure(
      serverStatus: Status,
      serverTrailers: Metadata = new Metadata,
      clientCause: Throwable = null,
      checkResponses: Boolean = true,
  )(implicit env: Env): Assertion = {
    import env.*

    val observer = new RecordingStreamObserver[Hello.Response]
    client.helloStreamed(Request, observer)
    assertClientFailure(observer.result, serverStatus, serverTrailers, clientCause)
    if (checkResponses) {
      observer.responses should have size 2
    }
    succeed
  }

  "On a streamed call" when {

    def createExpectedLogMessage(
        content: String,
        includeRequestTraceContext: Boolean = false,
    ): String = {
      val mainMessage = s"Request c.d.c.d.a.v.HelloService/HelloStreamed by testSender: $content"
      val traceContextMessage = s"\n  Request ${requestTraceContext.showTraceId}"
      if (includeRequestTraceContext) mainMessage + traceContextMessage else mainMessage
    }

    def assertRequestAndResponsesLogged: Assertion = {
      capturingLogger.assertNextMessage(
        _ should startWith(
          "Request c.d.c.d.a.v.HelloService/HelloStreamed by testSender: " +
            s"received headers Metadata(" +
            s"traceparent=${requestTraceContext.asW3CTraceContext.value.parent}," +
            s"grpc-accept-encoding=gzip,user-agent=grpc-java-inprocess"
        ),
        TRACE,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage(
          "received a message Request(Hello server)",
          includeRequestTraceContext = true,
        ),
        DEBUG,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage("finished receiving messages"),
        TRACE,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage("sending response headers Metadata()"),
        TRACE,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage(
          "sending response Response(Hello client)",
          includeRequestTraceContext = true,
        ),
        DEBUG,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage(
          "sending response Response(Hello client)",
          includeRequestTraceContext = true,
        ),
        DEBUG,
      )
    }

    "intercepting a successful request" must {
      "log progress" in withEnv() { implicit env =>
        import env.*

        setupStreamedService(_.onCompleted())

        val observer = new RecordingStreamObserver[Hello.Response]
        client.helloStreamed(Request, observer)
        observer.result.futureValue
        observer.responses should have size 2

        assertRequestAndResponsesLogged
        capturingLogger.assertNextMessageIs(createExpectedLogMessage("succeeded(OK)"), DEBUG)
        capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
      }
    }

    failureCases.forEvery {
      case (status, trailers, expectedDescription, expectedLogLevel, expectedLogMessage) =>
        s"intercepting a failure (${status.getCode})" must {
          "log the failure" in withEnv() { implicit env =>
            setupStreamedService(_.onError(status.asRuntimeException(trailers)))

            callStreamedServiceAndCheckClientFailure(
              status.withDescription(expectedDescription),
              trailers,
            )

            assertRequestAndResponsesLogged
            capturingLogger.assertNextMessageIs(
              createExpectedLogMessage(expectedLogMessage),
              expectedLogLevel,
              status.getCause,
            )
            capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
          }
        }
    }

    throwableCases.forEvery { case (description, throwable) =>
      s"intercepting an unexpected async $description" must {
        "log progress and the throwable" in withEnv() { implicit env =>
          setupStreamedService(_.onError(throwable))

          callStreamedServiceAndCheckClientFailure(
            Status.UNKNOWN.withDescription(throwable.getMessage)
          )

          assertRequestAndResponsesLogged
          capturingLogger.assertNextMessageIs(
            createExpectedLogMessage(s"failed with UNKNOWN/${throwable.getMessage}"),
            ERROR,
            throwable,
          )
          capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
        }
      }
    }

    throwableCases.forEvery { case (description, throwable) =>
      s"intercepting an unexpected sync $description" must {
        "log progress and the error" in withEnv() { implicit env =>
          setupStreamedService(_ => throw throwable)

          callStreamedServiceAndCheckClientFailure(Status.UNKNOWN)

          assertRequestAndResponsesLogged
          capturingLogger.assertNextMessageIs(
            createExpectedLogMessage("failed with an unexpected throwable"),
            ERROR,
            throwable,
          )

          throwable match {
            case NonFatal(_) =>
              capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
            case _: Throwable =>
              // since our latest gRPC upgrade (https://github.com/DACH-NY/canton/pull/15304),
              // the client might log one additional "completed" message before or after the
              // fatal error being logged by gRPC
              val capturedCompletedMessages = new AtomicInteger(0)
              if (capturingLogger.tryToPollMessage(createExpectedLogMessage("completed"), DEBUG)) {
                capturedCompletedMessages.getAndIncrement()
              }
              capturingLogger.assertNextMessageIs(
                s"A fatal error has occurred in $executionContextName. Terminating thread.",
                ERROR,
                throwable,
              )
              if (capturingLogger.tryToPollMessage(createExpectedLogMessage("completed"), DEBUG)) {
                capturedCompletedMessages.getAndIncrement()
              }
              withClue("the 'completed' message should appear at most once:") {
                capturedCompletedMessages.get() should be <= 1
              }
          }
        }
      }
    }

    cancelCases.forEvery {
      case (
            description,
            afterCancelAction,
            expectedLogLevel,
            expectedLogMessage,
            expectedThrowable,
          ) =>
        s"intercepting a cancellation and $description" must {
          "log the cancellation" in withEnv() { implicit env =>
            import env.*

            val receivedRequestP = Promise[Unit]()
            val sendSecondResponseP = Promise[Unit]()

            setupStreamedService { observer =>
              receivedRequestP.success(())
              // Send second response despite cancellation.
              // This cannot be prevented, as the sending raises with the on cancel handler.
              sendSecondResponseP.future.onComplete(_ =>
                afterCancelAction match {
                  case () => observer.onCompleted()
                  case status: Status => observer.onError(status.asRuntimeException())
                  case response: Hello.Response => observer.onNext(response)
                }
              )
            }

            val context = Context.current().withCancellation()
            context.run(() => {
              receivedRequestP.future.onComplete(_ => context.cancel(Exception))
              callStreamedServiceAndCheckClientFailure(
                ClientCancelsStatus,
                clientCause = Exception,
                checkResponses = false, // Some responses may get discarded due to cancellation.
              )
            })

            assertRequestAndResponsesLogged
            capturingLogger.assertNextMessageIs(createExpectedLogMessage("cancelled"), INFO)

            // Wait until the server has received the cancellation
            eventually() {
              apiRequestLogger.cancelled.get() shouldBe true
            }
            capturingLogger.assertNoMoreEvents()

            sendSecondResponseP.success(())

            afterCancelAction match {
              case _: Status =>
                capturingLogger.assertNextMessageIs(
                  createExpectedLogMessage(expectedLogMessage),
                  expectedLogLevel,
                  expectedThrowable,
                )
              case _: Hello.Response =>
                capturingLogger.assertNextMessage(
                  _ should include("sending response Response(Hello client)"),
                  DEBUG,
                )
              case () =>
                capturingLogger.assertNextMessage(
                  _ should include("HelloService/HelloStreamed by testSender: succeeded(OK)"),
                  DEBUG,
                )
            }
          }
        }
    }
  }

  "On a unary call with payload logging suppressed" when {
    def createExpectedLogMessage(
        content: String,
        includeRequestTraceContext: Boolean = false,
    ): String = {
      val mainMessage = s"Request c.d.c.d.a.v.HelloService/Hello by testSender: $content"
      val traceContextMessage = s"\n  Request ${requestTraceContext.showTraceId}"
      if (includeRequestTraceContext) mainMessage + traceContextMessage else mainMessage
    }

    "intercepting a successful request" must {
      "not log any messages" in withEnv(logMessagePayloads = false) { implicit env =>
        import env.*

        when(service.hello(Request)).thenReturn(Future.successful(Response))

        client.hello(Request).futureValue shouldBe Response

        capturingLogger.assertNextMessageIs(createExpectedLogMessage("received headers "), TRACE)
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage("received a message ", includeRequestTraceContext = true),
          DEBUG,
        )
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage("finished receiving messages"),
          TRACE,
        )
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage("sending response headers "),
          TRACE,
        )
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage("sending response ", includeRequestTraceContext = true),
          DEBUG,
        )
        capturingLogger.assertNextMessageIs(createExpectedLogMessage("succeeded(OK)"), DEBUG)
        capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
      }
    }

    "intercepting a failure" must {
      "not log any metadata" in withEnv(logMessagePayloads = false) { implicit env =>
        import env.*

        val status = InvalidArgumentStatus
        val expectedLogMessage = s"failed with INVALID_ARGUMENT/${status.getDescription}"

        when(service.hello(Request)).thenReturn(Future.failed(status.asRuntimeException(Trailers)))

        assertClientFailure(client.hello(Request), status, Trailers)

        capturingLogger.assertNextMessageIs(createExpectedLogMessage("received headers "), TRACE)
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage("received a message ", includeRequestTraceContext = true),
          DEBUG,
        )
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage("finished receiving messages"),
          TRACE,
        )
        capturingLogger.assertNextMessageIs(
          createExpectedLogMessage(expectedLogMessage),
          INFO,
          status.getCause,
        )
        capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
      }
    }
  }

  "On a streamed call with very short message limit" when {
    def createExpectedLogMessage(
        content: String,
        includeRequestTraceContext: Boolean = false,
    ): String = {
      val mainMessage = s"Request c.d.c.d.a.v.HelloService/HelloStreamed by testSender: $content"
      val traceContextMessage = s"\n  Request ${requestTraceContext.showTraceId}"
      if (includeRequestTraceContext) mainMessage + traceContextMessage else mainMessage
    }

    def assertRequestAndResponsesLogged: Assertion = {
      capturingLogger.assertNextMessageIs(
        "Request c.d.c.d.a.v.HelloService/HelloStreamed by testSender: " +
          "received headers Met...",
        TRACE,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage(
          "received a message Request(Hel...)",
          includeRequestTraceContext = true,
        ),
        DEBUG,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage("finished receiving messages"),
        TRACE,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage("sending response headers Met..."),
        TRACE,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage(
          "sending response Response(Hel...)",
          includeRequestTraceContext = true,
        ),
        DEBUG,
      )
      capturingLogger.assertNextMessageIs(
        createExpectedLogMessage(
          "sending response Response(Hel...)",
          includeRequestTraceContext = true,
        ),
        DEBUG,
      )
    }

    "intercepting a successful request" must {
      "log a short version of messages" in withEnv(maxStringLength = 3, maxMetadataSize = 3) {
        implicit env =>
          import env.*

          setupStreamedService(_.onCompleted())

          val observer = new RecordingStreamObserver[Hello.Response]
          client.helloStreamed(Request, observer)
          observer.result.futureValue
          observer.responses should have size 2

          assertRequestAndResponsesLogged
          capturingLogger.assertNextMessageIs(createExpectedLogMessage("succeeded(OK)"), DEBUG)
          capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
      }
    }

    "intercepting a failure" must {
      "log a short version of the metadata" in withEnv(maxStringLength = 3, maxMetadataSize = 3) {
        implicit env =>
          val status = InvalidArgumentStatus
          val expectedLogMessage =
            s"failed with INVALID_ARGUMENT/${status.getDescription}\n  Trailers: Met..."

          setupStreamedService(_.onError(status.asRuntimeException(Trailers)))

          callStreamedServiceAndCheckClientFailure(status, Trailers)

          assertRequestAndResponsesLogged
          capturingLogger.assertNextMessageIs(
            createExpectedLogMessage(expectedLogMessage),
            INFO,
            status.getCause,
          )
          capturingLogger.assertNextMessageIs(createExpectedLogMessage("completed"), DEBUG)
      }
    }
  }
}
