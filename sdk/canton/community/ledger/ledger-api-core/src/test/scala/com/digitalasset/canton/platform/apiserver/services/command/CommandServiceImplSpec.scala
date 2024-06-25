// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.RpcProtoExtractors
import com.daml.ledger.api.v2.checkpoint.Checkpoint
import com.daml.ledger.api.v2.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.ledger.api.v2.command_submission_service.{SubmitRequest, SubmitResponse}
import com.daml.ledger.api.v2.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.digitalasset.daml.lf.data.Ref
import com.daml.tracing.DefaultOpenTelemetry
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  ValidateUpgradingPackageResolutions,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, LoggingContextWithTrace}
import com.digitalasset.canton.platform.apiserver.services.command.CommandServiceImplSpec.*
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.SubmissionKey
import com.digitalasset.canton.platform.apiserver.services.tracking.{
  CompletionResponse,
  SubmissionTracker,
}
import com.digitalasset.canton.platform.apiserver.services.{ApiCommandService, tracking}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import com.google.rpc.Code
import com.google.rpc.status.Status as StatusProto
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Context, Deadline, Status}
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

@SuppressWarnings(Array("com.digitalasset.canton.TryFailed"))
class CommandServiceImplSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with BaseTest
    with HasExecutionContext {

  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private val telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build())

  s"the command service" should {
    "submit a request, and wait for a response" in withTestContext { testContext =>
      import testContext.*

      openChannel(
        new CommandServiceImpl(
          UnimplementedTransactionServices,
          submissionTracker,
          submit,
          config.NonNegativeFiniteDuration.ofSeconds(1000L),
          loggerFactory,
        )
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub.submitAndWaitForUpdateId(request).map { response =>
          verify(submissionTracker).track(
            eqTo(expectedSubmissionKey),
            eqTo(config.NonNegativeFiniteDuration.ofSeconds(1000L)),
            any[TraceContext => Future[Any]],
          )(any[ContextualizedErrorLogger], any[TraceContext])
          response.updateId should be("transaction ID")
          response.completionOffset shouldBe "offset"
        }
      }
    }

    "pass the provided deadline to the tracker as a timeout" in withTestContext { testContext =>
      import testContext.*

      val now = Instant.parse("2021-09-01T12:00:00Z")
      val deadlineTicker = new Deadline.Ticker {
        override def nanoTime(): Long =
          now.getEpochSecond * TimeUnit.SECONDS.toNanos(1) + now.getNano
      }

      openChannel(
        new CommandServiceImpl(
          UnimplementedTransactionServices,
          submissionTracker,
          submit,
          config.NonNegativeFiniteDuration.ofSeconds(1L),
          loggerFactory,
        ),
        deadlineTicker,
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub
          .withDeadline(Deadline.after(3600L, TimeUnit.SECONDS, deadlineTicker))
          .submitAndWaitForUpdateId(request)
          .map { response =>
            verify(submissionTracker).track(
              eqTo(expectedSubmissionKey),
              eqTo(config.NonNegativeFiniteDuration.ofSeconds(3600L)),
              any[TraceContext => Future[Any]],
            )(any[ContextualizedErrorLogger], any[TraceContext])
            response.updateId should be("transaction ID")
            succeed
          }
      }
    }

    "reject and do not submit on deadline exceeded" in withTestContext { testContext =>
      import testContext.*

      val service = new CommandServiceImpl(
        UnimplementedTransactionServices,
        submissionTracker,
        submit,
        config.NonNegativeFiniteDuration.ofSeconds(1L),
        loggerFactory,
      )

      val deadline = Context
        .current()
        .withDeadline(Deadline.after(0L, TimeUnit.NANOSECONDS), scheduledExecutor())

      deadline
        .call(() => {
          service
            .submitAndWaitForUpdateId(
              SubmitAndWaitRequest.of(Some(commands.copy(submissionId = submissionId)))
            )(
              LoggingContextWithTrace.ForTesting
            )
            .transform { response =>
              verify(submissionTracker, never).track(
                any[SubmissionKey],
                any[config.NonNegativeFiniteDuration],
                any[TraceContext => Future[Any]],
              )(any[ContextualizedErrorLogger], any[TraceContext])

              response.failed.map(inside(_) { case RpcProtoExtractors.Exception(status) =>
                status.getCode shouldBe Code.DEADLINE_EXCEEDED.getNumber
                status.getMessage should fullyMatch regex s"REQUEST_DEADLINE_EXCEEDED\\(3,submissi\\)\\: The gRPC deadline for request with commandId=$commandId and submissionId=$submissionId has expired by .* The request will not be processed further\\."
              })
            }
        })
        .thereafter { _ => deadline.close() }
    }

    "time out if the tracker times out" in withTestContext { testContext =>
      import testContext.*

      when(
        submissionTracker.track(
          eqTo(expectedSubmissionKey),
          any[config.NonNegativeFiniteDuration],
          any[TraceContext => Future[Any]],
        )(
          any[ContextualizedErrorLogger],
          any[TraceContext],
        )
      ).thenReturn(
        Future.fromTry(
          CompletionResponse.timeout("some-cmd-id", "some-submission-id")(
            ErrorLoggingContext(
              loggerFactory.getTracedLogger(this.getClass),
              LoggingContextWithTrace.ForTesting,
            )
          )
        )
      )

      val service = new CommandServiceImpl(
        UnimplementedTransactionServices,
        submissionTracker,
        submit,
        config.NonNegativeFiniteDuration.ofSeconds(1337L),
        loggerFactory,
      )

      openChannel(
        service: CommandServiceImpl
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub.submitAndWaitForUpdateId(request).failed.map {
          case RpcProtoExtractors.Exception(RpcProtoExtractors.Status(Code.DEADLINE_EXCEEDED)) =>
            succeed
          case unexpected => fail(s"Unexpected exception", unexpected)
        }
      }
    }

    "close the supplied tracker when closed" in withTestContext { testContext =>
      import testContext.*

      val service = new CommandServiceImpl(
        UnimplementedTransactionServices,
        submissionTracker,
        submit,
        config.NonNegativeFiniteDuration.ofSeconds(1337L),
        loggerFactory,
      )

      verifyZeroInteractions(submissionTracker)

      service.close()
      verify(submissionTracker).close()
      succeed
    }
  }

  private class TestContext {
    val trackerCompletionResponse = tracking.CompletionResponse(
      completion = completion,
      checkpoint = Some(
        Checkpoint(offset = Some(ParticipantOffset(ParticipantOffset.Value.Absolute("offset"))))
      ),
    )
    val commands = someCommands()
    val submissionTracker = mock[SubmissionTracker]
    val submit = mock[Traced[SubmitRequest] => Future[SubmitResponse]]
    when(
      submissionTracker.track(
        eqTo(expectedSubmissionKey),
        any[config.NonNegativeFiniteDuration],
        any[TraceContext => Future[Any]],
      )(
        any[ContextualizedErrorLogger],
        any[TraceContext],
      )
    ).thenReturn(Future.successful(trackerCompletionResponse))
  }

  private def withTestContext(test: TestContext => Future[Assertion]): Future[Assertion] =
    test(new TestContext)

  private def openChannel(
      service: CommandServiceImpl,
      deadlineTicker: Deadline.Ticker = Deadline.getSystemTicker,
  ): ResourceOwner[CommandServiceGrpc.CommandServiceStub] = {
    val commandsValidator = new CommandsValidator(
      validateUpgradingPackageResolutions = ValidateUpgradingPackageResolutions.Empty
    )
    val apiService = new ApiCommandService(
      service = service,
      commandsValidator = commandsValidator,
      currentLedgerTime = () => Instant.EPOCH,
      currentUtcTime = () => Instant.EPOCH,
      maxDeduplicationDuration = maxDeduplicationDuration,
      generateSubmissionId = () => submissionId,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    )
    for {
      name <- ResourceOwner.forValue(() => UUID.randomUUID().toString)
      _ <- ResourceOwner.forServer(
        InProcessServerBuilder
          .forName(name)
          .deadlineTicker(deadlineTicker)
          .addService(() => CommandServiceGrpc.bindService(apiService, parallelExecutionContext)),
        shutdownTimeout = 10.seconds,
      )
      channel <- ResourceOwner.forChannel(
        InProcessChannelBuilder.forName(name),
        shutdownTimeout = 10.seconds,
      )
    } yield CommandServiceGrpc.stub(channel)
  }
}

object CommandServiceImplSpec {
  private val UnimplementedTransactionServices = new CommandServiceImpl.TransactionServices(
    getTransactionTreeById = _ =>
      Future.failed(new RuntimeException("This should never be called.")),
    getTransactionById = _ => Future.failed(new RuntimeException("This should never be called.")),
  )

  private val OkStatus = StatusProto.of(Status.Code.OK.value, "", Seq.empty)

  val commandId = "command ID"
  val applicationId = "application ID"
  val submissionId = Ref.SubmissionId.assertFromString("submissionId")
  val maxDeduplicationDuration = java.time.Duration.ofDays(1)
  val party = "Alice"

  val command = Command.of(
    Command.Command.Create(
      CreateCommand.of(
        Some(Identifier("package", moduleName = "module", entityName = "entity")),
        Some(
          Record(
            Some(Identifier("package", moduleName = "module", entityName = "entity")),
            Seq(RecordField("something", Some(Value(Value.Sum.Bool(true))))),
          )
        ),
      )
    )
  )

  val completion = Completion(
    commandId = "command ID",
    status = Some(OkStatus),
    updateId = "transaction ID",
  )

  val expectedSubmissionKey = SubmissionKey(
    commandId = commandId,
    submissionId = submissionId,
    applicationId = applicationId,
    parties = Set(party),
  )

  private def someCommands() = Commands(
    commandId = commandId,
    applicationId = applicationId,
    actAs = Seq(party),
    commands = Seq(command),
  )
}
