// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import java.time.Duration
import java.util.concurrent.TimeUnit
import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.{
  IsStatusException,
  MockMessages,
  SuiteResourceManagementAroundAll,
}
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.daml.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.daml.ledger.api.v1.value.{Record, RecordField}
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
  NotOkResponse,
}
import com.daml.ledger.client.services.commands.{
  CommandClient,
  CommandSubmission,
  CompletionStreamElement,
}
import com.daml.ledger.client.services.testing.time.StaticTime
import com.daml.ledger.runner.common.Config
import com.daml.lf.crypto.Hash
import com.daml.lf.value.Value.ContractId
import com.daml.platform.participant.util.ValueConversions._
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.sandbox.services.TestCommands
import com.daml.util.Ctx
import com.google.rpc.code.Code
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.control.NonFatal

final class CommandClientIT
    extends AsyncWordSpec
    with TestCommands
    with SandboxFixture
    with Matchers
    with SuiteResourceManagementAroundAll
    with TryValues
    with Inside {

  private val defaultCommandClientConfiguration =
    CommandClientConfiguration(
      maxCommandsInFlight = 1,
      maxParallelSubmissions = 1,
      defaultDeduplicationTime = Duration.ofSeconds(30),
    )

  private val testLedgerId = domain.LedgerId("ledgerId")
  private val testNotLedgerId = domain.LedgerId("hotdog")

  private def commandClientWithoutTime(
      ledgerId: domain.LedgerId,
      applicationId: String = MockMessages.applicationId,
      configuration: CommandClientConfiguration = defaultCommandClientConfiguration,
  ): CommandClient =
    new CommandClient(
      CommandSubmissionServiceGrpc.stub(channel),
      CommandCompletionServiceGrpc.stub(channel),
      ledgerId,
      applicationId,
      configuration,
    )

  private def timeProvider(
      ledgerId: domain.LedgerId
  ): Future[TimeProvider] = {
    StaticTime
      .updatedVia(TimeServiceGrpc.stub(channel), ledgerId.unwrap)
      .recover { case NonFatal(_) => TimeProvider.UTC }
  }

  private def commandClient(
      ledgerId: domain.LedgerId = testLedgerId,
      applicationId: String = MockMessages.applicationId,
      configuration: CommandClientConfiguration = defaultCommandClientConfiguration,
  ): Future[CommandClient] =
    timeProvider(ledgerId)
      .map(_ => commandClientWithoutTime(ledgerId, applicationId, configuration))

  override protected def config: Config = super.config.copy(ledgerId = testLedgerId.unwrap)

  private val submittingPartyList = List(MockMessages.party)
  private val LedgerBegin = LedgerOffset(Boundary(LEDGER_BEGIN))

  private def submitRequest(commandId: String, individualCommands: Seq[Command]): SubmitRequest =
    buildRequest(testLedgerId, commandId, individualCommands)

  private def submitRequestWithId(commandId: String): SubmitRequest =
    submitRequest(
      commandId,
      List(
        CreateCommand(
          Some(templateIds.dummy),
          Some(
            Record(
              Some(templateIds.dummy),
              Seq(RecordField("operator", Option(MockMessages.party.asParty))),
            )
          ),
        ).wrap
      ),
    )

  private def commandSubmissionWithId(commandId: String): CommandSubmission =
    CommandSubmission(submitRequestWithId(commandId).getCommands)

  // Commands and completions can be read out of order. Since we use GRPC monocalls to send,
  // they can even be sent out of order.
  // With this, we make it happen on purpose, so order-based flakiness is caught quickly.
  private def randomDelay[T](t: T): Source[T, NotUsed] =
    Source.single(t).delay(FiniteDuration((Math.random() * 25).toLong, TimeUnit.MILLISECONDS))

  /** Reads a set of elements expected in the given source. Returns a pair of sets (elements seen, elements not seen).
    */
  private def readExpectedElements[T](
      src: Source[T, NotUsed],
      expected: Set[T],
      timeLimit: Span,
  ): Future[(Set[T], Set[T])] =
    src
      .scan((Set[T](), expected)) { case ((elementsSeen, elementsUnseen), t) =>
        (elementsSeen + t, elementsUnseen - t)
      }
      .takeWhile({ case (_, remainingElements) => remainingElements.nonEmpty }, inclusive = true)
      .takeWithin(timeLimit)
      .runWith(Sink.seq)
      .map(_.last) // one element is guaranteed

  private def submitCommand(
      req: SubmitRequest
  ): Future[Either[CompletionFailure, CompletionSuccess]] =
    commandClient().flatMap(_.trackSingleCommand(req))

  private def assertCommandFailsWithCode(
      submitRequest: SubmitRequest,
      expectedErrorCode: Code,
      expectedMessageSubString: String,
  ): Future[Assertion] =
    submitCommand(submitRequest).map { result =>
      inside(result) { case Left(notOk: NotOkResponse) =>
        notOk.grpcStatus.code should be(expectedErrorCode.value)
        notOk.grpcStatus.message should include(expectedMessageSubString)
      }
    }

  /** Reads a set of command IDs expected in the given client after the given checkpoint.
    * Returns a pair of sets (elements seen, elements not seen).
    */
  private def readExpectedCommandIds(
      client: CommandClient,
      checkpoint: LedgerOffset,
      expected: Set[String],
      timeLimit: Span = 6.seconds,
  ): Future[(Set[String], Set[String])] =
    readExpectedElements(
      client.completionSource(submittingPartyList, checkpoint).collect {
        case CompletionStreamElement.CompletionElement(c, _) => c.commandId
      },
      expected,
      timeLimit,
    )

  private def recordWithArgument(original: Record, fieldToInclude: RecordField): Record =
    original.update(_.fields.modify(recordFieldsWithArgument(_, fieldToInclude)))

  private def recordFieldsWithArgument(
      originalFields: Seq[RecordField],
      fieldToInclude: RecordField,
  ): Seq[RecordField] = {
    var replacedAnElement: Boolean = false
    val updated = originalFields.map { original =>
      if (original.label == fieldToInclude.label) {
        replacedAnElement = true
        fieldToInclude
      } else {
        original
      }
    }
    if (replacedAnElement) updated else originalFields :+ fieldToInclude
  }

  "Command Client" when {
    "asked for ledger end" should {

      "return it" in {
        for {
          client <- commandClient()
          _ <- client.getCompletionEnd()
        } yield {
          succeed
        }
      }

      "fail with the expected status on a ledger Id mismatch" in {
        commandClientWithoutTime(testNotLedgerId)
          .getCompletionEnd()
          .failed map IsStatusException(Status.NOT_FOUND)
      }
    }

    "submitting commands" should {

      "return the contexts for commands as they are submitted" in {
        val contexts = 1 to 10

        for {
          client <- commandClient()
          result <- Source(contexts.map(i => Ctx(i, commandSubmissionWithId(i.toString))))
            .via(client.submissionFlow())
            .map(_.map(_.isSuccess))
            .runWith(Sink.seq)
        } yield {
          result should contain theSameElementsAs contexts.map(Ctx(_, true))
        }
      }

      "fail with the expected status on a ledger Id mismatch" in {
        val aSubmission = commandSubmissionWithId("1")
        val submission = aSubmission.copy(
          commands = aSubmission.commands.update(_.ledgerId := testNotLedgerId.unwrap)
        )
        Source
          .single(Ctx(1, submission))
          .via(commandClientWithoutTime(testNotLedgerId).submissionFlow())
          .runWith(Sink.head)
          .map(err => IsStatusException(Status.NOT_FOUND)(err.value.failure.exception))
      }

      "fail with INVALID REQUEST for empty application ids" in {
        val request = submitRequestWithId("7000").update(_.commands.applicationId := "")
        val resF = for {
          client <- commandClient(applicationId = "")
          res <- client.submitSingleCommand(request)
        } yield res

        resF.failed.map { failure =>
          failure should be(a[StatusRuntimeException])
          val ex = failure.asInstanceOf[StatusRuntimeException]
          ex.getStatus.getCode.value() shouldEqual Code.INVALID_ARGUMENT.value
          ex.getStatus.getDescription should include("application_id")
        }
      }
    }

    "reading completions" should {

      "fail with INVALID REQUEST for empty application ids" in {
        val completionsF = for {
          client <- commandClient(applicationId = "")
          completionsSource = client.completionSource(submittingPartyList, LedgerBegin)
          completions <- completionsSource.takeWithin(5.seconds).runWith(Sink.seq)
        } yield completions

        completionsF.failed.map { failure =>
          failure should be(a[StatusRuntimeException])
          val ex = failure.asInstanceOf[StatusRuntimeException]
          ex.getStatus.getCode.value() shouldEqual Code.INVALID_ARGUMENT.value
          ex.getStatus.getDescription should include("application_id")
        }
      }

      "fail with the expected status on a ledger Id mismatch" in {
        commandClientWithoutTime(testNotLedgerId)
          .completionSource(submittingPartyList, LedgerBegin)
          .runWith(Sink.head)
          .failed map IsStatusException(Status.NOT_FOUND)
      }

      "return completions of commands submitted before subscription if they are after the offset" in {
        val numCommands = 10
        val offset = 5000
        val lastCommandId = offset + numCommands - 1
        val commandIds = offset to lastCommandId
        val commandIdStrings = Set(commandIds.map(_.toString): _*)

        // val for type inference
        val resultF = for {
          client <- commandClient()
          checkpoint <- client.getCompletionEnd()
          submissionResults <- Source(
            commandIds.map(i => Ctx(i, commandSubmissionWithId(i.toString)))
          )
            .flatMapMerge(10, randomDelay)
            .via(client.submissionFlow())
            .map(_.value)
            .runWith(Sink.seq)
          _ = submissionResults.foreach(v => v shouldBe a[Success[_]])

          result <- readExpectedCommandIds(client, checkpoint.getOffset, commandIdStrings)
        } yield {
          result
        }

        resultF map { case (seenCommandIds, remainingCommandIds) =>
          // N.B.: completions may include already-seen elements, and may be out of order
          seenCommandIds should contain allElementsOf commandIdStrings
          remainingCommandIds.toList should have length 0
          Succeeded
        }
      }

      "return completions of commands that are submitted after subscription" in {
        val numCommands = 10
        val offset = 5100
        val lastCommandId = offset + numCommands - 1
        val commandIds = offset to lastCommandId
        val commandIdStrings = Set(commandIds.map(_.toString): _*)

        for {
          client <- commandClient()
          checkpoint <- client.getCompletionEnd()
          _ <- Source(commandIds.map(i => Ctx(i, commandSubmissionWithId(i.toString))))
            .flatMapMerge(10, randomDelay)
            .via(client.submissionFlow())
            .map(_.context)
            .runWith(Sink.ignore)
          (seenCommandIds, remainingCommandIds) <- readExpectedCommandIds(
            client,
            checkpoint.getOffset,
            commandIdStrings,
          )
        } yield {
          seenCommandIds should contain allElementsOf commandIdStrings
          remainingCommandIds.toList should have length 0
        }
      }
    }

    "tracking commands" should {

      "return the contexts for commands as they are completed" in {
        val contexts = 6001.to(6010)

        for {
          client <- commandClient()
          tracker <- client.trackCommands[Int](submittingPartyList)
          result <- Source(contexts.map(i => Ctx(i, commandSubmissionWithId(i.toString))))
            .via(tracker)
            .map(_.context)
            .runWith(Sink.seq)
        } yield {
          result should contain theSameElementsAs contexts
        }
      }

      "complete the stream when there's nothing to track" in {
        for {
          client <- commandClient()
          tracker <- client.trackCommands[Int](submittingPartyList)
          _ <- Source.empty[Ctx[Int, CommandSubmission]].via(tracker).runWith(Sink.ignore)
        } yield {
          succeed
        }
      }

      "not accept commands with missing args, return INVALID_ARGUMENT" in {
        val expectedMessageSubstring =
          "Expecting 1 field for record"
        val commandWithInvalidArgs =
          submitRequest(
            "Creating_contracts_for_invalid_arg_test",
            List(CreateCommand(Some(templateIds.dummy), Some(Record())).wrap),
          )

        assertCommandFailsWithCode(
          commandWithInvalidArgs,
          Code.INVALID_ARGUMENT,
          expectedMessageSubstring,
        )
      }

      "not accept commands with args of the wrong type, return INVALID_ARGUMENT" in {
        val expectedMessageSubstring =
          "mismatching type"
        val command =
          submitRequest(
            "Boolean_param_with_wrong_type",
            List(
              CreateCommand(
                Some(templateIds.dummy),
                Some(
                  List("operator" -> true.asBoolean)
                    .asRecordOf(templateIds.dummy)
                ),
              ).wrap
            ),
          )

        assertCommandFailsWithCode(
          command,
          Code.INVALID_ARGUMENT,
          expectedMessageSubstring,
        )
      }

      "not accept commands with unknown args, return INVALID_ARGUMENT" in {
        val expectedMessageSubstring =
          "Missing record field"
        val command =
          submitRequest(
            "Param_with_wrong_name",
            List(
              CreateCommand(
                Some(templateIds.dummy),
                Some(
                  List("hotdog" -> true.asBoolean)
                    .asRecordOf(templateIds.dummy)
                ),
              ).wrap
            ),
          )

        assertCommandFailsWithCode(
          command,
          Code.INVALID_ARGUMENT,
          expectedMessageSubstring,
        )
      }

      "not accept commands with malformed decimals, return INVALID_ARGUMENT" in {
        val commandId = "Malformed_decimal"
        val expectedMessageSubString =
          """Could not read Numeric string "1E-19""""

        val command = submitRequest(
          commandId,
          List(
            CreateCommand(
              Some(templateIds.parameterShowcase),
              Some(recordWithArgument(paramShowcaseArgs, RecordField("decimal", "1E-19".asNumeric))),
            ).wrap
          ),
        )

        assertCommandFailsWithCode(command, Code.INVALID_ARGUMENT, expectedMessageSubString)
      }

      "not accept commands with bad obligables, return INVALID_ARGUMENT" in {
        val command =
          submitRequest(
            "Obligable_error",
            List(
              CreateCommand(
                Some(templateIds.dummy),
                Some(
                  List("operator" -> ("not" + MockMessages.party).asParty)
                    .asRecordOf(templateIds.dummy)
                ),
              ).wrap
            ),
          )

        assertCommandFailsWithCode(command, Code.INVALID_ARGUMENT, "requires authorizers")
      }

      "not accept exercises with bad contract IDs, return ABORTED" in {
        val contractId = ContractId.V1(
          Hash.hashPrivateKey(
            "#deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef-123"
          )
        )
        val command =
          submitRequest(
            "Exercise_contract_not_found",
            List(
              ExerciseCommand(
                Some(templateIds.dummy),
                contractId.coid,
                "DummyChoice1",
                Some(unit),
              ).wrap
            ),
          )

        assertCommandFailsWithCode(command, Code.NOT_FOUND, "CONTRACT_NOT_FOUND")
      }
    }
  }
}
