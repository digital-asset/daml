// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.{CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.{Record, RecordField}
import com.digitalasset.ledger.client.services.commands.{CommandClient, CompletionStreamElement}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.TestTemplateIds
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.platform.tests.integration.ledger.api.ParameterShowcaseTesting
import com.digitalasset.util.Ctx
import com.google.rpc.code.Code
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, Succeeded, TryValues}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandClientIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with TestExecutionSequencerFactory
    with Matchers
    with SuiteResourceManagementAroundAll
    with ParameterShowcaseTesting
    with TryValues
    with MultiLedgerCommandUtils
    with TestTemplateIds {

  private val submittingParty: String = submitRequest.getCommands.party
  private val submittingPartyList = List(submittingParty)
  private val LedgerBegin = LedgerOffset(Boundary(LEDGER_BEGIN))

  private def submitRequestWithId(id: String): SubmitRequest =
    submitRequest.copy(commands = submitRequest.commands.map(_.copy(commandId = id)))

  // Commands and completions can be read out of order. Since we use GRPC monocalls to send,
  // they can even be sent out of order.
  // With this, we make it happen on purpose, so order-based flakiness is caught quickly.
  private def randomDelay[T](t: T): Source[T, NotUsed] =
    Source.single(t).delay(FiniteDuration((Math.random() * 25).toLong, TimeUnit.MILLISECONDS))

  /**
    * Reads a set of elements expected in the given source. Returns a pair of sets (elements seen, elements not seen).
    */
  private def readExpectedElements[T](
      src: Source[T, NotUsed],
      expected: Set[T],
      timeLimit: Span = 3.seconds): Future[(Set[T], Set[T])] =
    src
      .scan((Set[T](), expected)) {
        case ((elementsSeen, elementsUnseen), t) =>
          (elementsSeen + t, elementsUnseen - t)
      }
      .takeWhile({ case (_, remainingElements) => remainingElements.nonEmpty }, inclusive = true)
      .takeWithin(timeLimit)
      .runWith(Sink.seq)
      .map(_.last) // one element is guaranteed

  /**
    * Reads a set of command IDs expected in the given client after the given checkpoint.
    * Returns a pair of sets (elements seen, elements not seen).
    */
  private def readExpectedCommandIds(
      client: CommandClient,
      checkpoint: LedgerOffset,
      expected: Set[String],
      timeLimit: Span = 3.seconds): Future[(Set[String], Set[String])] =
    readExpectedElements(client.completionSource(submittingPartyList, checkpoint).collect {
      case CompletionStreamElement.CompletionElement(c) => c.commandId
    }, expected, timeLimit)

  "Command Client" when {
    "asked for ledger end" should {

      "return it" in allFixtures { ctx =>
        for {
          client <- ctx.commandClient()
          _ <- client.getCompletionEnd
        } yield {
          succeed
        }
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { ctx =>
        ctx.commandClientWithoutTime(testNotLedgerId).getCompletionEnd.failed map IsStatusException(
          Status.NOT_FOUND)
      }
    }

    "submitting commands" should {

      "return the contexts for commands as they are submitted" in allFixtures { ctx =>
        val contexts = 1 to 10

        for {
          client <- ctx.commandClient()
          result <- Source(contexts.map(i => Ctx(i, submitRequestWithId(i.toString))))
            .via(client.submissionFlow)
            .map(_.map(_.isSuccess))
            .runWith(Sink.seq)
        } yield {
          result should contain theSameElementsAs contexts.map(Ctx(_, true))
        }
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { ctx =>
        Source
          .single(
            Ctx(1, submitRequestWithId(1.toString).update(_.commands.ledgerId := testNotLedgerId)))
          .via(ctx.commandClientWithoutTime(testNotLedgerId).submissionFlow)
          .runWith(Sink.head)
          .map(err => IsStatusException(Status.NOT_FOUND)(err.value.failure.exception))
      }

      "fail with INVALID REQUEST for empty application ids" in allFixtures { ctx =>
        val resF = for {
          client <- ctx.commandClient(applicationId = "")
          request = submitRequestWithId(7000.toString).update(_.commands.applicationId := "")
          res <- client.submitSingleCommand(request)
        } yield (res)

        resF.failed.map { failure =>
          failure should be(a[StatusRuntimeException])
          val ex = failure.asInstanceOf[StatusRuntimeException]
          ex.getStatus.getCode.value() shouldEqual Code.INVALID_ARGUMENT.value
          ex.getStatus.getDescription should include("application_id")
        }
      }
    }

    "reading completions" should {

      "fail with INVALID REQUEST for empty application ids" in allFixtures { ctx =>
        val completionsF = for {
          client <- ctx.commandClient(applicationId = "")
          completionsSource = client.completionSource(submittingPartyList, LedgerBegin)
          completions <- completionsSource.takeWithin(5.seconds).runWith(Sink.seq)
        } yield (completions)

        completionsF.failed.map { failure =>
          failure should be(a[StatusRuntimeException])
          val ex = failure.asInstanceOf[StatusRuntimeException]
          ex.getStatus.getCode.value() shouldEqual Code.INVALID_ARGUMENT.value
          ex.getStatus.getDescription should include("application_id")
        }
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { ctx =>
        ctx
          .commandClientWithoutTime(testNotLedgerId)
          .completionSource(submittingPartyList, LedgerBegin)
          .runWith(Sink.head)
          .failed map IsStatusException(Status.NOT_FOUND)
      }

      "return completions of commands submitted before subscription if they are after the offset" in allFixtures {
        ctx =>
          val numCommands = 10
          val offset = 5000
          val lastCommandId = offset + numCommands - 1
          val commandIds = offset to lastCommandId
          val commandIdStrings = Set(commandIds.map(_.toString): _*)

          // val for type inference
          val resultF = for {
            client <- ctx.commandClient()
            checkpoint <- client.getCompletionEnd
            submissionResults <- Source(
              commandIds.map(i => Ctx(i, submitRequestWithId(i.toString))))
              .flatMapMerge(10, randomDelay)
              .via(client.submissionFlow)
              .map(_.value)
              .runWith(Sink.seq)
            _ = submissionResults.foreach(v => v shouldBe a[Success[_]])

            result <- readExpectedCommandIds(client, checkpoint.getOffset, commandIdStrings)
          } yield {
            result
          }

          resultF map {
            case (seenCommandIds, remainingCommandIds) =>
              // N.B.: completions may include already-seen elements, and may be out of order
              seenCommandIds should contain allElementsOf commandIdStrings
              remainingCommandIds.toList should have length 0
              Succeeded
          }
      }

      "return completions of commands that are submitted after subscription" in allFixtures { ctx =>
        val numCommands = 10
        val offset = 5100
        val lastCommandId = offset + numCommands - 1
        val commandIds = offset to lastCommandId
        val commandIdStrings = Set(commandIds.map(_.toString): _*)

        // val for type inference
        val resultF = for {
          client <- ctx.commandClient()
          checkpoint <- client.getCompletionEnd
          result = readExpectedCommandIds(client, checkpoint.getOffset, commandIdStrings)
          _ <- Source(commandIds.map(i => Ctx(i, submitRequestWithId(i.toString))))
            .flatMapMerge(10, randomDelay)
            .via(client.submissionFlow)
            .map(_.context)
            .runWith(Sink.ignore)
        } yield {
          result
        }

        resultF.flatten map {
          case (seenCommandIds, remainingCommandIds) =>
            // N.B.: completions may include already-seen elements, and may be out of order
            seenCommandIds should contain allElementsOf commandIdStrings
            remainingCommandIds.toList should have length 0
            Succeeded
        }
      }
    }

    "tracking commands" should {

      "return the contexts for commands as they are completed" in allFixtures { ctx =>
        val contexts = 6001.to(6010)

        for {
          client <- ctx.commandClient()
          tracker <- client.trackCommands[Int](submittingPartyList)
          result <- Source(contexts.map(i => Ctx(i, submitRequestWithId(i.toString))))
            .via(tracker)
            .map(_.context)
            .runWith(Sink.seq)
        } yield {
          result should contain theSameElementsAs contexts
        }
      }

      "complete the stream when there's nothing to track" in allFixtures { ctx =>
        for {
          client <- ctx.commandClient()
          tracker <- client.trackCommands[Int](submittingPartyList)
          result <- Source.empty[Ctx[Int, SubmitRequest]].via(tracker).runWith(Sink.ignore)
        } yield {
          succeed
        }
      }

      "not accept double spends, return INVALID_ARGUMENT" in allFixtures { c =>
        for {
          contract <- c.submitCreate(
            "Creating contracts for double spend test",
            templateIds.dummyFactory,
            List("operator" -> submittingParty.asParty).asRecordFields,
            submittingParty)
          _ <- c.submitExercise(
            "Double spend test exercise 1",
            templateIds.dummyFactory,
            unit,
            "DummyFactoryCall",
            contract.contractId,
            submittingParty)
          assertion <- c.testingHelpers.assertCommandFailsWithCode(
            c.command(
              "Double spend test exercise 2",
              List(
                ExerciseCommand(
                  Some(templateIds.dummyFactory),
                  contract.contractId,
                  "DummyFactoryCall",
                  Some(unit)).wrap)),
            Code.INVALID_ARGUMENT,
            "dependency",
            ignoreCase = true
          )
        } yield {
          assertion
        }
      }

      "not accept commands with missing args, return INVALID_ARGUMENT" in allFixtures { c =>
        val expectedMessageSubstring =
          "Expecting 1 field for record"
        val commandWithInvalidArgs =
          c.command(
            "Creating contracts for invalid arg test",
            List(CreateCommand(Some(templateIds.dummy), Some(Record())).wrap))

        c.testingHelpers.assertCommandFailsWithCode(
          commandWithInvalidArgs,
          Code.INVALID_ARGUMENT,
          expectedMessageSubstring
        )
      }

      "not accept commands with args of the wrong type, return INVALID_ARGUMENT" in allFixtures {
        c =>
          val expectedMessageSubstring =
            "mismatching type"
          val command =
            c.command(
              "Boolean param with wrong type",
              List(
                CreateCommand(
                  Some(templateIds.dummy),
                  Some(List("operator" -> true.asBoolean)
                    .asRecordOf(templateIds.dummy))).wrap)
            )

          c.testingHelpers.assertCommandFailsWithCode(
            command,
            Code.INVALID_ARGUMENT,
            expectedMessageSubstring
          )
      }

      "not accept commands with unknown args, return INVALID_ARGUMENT" in allFixtures { c =>
        val expectedMessageSubstring =
          "Mismatching record label"
        val command =
          c.command(
            "Param with wrong name",
            List(
              CreateCommand(
                Some(templateIds.dummy),
                Some(List("hotdog" -> true.asBoolean)
                  .asRecordOf(templateIds.dummy))).wrap)
          )

        c.testingHelpers.assertCommandFailsWithCode(
          command,
          Code.INVALID_ARGUMENT,
          expectedMessageSubstring
        )
      }

      "return stack trace in case of interpretation error" in allFixtures { c =>
        // Note: we only check that the beginning of the error matches otherwise every time the package id
        // changes this test fails.
        val expectedMessageSubstring =
          "Command interpretation error in LF-DAMLe: Interpretation error: Error: User abort: Assertion failed. Details: Last location: [DA.Internal.Assert:19], partial transaction: root node"
        val command = c.createCommand(
          "Dummy for failing assert",
          templateIds.dummy,
          List("operator" -> "party".asParty).asRecordFields,
          "party")
        for {
          tx <- c.testingHelpers.submitAndListenForSingleResultOfCommand(
            command,
            TransactionFilter(Map("party" -> Filters.defaultInstance)))
          create = c.testingHelpers.findCreatedEventIn(tx, templateIds.dummy)
          res <- c.testingHelpers.assertCommandFailsWithCode(
            c.exerciseCommand(
              "Failing assert",
              templateIds.dummy,
              Nil.asRecordValue,
              "FailingClone",
              create.contractId,
              "party"),
            Code.INVALID_ARGUMENT,
            expectedMessageSubstring
          )
        } yield res

      }

      "not accept commands with malformed decimals, return INVALID_ARGUMENT" in allFixtures { c =>
        val commandId = "Malformed decimal"
        val expectedMessageSubString =
          """Could not read Decimal string "1E-19""""

        val command = c.command(
          commandId,
          List(
            CreateCommand(
              Some(templateIds.parameterShowcase),
              Some(
                c.testingHelpers.recordWithArgument(
                  Record(
                    Some(templateIds.parameterShowcase),
                    paramShowcaseArgs(templateIds.testPackageId)),
                  RecordField("decimal", "1E-19".asDecimal)))
            ).wrap)
        )

        c.testingHelpers.assertCommandFailsWithCode(
          command,
          Code.INVALID_ARGUMENT,
          expectedMessageSubString)
      }

      "not accept commands with bad obligables, return INVALID_ARGUMENT" in allFixtures { c =>
        val command =
          c.command(
            "Obligable error",
            List(
              CreateCommand(
                Some(templateIds.dummy),
                Some(List("operator" -> ("not" + submittingParty).asParty)
                  .asRecordOf(templateIds.dummy))).wrap)
          )

        c.testingHelpers.assertCommandFailsWithCode(
          command,
          Code.INVALID_ARGUMENT,
          "requires authorizers")
      }

      "not accept exercises with bad contract IDs, return INVALID_ARGUMENT" in allFixtures { c =>
        val contractId = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef-123"
        val command =
          c.command(
            "Exercise contract not found",
            List(
              ExerciseCommand(Some(templateIds.dummy), contractId, "DummyChoice1", Some(unit)).wrap)
          )

        c.testingHelpers.assertCommandFailsWithCode(command, Code.INVALID_ARGUMENT, "error")
      }

      "not accept exercises of bad choices, return INVALID_ARGUMENT" in allFixtures { c =>
        for {
          contract <- c.submitCreate(
            "Creating contract for bad choice test",
            templateIds.dummyFactory,
            List("operator" -> submittingParty.asParty).asRecordFields,
            submittingParty)
          assertion <- c.testingHelpers.assertCommandFailsWithCode(
            c.command(
              "Bad choice test",
              List(
                ExerciseCommand(
                  Some(templateIds.dummyFactory),
                  contract.contractId,
                  "hotdog",
                  Some(unit)).wrap)),
            Code.INVALID_ARGUMENT,
            "Couldn't find requested choice"
          )
        } yield {
          assertion
        }
      }
    }
  }
}
