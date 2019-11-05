// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.util.UUID

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.MockMessages.{party, submitRequest}
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundEach
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.{Command, CreateAndExerciseCommand}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.{
  LEDGER_BEGIN,
  LEDGER_END
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.digitalasset.ledger.api.v1.transaction.TreeEvent.Kind
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.transaction_service.GetLedgerEndResponse
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.platform.participant.util.ValueConversions._
import com.google.rpc.code.Code
import org.scalatest.Inside._
import org.scalatest._
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

// scalafmt cannot deal with this file
// format: off
@SuppressWarnings(Array("org.wartremover.warts.Any"))
abstract class CommandTransactionChecks
  extends AsyncWordSpec
    with MultiLedgerFixture
    with SuiteResourceManagementAroundEach
    with ScalaFutures
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues {
  protected def submitCommand(ctx: LedgerContext, req: SubmitRequest): Future[Completion]

  protected val testTemplateIds = new TestTemplateIds(config)
  protected val templateIds = testTemplateIds.templateIds
  protected val testIdsGenerator = new TestIdsGenerator(config)

  override protected def config: Config = Config.default

  def assertCompletionIsSuccessful(completion: Completion): Assertion = {
    inside(completion) {
      case c => c.getStatus should have('code (0))
    }
  }

  s"Command and Transaction Services" when {

    "interacting with the ledger" should {

      "having many transactions all of them has a unique event id" in allFixtures { ctx =>
        val eventIdsF = ctx.transactionClient
          .getTransactions(
            LedgerOffset(Boundary(LEDGER_BEGIN)),
            Some(LedgerOffset(Boundary(LEDGER_END))),
            TransactionFilter(config.parties.map(_ -> Filters()).toMap)
          )
          .map(_.events
            .map(_.event)
            .collect {
              case Archived(ArchivedEvent(eventId, _, _, _)) => eventId
              case Created(CreatedEvent(eventId, _, _, _, _, _, _, _, _)) => eventId
            })
          .takeWithin(5.seconds) //TODO: work around as ledger end is broken. see DEL-3151
          .runWith(Sink.seq)

        eventIdsF map { eventIds =>
          val eventIdList = eventIds.flatten
          val eventIdSet = eventIdList.toSet
          eventIdList.size shouldEqual eventIdSet.size
        }
      }

      "handle bad Decimals correctly" in allFixtures { ctx =>
        val alice = "Alice"
        for {
          _ <- ctx.testingHelpers.failingCreate(
            testIdsGenerator.testCommandId("Decimal-scale"),
            alice,
            templateIds.decimalRounding,
            Record(fields = List(RecordField(value = Some(alice.asParty)), RecordField(value = Some("0.00000000005".asNumeric)))),
            Code.INVALID_ARGUMENT,
            "Cannot represent 0.00000000005 as (Numeric 10) without lost of precision"
          )
          _ <- ctx.testingHelpers.failingCreate(
            testIdsGenerator.testCommandId("Decimal-bounds-positive"),
            alice,
            templateIds.decimalRounding,
            Record(fields = List(RecordField(value = Some(alice.asParty)), RecordField(value = Some("10000000000000000000000000000.0000000000".asNumeric)))),
            Code.INVALID_ARGUMENT,
            s"Out-of-bounds (Numeric 10)"
          )
          _ <- ctx.testingHelpers.failingCreate(
            testIdsGenerator.testCommandId("Decimal-bounds-negative"),
            alice,
            templateIds.decimalRounding,
            Record(fields = List(RecordField(value = Some(alice.asParty)), RecordField(value = Some("-10000000000000000000000000000.0000000000".asNumeric)))),
            Code.INVALID_ARGUMENT,
            s"Out-of-bounds (Numeric 10)"
          )
        } yield {
          succeed
        }
      }
    }

    "client sends a CreateAndExerciseCommand" should {
      val validCreateAndExercise = CreateAndExerciseCommand(
        Some(templateIds.dummy),
        Some(Record(fields = List(RecordField(value = Some(Value(Value.Sum.Party(party))))))),
        "DummyChoice1",
        Some(Value(Value.Sum.Record(Record())))
      )
      val partyFilter = TransactionFilter(Map(party -> Filters(None)))

      def newRequest(context: LedgerContext, cmd: CreateAndExerciseCommand) = submitRequest
        .update(
          _.commands.commands := Seq[Command](Command(Command.Command.CreateAndExercise(cmd))),
          _.commands.ledgerId := context.ledgerId.unwrap
        )

      def submitSuccessfully(c: LedgerContext, r: SubmitRequest): Future[Assertion] =
        submitCommand(c, r).map(inside(_) { case c => c.getStatus should have('code (0)) })

      def successfulCommands(c: LedgerContext, workflowId: String): Future[Assertion] = {
        val cmdId = testIdsGenerator.testCommandId(s"valid-create-and-exercise-cmd-${UUID.randomUUID()}")
        val request = newRequest(c, validCreateAndExercise)
          .update(_.commands.commandId := cmdId)
          .update(_.commands.workflowId := workflowId)

        for {
          GetLedgerEndResponse(Some(currentEnd)) <- c.transactionClient.getLedgerEnd

          _ <- submitSuccessfully(c, request)

          txTree <- c.transactionClient
            .getTransactionTrees(currentEnd, None, partyFilter)
            .runWith(Sink.head)

          flatTransaction <- c.transactionClient
            .getTransactions(currentEnd, None, partyFilter)
            .runWith(Sink.head)

        } yield {
          flatTransaction.commandId shouldBe cmdId
          // gerolf-da 2019-04-17: #575 takes care of whether we should even emit the flat transaction or not
          flatTransaction.events shouldBe empty

          txTree.rootEventIds should have length 2
          txTree.commandId shouldBe cmdId

          val Seq(Kind.Created(createdEvent), Kind.Exercised(exercisedEvent)) =
            txTree.rootEventIds.map(txTree.eventsById(_).kind)

          createdEvent.templateId shouldBe Some(templateIds.dummy)

          exercisedEvent.choice shouldBe "DummyChoice1"
          exercisedEvent.contractId shouldBe createdEvent.contractId
          exercisedEvent.consuming shouldBe true
        }

      }

      "process valid commands with workflow ids successfully" in allFixtures { c =>
        successfulCommands(c, workflowId = UUID.randomUUID().toString)
      }

      "process valid commands with empty workflow ids successfully" in allFixtures { c =>
        successfulCommands(c, workflowId = "")
      }

      "fail for invalid create arguments" in allFixtures { implicit c =>
        val createAndExercise = validCreateAndExercise.copy(createArguments = Some(Record()))
        val request = newRequest(c, createAndExercise)
          .update(_.commands.commandId := testIdsGenerator.testCommandId("fail-for-invalid-create-args"))

        val response = submitCommand(c, request)
        response.map(_.getStatus should have('code (Code.INVALID_ARGUMENT.value)))
      }

      "fail for invalid choice arguments" in allFixtures { implicit c =>
        val createAndExercise =
          validCreateAndExercise.copy(choiceArgument = Some(Value(Value.Sum.Bool(false))))
        val request = newRequest(c, createAndExercise)
          .update(_.commands.commands := Seq[Command](Command(Command.Command.CreateAndExercise(createAndExercise))))
          .update(_.commands.commandId := testIdsGenerator.testCommandId("fail-for-invalid-choice-args"))

        val response = submitCommand(c, request)
        response.map(_.getStatus should have('code (Code.INVALID_ARGUMENT.value)))
      }

      "fail for an invalid choice" in allFixtures { implicit c =>
        val createAndExercise = validCreateAndExercise.copy(choice = "DoesNotExist")

        val request = newRequest(c, createAndExercise)
          .update(_.commands.commands := Seq[Command](Command(Command.Command.CreateAndExercise(createAndExercise))))
          .update(_.commands.commandId := testIdsGenerator.testCommandId("fail-for-invalid-choice"))

        val response = submitCommand(c, request)
        response.map(_.getStatus should have('code (Code.INVALID_ARGUMENT.value)))
      }
    }
  }

  private def removeLabels(fields: Seq[RecordField]): Seq[RecordField] = {
    fields.map { f =>
      f.value match {
        case Some(Value(Value.Sum.Record(r))) =>
          RecordField("", Some(Value(Value.Sum.Record(removeLabelsFromRecord(r)))))
        case other =>
          RecordField("", other)
      }
    }
  }

  private def removeLabelsFromRecord(r: Record): Record = {
    r.update(_.fields.modify(removeLabels))
  }

}
