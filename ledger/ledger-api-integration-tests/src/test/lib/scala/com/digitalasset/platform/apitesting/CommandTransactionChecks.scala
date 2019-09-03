// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.util.UUID

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.MockMessages.{party, submitRequest}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Command.Command.Create
import com.digitalasset.ledger.api.v1.commands.{
  Command,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseCommand
}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.transaction.TreeEvent.Kind
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.transaction_service.GetLedgerEndResponse
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.Value.Sum.{Bool, ContractId, Text, Timestamp}
import com.digitalasset.ledger.api.v1.value.{
  Identifier,
  Optional,
  Record,
  RecordField,
  Value,
  Variant
}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.TestParties._
import com.digitalasset.platform.participant.util.ValueConversions._
import com.google.rpc.code.Code
import org.scalatest.Inside._
import org.scalatest._
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import scalaz.syntax.tag._

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

// scalafmt cannot deal with this file
// format: off
@SuppressWarnings(Array("org.wartremover.warts.Any"))
abstract class CommandTransactionChecks
  extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
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

  private lazy val dummyTemplates =
    List(templateIds.dummy, templateIds.dummyFactory, templateIds.dummyWithParam)
  private val operator = Grace
  private val receiver = Heidi
  private val giver = Alice
  private val owner = Bob
  private val delegate = Charlie
  private val observers = List(Eve, Frank)

  private val integerListRecordLabel = "integerList"

  private val paramShowcaseArgs: Record = {
    val variantId = None
    val variant = Value(Value.Sum.Variant(Variant(variantId, "SomeInteger", 1.asInt64)))
    val nestedVariant = Vector("value" -> variant).asRecordValue
    val integerList = Vector(1, 2).map(_.toLong.asInt64).asList
    Record(
      Some(templateIds.parameterShowcase),
      Vector(
        RecordField("operator", Alice.asParty),
        RecordField("integer", 1.asInt64),
        RecordField("decimal", "1.1000000000".asNumeric),
        RecordField("text", Value(Text("text"))),
        RecordField("bool", Value(Bool(true))),
        RecordField("time", Value(Timestamp(0))),
        RecordField("nestedOptionalInteger", nestedVariant),
        RecordField(integerListRecordLabel, integerList),
        RecordField(
          "optionalText",
          Value(Value.Sum.Optional(Optional(Some(Value(Text("present")))))))
      )
    )
  }

  private val emptyRecordValue = Value(Value.Sum.Record(Record()))

  def assertCompletionIsSuccessful(completion: Completion): Assertion = {
    inside(completion) {
      case c => c.getStatus should have('code (0))
    }
  }

  s"Command and Transaction Services" when {
    "reading completions" should {
      "return the completion of submitted commands for the submitting application" in allFixtures {
        ctx =>
          val commandId = testIdsGenerator.testCommandId("Submitting_application_sees_this")
          val request = createCommandWithId(ctx, commandId)
          for {
            commandClient <- ctx.commandClient()
            offset <- commandClient.getCompletionEnd.map(_.getOffset)
            _ <- ctx.testingHelpers.submitSuccessfully(request)
            completionAfterCheckpoint <- ctx.testingHelpers.listenForCompletionAsApplication(
              M.applicationId,
              request.getCommands.party,
              offset,
              commandId)
          } yield {
            completionAfterCheckpoint.value.status.value should have('code (0))
          }
      }

      "not expose completions of submitted commands to other applications" in allFixtures { ctx =>
        val commandId = testIdsGenerator.testCommandId("The_other_application_does_not_see_this")
        val request = createCommandWithId(ctx, commandId)
        for {
          commandClient <- ctx.commandClient()
          offset <- commandClient.getCompletionEnd.map(_.getOffset)
          _ <- ctx.testingHelpers.submitSuccessfully(request)
          completionsAfterCheckpoint <- ctx.testingHelpers.listenForCompletionAsApplication(
            "anotherApplication",
            request.getCommands.party,
            offset,
            commandId)
        } yield {
          completionsAfterCheckpoint shouldBe empty
        }
      }

      "not expose completions of submitted commands to the application if it down't include the submitting party" in allFixtures {
        ctx =>
          val commandId =
            testIdsGenerator.testCommandId("The_application_should_subscribe_with_the_submitting_party_to_see_this")
          val request = createCommandWithId(ctx, commandId)
          for {
            commandClient <- ctx.commandClient()
            offset <- commandClient.getCompletionEnd.map(_.getOffset)
            _ <- ctx.testingHelpers.submitSuccessfully(request)
            completionsAfterCheckpoint <- ctx.testingHelpers.listenForCompletionAsApplication(
              request.getCommands.applicationId,
              "not " + request.getCommands.party,
              offset,
              commandId)
          } yield {
            completionsAfterCheckpoint shouldBe empty
          }
      }
    }

    "interacting with the ledger" should {
      //TODO: this is a quick copy of the test above to have a test case for DEL-3062. we need to clean this up. see: DEL-3097
      "expose contract Ids that are ready to be used for exercising choices using GetTransactionTrees" in allFixtures {
        ctx =>
          // note that the submitting party is not a stakeholder in any event,
          // so this test relies on the sandbox exposing the transactions to the
          // submitter.
          val factoryCreation = testIdsGenerator.testCommandId("Creating_factory_Trees")
          val exercisingChoice = testIdsGenerator.testCommandId("Exercising_choice_on_factory_Trees")
          for {
            factoryContractId <- findCreatedEventInResultOf(
              ctx,
              factoryCreation,
              templateIds.dummyFactory)
            transaction <- ctx.testingHelpers.submitAndListenForSingleTreeResultOfCommand(
              requestToCallExerciseWithId(ctx, factoryContractId.contractId, testIdsGenerator.testCommandId(exercisingChoice)),
              getAllContracts)
          } yield {
            val exercisedEvent = ctx.testingHelpers.topLevelExercisedIn(transaction).head
            val creates =
              ctx.testingHelpers.createdEventsInTreeNodes(exercisedEvent.childEventIds.map(transaction.eventsById))
            exercisedEvent.contractId shouldEqual factoryContractId.contractId
            exercisedEvent.consuming shouldBe true
            creates should have length 2
          }
      }

      // An equivalent of this tests the non-tree api in TransactionIT
      "accept all kinds of arguments in choices (Choice1, different args)" in allFixtures { ctx =>
        val newArgs =
          ctx.testingHelpers.recordWithArgument(
            paramShowcaseArgs,
            RecordField("decimal", Some(Value(Value.Sum.Numeric("37.0000000000")))))
        verifyParamShowcaseChoice(
          ctx,
          "Choice1", // choice name
          "different_args",
          paramShowcaseArgumentsToChoice1Argument(newArgs), // submitted choice args
          // Daml-lf-engine integration works with non-verbose setting,
          // because we do not send the verbose flag in the request
          newArgs
        ) // expected args
      }

      "accept all kinds of arguments in choices (Choice2)" in allFixtures { ctx =>
        val newArgs =
          ctx.testingHelpers.recordWithArgument(
            paramShowcaseArgs,
            RecordField("integer", Some(Value(Value.Sum.Int64(74)))))
        verifyParamShowcaseChoice(
          ctx,
          "Choice2", // choice name
          "changing_integer",
          // submitted choice args
          Value(
            Value.Sum.Record(
              Record(fields = List(RecordField("newInteger", Some(Value(Value.Sum.Int64(74)))))))),
          newArgs
        ) // expected args
      }

      /*
       * TODO(FM) for absolutely mysterious reasons this times out, but the equivalent one in TransactionServiceIT
       * does not. find out why. this seems to be quadratic
       */
      "accept huge submissions with a large number of commands" ignore allFixtures { ctx =>
        val commandId = "Huge-composite-command"
        val originalCommand = createCommandWithId(ctx, commandId)
        val targetNumberOfSubCommands = 15 // That's around the maximum gRPC input size
      val superSizedCommand =
        originalCommand.update(_.commands.commands.modify(original =>
          List.fill(targetNumberOfSubCommands)(original.head)))
        ctx.testingHelpers.submitAndListenForSingleResultOfCommand(superSizedCommand, getAllContracts) map {
          tx =>
            tx.events.size shouldEqual targetNumberOfSubCommands
        }
      }

      "run callable payout and return the right events" in allFixtures { ctx =>
        val commandId = testIdsGenerator.testCommandId("callable_payout_command")
        val arg = Record(
          Some(templateIds.callablePayout),
          List(
            RecordField("giver", Some(Value(Value.Sum.Party(giver)))),
            RecordField("receiver", Some(Value(Value.Sum.Party(receiver))))
          )
        )

        val earg = Record(
          Some(templateIds.callablePayoutTransfer),
          List(
            RecordField("newReceiver", Some(Value(Value.Sum.Party("newReceiver"))))
          ))

        // create the contract with giver listen for the event with receiver
        val createF: Future[CreatedEvent] =
          ctx.testingHelpers.simpleCreateWithListener(commandId, giver, receiver, templateIds.callablePayout, arg)

        val exercise = (party: String) =>
          (contractId: String) =>
            ctx.testingHelpers.transactionsFromSimpleExercise(
              commandId + "exe",
              party,
              templateIds.callablePayout,
              contractId,
              "Transfer",
              Value(Value.Sum.Record(earg)))

        for {
          cr <- createF
          ex <- exercise(receiver)(cr.contractId)
        } yield {
          val es: Vector[Event] = ex.flatMap(_.events).toVector
          val events = es :+ Event(Created(cr))
          events.size shouldEqual 2
          val creates = events.flatMap(e => e.event.created.toList)
          creates.size shouldEqual 1
        }
      }

      "expose transactions to non-submitting stakeholders without the commandId" in allFixtures { ctx =>
        val receiver = "receiver"
        val giver = "giver"
        val commandId = testIdsGenerator.testCommandId("Testing_if_non-submitting_stakeholder_sees_the_commandId")
        val createCmd = createAgreementFactory(ctx, receiver, giver, commandId)
        ctx.testingHelpers.submitAndListenForSingleResultOfCommand(
          createCmd,
          TransactionFilter(Map(receiver -> Filters.defaultInstance))).map { tx =>
          tx.commandId should not equal commandId
          tx.commandId shouldEqual ""
        }
      }

      "accept exercising a well-authorized multi-actor choice" in allFixtures { ctx =>
        val triProposalArg = mkTriProposalArg(operator, receiver, giver)
        for {
          agreement <- createAgreement(ctx, "MA1", receiver, giver)
          triProposal <- ctx.testingHelpers.simpleCreate(
            testIdsGenerator.testCommandId("MA1proposal"),
            operator,
            templateIds.triProposal,
            triProposalArg)
          tx <- ctx.testingHelpers.simpleExercise(
            testIdsGenerator.testCommandId("MA1acceptance"),
            giver,
            templateIds.agreement,
            agreement.contractId,
            "AcceptTriProposal",
            mkCidArg(triProposal.contractId))
        } yield {
          val agreementExercised = ctx.testingHelpers.getHead(ctx.testingHelpers.topLevelExercisedIn(tx))
          agreementExercised.contractId shouldBe agreement.contractId
          val triProposalExercised =
            ctx.testingHelpers.getHead(ctx.testingHelpers.exercisedEventsInNodes(agreementExercised.childEventIds.map(tx.eventsById)))
          triProposalExercised.contractId shouldBe triProposal.contractId
          triProposalExercised.actingParties.toSet shouldBe Set(receiver, giver)
          val triAgreement =
            ctx.testingHelpers.getHead(
              ctx.testingHelpers.createdEventsInTreeNodes(triProposalExercised.childEventIds.map(tx.eventsById)))
          val expectedFields = removeLabels(triProposalArg.fields)
          triAgreement.getCreateArguments.fields shouldBe expectedFields
        }
      }

      "accept exercising a well-authorized multi-actor choice with coinciding controllers" in allFixtures { ctx =>
        val triProposalArg = mkTriProposalArg(operator, giver, giver)
        for {
          triProposal <- ctx.testingHelpers.simpleCreate(
            testIdsGenerator.testCommandId("MA2proposal"),
            operator,
            templateIds.triProposal,
            triProposalArg)
          tx <- ctx.testingHelpers.simpleExercise(
            testIdsGenerator.testCommandId("MA2acceptance"),
            giver,
            templateIds.triProposal,
            triProposal.contractId,
            "TriProposalAccept",
            emptyRecordValue)
        } yield {
          val triProposalExercised = ctx.testingHelpers.getHead(ctx.testingHelpers.topLevelExercisedIn(tx))
          triProposalExercised.contractId shouldBe triProposal.contractId
          triProposalExercised.actingParties.toSet shouldBe Set(giver)
          val triAgreement =
            ctx.testingHelpers.getHead(
              ctx.testingHelpers.createdEventsInTreeNodes(triProposalExercised.childEventIds.map(tx.eventsById)))
          val expectedFields =
            removeLabels(triProposalArg.fields)
          triAgreement.getCreateArguments.fields shouldBe expectedFields
        }
      }

      "reject exercising a multi-actor choice with missing authorizers" in allFixtures { ctx =>
        val triProposalArg = mkTriProposalArg(operator, receiver, giver)
        for {
          triProposal <- ctx.testingHelpers.simpleCreate(
            testIdsGenerator.testCommandId("MA3proposal"),
            operator,
            templateIds.triProposal,
            triProposalArg)
          assertion <- ctx.testingHelpers.failingExercise(
            testIdsGenerator.testCommandId("MA3acceptance"),
            giver,
            templateIds.triProposal,
            triProposal.contractId,
            "TriProposalAccept",
            emptyRecordValue,
            Code.INVALID_ARGUMENT,
            "requires authorizers"
          )
        } yield {
          assertion
        }
      }
      //
      //      // NOTE(MH): This is the current, most conservative semantics of
      //      // multi-actor choice authorization. It is likely that this will change
      //      // in the future. Should we delete this test, we should also remove the
      //      // 'UnrestrictedAcceptTriProposal' choice from the 'Agreement' template.
      //      //TODO: check this with Martin Hu or Robin
            "reject exercising a multi-actor choice with too many authorizers" ignore allFixtures { ctx =>
              val triProposalArg = mkTriProposalArg(operator, giver, giver)
              for {
                agreement <- createAgreement(ctx, testIdsGenerator.testCommandId("MA4"), receiver, giver)
                triProposal <- ctx.testingHelpers.simpleCreate(
                  testIdsGenerator.testCommandId("MA4proposal"),
                  operator,
                  templateIds.triProposal,
                  triProposalArg)
                assertion <- ctx.testingHelpers.failingExercise(
                  testIdsGenerator.testCommandId("MA4acceptance"),
                  giver,
                  templateIds.agreement,
                  agreement.contractId,
                  "UnrestrictedAcceptTriProposal",
                  mkCidArg(triProposal.contractId),
                  Code.INVALID_ARGUMENT,
                  "requires controllers"
                )
              } yield {
                assertion
              }
            }

      "DAML engine returns Unit as argument to Nothing" in allFixtures { ctx =>
        val commandId = testIdsGenerator.testCommandId("Creating_contract_with_a_Nothing_argument")

        val variantId = None

        val createArguments = Record(
          Some(templateIds.nothingArgument),
          Vector(
            RecordField("operator", Alice.asParty),
            RecordField("arg1", Value(Value.Sum.Optional(Optional())))
          )
        )
        val commandList =
          List(CreateCommand(Some(templateIds.nothingArgument), Some(createArguments)).wrap)
        val command: SubmitRequest =
          ctx.testingHelpers.submitRequestWithId(commandId, Alice).update(_.commands.commands := commandList)

        for {
          tx <- ctx.testingHelpers.submitAndListenForSingleResultOfCommand(command, getAllContracts)
        } yield {
          val creates = ctx.testingHelpers.createdEventsIn(tx)
          val create = ctx.testingHelpers.getHead(creates)
          // only compare the field values since the server currently does not return the
          // record identifier or labels when the request does not set verbose=true .
          create.getCreateArguments.fields.map(_.value) shouldEqual
            createArguments.fields.map(_.value)
          succeed
        }
      }

      "having many transactions all of them has a unique event id" in allFixtures { ctx =>
        val eventIdsF = ctx.transactionClient
          .getTransactions(
            LedgerOffsets.LedgerBegin,
            Some(LedgerOffsets.LedgerEnd),
            getAllContracts
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

      "disclose create to observers" in allFixtures { ctx =>
        val withObserversArg = mkWithObserversArg(giver, observers)
        observers.foldLeft(Future.successful(())) {
          case (f, observer) =>
            f flatMap { _ =>
              for {
                withObservers <- ctx.testingHelpers.simpleCreateWithListener(
                  testIdsGenerator.testCommandId(s"Obs1create:$observer"),
                  giver,
                  observer,
                  templateIds.withObservers,
                  withObserversArg)
              } yield {
                val expectedFields =
                  removeLabels(withObserversArg.fields)
                withObservers.getCreateArguments.fields shouldEqual expectedFields
                ()
              }
            }
        }.map(_ => succeed)
      }

      "disclose exercise to observers" in allFixtures { ctx =>
        val withObserversArg = mkWithObserversArg(giver, observers)
        observers.foldLeft(Future.successful(())) {
          case (f, observer) =>
            f flatMap { _ =>
              for {
                withObservers <- ctx.testingHelpers.simpleCreate(
                  testIdsGenerator.testCommandId(s"Obs2create:$observer"),
                  giver,
                  templateIds.withObservers,
                  withObserversArg)
                tx <- ctx.testingHelpers.simpleExerciseWithListener(
                  testIdsGenerator.testCommandId(s"Obs2exercise:$observer"),
                  giver,
                  observer,
                  templateIds.withObservers,
                  withObservers.contractId,
                  "Ping",
                  emptyRecordValue)
              } yield {
                val withObserversExercised = ctx.testingHelpers.getHead(ctx.testingHelpers.topLevelExercisedIn(tx))
                withObserversExercised.contractId shouldBe withObservers.contractId
                ()
              }
            }
        }.map(_ => succeed)
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

      "handle exercise by key" in allFixtures { ctx =>
        val keyPrefix = UUID.randomUUID.toString

        def textKeyRecord(p: String, k: String, disclosedTo: List[String]): Record =
          Record(
            fields =
              List(
                RecordField(value = p.asParty),
                RecordField(value = s"$keyPrefix-$k".asText),
                RecordField(value = disclosedTo.map(_.asParty).asList)))

        val key = "some-key"

        def textKeyKey(p: String, k: String): Value =
          Value(Value.Sum.Record(Record(fields = List(RecordField(value = p.asParty), RecordField(value = s"$keyPrefix-$k".asText)))))

        for {
          _ <- ctx.testingHelpers.failingExerciseByKey(
            testIdsGenerator.testCommandId("EK-test-alice-exercise-before-create"),
            Alice,
            templateIds.textKey,
            textKeyKey(Alice, key),
            "TextKeyChoice",
            emptyRecordValue,
            Code.INVALID_ARGUMENT,
            "couldn't find key"
          )
          _ <- ctx.testingHelpers.simpleCreate(
            testIdsGenerator.testCommandId("EK-test-cid1"),
            Alice,
            templateIds.textKey,
            textKeyRecord(Alice, key, List(Bob))
          )
          // now we exercise by key, thus archiving it, and then verify
          // that we cannot look it up anymore
          _ <- ctx.testingHelpers.simpleExerciseByKey(
            testIdsGenerator.testCommandId("EK-test-alice-exercise"),
            Alice,
            templateIds.textKey,
            textKeyKey(Alice, key),
            "TextKeyChoice",
            emptyRecordValue)
          _ <- ctx.testingHelpers.failingExerciseByKey(
            testIdsGenerator.testCommandId("EK-test-alice-exercise-consumed"),
            Alice,
            templateIds.textKey,
            textKeyKey(Alice, key),
            "TextKeyChoice",
            emptyRecordValue,
            Code.INVALID_ARGUMENT,
            "couldn't find key"
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

      def successfulCommands(c: LedgerContext, workflowId: String) = {
        val cmdId = testIdsGenerator.testCommandId(s"valid-create-and-exercise-cmd-${UUID.randomUUID()}")
        val request = newRequest(c, validCreateAndExercise)
          .update(_.commands.commandId := cmdId)
          .update(_.commands.workflowId := workflowId)

        for {
          GetLedgerEndResponse(Some(currentEnd)) <- c.transactionClient.getLedgerEnd

          _ <- c.testingHelpers.submitSuccessfully(request)

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

  private def createCommandWithId(ctx: LedgerContext, commandId: String) = {
    val reqWithId = ctx.testingHelpers.submitRequestWithId(commandId, Alice)
    val arguments = List("operator" -> Alice.asParty)

    reqWithId.update(
      _.commands.party := Alice,
      _.commands.commands := dummyTemplates.map(i => Command(create(i, arguments)))
    )
  }

  private def create(templateId: Identifier, arguments: immutable.Seq[(String, Value)]): Create = {
    Create(CreateCommand(Some(templateId), Some(arguments.asRecordOf(templateId))))
  }

  private lazy val getAllContracts = TransactionFilters.allForParties(config.parties.toArray: _*)

  private def createContracts(ctx: LedgerContext, commandId: String) = {
    val command = createCommandWithId(ctx, commandId)
    ctx.testingHelpers.submitAndListenForSingleResultOfCommand(command, getAllContracts)
  }

  private def findCreatedEventInResultOf(
                                          ctx: LedgerContext,
                                          cid: String,
                                          templateToLookFor: Identifier): Future[CreatedEvent] = {
    for {
      tx <- createContracts(ctx, cid)
    } yield {
      ctx.testingHelpers.findCreatedEventIn(tx, templateToLookFor)
    }
  }

  private def requestToCallExerciseWithId(
                                           ctx: LedgerContext,
                                           factoryContractId: String,
                                           commandId: String) =
    ctx.testingHelpers.submitRequestWithId(commandId, Alice).update(
      _.commands.commands := List(
        ExerciseCommand(
          Some(templateIds.dummyFactory),
          factoryContractId,
          "DummyFactoryCall",
          Some(Value(Sum.Record(Record())))).wrap)
    )

  // Create an instance of the 'Agreement' template.
  private def createAgreement(
                               ctx: LedgerContext,
                               commandId: String,
                               receiver: String,
                               giver: String
                             ): Future[CreatedEvent] = {
    val agreementFactoryArg = List(
      "receiver" -> receiver.asParty,
      "giver" -> giver.asParty
    ).asRecordOf(templateIds.agreementFactory)
    for {
      agreementFactory <- ctx.testingHelpers.simpleCreate(
        testIdsGenerator.testCommandId(s"$commandId-factory"),
        giver,
        templateIds.agreementFactory,
        agreementFactoryArg)
      tx <- ctx.testingHelpers.simpleExercise(
        testIdsGenerator.testCommandId(s"$commandId-agreement"),
        receiver,
        templateIds.agreementFactory,
        agreementFactory.contractId,
        "AgreementFactoryAccept",
        emptyRecordValue)
    } yield {
      val agreementFactoryExercised = ctx.testingHelpers.getHead(ctx.testingHelpers.topLevelExercisedIn(tx))
      agreementFactoryExercised.contractId shouldBe agreementFactory.contractId
      ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsInTreeNodes(agreementFactoryExercised.childEventIds.map(tx.eventsById)))
    }
  }

  private def mkTriProposalArg(
                                operator: String,
                                receiver: String,
                                giver: String
                              ): Record =
    Record(
      Some(templateIds.triProposal),
      Vector(
        RecordField("operator", operator.asParty),
        RecordField("receiver", receiver.asParty),
        RecordField("giver", giver.asParty))
    )

  private def mkCidArg(contractId: String): Value =
    List("cid" -> Value(ContractId(contractId))).asRecordValue

  private def mkWithObserversArg(
                                  giver: String,
                                  observers: List[String]
                                ): Record =
    Record(
      Some(templateIds.withObservers),
      Vector(
        RecordField("giver", giver.asParty),
        RecordField("observers", observers.map(_.asParty).asList))
    )


  private def createParamShowcaseWith(
                                       ctx: LedgerContext,
                                       commandId: String,
                                       createArguments: Record) = {
    val commandList = List(
      CreateCommand(Some(templateIds.parameterShowcase), Some(createArguments)).wrap)

    ctx.testingHelpers
      .submitRequestWithId(commandId, Alice)
      .update(_.commands.commands := commandList)
  }

  private def paramShowcaseArgumentsToChoice1Argument(args: Record): Value =
    Value(
      Value.Sum.Record(
        args
          .update(_.fields.modify { originalFields =>
            originalFields.collect {
              // prune "operator" -- we do not have it in the choice params
              case original if original.label != "operator" =>
                val newLabel = "new" + original.label.capitalize
                RecordField(newLabel, original.value)
            }
          })
          .update(_.recordId.set(templateIds.parameterShowcaseChoice1))))

  private def verifyParamShowcaseChoice(
                                         ctx: LedgerContext,
                                         choice: String,
                                         lbl: String,
                                         exerciseArg: Value,
                                         expectedCreateArg: Record): Future[Assertion] = {
    val command: SubmitRequest =
      createParamShowcaseWith(
        ctx,
        testIdsGenerator.testCommandId(s"Creating_contract_with_a_multitude_of_param_types_for_exercising__$choice#$lbl"),
        paramShowcaseArgs)
    for {
      tx <- ctx.testingHelpers.submitAndListenForSingleResultOfCommand(command, getAllContracts)
      contractId = ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsIn(tx)).contractId
      // first, verify that if we submit with the same inputs they're equal
      exercise = ExerciseCommand(
        Some(templateIds.parameterShowcase),
        contractId,
        choice,
        exerciseArg).wrap
      tx <- ctx.testingHelpers.submitAndListenForSingleTreeResultOfCommand(
        ctx.testingHelpers.submitRequestWithId(testIdsGenerator.testCommandId(s"Exercising_with_a_multitiude_of_params__$choice#$lbl"), Alice)
          .update(_.commands.commands := List(exercise)),
        getAllContracts,
        true
      )
    } yield {
      // check that we have the exercise
      val exercise = ctx.testingHelpers.getHead(ctx.testingHelpers.topLevelExercisedIn(tx))

      //Note: Daml-lf engine returns no labels
      // if verbose flag is off as prescribed
      // and these tests work with verbose=false requests
      val expectedExerciseFields =
      removeLabels(exerciseArg.getRecord.fields)
      val expectedCreateFields =
        removeLabels(expectedCreateArg.fields)

      exercise.templateId shouldEqual Some(templateIds.parameterShowcase)
      exercise.choice shouldEqual choice
      exercise.actingParties should contain(Alice)
      exercise.getChoiceArgument.getRecord.fields shouldEqual expectedExerciseFields
      // check that we have the create
      val create = ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsInTreeNodes(exercise.childEventIds.map(tx.eventsById)))
      create.getCreateArguments.fields shouldEqual expectedCreateFields
      //    expectedCreateFields
      succeed
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

  private def createAgreementFactory(ctx: LedgerContext, receiver: String, giver: String, commandId: String) = {
    ctx.testingHelpers.submitRequestWithId(commandId, giver)
      .update(
        _.commands.commands := List(
          Command(
            create(
              templateIds.agreementFactory,
              List(receiver -> receiver.asParty, giver -> giver.asParty))))
      )
  }
}
