// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.util.UUID

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.MockMessages.{party, submitRequest, commandId}
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
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.TreeEvent.Kind
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
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
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement
import com.digitalasset.platform.apitesting.LedgerBackend.SandboxInMemory
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.participant.util.ValueConversions._
import com.google.rpc.code.Code
import org.scalatest.Inside._
import org.scalatest._
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}

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
        with OptionValues
        with TestTemplateIds {
  protected def submitCommand(ctx: LedgerContext, req: SubmitRequest): Future[Completion]

  override protected val config: Config = Config.default

  private lazy val dummyTemplates =
    List(templateIds.dummy, templateIds.dummyFactory, templateIds.dummyWithParam)
  private val operator = "operator"
  private val receiver = "receiver"
  private val giver = "giver"
  private val owner = "owner"
  private val delegate = "delegate"
  private val observers = List("observer1", "observer2")

  private val integerListRecordLabel = "integerList"

  private val paramShowcaseArgs: Record = {
    val variantId = None
    val variant = Value(Value.Sum.Variant(Variant(variantId, "SomeInteger", 1.asInt64)))
    val nestedVariant = Vector("value" -> variant).asRecordValue
    val integerList = Vector(1, 2).map(_.toLong.asInt64).asList
    Record(
      Some(templateIds.parameterShowcase),
      Vector(
        RecordField("operator", "party".asParty),
        RecordField("integer", 1.asInt64),
        RecordField("decimal", "1.1".asDecimal),
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

  def submitSuccessfully(ctx: LedgerContext, req: SubmitRequest): Future[Assertion] =
    submitCommand(ctx, req).map(assertCompletionIsSuccessful)

  def assertCompletionIsSuccessful(completion: Completion): Assertion = {
    inside(completion) {
      case c => c.getStatus should have('code (0))
    }
  }

  s"Command and Transaction Services" when {
    "reading completions" should {
      "return the completion of submitted commands for the submitting application" in allFixtures {
        ctx =>
          val commandId = cid("Submitting application sees this")
          val request = createCommandWithId(ctx, commandId)
          for {
            commandClient <- ctx.commandClient()
            offset <- commandClient.getCompletionEnd.map(_.getOffset)
            _ <- submitSuccessfully(ctx, request)
            completionAfterCheckpoint <- listenForCompletionAsApplication(
              ctx,
              M.applicationId,
              request.getCommands.party,
              offset,
              commandId)
          } yield {
            completionAfterCheckpoint.value.status.value should have('code (0))
          }
      }

      "not expose completions of submitted commands to other applications" in allFixtures { ctx =>
        val commandId = cid("The other application does not see this")
        val request = createCommandWithId(ctx, commandId)
        for {
          commandClient <- ctx.commandClient()
          offset <- commandClient.getCompletionEnd.map(_.getOffset)
          _ <- submitSuccessfully(ctx, request)
          completionsAfterCheckpoint <- listenForCompletionAsApplication(
            ctx,
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
            cid("The application should subscribe with the submitting party to see this")
          val request = createCommandWithId(ctx, commandId)
          for {
            commandClient <- ctx.commandClient()
            offset <- commandClient.getCompletionEnd.map(_.getOffset)
            _ <- submitSuccessfully(ctx, request)
            completionsAfterCheckpoint <- listenForCompletionAsApplication(
              ctx,
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
          val factoryCreation = cid("Creating factory (Trees)")
          val exercisingChoice = cid("Exercising choice on factory (Trees)")
          for {
            factoryContractId <- findCreatedEventInResultOf(
              ctx,
              factoryCreation,
              templateIds.dummyFactory)
            transaction <- ctx.testingHelpers.submitAndListenForSingleTreeResultOfCommand(
              requestToCallExerciseWithId(ctx, factoryContractId.contractId, exercisingChoice),
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
            RecordField("decimal", Some(Value(Value.Sum.Decimal("37.0")))))
        verifyParamShowcaseChoice(
          ctx,
          "Choice1", // choice name
          "different args",
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
          "changing 'integer'",
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
        val commandId = cid("Huge composite command")
        val originalCommand = createCommandWithId(ctx, commandId)
        val targetNumberOfSubCommands = 15000 // That's around the maximum gRPC input size
        val superSizedCommand =
          originalCommand.update(_.commands.update(_.commands.modify(original =>
            List.fill(targetNumberOfSubCommands)(original.head))))
        ctx.testingHelpers.submitAndListenForSingleResultOfCommand(superSizedCommand, getAllContracts) map {
          tx =>
            tx.events.size shouldEqual targetNumberOfSubCommands
        }
      }

      "run callable payout and return the right events" in allFixtures { ctx =>
        val commandId = cid("callable payout command")
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
          simpleCreateWithListener(ctx, commandId, giver, receiver, templateIds.callablePayout, arg)

        val exercise = (party: String) =>
          (contractId: String) =>
            transactionsFromSimpleExercise(
              ctx,
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
        val commandId = cid("Testing if non-submitting stakeholder sees the commandId")
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
          triProposal <- simpleCreate(
            ctx,
            "MA1proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          tx <- simpleExercise(
            ctx,
            "MA1acceptance",
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
          triProposal <- simpleCreate(
            ctx,
            "MA2proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          tx <- simpleExercise(
            ctx,
            "MA2acceptance",
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
          triProposal <- simpleCreate(
            ctx,
            "MA3proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          assertion <- failingExercise(
            ctx,
            "MA3acceptance",
            giver,
            templateIds.triProposal,
            triProposal.contractId,
            "TriProposalAccept",
            emptyRecordValue,
            Code.INVALID_ARGUMENT,
            "requires controllers"
          )
        } yield {
          assertion
        }
      }

      // NOTE(MH): This is the current, most conservative semantics of
      // multi-actor choice authorization. It is likely that this will change
      // in the future. Should we delete this test, we should also remove the
      // 'UnrestrictedAcceptTriProposal' choice from the 'Agreement' template.
      //TODO: check this with Martin Hu or Robin
      "reject exercising a multi-actor choice with too many authorizers" ignore allFixtures { ctx =>
        val triProposalArg = mkTriProposalArg(operator, giver, giver)
        for {
          agreement <- createAgreement(ctx, "MA4", receiver, giver)
          triProposal <- simpleCreate(
            ctx,
            "MA4proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          assertion <- failingExercise(
            ctx,
            "MA4acceptance",
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

      "permit fetching a divulged contract" in forAllMatchingFixtures {
        case TestFixture(SandboxInMemory, ctx) =>
        def pf(label: String, party: String) =
          RecordField(label, Some(Value(Value.Sum.Party(party))))
        val odArgs = Seq(pf("owner", owner), pf("delegate", delegate))
        val delegatedCreate = simpleCreate(
          ctx,
          cid("SDVl3"),
          owner,
          templateIds.delegated,
          Record(Some(templateIds.delegated), Seq(pf("owner", owner))))
        val delegationCreate = simpleCreate(
          ctx,
          cid("SDVl4"),
          owner,
          templateIds.delegation,
          Record(Some(templateIds.delegation), odArgs))
        val showIdCreate = simpleCreate(
          ctx,
          cid("SDVl5"),
          owner,
          templateIds.showDelegated,
          Record(Some(templateIds.showDelegated), odArgs))
        val exerciseOfFetch = for {
          delegatedEv <- delegatedCreate
          delegationEv <- delegationCreate
          showIdEv <- showIdCreate
          fetchArg = Record(
            None,
            Seq(RecordField("", Some(Value(Value.Sum.ContractId(delegatedEv.contractId))))))
          showResult <- failingExercise(
            ctx,
            cid("SDVl6"),
            submitter = owner,
            template = templateIds.showDelegated,
            contractId = showIdEv.contractId,
            choice = "ShowIt",
            arg = Value(Value.Sum.Record(fetchArg)),
            Code.OK,
            pattern = ""
          )
          fetchResult <- failingExercise(
            ctx,
            cid("SDVl7"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchDelegated",
            arg = Value(Value.Sum.Record(fetchArg)),
            Code.OK,
            pattern = ""
          )
        } yield fetchResult
        exerciseOfFetch
      }

      "reject fetching an undisclosed contract" in allFixtures { ctx =>
        def pf(label: String, party: String) =
          RecordField(label, Some(Value(Value.Sum.Party(party))))
        val delegatedCreate = simpleCreate(
          ctx,
          cid("TDVl3"),
          owner,
          templateIds.delegated,
          Record(Some(templateIds.delegated), Seq(pf("owner", owner))))
        val delegationCreate = simpleCreate(
          ctx,
          cid("TDVl4"),
          owner,
          templateIds.delegation,
          Record(Some(templateIds.delegation), Seq(pf("owner", owner), pf("delegate", delegate))))
        val exerciseOfFetch = for {
          delegatedEv <- delegatedCreate
          delegationEv <- delegationCreate
          fetchArg = Record(
            None,
            Seq(RecordField("", Some(Value(Value.Sum.ContractId(delegatedEv.contractId))))))
          fetchResult <- failingExercise(
            ctx,
            cid("TDVl5"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchDelegated",
            arg = Value(Value.Sum.Record(fetchArg)),
            Code.INVALID_ARGUMENT,
            pattern = "dependency error: couldn't find contract"
          )
        } yield fetchResult
        exerciseOfFetch
      }

      "DAML engine returns Unit as argument to Nothing" in allFixtures { ctx =>
        val commandId = cid("Creating contract with a Nothing argument")

        val variantId = None

        val createArguments = Record(
          Some(templateIds.nothingArgument),
          Vector(
            RecordField("operator", "party".asParty),
            RecordField("arg1", Value(Value.Sum.Optional(Optional())))
          )
        )
        val commandList =
          List(CreateCommand(Some(templateIds.nothingArgument), Some(createArguments)).wrap)
        val command: SubmitRequest =
          submitRequestWithId(ctx, commandId).update(_.commands.commands := commandList)

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
              (LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))),
              Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))),
              getAllContracts
            )
            .map(_.events
                .map(_.event)
                .collect {
                  case Archived(ArchivedEvent(eventId, _, _, _)) => eventId
                  case Created(CreatedEvent(eventId, _, _, _, _)) => eventId
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
                withObservers <- simpleCreateWithListener(
                  ctx,
                  "Obs1create:" + observer,
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
                withObservers <- simpleCreate(
                  ctx,
                  "Obs2create:" + observer,
                  giver,
                  templateIds.withObservers,
                  withObserversArg)
                tx <- simpleExerciseWithListener(
                  ctx,
                  "Obs2exercise:" + observer,
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
      // this is basically a port of
      // `daml-lf/tests/scenario/daml-1.3/contract-keys/Test.daml`.
      "process contract keys" in allFixtures { ctx =>
        // TODO currently we run multiple suites with the same sandbox, therefore we must generate
        // unique keys. This is not so great though, it'd be better to have a clean environment.
        val keyPrefix = UUID.randomUUID.toString
        def textKeyRecord(p: String, k: String): Record =
          Record(
            fields =
                List(RecordField(value = p.asParty), RecordField(value = s"$k-$keyPrefix".asText)))
        def textKeyValue(p: String, k: String): Value =
          Value(Value.Sum.Record(textKeyRecord(p, k)))
        val key = "some-key"
        val alice = "Alice"
        val bob = "Bob"
        for {
          cid1 <- simpleCreate(
            ctx,
            "CK-test-cid1",
            alice,
            templateIds.textKey,
            textKeyRecord(alice, key)
          )
          // duplicate keys are not ok
          _ <- failingCreate(
             ctx,
             "CK-test-duplicate-key",
             alice,
             templateIds.textKey,
             textKeyRecord(alice, key),
             Code.INVALID_ARGUMENT,
             "DuplicateKey"
           )
          // create handles to perform lookups / fetches
          aliceTKO <- simpleCreate(
              ctx,
              "CK-test-aliceTKO",
              alice,
              templateIds.textKeyOperations,
              Record(fields = List(RecordField(value = alice.asParty))))
          bobTKO <- simpleCreate(
              ctx,
              "CK-test-bobTKO",
              bob,
              templateIds.textKeyOperations,
              Record(fields = List(RecordField(value = bob.asParty)))
            )
          // unauthorized lookups are not OK
          // both existing lookups...
          lookupNone = Value(Value.Sum.Optional(Optional(None)))
          lookupSome = (cid: String) => Value(Value.Sum.Optional(Optional(Some(cid.asContractId))))
          _ <- failingExercise(
            ctx,
            "CK-test-bob-unauthorized-1",
            bob,
            templateIds.textKeyOperations,
            bobTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(Record(fields = List(
                RecordField(value = textKeyValue(alice, key)),
                RecordField(value = lookupSome(cid1.contractId)))))),
            Code.INVALID_ARGUMENT,
            "requires authorizers"
          )
          // ..and non-existing ones
          _ <- failingExercise(
            ctx,
            "CK-test-bob-unauthorized-2",
            bob,
            templateIds.textKeyOperations,
            bobTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyValue(alice, "bogus-key")),
                  RecordField(value = lookupNone))))),
            Code.INVALID_ARGUMENT,
            "requires authorizers")
          // successful, authorized lookup
          _ <- simpleExercise(
            ctx,
            "CK-test-alice-lookup-found",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyValue(alice, key)),
                  RecordField(value = lookupSome(cid1.contractId)))))))
          // successful fetch
          _ <- simpleExercise(
            ctx,
            "CK-test-alice-fetch-found",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOFetch",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyValue(alice, key)),
                  RecordField(value = cid1.contractId.asContractId))))))
          // failing, authorized lookup
          _ <- simpleExercise(
            ctx,
            "CK-test-alice-lookup-not-found",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyValue(alice, "bogus-key")),
                  RecordField(value = lookupNone))))))
          // failing fetch
          _ <- failingExercise(
            ctx,
            "CK-test-alice-fetch-not-found",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOFetch",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyValue(alice, "bogus-key")),
                  RecordField(value = cid1.contractId.asContractId))))),
            Code.INVALID_ARGUMENT,
            "couldn't find key")
          // now we exercise the contract, thus archiving it, and then verify
          // that we cannot look it up anymore
          _ <- simpleExercise(
            ctx,
            "CK-test-alice-consume-cid1",
            alice,
            templateIds.textKey,
            cid1.contractId,
            "TextKeyChoice",
            emptyRecordValue)
          lookupAfterConsume <- simpleExercise(
            ctx,
            "CK-test-alice-lookup-after-consume",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyValue(alice, key)),
                  RecordField(value = lookupNone))))))
          cid2 <- simpleCreate(
            ctx,
            "CK-test-cid2",
            alice,
            templateIds.textKey,
            textKeyRecord(alice, "test-key-2")
          )
          _ <- simpleExercise(
            ctx,
            "CK-test-alice-consume-and-lookup",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOConsumeAndLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = cid2.contractId.asContractId),
                  RecordField(value = textKeyValue(alice, "test-key-2"))))))
          )
        } yield {
          succeed
        }
      }

      "handle bad Decimals correctly" in allFixtures { ctx =>
        val alice = "Alice"
        for {
          _ <- failingCreate(
            ctx,
            "Decimal-scale",
            alice,
            templateIds.decimalRounding,
            Record(fields = List(RecordField(value = Some(alice.asParty)), RecordField(value = Some("0.00000000005".asDecimal)))),
            Code.INVALID_ARGUMENT,
            "Could not read Decimal string"
          )
          _ <- failingCreate(
            ctx,
            "Decimal-bounds-positive",
            alice,
            templateIds.decimalRounding,
            Record(fields = List(RecordField(value = Some(alice.asParty)), RecordField(value = Some("10000000000000000000000000000.0000000000".asDecimal)))),
            Code.INVALID_ARGUMENT,
            "Could not read Decimal string"
          )
          _ <- failingCreate(
            ctx,
            "Decimal-bounds-negative",
            alice,
            templateIds.decimalRounding,
            Record(fields = List(RecordField(value = Some(alice.asParty)), RecordField(value = Some("-10000000000000000000000000000.0000000000".asDecimal)))),
            Code.INVALID_ARGUMENT,
            "Could not read Decimal string"
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
      val ledgerEnd =
        LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))
      val partyFilter = TransactionFilter(Map(party -> Filters(None)))

      def newRequest(cmd: CreateAndExerciseCommand) = submitRequest
        .update(_.commands.commands := Seq[Command](Command(Command.Command.CreateAndExercise(cmd))))
        .update(_.commands.ledgerId := config.getLedgerId)

      "process valid commands successfully" in allFixtures{ c =>
        val request = newRequest(validCreateAndExercise)

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
          flatTransaction.commandId shouldBe commandId
          // gerolf-da 2019-04-17: #575 takes care of whether we should even emit the flat transaction or not
          flatTransaction.events shouldBe empty

          txTree.rootEventIds should have length 2
          txTree.commandId shouldBe commandId

          val Seq(Kind.Created(createdEvent), Kind.Exercised(exercisedEvent)) =
            txTree.rootEventIds.map(txTree.eventsById(_).kind)

          createdEvent.templateId shouldBe Some(templateIds.dummy)

          exercisedEvent.choice shouldBe "DummyChoice1"
          exercisedEvent.contractId shouldBe createdEvent.contractId
          exercisedEvent.consuming shouldBe true
          exercisedEvent.contractCreatingEventId shouldBe createdEvent.eventId
        }
      }

      "fail for invalid create arguments" in allFixtures{ implicit c =>
        val createAndExercise = validCreateAndExercise.copy(createArguments = Some(Record()))
        val request = newRequest(createAndExercise)

        val response = submitCommand(c, request)
        response.map(_.getStatus should have('code (Code.INVALID_ARGUMENT.value)))
      }

      "fail for invalid choice arguments" in allFixtures{ implicit c =>
        val createAndExercise =
          validCreateAndExercise.copy(choiceArgument = Some(Value(Value.Sum.Bool(false))))
        val request = newRequest(createAndExercise)
          .update(_.commands.commands := Seq[Command](Command(Command.Command.CreateAndExercise(createAndExercise))))

        val response = submitCommand(c, request)
        response.map(_.getStatus should have('code (Code.INVALID_ARGUMENT.value)))
      }

      "fail for an invalid choice" in allFixtures{ implicit c =>
        val createAndExercise = validCreateAndExercise.copy(choice = "DoesNotExist")

        val request = newRequest(createAndExercise)
          .update(_.commands.commands := Seq[Command](Command(Command.Command.CreateAndExercise(createAndExercise))))

        val response = submitCommand(c, request)
        response.map(_.getStatus should have('code (Code.INVALID_ARGUMENT.value)))
      }
    }
  }

  private def cid(commandId: String) = s"$commandId"

  def submitRequestWithId(ctx: LedgerContext, commandId: String): SubmitRequest =
    M.submitRequest.update(
      _.commands.modify(_.copy(commandId = commandId, ledgerId = ctx.ledgerId)))

  private def createCommandWithId(ctx: LedgerContext, commandId: String) = {
    val reqWithId = submitRequestWithId(ctx, commandId)
    val arguments = List("operator" -> "party".asParty)

    reqWithId.update(
      _.commands.update(_.commands := dummyTemplates.map(i => Command(create(i, arguments)))))
  }

  private def create(templateId: Identifier, arguments: immutable.Seq[(String, Value)]): Create = {
    Create(CreateCommand(Some(templateId), Some(arguments.asRecordOf(templateId))))
  }

  private def listenForCompletionAsApplication(
      ctx: LedgerContext,
      applicationId: String,
      requestingParty: String,
      offset: LedgerOffset,
      commandIdToListenFor: String) = {
    ctx.commandClient(applicationId = applicationId).flatMap { commandClient =>
      commandClient
          .completionSource(List(requestingParty), offset)
          .collect {
            case CompletionStreamElement.CompletionElement(completion)
              if completion.commandId == commandIdToListenFor =>
              completion
          }
          .take(1)
          .takeWithin(3.seconds)
          .runWith(Sink.seq)
          .map(_.headOption)
    }
  }

  private lazy val getAllContracts = M.transactionFilter

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
      commandId: String) = {
    submitRequestWithId(ctx, commandId).update(
      _.commands.commands := List(
        ExerciseCommand(
          Some(templateIds.dummyFactory),
          factoryContractId,
          "DummyFactoryCall",
          Some(Value(Sum.Record(Record())))).wrap))
  }

  // Exercise a choice and return all resulting create events.
  private def simpleExercise(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[TransactionTree] =
    simpleExerciseWithListener(ctx, commandId, submitter, submitter, template, contractId, choice, arg)

  // Exercise a choice that is supposed to fail.
  private def failingExercise(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value,
      code: Code,
      pattern: String
  ): Future[Assertion] =
    assertCommandFailsWithCode(
      ctx,
      submitRequestWithId(ctx, cid(commandId))
          .update(
            _.commands.commands :=
                List(ExerciseCommand(Some(template), contractId, choice, Some(arg)).wrap),
            _.commands.party := submitter
          ),
      code,
      pattern
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
      agreementFactory <- simpleCreate(
        ctx,
        commandId + "factory",
        giver,
        templateIds.agreementFactory,
        agreementFactoryArg)
      tx <- simpleExercise(
        ctx,
        commandId + "agreement",
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
    submitRequestWithId(ctx, commandId).update(
      _.commands.modify(_.update(_.commands := commandList)))
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
        cid(s"Creating contract with a multitude of param types for exercising ($choice, $lbl)"),
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
        submitRequestWithId(ctx, cid(s"Exercising with a multitiude of params ($choice, $lbl)"))
            .update(_.commands.update(_.commands := List(exercise))),
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
      exercise.actingParties should contain("party")
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
    submitRequestWithId(ctx, commandId)
        .update(
          _.commands.commands := List(
            Command(
              create(
                templateIds.agreementFactory,
                List(receiver -> receiver.asParty, giver -> giver.asParty)))),
          _.commands.party := giver
        )
  }

  // Create a template instance and return the resulting create event.
  private def simpleCreateWithListener(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] = {
    for {
      tx <- ctx.testingHelpers.submitAndListenForSingleResultOfCommand(
        submitRequestWithId(ctx, cid(commandId))
            .update(
              _.commands.commands :=
                  List(CreateCommand(Some(template), Some(arg)).wrap),
              _.commands.party := submitter
            ),
        TransactionFilter(Map(listener -> Filters.defaultInstance))
      )
    } yield {
      ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsIn(tx))
    }
  }

  // Create a template instance and return the resulting create event.
  private def simpleCreate(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] =
    simpleCreateWithListener(ctx, commandId, submitter, submitter, template, arg)

  private def failingCreate(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record,
      code: Code,
      pattern: String
  ): Future[Assertion] =
    assertCommandFailsWithCode(
      ctx,
      submitRequestWithId(ctx, cid(commandId))
          .update(
            _.commands.commands :=
                List(CreateCommand(Some(template), Some(arg)).wrap),
            _.commands.party := submitter
          ),
      code,
      pattern
    )

  // Exercise a choice and return all resulting create events.
  private def simpleExerciseWithListener(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[TransactionTree] = {
    ctx.testingHelpers.submitAndListenForSingleTreeResultOfCommand(
      submitRequestWithId(ctx, cid(commandId))
          .update(
            _.commands.commands :=
                List(ExerciseCommand(Some(template), contractId, choice, Some(arg)).wrap),
            _.commands.party := submitter
          ),
      TransactionFilter(Map(listener -> Filters.defaultInstance)),
      false
    )
  }

  private def simpleCreateWithListenerForTransactions(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] = {
    for {
      tx <- ctx.testingHelpers.submitAndListenForSingleResultOfCommand(
        submitRequestWithId(ctx, cid(commandId))
            .update(
              _.commands.commands :=
                  List(CreateCommand(Some(template), Some(arg)).wrap),
              _.commands.party := submitter
            ),
        TransactionFilter(Map(listener -> Filters.defaultInstance))
      )
    } yield {
      ctx.testingHelpers.getHead(ctx.testingHelpers.createdEventsIn(tx))
    }
  }

  private def transactionsFromsimpleCreate(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] =
    simpleCreateWithListenerForTransactions(ctx, commandId, submitter, submitter, template, arg)

  private def submitAndListenForTransactionResultOfCommand(
      ctx: LedgerContext,
      command: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true): Future[Seq[Transaction]] = {
    submitAndListenForTransactionResultsOfCommand(ctx, command, transactionFilter, filterCid)
  }

  private def submitAndListenForTransactionResultsOfCommand(
      ctx: LedgerContext,
      submitRequest: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true): Future[immutable.Seq[Transaction]] = {
    val commandId = submitRequest.getCommands.commandId
    for {
      txEndOffset <- ctx.testingHelpers.submitSuccessfullyAndReturnOffset(submitRequest)
      transactions <- listenForTransactionResultOfCommand(
        ctx,
        transactionFilter,
        if (filterCid) Some(commandId) else None,
        txEndOffset)
    } yield {
      transactions
    }
  }

  private def listenForTransactionResultOfCommand(
      ctx: LedgerContext,
      transactionFilter: TransactionFilter,
      commandId: Option[String],
      txEndOffset: LedgerOffset): Future[immutable.Seq[Transaction]] = {
    ctx.transactionClient
        .getTransactions(
          txEndOffset,
          None,
          transactionFilter
        )
        .filter(x => commandId.fold(true)(cid => x.commandId == cid))
        .take(1)
        .takeWithin(3.seconds)
        .runWith(Sink.seq)
  }

  private def simpleExerciseWithListenerForTransactions(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[Seq[Transaction]] = {
    submitAndListenForTransactionResultOfCommand(
      ctx,
      submitRequestWithId(ctx, cid(commandId))
          .update(
            _.commands.commands :=
                List(ExerciseCommand(Some(template), contractId, choice, Some(arg)).wrap),
            _.commands.party := submitter
          ),
      TransactionFilter(Map(listener -> Filters.defaultInstance)),
      false
    )
  }

  private def transactionsFromSimpleExercise(
      ctx: LedgerContext,
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value): Future[Seq[Transaction]] =
    simpleExerciseWithListenerForTransactions(
      ctx,
      commandId,
      submitter,
      submitter,
      template,
      contractId,
      choice,
      arg)

  private def assertCommandFailsWithCode(
      ctx: LedgerContext,
      submitRequest: SubmitRequest,
      expectedErrorCode: Code,
      expectedMessageSubString: String): Future[Assertion] = {
    for {
      ledgerEnd <- ctx.transactionClient.getLedgerEnd
      completion <- submitCommand(ctx, submitRequest)
      // TODO(FM) in the contract keys test this hangs forever after expecting a failedExercise.
      // Could it be that the ACS behaves like that sometimes? In that case that'd be a bug. We must investigate
      /*
      txs <- ctx.testingHelpers.listenForResultOfCommand(
        ctx.testingHelpers.getAllContracts(List(submitRequest.getCommands.party)),
        Some(submitRequest.getCommands.commandId),
        ledgerEnd.getOffset)
     */
    } yield {
      completion.getStatus should have('code (expectedErrorCode.value))
      completion.getStatus.message should include(expectedMessageSubString)
      // txs shouldBe empty
    }
  }
}
