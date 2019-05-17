// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.util.UUID

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.MockMessages.{commandId, party, submitRequest}
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
        val commandId = cid("The other application does not see this")
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
            cid("The application should subscribe with the submitting party to see this")
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
          triProposal <- ctx.testingHelpers.simpleCreate(
            "MA1proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          tx <- ctx.testingHelpers.simpleExercise(
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
          triProposal <- ctx.testingHelpers.simpleCreate(
            "MA2proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          tx <- ctx.testingHelpers.simpleExercise(
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
          triProposal <- ctx.testingHelpers.simpleCreate(
            "MA3proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          assertion <- ctx.testingHelpers.failingExercise(
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
          triProposal <- ctx.testingHelpers.simpleCreate(
            "MA4proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          assertion <- ctx.testingHelpers.failingExercise(
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

      "permit fetching a divulged contract" in allFixtures { ctx =>
        def pf(label: String, party: String) =
          RecordField(label, Some(Value(Value.Sum.Party(party))))
        // TODO currently we run multiple suites with the same sandbox, therefore we must generate
        // unique keys. This is not so great though, it'd be better to have a clean environment.
        val key = s"${UUID.randomUUID.toString}-key"
        val odArgs = Seq(
          pf("owner", owner),
          pf("delegate", delegate)
        )
        val delegatedCreate = ctx.testingHelpers.simpleCreate(
          cid("SDVl3"),
          owner,
          templateIds.delegated,
          Record(Some(templateIds.delegated), Seq(pf("owner", owner), RecordField(value = Some(Value(Value.Sum.Text(key)))))))
        val delegationCreate = ctx.testingHelpers.simpleCreate(
          cid("SDVl4"),
          owner,
          templateIds.delegation,
          Record(Some(templateIds.delegation), odArgs))
        val showIdCreate = ctx.testingHelpers.simpleCreate(
          cid("SDVl5"),
          owner,
          templateIds.showDelegated,
          Record(Some(templateIds.showDelegated), odArgs))
        for {
          delegatedEv <- delegatedCreate
          delegationEv <- delegationCreate
          showIdEv <- showIdCreate
          fetchArg = Record(
            None,
            Seq(RecordField("", Some(Value(Value.Sum.ContractId(delegatedEv.contractId))))))
          lookupArg = (expected: Option[String]) => Record(
            None,
            Seq(
              pf("", "owner"),
              RecordField(value = Some(Value(Value.Sum.Text(key)))),
              RecordField(value = expected match {
                case None => Value(Value.Sum.Optional(Optional(None)))
                case Some(cid) => Value(Value.Sum.Optional(Optional(Some(cid.asContractId))))
              })
            )
          )
          _ <- ctx.testingHelpers.simpleExercise(
            cid("SDVl6"),
            submitter = owner,
            template = templateIds.showDelegated,
            contractId = showIdEv.contractId,
            choice = "ShowIt",
            arg = Value(Value.Sum.Record(fetchArg)),
          )
          _ <- ctx.testingHelpers.simpleExercise(
            cid("SDVl7"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchDelegated",
            arg = Value(Value.Sum.Record(fetchArg)),
          )
          _ <- ctx.testingHelpers.simpleExercise(
            cid("SDVl8"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "LookupDelegated",
            arg = Value(Value.Sum.Record(lookupArg(Some(delegatedEv.contractId)))),
          )
        } yield (succeed)
      }

      "reject fetching an undisclosed contract" in allFixtures { ctx =>
        def pf(label: String, party: String) =
          RecordField(label, Some(Value(Value.Sum.Party(party))))
        // TODO currently we run multiple suites with the same sandbox, therefore we must generate
        // unique keys. This is not so great though, it'd be better to have a clean environment.
        val key = s"${UUID.randomUUID.toString}-key"
        val delegatedCreate = ctx.testingHelpers.simpleCreate(
          cid("TDVl3"),
          owner,
          templateIds.delegated,
          Record(Some(templateIds.delegated), Seq(pf("owner", owner), RecordField(value = Some(Value(Value.Sum.Text(key)))))))
        val delegationCreate = ctx.testingHelpers.simpleCreate(
          cid("TDVl4"),
          owner,
          templateIds.delegation,
          Record(Some(templateIds.delegation), Seq(pf("owner", owner), pf("delegate", delegate))))
        for {
          delegatedEv <- delegatedCreate
          delegationEv <- delegationCreate
          fetchArg = Record(
            None,
            Seq(RecordField("", Some(Value(Value.Sum.ContractId(delegatedEv.contractId))))))
          lookupArg = (expected: Option[String]) => Record(
            None,
            Seq(
              pf("", "owner"),
              RecordField(value = Some(Value(Value.Sum.Text(key)))),
              RecordField(value = expected match {
                case None => Value(Value.Sum.Optional(Optional(None)))
                case Some(cid) => Value(Value.Sum.Optional(Optional(Some(cid.asContractId))))
              }),
            )
          )
          fetchResult <- ctx.testingHelpers.failingExercise(
            cid("TDVl5"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchDelegated",
            arg = Value(Value.Sum.Record(fetchArg)),
            Code.INVALID_ARGUMENT,
            pattern = "dependency error: couldn't find contract"
          )
          _ <- ctx.testingHelpers.simpleExercise(
            cid("TDVl6"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "LookupDelegated",
            arg = Value(Value.Sum.Record(lookupArg(None))),
          )
        } yield (succeed)
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
          ctx.testingHelpers.submitRequestWithId(commandId).update(_.commands.commands := commandList)

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
                  case Created(CreatedEvent(eventId, _, _, _, _, _)) => eventId
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
                withObservers <- ctx.testingHelpers.simpleCreate(
                  "Obs2create:" + observer,
                  giver,
                  templateIds.withObservers,
                  withObserversArg)
                tx <- ctx.testingHelpers.simpleExerciseWithListener(
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
        def textKeyRecord(p: String, k: String, disclosedTo: List[String]): Record =
          Record(
            fields =
                List(
                  RecordField(value = p.asParty),
                  RecordField(value = s"$keyPrefix-$k".asText),
                  RecordField(value = disclosedTo.map(_.asParty).asList)))
        val key = "some-key"
        val alice = "Alice"
        val bob = "Bob"
        def textKeyValue(p: String, k: String, disclosedTo: List[String]): Value =
          Value(Value.Sum.Record(textKeyRecord(p, k, disclosedTo)))
        def textKeyKey(p: String, k: String): Value =
          Value(Value.Sum.Record(Record(fields = List(RecordField(value = p.asParty), RecordField(value = s"$keyPrefix-$k".asText)))))
        for {
          cid1 <- ctx.testingHelpers.simpleCreate(
            "CK-test-cid1",
            alice,
            templateIds.textKey,
            textKeyRecord(alice, key, List(bob))
          )
          // duplicate keys are not ok
          _ <- ctx.testingHelpers.failingCreate(
             "CK-test-duplicate-key",
             alice,
             templateIds.textKey,
             textKeyRecord(alice, key, List(bob)),
             Code.INVALID_ARGUMENT,
             "DuplicateKey"
           )
          // create handles to perform lookups / fetches
          aliceTKO <- ctx.testingHelpers.simpleCreate(
              "CK-test-aliceTKO",
              alice,
              templateIds.textKeyOperations,
              Record(fields = List(RecordField(value = alice.asParty))))
          bobTKO <- ctx.testingHelpers.simpleCreate(
              "CK-test-bobTKO",
              bob,
              templateIds.textKeyOperations,
              Record(fields = List(RecordField(value = bob.asParty)))
            )
          // unauthorized lookups are not OK
          // both existing lookups...
          lookupNone = Value(Value.Sum.Optional(Optional(None)))
          lookupSome = (cid: String) => Value(Value.Sum.Optional(Optional(Some(cid.asContractId))))
          _ <- ctx.testingHelpers.failingExercise(
            "CK-test-bob-unauthorized-1",
            bob,
            templateIds.textKeyOperations,
            bobTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(Record(fields = List(
                RecordField(value = textKeyKey(alice, key)),
                RecordField(value = lookupSome(cid1.contractId)))))),
            Code.INVALID_ARGUMENT,
            "requires authorizers"
          )
          // ..and non-existing ones
          _ <- ctx.testingHelpers.failingExercise(
            "CK-test-bob-unauthorized-2",
            bob,
            templateIds.textKeyOperations,
            bobTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyKey(alice, "bogus-key")),
                  RecordField(value = lookupNone))))),
            Code.INVALID_ARGUMENT,
            "requires authorizers")
          // successful, authorized lookup
          _ <- ctx.testingHelpers.simpleExercise(
            "CK-test-alice-lookup-found",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyKey(alice, key)),
                  RecordField(value = lookupSome(cid1.contractId)))))))
          // successful fetch
          _ <- ctx.testingHelpers.simpleExercise(
            "CK-test-alice-fetch-found",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOFetch",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyKey(alice, key)),
                  RecordField(value = cid1.contractId.asContractId))))))
          // failing, authorized lookup
          _ <- ctx.testingHelpers.simpleExercise(
            "CK-test-alice-lookup-not-found",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyKey(alice, "bogus-key")),
                  RecordField(value = lookupNone))))))
          // failing fetch
          _ <- ctx.testingHelpers.failingExercise(
            "CK-test-alice-fetch-not-found",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOFetch",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyKey(alice, "bogus-key")),
                  RecordField(value = cid1.contractId.asContractId))))),
            Code.INVALID_ARGUMENT,
            "couldn't find key")
          // now we exercise the contract, thus archiving it, and then verify
          // that we cannot look it up anymore
          _ <- ctx.testingHelpers.simpleExercise(
            "CK-test-alice-consume-cid1",
            alice,
            templateIds.textKey,
            cid1.contractId,
            "TextKeyChoice",
            emptyRecordValue)
          lookupAfterConsume <- ctx.testingHelpers.simpleExercise(
            "CK-test-alice-lookup-after-consume",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = textKeyKey(alice, key)),
                  RecordField(value = lookupNone))))))
          cid2 <- ctx.testingHelpers.simpleCreate(
            "CK-test-cid2",
            alice,
            templateIds.textKey,
            textKeyRecord(alice, "test-key-2", List(bob))
          )
          _ <- ctx.testingHelpers.simpleExercise(
            "CK-test-alice-consume-and-lookup",
            alice,
            templateIds.textKeyOperations,
            aliceTKO.contractId,
            "TKOConsumeAndLookup",
            Value(
              Value.Sum.Record(
                Record(fields = List(
                  RecordField(value = cid2.contractId.asContractId),
                  RecordField(value = textKeyKey(alice, "test-key-2"))))))
          )
        } yield {
          succeed
        }
      }

      "handle bad Decimals correctly" in allFixtures { ctx =>
        val alice = "Alice"
        for {
          _ <- ctx.testingHelpers.failingCreate(
            "Decimal-scale",
            alice,
            templateIds.decimalRounding,
            Record(fields = List(RecordField(value = Some(alice.asParty)), RecordField(value = Some("0.00000000005".asDecimal)))),
            Code.INVALID_ARGUMENT,
            "Could not read Decimal string"
          )
          _ <- ctx.testingHelpers.failingCreate(
            "Decimal-bounds-positive",
            alice,
            templateIds.decimalRounding,
            Record(fields = List(RecordField(value = Some(alice.asParty)), RecordField(value = Some("10000000000000000000000000000.0000000000".asDecimal)))),
            Code.INVALID_ARGUMENT,
            "Could not read Decimal string"
          )
          _ <- ctx.testingHelpers.failingCreate(
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
        .update(_.commands.ledgerId := config.assertStaticLedgerId)

      "process valid commands successfully" in allFixtures{ c =>
        val request = newRequest(validCreateAndExercise)

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

  private def createCommandWithId(ctx: LedgerContext, commandId: String) = {
    val reqWithId = ctx.testingHelpers.submitRequestWithId(commandId)
    val arguments = List("operator" -> "party".asParty)

    reqWithId.update(
      _.commands.update(_.commands := dummyTemplates.map(i => Command(create(i, arguments)))))
  }

  private def create(templateId: Identifier, arguments: immutable.Seq[(String, Value)]): Create = {
    Create(CreateCommand(Some(templateId), Some(arguments.asRecordOf(templateId))))
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
    ctx.testingHelpers.submitRequestWithId(commandId).update(
      _.commands.commands := List(
        ExerciseCommand(
          Some(templateIds.dummyFactory),
          factoryContractId,
          "DummyFactoryCall",
          Some(Value(Sum.Record(Record())))).wrap))
  }

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
        commandId + "factory",
        giver,
        templateIds.agreementFactory,
        agreementFactoryArg)
      tx <- ctx.testingHelpers.simpleExercise(
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
    ctx.testingHelpers.submitRequestWithId(commandId).update(
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
        ctx.testingHelpers.submitRequestWithId(cid(s"Exercising with a multitiude of params ($choice, $lbl)"))
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
    ctx.testingHelpers.submitRequestWithId(commandId)
        .update(
          _.commands.commands := List(
            Command(
              create(
                templateIds.agreementFactory,
                List(receiver -> receiver.asParty, giver -> giver.asParty)))),
          _.commands.party := giver
        )
  }
}
