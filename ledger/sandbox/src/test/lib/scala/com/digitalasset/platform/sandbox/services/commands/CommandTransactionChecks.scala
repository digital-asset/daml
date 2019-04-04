// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.commands

import java.time.Duration
import java.util.UUID

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.{MockMessages => M}
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.transaction_filter._
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc
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
import com.digitalasset.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.ledger.client.services.commands.{CommandClient, CompletionStreamElement}
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.platform.sandbox.TestTemplateIdentifiers
import com.digitalasset.platform.sandbox.utils.LedgerTestingHelpers
import com.google.protobuf.empty.Empty
import com.google.rpc.code.Code
import io.grpc.Channel
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Option2Iterable"
  ))
class CommandTransactionChecks(
    submitCommand: SubmitRequest => Future[Completion],
    instanceId: String,
    testPackageId: => String,
    ledgerIdOnServer: => String,
    channel: => Channel,
    timeProviderForClient: => TimeProvider,
    getMaterializer: => ActorMaterializer,
    getEsf: => ExecutionSequencerFactory)
    extends WordSpec
    with Matchers
    with OptionValues
    with ScalaFutures {

  private implicit lazy val mat: ActorMaterializer = getMaterializer
  private implicit lazy val esf: ExecutionSequencerFactory = getEsf

  private implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  override implicit lazy val patienceConfig: PatienceConfig = PatienceConfig(Span(60L, Seconds))

  private lazy val commandClient = newCommandClient()

  private lazy val templateIds = new TestTemplateIdentifiers(testPackageId)
  private lazy val dummyTemplates =
    List(templateIds.dummy, templateIds.dummyFactory, templateIds.dummyWithParam)
  private lazy val getAllContracts = M.transactionFilter

  private val operator = "operator"
  private val receiver = "receiver"
  private val giver = "giver"
  private val owner = "owner"
  private val delegate = "delegate"
  private val observers = List("observer1", "observer2")

  private def newCommandClient(applicationId: String = M.applicationId) =
    new CommandClient(
      CommandSubmissionServiceGrpc.stub(channel),
      CommandCompletionServiceGrpc.stub(channel),
      ledgerIdOnServer,
      applicationId,
      CommandClientConfiguration(1, 1, true, Duration.ofSeconds(10L)),
      Some(timeProviderForClient)
    )

  private lazy val transactionClient =
    new TransactionClient(ledgerIdOnServer, TransactionServiceGrpc.stub(channel))

  private lazy val helpers = new LedgerTestingHelpers(submitCommand, transactionClient)

  import helpers._

  private val emptyRecordValue = Value(Value.Sum.Record(Record()))
  s"Command and Transaction Services $instanceId" when {

    "reading completions" should {

      "return the completion of submitted commands for the submitting application" in {
        val commandId = cid("Submitting application sees this")
        val request = createCommandWithId(commandId)
        whenReady(for {
          offset <- commandClient.getCompletionEnd.map(_.getOffset)
          _ <- submitSuccessfully(request)
          completionAfterCheckpoint <- listenForCompletionAsApplication(
            M.applicationId,
            request.getCommands.party,
            offset,
            commandId)
        } yield {
          completionAfterCheckpoint
        }) {
          _.value.status.value should have('code (0))
        }
      }

      "not expose completions of submitted commands to other applications" in {
        val commandId = cid("The other application does not see this")
        val request = createCommandWithId(commandId)
        whenReady(for {
          offset <- commandClient.getCompletionEnd.map(_.getOffset)
          _ <- submitSuccessfully(request)
          completionsAfterCheckpoint <- listenForCompletionAsApplication(
            "anotherApplication",
            request.getCommands.party,
            offset,
            commandId)
        } yield {
          completionsAfterCheckpoint
        })(_ shouldBe empty)
      }

      "not expose completions of submitted commands to the application if it down't include the submitting party" in {
        val commandId =
          cid("The application should subscribe with the submitting party to see this")
        val request = createCommandWithId(commandId)
        whenReady(for {
          offset <- commandClient.getCompletionEnd.map(_.getOffset)
          _ <- submitSuccessfully(request)
          completionsAfterCheckpoint <- listenForCompletionAsApplication(
            request.getCommands.applicationId,
            "not " + request.getCommands.party,
            offset,
            commandId)
        } yield {
          completionsAfterCheckpoint
        })(_ shouldBe empty)
      }
    }

    "interacting with the ledger" should {

      //TODO: this is a quick copy of the test above to have a test case for DEL-3062. we need to clean this up. see: DEL-3097
      "expose contract Ids that are ready to be used for exercising choices using GetTransactionTrees" in {
        // note that the submitting party is not a stakeholder in any event,
        // so this test relies on the sandbox exposing the transactions to the
        // submitter.
        val factoryCreation = cid("Creating factory (Trees)")
        val exercisingChoice = cid("Exercising choice on factory (Trees)")
        whenReady(for {
          factoryContractId <- findCreatedEventInResultOf(factoryCreation, templateIds.dummyFactory)
          transaction <- submitAndListenForSingleTreeResultOfCommand(
            requestToCallExerciseWithId(factoryContractId.contractId, exercisingChoice),
            getAllContracts)
        } yield transaction -> factoryContractId) {
          case (tx, factoryContract) =>
            val exercisedEvent = topLevelExercisedIn(tx).head
            val creates = createdEventsInTreeNodes(exercisedEvent.childEventIds.map(tx.eventsById))

            exercisedEvent.contractId shouldEqual factoryContract.contractId
            exercisedEvent.consuming shouldBe true
            creates should have length 2
        }
      }

      // An equivalent of this tests the non-tree api in TransactionIT
      "accept all kinds of arguments in choices (Choice1, different args)" in {
        val newArgs =
          recordWithArgument(
            paramShowcaseArgs,
            RecordField("decimal", Some(Value(Value.Sum.Decimal("37.0")))))
        verifyParamShowcaseChoice(
          "Choice1", // choice name
          "different args",
          paramShowcaseArgumentsToChoice1Argument(newArgs), // submitted choice args
          // Daml-lf-engine integration works with non-verbose setting,
          // because we do not send the verbose flag in the request
          newArgs
        ) // expected args
      }

      "accept all kinds of arguments in choices (Choice2)" in {
        val newArgs =
          recordWithArgument(
            paramShowcaseArgs,
            RecordField("integer", Some(Value(Value.Sum.Int64(74)))))
        verifyParamShowcaseChoice(
          "Choice2", // choice name
          "changing 'integer'",
          // submitted choice args
          Value(
            Value.Sum.Record(
              Record(fields = List(RecordField("newInteger", Some(Value(Value.Sum.Int64(74)))))))),
          newArgs
        ) // expected args
      }

      // TODO An equivalent of this in TransactionIT tests this with a lower number of commands.
      // This can be removed once we can raise the command limit there.
      "accept huge submissions with a large number of commands" in {
        val commandId = cid("Huge composite command")
        val originalCommand = createCommandWithId(commandId)
        val targetNumberOfSubCommands = 15000 // That's around the maximum gRPC input size
        val superSizedCommand =
          originalCommand.update(_.commands.update(_.commands.modify(original =>
            List.fill(targetNumberOfSubCommands)(original.head))))
        whenReady(submitAndListenForSingleResultOfCommand(superSizedCommand, getAllContracts)) {
          tx =>
            tx.events.size shouldEqual targetNumberOfSubCommands
        }
      }

      "run callable payout and return the right events" in {
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
          simpleCreateWithListener(commandId, giver, receiver, templateIds.callablePayout, arg)

        val exercise = (party: String) =>
          (contractId: String) =>
            transactionsFromSimpleExercise(
              commandId + "exe",
              party,
              templateIds.callablePayout,
              contractId,
              "Transfer",
              Value(Value.Sum.Record(earg)))

        whenReady(for {
          cr <- createF
          ex <- exercise(receiver)(cr.contractId)
        } yield {
          val es: Vector[Event] = ex.flatMap(_.events).toVector
          es :+ Event(Created(cr))
        }) { events =>
          events.size shouldEqual 2
          val creates = events.flatMap(e => e.event.created.toList)

          creates.size shouldEqual 1

        }
      }

      "expose transactions to non-submitting stakeholders without the commandId" in {
        val receiver = "receiver"
        val giver = "giver"
        val commandId = cid("Testing if non-submitting stakeholder sees the commandId")
        val createCmd = createAgreementFactory(receiver, giver, commandId)
        whenReady {
          submitAndListenForSingleResultOfCommand(
            createCmd,
            TransactionFilter(Map(receiver -> Filters.defaultInstance)))
        } { tx =>
          tx.commandId should not equal commandId
          tx.commandId shouldEqual ""
        }
      }

      "accept exercising a well-authorized multi-actor choice" in {
        val triProposalArg = mkTriProposalArg(operator, receiver, giver)
        whenReady(for {
          agreement <- createAgreement("MA1", receiver, giver)
          triProposal <- simpleCreate(
            "MA1proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          tx <- simpleExercise(
            "MA1acceptance",
            giver,
            templateIds.agreement,
            agreement.contractId,
            "AcceptTriProposal",
            mkCidArg(triProposal.contractId))
        } yield {
          (agreement, triProposal, tx)
        }) {
          case (agreement, triProposal, tx) =>
            val agreementExercised = getHead(topLevelExercisedIn(tx))
            agreementExercised.contractId shouldBe agreement.contractId
            val triProposalExercised =
              getHead(exercisedEventsInNodes(agreementExercised.childEventIds.map(tx.eventsById)))
            triProposalExercised.contractId shouldBe triProposal.contractId
            triProposalExercised.actingParties.toSet shouldBe Set(receiver, giver)
            val triAgreement =
              getHead(
                createdEventsInTreeNodes(triProposalExercised.childEventIds.map(tx.eventsById)))
            val expectedFields = removeLabels(triProposalArg.fields)
            triAgreement.getCreateArguments.fields shouldBe expectedFields
        }
      }

      "accept exercising a well-authorized multi-actor choice with coinciding controllers" in {
        val triProposalArg = mkTriProposalArg(operator, giver, giver)
        whenReady(for {
          triProposal <- simpleCreate(
            "MA2proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          tx <- simpleExercise(
            "MA2acceptance",
            giver,
            templateIds.triProposal,
            triProposal.contractId,
            "TriProposalAccept",
            emptyRecordValue)
        } yield {
          (triProposal, tx)
        }) {
          case (triProposal, tx) =>
            val triProposalExercised = getHead(topLevelExercisedIn(tx))
            triProposalExercised.contractId shouldBe triProposal.contractId
            triProposalExercised.actingParties.toSet shouldBe Set(giver)
            val triAgreement =
              getHead(
                createdEventsInTreeNodes(triProposalExercised.childEventIds.map(tx.eventsById)))
            val expectedFields =
              removeLabels(triProposalArg.fields)
            triAgreement.getCreateArguments.fields shouldBe expectedFields
        }
      }

      "reject exercising a multi-actor choice with missing authorizers" in {
        val triProposalArg = mkTriProposalArg(operator, receiver, giver)
        whenReady(for {
          triProposal <- simpleCreate(
            "MA3proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          assertion <- failingExercise(
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
        })(identity)
      }

      // NOTE(MH): This is the current, most conservative semantics of
      // multi-actor choice authorization. It is likely that this will change
      // in the future. Should we delete this test, we should also remove the
      // 'UnrestrictedAcceptTriProposal' choice from the 'Agreement' template.
      //TODO: check this with Martin Hu or Robin
      "reject exercising a multi-actor choice with too many authorizers" ignore {
        val triProposalArg = mkTriProposalArg(operator, giver, giver)
        whenReady(for {
          agreement <- createAgreement("MA4", receiver, giver)
          triProposal <- simpleCreate(
            "MA4proposal",
            operator,
            templateIds.triProposal,
            triProposalArg)
          assertion <- failingExercise(
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
        })(identity)
      }

      "reject fetching an undisclosed contract" in {
        def pf(label: String, party: String) =
          RecordField(label, Some(Value(Value.Sum.Party(party))))
        val delegatedCreate = simpleCreate(
          cid("TDVl3"),
          owner,
          templateIds.delegated,
          Record(Some(templateIds.delegated), Seq(pf("owner", owner))))
        val delegationCreate = simpleCreate(
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
            cid("TDVl5"),
            submitter = delegate,
            template = templateIds.delegation,
            contractId = delegationEv.contractId,
            choice = "FetchDelegated",
            arg = Value(Value.Sum.Record(fetchArg)),
            // TODO SC proper error spec here
            Code.UNKNOWN,
            pattern = "foobar"
          )
        } yield fetchResult
        whenReady(exerciseOfFetch)(identity)
      }

      "DAML engine returns Unit as argument to Nothing" in {
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
          submitRequestWithId(commandId).update(_.commands.commands := commandList)

        whenReady(for {
          tx <- submitAndListenForSingleResultOfCommand(command, getAllContracts)
        } yield {
          val creates = createdEventsIn(tx)
          val create = getHead(creates)
          // only compare the field values since the server currently does not return the
          // record identifier or labels when the request does not set verbose=true .
          create.getCreateArguments.fields.map(_.value) shouldEqual
            createArguments.fields.map(_.value)
        })(_ => succeed)
      }

      "having many transactions all of them has a unique event id" in {
        val eventIdsF = transactionClient
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

        whenReady(eventIdsF) { eventIds =>
          val eventIdList = eventIds.flatten
          val eventIdSet = eventIdList.toSet
          eventIdList.size shouldEqual eventIdSet.size
        }
      }

      "disclose create to observers" in {
        val withObserversArg = mkWithObserversArg(giver, observers)
        observers.map(observer =>
          whenReady(for {
            withObservers <- simpleCreateWithListener(
              "Obs1create:" + observer,
              giver,
              observer,
              templateIds.withObservers,
              withObserversArg)
          } yield {
            val expectedFields =
              removeLabels(withObserversArg.fields)
            withObservers.getCreateArguments.fields shouldEqual expectedFields

          })(identity))
      }

      "disclose exercise to observers" in {
        val withObserversArg = mkWithObserversArg(giver, observers)
        observers.map(observer =>
          whenReady(for {
            withObservers <- simpleCreate(
              "Obs2create:" + observer,
              giver,
              templateIds.withObservers,
              withObserversArg)
            tx <- simpleExerciseWithListener(
              "Obs2exercise:" + observer,
              giver,
              observer,
              templateIds.withObservers,
              withObservers.contractId,
              "Ping",
              emptyRecordValue)
          } yield {
            val withObserversExercised = getHead(topLevelExercisedIn(tx))
            withObserversExercised.contractId shouldBe withObservers.contractId
          })(identity))
      }

      // this is basically a port of
      // `daml-lf/tests/scenario/daml-1.3/contract-keys/Test.daml`.
      //
      // scalafmt cannot deal with this function
      // format: off
      "process contract keys" in {
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
        val cid1 = whenReady(
          simpleCreate(
            "CK-test-cid1",
            alice,
            templateIds.textKey,
            textKeyRecord(alice, key)
          ))(identity)
        // duplicate keys are not ok
        whenReady(
          failingCreate(
            "CK-test-duplicate-key",
            alice,
            templateIds.textKey,
            textKeyRecord(alice, key),
            Code.INVALID_ARGUMENT,
            "DuplicateKey"
          ))(identity)
        // create handles to perform lookups / fetches
        val aliceTKO = whenReady(
          simpleCreate(
            "CK-test-aliceTKO",
            alice,
            templateIds.textKeyOperations,
            Record(fields = List(RecordField(value = alice.asParty))))
        )(identity)
        val bobTKO = whenReady(
          simpleCreate(
            "CK-test-bobTKO",
            bob,
            templateIds.textKeyOperations,
            Record(fields = List(RecordField(value = bob.asParty)))
          )
        )(identity)
        // unauthorized lookups are not OK
        // both existing lookups...
        val lookupNone: Value = Value(Value.Sum.Optional(Optional(None)))
        def lookupSome(cid: String): Value =
          Value(Value.Sum.Optional(Optional(Some(cid.asContractId))))
        whenReady(
          failingExercise(
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
          ))(identity)
        // ..and non-existing ones
        whenReady(
          failingExercise(
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
            "requires authorizers"
          ))(identity)
        // successful, authorized lookup
        whenReady(
          simpleExercise(
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
        )(identity)
        // successful fetch
        whenReady(
          simpleExercise(
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
        )(identity)
        // failing, authorized lookup
        whenReady(
          simpleExercise(
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
        )(identity)
        // failing fetch
        whenReady(
          failingExercise(
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
        )(identity)
        // now we exercise the contract, thus archiving it, and then verify
        // that we cannot look it up anymore
        whenReady(for {
          _ <- simpleExercise(
            "CK-test-alice-consume-cid1",
            alice,
            templateIds.textKey,
            cid1.contractId,
            "TextKeyChoice",
            emptyRecordValue
          )
          _ <- simpleExercise(
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
        } yield ())(identity)
        // now we make sure that local key deletions are recorded
        whenReady(for {
          cid2 <- simpleCreate(
            "CK-test-cid2",
            alice,
            templateIds.textKey,
            textKeyRecord(alice, "test-key-2")
          )
          _ <- simpleExercise(
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
        } yield ())(identity)
      }
    }
  }

  private def createAgreementFactory(receiver: String, giver: String, commandId: String) = {
    submitRequestWithId(commandId)
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
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] = {
    for {
      tx <- submitAndListenForSingleResultOfCommand(
        submitRequestWithId(cid(commandId))
          .update(
            _.commands.commands :=
              List(CreateCommand(Some(template), Some(arg)).wrap),
            _.commands.party := submitter
          ),
        TransactionFilter(Map(listener -> Filters.defaultInstance))
      )
    } yield {
      getHead(createdEventsIn(tx))
    }
  }

  // Create a template instance and return the resulting create event.
  private def simpleCreate(
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] =
    simpleCreateWithListener(commandId, submitter, submitter, template, arg)

  private def failingCreate(
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record,
      code: Code,
      pattern: String
  ): Future[Assertion] =
    assertCommandFailsWithCode(
      submitRequestWithId(cid(commandId))
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
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[TransactionTree] = {
    submitAndListenForSingleTreeResultOfCommand(
      submitRequestWithId(cid(commandId))
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
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] = {
    for {
      tx <- submitAndListenForSingleResultOfCommand(
        submitRequestWithId(cid(commandId))
          .update(
            _.commands.commands :=
              List(CreateCommand(Some(template), Some(arg)).wrap),
            _.commands.party := submitter
          ),
        TransactionFilter(Map(listener -> Filters.defaultInstance))
      )
    } yield {
      getHead(createdEventsIn(tx))
    }
  }

  private def transactionsFromsimpleCreate(
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] =
    simpleCreateWithListenerForTransactions(commandId, submitter, submitter, template, arg)

  private def simpleExerciseWithListenerForTransactions(
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[Seq[Transaction]] = {
    submitAndListenForTransactionResultOfCommand(
      submitRequestWithId(cid(commandId))
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
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value): Future[Seq[Transaction]] =
    simpleExerciseWithListenerForTransactions(
      commandId,
      submitter,
      submitter,
      template,
      contractId,
      choice,
      arg)

  def submitAndListenForTransactionResultOfCommand(
      command: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true): Future[Seq[Transaction]] = {
    submitAndListenForTransactionResultsOfCommand(command, transactionFilter, filterCid)
  }

  def submitAndListenForTransactionResultsOfCommand(
      submitRequest: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true): Future[immutable.Seq[Transaction]] = {
    val commandId = submitRequest.getCommands.commandId
    for {
      txEndOffset <- submitSuccessfullyAndReturnOffset(submitRequest)
      transactions <- listenForTransactionResultOfCommand(
        transactionFilter,
        if (filterCid) Some(commandId) else None,
        txEndOffset)
    } yield {
      transactions
    }
  }

  def listenForTransactionResultOfCommand(
      transactionFilter: TransactionFilter,
      commandId: Option[String],
      txEndOffset: LedgerOffset): Future[immutable.Seq[Transaction]] = {
    transactionClient
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

  // Exercise a choice and return all resulting create events.
  private def simpleExercise(
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[TransactionTree] =
    simpleExerciseWithListener(commandId, submitter, submitter, template, contractId, choice, arg)

  // Exercise a choice that is supposed to fail.
  private def failingExercise(
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
      submitRequestWithId(cid(commandId))
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
        commandId + "factory",
        giver,
        templateIds.agreementFactory,
        agreementFactoryArg)
      tx <- simpleExercise(
        commandId + "agreement",
        receiver,
        templateIds.agreementFactory,
        agreementFactory.contractId,
        "AgreementFactoryAccept",
        emptyRecordValue)
    } yield {
      val agreementFactoryExercised = getHead(topLevelExercisedIn(tx))
      agreementFactoryExercised.contractId shouldBe agreementFactory.contractId
      getHead(createdEventsInTreeNodes(agreementFactoryExercised.childEventIds.map(tx.eventsById)))
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

  private def createParamShowcaseWith(commandId: String, createArguments: Record) = {
    val commandList = List(
      CreateCommand(Some(templateIds.parameterShowcase), Some(createArguments)).wrap)
    submitRequestWithId(commandId).update(_.commands.modify(_.update(_.commands := commandList)))
  }

  private def findCreatedEventInResultOf(
      cid: String,
      templateToLookFor: Identifier): Future[CreatedEvent] = {
    for {
      tx <- createContracts(cid)
    } yield {
      findCreatedEventIn(tx, templateToLookFor)
    }
  }

  private def filterSingleTemplate(templateClientSubscribedTo: Identifier) = {
    TransactionFilter(
      Map(M.party -> Filters(Some(InclusiveFilters(List(templateClientSubscribedTo))))))
  }

  private def listenForCompletionAsApplication(
      appId: String,
      requestingParty: String,
      offset: LedgerOffset,
      commandIdToListenFor: String) = {
    newCommandClient(appId)
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

  private def requestToCallExerciseWithId(factoryContractId: String, commandId: String) = {
    submitRequestWithId(commandId).update(
      _.commands.commands := List(
        ExerciseCommand(
          Some(templateIds.dummyFactory),
          factoryContractId,
          "DummyFactoryCall",
          Some(Value(Sum.Record(Record())))).wrap))
  }

  private def createContracts(commandId: String) = {
    val command = createCommandWithId(commandId)
    submitAndListenForSingleResultOfCommand(command, getAllContracts)
  }

  private def createCommandWithId(commandId: String) = {
    val reqWithId = submitRequestWithId(commandId)
    val arguments = List("operator" -> "party".asParty)

    reqWithId.update(
      _.commands.update(_.commands := dummyTemplates.map(i => Command(create(i, arguments)))))
  }

  private def submitRequestWithId(id: String) =
    M.submitRequest.update(_.commands.modify(_.copy(commandId = id, ledgerId = ledgerIdOnServer)))

  private def create(templateId: Identifier, arguments: immutable.Seq[(String, Value)]): Create = {
    Create(CreateCommand(Some(templateId), Some(arguments.asRecordOf(templateId))))

  }

  private def cid(commandId: String) = s"$commandId $instanceId"

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

  private def compositeCommandRelativeWithExercises(commandId: String) = {
    submitRequestWithId(commandId)
      .update(
        _.commands.commands := List(
          Command(
            Create(
              CreateCommand(
                Some(templateIds.dummyContractFactory),
                Some(List("operator" -> "party".asParty)
                  .asRecordOf(templateIds.dummyContractFactory))))),
          // The case we are testing is specifically for relative contract Ids
          // referring back to the same transaction within a composite command
          // this is why we must use these fixed contract ids.
          Command(
            Exercise(ExerciseCommand(
              Some(templateIds.dummyContractFactory),
              s"#/0<${templateIds.dummyContractFactory.moduleName}:${templateIds.dummyContractFactory.entityName}@${templateIds.dummyContractFactory.packageId}>",
              "DummyContractFactoryCall",
              Some(Value(Sum.Unit(Empty())))
            ))),
          Command(
            Exercise(ExerciseCommand(
              Some(templateIds.dummyContractFactory),
              s"#/1<${templateIds.dummy.moduleName}:${templateIds.dummy.entityName}@${templateIds.dummy.packageId}>",
              "Choice",
              Some(Value(Sum.Unit(Empty())))
            )))
        )
      )
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
      choice: String,
      lbl: String,
      exerciseArg: Value,
      expectedCreateArg: Record) = {
    val command: SubmitRequest =
      createParamShowcaseWith(
        cid(s"Creating contract with a multitude of param types for exercising ($choice, $lbl)"),
        paramShowcaseArgs)
    whenReady(for {
      tx <- submitAndListenForSingleResultOfCommand(command, getAllContracts)
      contractId = getHead(createdEventsIn(tx)).contractId
      // first, verify that if we submit with the same inputs they're equal
      exercise = ExerciseCommand(
        Some(templateIds.parameterShowcase),
        contractId,
        choice,
        exerciseArg).wrap
      tx <- submitAndListenForSingleTreeResultOfCommand(
        submitRequestWithId(cid(s"Exercising with a multitiude of params ($choice, $lbl)"))
          .update(_.commands.update(_.commands := List(exercise))),
        getAllContracts,
        true
      )
    } yield {
      // check that we have the exercise
      val exercise = getHead(topLevelExercisedIn(tx))

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
      val create = getHead(createdEventsInTreeNodes(exercise.childEventIds.map(tx.eventsById)))
      create.getCreateArguments.fields shouldEqual expectedCreateFields
//      expectedCreateFields
    })(_ => succeed)
  }
}
