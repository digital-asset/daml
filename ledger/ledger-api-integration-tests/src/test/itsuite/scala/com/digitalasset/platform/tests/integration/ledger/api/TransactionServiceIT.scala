// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.time.{Duration, Instant}

import akka.Done
import akka.stream.scaladsl.{Flow, Sink}
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.domain.EventId
import com.digitalasset.ledger.api.testing.utils.MockMessages.{party, _}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  MockMessages,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.commands.Command.Command.Create
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter, _}
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.Value.Sum.{Bool, ContractId}
import com.digitalasset.ledger.api.v1.value.{
  Identifier,
  Optional,
  Record,
  RecordField,
  Value,
  Variant
}
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.api.v1.event.EventOps._
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture, TestTemplateIds}
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.platform.services.time.TimeProviderType
import com.google.rpc.code.Code
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest._
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import scalaz.syntax.tag._
import scalaz.{ICons, NonEmptyList, Tag}

import scala.collection.{breakOut, immutable}
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TransactionServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with Inside
    with AsyncTimeLimitedTests
    with TestExecutionSequencerFactory
    with TransactionServiceHelpers
    with ParameterShowcaseTesting
    with OptionValues
    with Matchers
    with TestTemplateIds {

  override protected val config: Config =
    Config.default.withTimeProvider(TimeProviderType.WallClock)

  override val timeLimit: Span = 300.seconds

  private def newClient(stub: TransactionService, ledgerId: String): TransactionClient =
    new TransactionClient(ledgerId, stub)

  private val getAllContracts = transactionFilter

  private def filterSingleTemplate(templateClientSubscribedTo: Identifier) = {
    TransactionFilter(
      Map(party -> Filters(Some(InclusiveFilters(List(templateClientSubscribedTo))))))
  }

  private val NonEmptyList(party1, ICons(party2, ICons(party3, _))) = config.parties

  private val smallCommandCount = 5

  private val configuredParties = config.parties.list.toList

  private val filterForAllParties = TransactionFilter(
    configuredParties.map(_ -> Filters.defaultInstance).toMap)

  private val unitArg = Value(Sum.Record(Record.defaultInstance))

  "Transaction Service" when {

    "reading transactions with LedgerBegin as end offset" should {

      "serve an empty stream of transactions" in allFixtures { context =>
        for {
          transactions <- context.transactionClient
            .getTransactions(ledgerBegin, Some(ledgerBegin), getAllContracts)
            .runWith(Sink.seq)
        } yield {
          transactions shouldBe empty
        }
      }
    }

    "submitting and reading transactions" should {

      "serve elements until canceled from downstream" in allFixtures { context =>
        val elemsToTake = 10L

        for {
          _ <- insertCommands(
            getTrackerFlow(context),
            "cancellation-test",
            14,
            config.assertStaticLedgerId)
          transactions <- context.transactionClient
            .getTransactions(ledgerBegin, None, getAllContracts)
            .take(elemsToTake)
            .runWith(Sink.seq)
        } yield {
          transactions should have length elemsToTake
        }
      }

      "deduplicate commands with identical command + application ID pairs" in allFixtures {
        context =>
          val client = context.transactionClient
          for {
            le <- client.getLedgerEnd
            _ <- insertCommands("deduplicated", 1, context)
            _ = insertCommands("deduplicated", 1, context) // we don't wait for this since the result won't be seen
            txs <- client
              .getTransactions(le.getOffset, None, getAllContracts)
              .takeWithin(2.seconds)
              .runWith(Sink.seq)
          } yield {
            txs should have length 1
          }
      }

      "respond with empty stream if TransactionFilter is empty" in allFixtures { context =>
        context.transactionClient
          .getTransactions(ledgerBegin, None, TransactionFilter())
          .runWith(Sink.seq)
          .map(_ shouldBe empty)
      }

      "complete the stream by itself as soon as LedgerEnd is hit" in allFixtures { context =>
        val resultsF = context.transactionClient
          .getTransactions(ledgerBegin, Some(ledgerEnd), getAllContracts)
          .runWith(Sink.seq)

        for {
          _ <- insertCommands(
            getTrackerFlow(context),
            "stream-completion-test",
            14,
            config.assertStaticLedgerId)
          _ <- resultsF
        } yield {
          succeed // resultF would not complete unless the server terminates the connection
        }
      }

      "serve the complete sequence of transactions even if processing is stopped and resumed mid-stream" in allFixtures {
        context =>
          val client = context.transactionClient
          val commandsPerSection = smallCommandCount

          val sharedPrefix = "stream-partial-read-test"
          val firstSectionPrefix = sharedPrefix + "-1"

          for {
            ledgerEndResponse <- client.getLedgerEnd
            _ <- insertCommands(firstSectionPrefix, commandsPerSection, context)
            firstSection <- client
              .getTransactions(ledgerEndResponse.getOffset, None, getAllContracts)
              .filter(_.commandId.startsWith(sharedPrefix))
              .take(commandsPerSection.toLong)
              .runWith(Sink.seq)
            _ = firstSection should have size commandsPerSection.toLong
            ledgerEndAfterFirstSection = lastOffsetIn(firstSection).value

            _ <- insertCommands(sharedPrefix + "-2", commandsPerSection, context)

            secondSection <- client
              .getTransactions(ledgerEndAfterFirstSection, None, getAllContracts)
              .take(commandsPerSection.toLong)
              .runWith(Sink.seq)

            _ = secondSection should have size commandsPerSection.toLong
            ledgerEndAfterSecondSection = lastOffsetIn(secondSection).value

            completeSequence <- client
              .getTransactions(
                ledgerEndResponse.getOffset,
                Some(ledgerEndAfterSecondSection),
                getAllContracts)
              .filter(_.commandId.startsWith(sharedPrefix))
              .completionTimeout(3.seconds)
              .runWith(Sink.seq)

          } yield {
            completeSequence shouldEqual (firstSection ++ secondSection)
            completeSequence should have length (commandsPerSection.toLong * 2)
          }
      }

      "serve the same data for identical requests coming in parallel" in allFixtures { context =>
        val client = context.transactionClient
        val commandPrefix = "parallel-subscription-test"
        val subscriptions = 10

        for {
          ledgerEndOnStart <- client.getLedgerEnd
          _ <- insertCommands(commandPrefix, smallCommandCount, context)
          readTransactions = () =>
            client
              .getTransactions(ledgerEndOnStart.getOffset, None, getAllContracts)
              .filter(_.commandId.startsWith(commandPrefix))
              .take(smallCommandCount.toLong)
              .runWith(Sink.seq)
          results <- 1
            .to(subscriptions)
            .map(_ => readTransactions())
            .foldLeft(Future.successful(Vector.empty[Seq[Transaction]]))((accF, currF) =>
              accF.flatMap(acc => currF.map(acc :+ _)))

        } yield {
          results.foreach(_ should have size smallCommandCount.toLong)
          results.sliding(2).foreach { case Vector(s1, s2) => s1 shouldEqual s2 }
          results.size shouldEqual subscriptions
        }

      }

      "not expose data to parties without privileges to see it" in allFixtures { context =>
        val client = context.transactionClient
        val commandPrefix = "visibility-test"

        val anotherParty = "Alice"
        for {
          ledgerEndResponse <- client.getLedgerEnd
          _ <- insertCommands(commandPrefix, 1, context)
          // At this point we verified that the value has been written to the submitter's LSM.
          // This test code assumes that the value would be written to other parties' LSMs within 100 ms.
          transactions <- client
            .getTransactions(
              ledgerEndResponse.getOffset,
              None,
              TransactionFilter(Map(anotherParty -> Filters.defaultInstance)))
            .filter(_.commandId.startsWith(commandPrefix))
            .takeWithin(100.millis)
            .runWith(Sink.seq)

        } yield {
          transactions shouldBe empty
        }
      }

      "serve an empty stream if start offset equals the end offset (supplied)" in allFixtures {
        context =>
          val client = context.transactionClient

          for {
            ledgerEnd <- client.getLedgerEnd
            transactions <- client
              .getTransactions(ledgerEnd.getOffset, Some(ledgerEnd.getOffset), getAllContracts)
              .runWith(Sink.seq)
          } yield {
            transactions shouldBe empty
          }
      }

      "serve an empty stream if start offset equals the end offset (resolved on server from Boundary)" in allFixtures {
        context =>
          for {
            savedLedgerEnd <- context.transactionClient.getLedgerEnd
            txs <- context.transactionClient
              .getTransactions(savedLedgerEnd.getOffset, Some(ledgerEnd), getAllContracts)
              .runWith(Sink.seq)
          } yield {
            txs shouldBe empty
          }
      }

      "return INVALID_ARGUMENT if start offset is after end offset" in allFixtures { context =>
        val client = context.transactionClient

        for {
          savedLedgerEnd <- client.getLedgerEnd
          _ <- insertCommands("end-before-start-test", 1, context)
          tx <- client
            .getTransactions(savedLedgerEnd.getOffset, None, getAllContracts)
            .runWith(Sink.head)
          higherLedgerOffset = tx.offset
          error <- client
            .getTransactions(
              LedgerOffset(LedgerOffset.Value.Absolute(higherLedgerOffset)),
              Some(savedLedgerEnd.getOffset),
              getAllContracts)
            .runWith(Sink.head)
            .failed
        } yield {
          IsStatusException(Status.INVALID_ARGUMENT)(error)
        }
      }

      "expose transactions to non-submitting stakeholders without the commandId" in allFixtures {
        c =>
          c.submitCreateAndReturnTransaction(
              "Checking commandId visibility for non-submitter party",
              templateIds.agreementFactory,
              List("receiver" -> party1.asParty, "giver" -> party2.asParty).asRecordFields,
              party2,
              party1
            )
            .map(_.commandId shouldBe empty)

      }

      "expose only the requested templates to the client" in allFixtures { context =>
        val commandId = "Client should see only the Dummy create."
        val templateInSubscription = templateIds.dummy
        val otherTemplateCreated = templateIds.dummyFactory
        for {
          tx <- context.testingHelpers.submitAndListenForSingleResultOfCommand(
            context.command(
              commandId,
              List(templateInSubscription, otherTemplateCreated).map(tid =>
                Command(create(tid, List("operator" -> "party".asParty))))),
            filterSingleTemplate(templateInSubscription)
          )
        } yield {
          val singleEvent = getHead(tx.events.map(_.event))
          singleEvent match {
            case Created(createdEvent) =>
              createdEvent.templateId.value shouldEqual templateInSubscription
            case other => fail(s"Expected create event, got $other")
          }
        }
      }

      "expose contract Ids that are ready to be used for exercising choices" in allFixtures {
        context =>
          val factoryCreation = "Creating factory"
          val exercisingChoice = "Exercising choice on factory"
          val exercisedTemplate = templateIds.dummyFactory
          for {
            createdEvent <- context.submitCreate(
              factoryCreation,
              exercisedTemplate,
              List("operator" -> "party".asParty).asRecordFields,
              "party")
            factoryContractId = createdEvent.contractId
            exerciseTx <- context.testingHelpers.submitAndListenForSingleResultOfCommand(
              context.command(
                exercisingChoice,
                List(exerciseCallChoice(exercisedTemplate, factoryContractId).wrap)),
              getAllContracts
            )
          } yield {
            val events = exerciseTx.events.map(_.event)
            val (created, archived) = events.partition(_.isCreated)
            created should have length 2
            getHead(archived).archived.value.contractId shouldEqual factoryContractId
          }
      }

      "expose contract Ids that are results of exercising choices when filtering by template" in allFixtures {
        context =>
          val factoryCreation = "Creating second factory"
          val exercisingChoice = "Exercising choice on second factory"
          val exercisedTemplate = templateIds.dummyFactory
          for {
            creation <- context.submitCreate(
              factoryCreation,
              exercisedTemplate,
              List("operator" -> "party".asParty).asRecordFields,
              "party")
            factoryContractId = creation.contractId

            offsetToListenFrom <- context.testingHelpers.submitSuccessfullyAndReturnOffset(
              context.command(
                exercisingChoice,
                List(exerciseCallChoice(exercisedTemplate, factoryContractId).wrap)))

            txsWithCreate <- context.testingHelpers.listenForResultOfCommand(
              filterSingleTemplate(templateIds.dummyWithParam),
              Some(exercisingChoice),
              offsetToListenFrom)
            txsWithArchive <- context.testingHelpers.listenForResultOfCommand(
              filterSingleTemplate(templateIds.dummyFactory),
              Some(exercisingChoice),
              offsetToListenFrom)

          } yield {
            val txCreate = getHead(txsWithCreate)
            val txArchive = getHead(txsWithArchive)

            // Create
            txCreate.commandId shouldEqual exercisingChoice
            val createdEvent = getHead(createdEventsIn(txCreate))
            createdEvent.getTemplateId shouldEqual templateIds.dummyWithParam

            // Archive
            txArchive.commandId shouldEqual exercisingChoice
            val archivedEvent = getHead(archivedEventsIn(txArchive))
            archivedEvent.getTemplateId shouldEqual templateIds.dummyFactory
          }
      }

      "reject exercising a choice where an assertion fails" in allFixtures { c =>
        for {
          dummy <- c.submitCreate(
            "Create for assertion failing test",
            templateIds.dummy,
            List("operator" -> party.asParty).asRecordFields,
            party)
          assertion <- failingExercise(
            c,
            "Assertion failing exercise",
            party,
            templateIds.dummy,
            dummy.contractId,
            "ConsumeIfTimeIsBetween",
            List(
              "begin" -> Instant.now().plus(Duration.ofDays(1)).asTime,
              "end" -> Instant.now().plus(Duration.ofDays(2)).asTime).asRecordValue,
            Code.INVALID_ARGUMENT,
            "error"
            // the error message is different with the unified interpreter, so the text is matched to error
            // N.B.: The full DAML error message here is not very lucid.
          )
        } yield {
          assertion
        }
      }

      "accept and serve all kinds of arguments in creates" in allFixtures { c =>
        val template = templateIds.parameterShowcase
        val expectedArg = paramShowcaseArgsWithoutLabels
        for {
          create <- c.submitCreate(
            "Creating contract with a multitude of param types",
            template,
            paramShowcaseArgs(templateIds.testPackageId),
            "party",
            verbose = false)
        } yield {
          create.getCreateArguments.recordId shouldBe empty
          create.getCreateArguments.fields shouldEqual expectedArg
        }
      }

      "accept and serve all kinds of verbose arguments in creates" in allFixtures { c =>
        val template = templateIds.parameterShowcase
        val arg = paramShowcaseArgs(templateIds.testPackageId)

        val variant = Value(Value.Sum.Variant(Variant(None, "SomeInteger", 1.asInt64)))
        for {
          create <- c.submitCreate(
            "Creating contract with a multitude of verbose param types",
            template,
            arg,
            "party",
            verbose = true)
        } yield {
          val args = create.getCreateArguments
          args.recordId should contain(template)
          args.fields shouldEqual arg
          args.fields
            .collectFirst { case f if f.label == "nestedOptionalInteger" => f.value.value }
            .value
            .getRecord
            .fields
            .headOption
            .value
            .value // This is the actual field access.
            .value
            .getVariant
            .variantId should contain(
            template.copy(name = "Test.OptionalInteger", entityName = "OptionalInteger"))
        }
      }

      "accept and serve all kinds of arguments in choices" in allFixtures { c =>
        val args =
          c.testingHelpers.recordFieldsWithArgument(
            paramShowcaseArgs(templateIds.testPackageId),
            RecordField("decimal", Some("37.0".asDecimal)))
        val expectedArg =
          paramShowcaseArgsWithoutLabels.updated(2, RecordField("", Some("37.0".asDecimal)))

        verifyParamShowcaseChoice(
          c,
          "Choice1", // choice name
          "same args",
          paramShowcaseArgumentsToChoice1Argument(Record(None, args)), // submitted choice args
          expectedArg) // expected args
      }

      "accept huge submissions with a long lists" in allFixtures { c =>
        val listElements = 10000 // Gets very slow / times out after this
        val integerList = 1.to(listElements).map(_.toLong.asInt64).asList

        val arg: Seq[RecordField] = c.testingHelpers
          .recordFieldsWithArgument(
            paramShowcaseArgs(templateIds.testPackageId),
            RecordField(integerListRecordLabel, integerList))
        val expectedArg = paramShowcaseArgsWithoutLabels.updated(7, RecordField("", integerList))

        val template = templateIds.parameterShowcase
        for {
          create <- c.submitCreate(
            "Huge command with a long list",
            template,
            arg,
            "party"
          )
        } yield {
          create.getCreateArguments.fields shouldEqual expectedArg
        }
      }

      "not archive the exercised contract on non-consuming choices" in allFixtures { c =>
        val receiver = "party"
        val giver = "Alice"
        for {
          created <- c.submitCreateWithListenerAndReturnEvent(
            "Creating Agreement Factory",
            templateIds.agreementFactory,
            List("receiver" -> receiver.asParty, "giver" -> giver.asParty).asRecordFields,
            giver,
            giver)

          choiceResult <- c.testingHelpers.submitAndListenForSingleResultOfCommand(
            c.command(
                "Calling non-consuming choice",
                List(
                  ExerciseCommand(
                    Some(templateIds.agreementFactory),
                    created.contractId,
                    "CreateAgreement",
                    Some(unitArg)).wrap)
              )
              .update(_.commands.party := receiver),
            TransactionFilter(Map(receiver -> Filters.defaultInstance))
          )
        } yield {

          archivedEventsIn(choiceResult) shouldBe empty
          getHead(createdEventsIn(choiceResult)) should have(
            'templateId (Some(templateIds.agreement)))
        }
      }

      "require only authorization of chosen branching signatory" in allFixtures { c =>
        val branchingSignatoriesArg =
          getBranchingSignatoriesArg(true, party1, party2)
        val expectedArg = branchingSignatoriesArg.map(_.copy(label = ""))

        for {
          branchingSignatories <- c.submitCreateWithListenerAndReturnEvent(
            "BranchingSignatoriesTrue",
            templateIds.branchingSignatories,
            branchingSignatoriesArg,
            party1,
            party1)
        } yield {
          branchingSignatories.getCreateArguments.fields shouldEqual expectedArg
        }
      }

      "not disclose create to non-chosen branching signatory" in allFixtures { c =>
        val branchingSignatoriesArg =
          getBranchingSignatoriesArg(false, party1, party2)
        c.submitCreateWithListenerAndAssertNotVisible(
          "BranchingSignatoriesFalse",
          templateIds.branchingSignatories,
          branchingSignatoriesArg,
          party2,
          party1)
      }

      "disclose create to chosen branching controller" in allFixtures { c =>
        val templateId = templateIds.branchingControllers
        val branchingControllersArgs = getBranchingControllerArgs(party1, party2, party3, true)
        val expectedArg = branchingControllersArgs.map(_.copy(label = ""))
        for {
          branchingControllers <- c.submitCreateWithListenerAndReturnEvent(
            "BranchingControllersTrue",
            templateId,
            branchingControllersArgs,
            party1,
            party2)
        } yield {
          branchingControllers.getCreateArguments.fields shouldEqual expectedArg
        }
      }

      "not disclose create to non-chosen branching controller" in allFixtures { c =>
        val templateId = templateIds.branchingControllers
        val branchingControllersArgs =
          getBranchingControllerArgs(party1, party2, party3, false)
        c.submitCreateWithListenerAndAssertNotVisible(
          "BranchingControllersFalse",
          templateId,
          branchingControllersArgs,
          party1,
          party2)
      }

      "disclose create to observers" in allFixtures { c =>
        val giver = party1
        val observers = List(party2, party3)
        val withObserversArg =
          Vector(
            RecordField("giver", giver.asParty),
            RecordField("observers", observers.map(_.asParty).asList))

        val expectedArg = withObserversArg.map(_.copy(label = ""))
        Future
          .sequence(observers.map(observer =>
            for {
              withObservers <- c.submitCreateWithListenerAndReturnEvent(
                "Obs1create:" + observer,
                templateIds.withObservers,
                withObserversArg,
                giver,
                observer)
            } yield {
              withObservers.getCreateArguments.fields shouldEqual expectedArg
          }))
          .map(_ => succeed)
      }

      "DAML engine returns Unit as argument to Nothing" in allFixtures { c =>
        val createArguments =
          Vector(
            RecordField("operator", "party".asParty),
            RecordField("arg1", Value(Value.Sum.Optional(Optional(None))))
          )

        val expectedArgs = createArguments.map(_.copy(label = ""))

        c.submitCreate(
            "Creating contract with a Nothing argument",
            templateIds.nothingArgument,
            createArguments,
            "party")
          .map(_.getCreateArguments.fields shouldEqual expectedArgs)
      }

      "expose the agreement text in CreatedEvents for templates with an explicit agreement text" in allFixtures {
        c =>
          createAgreement(c, "AgreementTextTest", party1, party2).map(
            _.agreementText shouldBe Some(
              s"'$party2' promise to pay the '$party1' on demand the sum of five pounds.")
          )
      }

      "expose the default agreement text in CreatedEvents for templates with no explicit agreement text" in allFixtures {
        c =>
          val resultF = c.submitCreate(
            "Creating dummy contract for default agreement text test",
            templateIds.dummy,
            List(RecordField("operator", party1.asParty)),
            party1)

          resultF.map(_.agreementText shouldBe Some(""))
      }

      "accept exercising a well-authorized multi-actor choice" in allFixtures { c =>
        val List(operator, receiver, giver) = List(party1, party2, party3)
        val triProposalArg = mkTriProposalArg(operator, receiver, giver)

        val expectedArg = triProposalArg.map(_.copy(label = ""))

        for {
          agreement <- createAgreement(c, "MA1", receiver, giver)
          triProposal <- c.submitCreate(
            "MA1proposal",
            templateIds.triProposal,
            triProposalArg,
            operator)
          tx <- c.submitExercise(
            "MA1acceptance",
            templateIds.agreement,
            List("cid" -> Value(ContractId(triProposal.contractId))).asRecordValue,
            "AcceptTriProposal",
            agreement.contractId,
            giver
          )

        } yield {
          val triAgreement = getHead(c.testingHelpers.createdEventsIn(tx))
          triAgreement.getCreateArguments.fields shouldBe expectedArg
        }
      }

      "accept exercising a well-authorized multi-actor choice with coinciding controllers" in allFixtures {
        c =>
          val List(operator, receiver, giver) = List(party1, party2, party3)
          val triProposalArg = mkTriProposalArg(operator, giver, giver)
          val expectedArg = triProposalArg.map(_.copy(label = ""))
          for {
            triProposal <- c.submitCreate(
              "MA2proposal",
              templateIds.triProposal,
              triProposalArg,
              operator)
            tx <- c.submitExercise(
              "MA2acceptance",
              templateIds.triProposal,
              unitArg,
              "TriProposalAccept",
              triProposal.contractId,
              giver)
          } yield {
            val triAgreement = getHead(c.testingHelpers.createdEventsIn(tx))
            triAgreement.getCreateArguments.fields shouldBe expectedArg
          }
      }

      "reject exercising a multi-actor choice with missing authorizers" in allFixtures { c =>
        val List(operator, receiver, giver) = List(party1, party2, party3)
        val triProposalArg = mkTriProposalArg(operator, receiver, giver)
        for {
          triProposal <- c.submitCreate(
            "MA3proposal",
            templateIds.triProposal,
            triProposalArg,
            operator)
          assertion <- failingExercise(
            c,
            "MA3acceptance",
            giver,
            templateIds.triProposal,
            triProposal.contractId,
            "TriProposalAccept",
            unitArg,
            Code.INVALID_ARGUMENT,
            // the error message is different with the unified interpreter, so the text is matched to error
            "error"
          )
        } yield {
          assertion
        }
      }

      // NOTE(MH): This is the current, most conservative semantics of
      // multi-actor choice authorization. It is likely that this will change
      // in the future. Should we delete this test, we should also remove the
      // 'UnrestrictedAcceptTriProposal' choice from the 'Agreement' template.
      "reject exercising a multi-actor choice with too many authorizers" in allFixtures { c =>
        val List(operator, receiver, giver) = List(party1, party2, party3)
        val triProposalArg = mkTriProposalArg(operator, giver, giver)
        for {
          agreement <- createAgreement(c, "MA4", receiver, giver)
          triProposal <- c.submitCreate(
            "MA4proposal",
            templateIds.triProposal,
            triProposalArg,
            operator)
          assertion <- failingExercise(
            c,
            "MA4acceptance",
            giver,
            templateIds.agreement,
            agreement.contractId,
            "AcceptTriProposal",
            List("cid" -> Value(ContractId(triProposal.contractId))).asRecordValue,
            Code.INVALID_ARGUMENT,
            "error"
            // the error message is different with the unified interpreter, so the text is matched to error
          )
        } yield {
          assertion
        }
      }

      "not reorder fields in data structures of choices" in allFixtures { c =>
        val arguments = List("street", "city", "state", "zip")
        for {
          dummy <- c.submitCreate(
            "Create dummy for creating AddressWrapper",
            templateIds.dummy,
            List("operator" -> party.asParty).asRecordFields,
            party)
          exercise <- c.submitExercise(
            "Creating AddressWrapper",
            templateIds.dummy,
            List("address" -> arguments.map(e => e -> e.asText).asRecordValue).asRecordValue,
            "WrapWithAddress",
            dummy.contractId,
            party
          )
        } yield {
          val events = c.testingHelpers.createdEventsIn(exercise)
          events should have length 1
          val createRecordFields = events.headOption.value.createArguments.value.fields
          createRecordFields should have length 2
          val addressRecordFields = createRecordFields
            .map(_.value.value)
            .collectFirst {
              case Value(Value.Sum.Record(Record(_, fields))) => fields
            }
            .value
          addressRecordFields.map(_.value.value) should contain theSameElementsInOrderAs arguments
            .map(_.asText)
        }
      }

      "serve the proper content for each party, regardless of single/multi party subscription" in allFixtures {
        c =>
          val configuredParties = config.parties.list.toList
          for {
            mpResults <- c.transactionClient
              .getTransactions(
                ledgerBegin,
                Some(ledgerEnd),
                TransactionFilter(configuredParties.map(_ -> Filters.defaultInstance).toMap))
              .runWith(Sink.seq)
            spResults <- Future.sequence(configuredParties.map { party =>
              c.transactionClient
                .getTransactions(
                  ledgerBegin,
                  Some(ledgerEnd),
                  TransactionFilter(
                    Map(party -> Filters.defaultInstance)
                  ))
                .runWith(Sink.seq)
            })
          } yield {
            val brokenUpMultiPartyEvents = mpResults.flatMap(tx =>
              tx.events.flatMap { event =>
                withClue("All disclosed events should have a non-empty set of witnesses")(
                  event.witnesses should not be empty)
                event.witnesses.map(w => event.withWitnesses(List(w)))
            })
            val singlePartyEvents = spResults.flatten.flatMap(_.events)

            brokenUpMultiPartyEvents should contain theSameElementsAs singlePartyEvents
          }
      }

    }

    "ledger Ids don't match" should {

      "fail with the expected status" in allFixtures { context =>
        newClient(context.transactionService, "notLedgerId")
          .getTransactions(ledgerBegin, Some(ledgerEnd), getAllContracts)
          .runWith(Sink.head)
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

    }

    "querying ledger end" should {

      "return the value if ledger Ids match" in allFixtures { context =>
        context.transactionClient.getLedgerEnd.map(_ => succeed)
      }

      "return NOT_FOUND if ledger Ids don't match" in allFixtures { context =>
        newClient(context.transactionService, "not" + config.assertStaticLedgerId).getLedgerEnd.failed
          .map(IsStatusException(Status.NOT_FOUND))

      }
    }

    "asking for historical transaction trees by id" should {

      "return the transaction tree if it exists, and the party can see it" in allFixtures {
        context =>
          val beginOffset =
            LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
          for {
            _ <- insertCommands(
              getTrackerFlow(context),
              "tree-provenance-by-id",
              1,
              config.assertStaticLedgerId)
            firstTransaction <- context.transactionClient
              .getTransactions(beginOffset, None, transactionFilter)
              .runWith(Sink.head)
            transactionId = firstTransaction.transactionId
            response <- context.transactionClient
              .getTransactionById(transactionId, List("party"))
            notVisibleError <- context.transactionClient
              .getTransactionById(transactionId, List("Alice"))
              .failed
          } yield {
            response.transaction should not be empty
            inside(notVisibleError) {
              case sre: StatusRuntimeException =>
                sre.getStatus.getCode shouldEqual Status.INVALID_ARGUMENT.getCode
                sre.getStatus.getDescription shouldEqual "Transaction not found, or not visible."
            }
          }
      }

      "return INVALID_ARGUMENT if it does not exist" in allFixtures { context =>
        context.transactionClient
          .getTransactionById(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            List("party"))
          .failed
          .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        newClient(context.transactionService, "not" + config.assertStaticLedgerId)
          .getTransactionById(transactionId, List("party"))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with INVALID_ARGUMENT status if the requesting parties field is empty" in allFixtures {
        context =>
          context.transactionClient
            .getTransactionById(transactionId, Nil)
            .failed
            .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return the same events for each tx as the transaction stream itself" in allFixtures {
        context =>
          val requestingParties = transactionFilter.filtersByParty.keySet
          context.transactionClient
            .getTransactions(ledgerBegin, Some(ledgerEnd), transactionFilter, true)
            .mapAsyncUnordered(16) { tx =>
              context.transactionClient
                .getTransactionById(tx.transactionId, requestingParties.toList)
                .map(tx -> _.getTransaction)
            }
            .runFold(succeed) { (acc, pair) =>
              inside(pair) {
                case (tx, tree) =>
                  tx.transactionId shouldEqual tree.transactionId
                  tx.traceContext shouldEqual tree.traceContext
                  tx.commandId shouldEqual tree.commandId
                  tx.effectiveAt shouldEqual tree.effectiveAt
                  tx.workflowId shouldEqual tree.workflowId
                  // tx.offset shouldEqual tree.offset We don't return the offset.
                  // TODO we can't get proper Archived Event Ids while the old daml core interpreter is in place. ADD JIRA
                  val flatEvents = tx.events.map {
                    case Event(Archived(v)) => Archived(v.copy(eventId = ""))
                    case other => other.event
                  }
                  val treeEvents =
                    tree.rootEventIds.flatMap(e => getEventsFromTree(e, tree.eventsById, Nil))

                  withClue("Non-requesting party present among witnesses") {
                    treeEvents.foreach { event =>
                      event.witnesses.foreach(party => requestingParties should contain(party))
                    }
                  }

                  treeEvents.filter(_.isCreated) should contain theSameElementsAs flatEvents.filter(
                    _.isCreated)
                  // there are some transient archives present in the events generated from the tree
                  treeEvents.filter(_.isArchived) should contain allElementsOf flatEvents.filter(
                    _.isArchived)
                  succeed
              }
            }
      }
    }

    "asking for historical flat transactions by id" should {

      "return the flat transaction if it exists, and the party can see it" in allFixtures {
        context =>
          val beginOffset =
            LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
          for {
            _ <- insertCommands(
              getTrackerFlow(context),
              "flat-provenance-by-id",
              1,
              config.assertStaticLedgerId)
            firstTransaction <- context.transactionClient
              .getTransactions(beginOffset, None, transactionFilter)
              .runWith(Sink.head)
            transactionId = firstTransaction.transactionId
            response <- context.transactionClient
              .getFlatTransactionById(transactionId, List("party"))
            notVisibleError <- context.transactionClient
              .getFlatTransactionById(transactionId, List("Alice"))
              .failed
          } yield {
            response.transaction should not be empty
            inside(notVisibleError) {
              case sre: StatusRuntimeException =>
                sre.getStatus.getCode shouldEqual Status.INVALID_ARGUMENT.getCode
                sre.getStatus.getDescription shouldEqual "Transaction not found, or not visible."
            }
          }
      }

      "return INVALID_ARGUMENT if it does not exist" in allFixtures { context =>
        context.transactionClient
          .getFlatTransactionById(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            List("party"))
          .failed
          .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        newClient(context.transactionService, "not" + config.assertStaticLedgerId)
          .getFlatTransactionById(transactionId, List("party"))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with INVALID_ARGUMENT status if the requesting parties field is empty" in allFixtures {
        context =>
          context.transactionClient
            .getFlatTransactionById(transactionId, Nil)
            .failed
            .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return the same events for each tx as the transaction stream itself" in allFixtures {
        context =>
          val requestingParties = transactionFilter.filtersByParty.keySet
          context.transactionClient
            .getTransactions(ledgerBegin, Some(ledgerEnd), transactionFilter, true)
            .mapAsyncUnordered(16) { tx =>
              context.transactionClient
                .getFlatTransactionById(tx.transactionId, requestingParties.toList)
                .map(tx -> _.getTransaction)
            }
            .runFold(succeed) {
              case (acc, (original, byId)) =>
                byId shouldBe original
            }
      }
    }

    "asking for historical transaction trees by event id" should {
      "return the transaction tree if it exists" in allFixtures { context =>
        val beginOffset =
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
        for {
          _ <- insertCommands(
            getTrackerFlow(context),
            "tree provenance-by-event-id",
            1,
            config.assertStaticLedgerId)
          tx <- context.transactionClient
            .getTransactions(beginOffset, None, transactionFilter)
            .runWith(Sink.head)
          eventId = tx.events.headOption
            .map(_.event match {
              case Archived(v) => v.eventId
              case Created(v) => v.eventId
              case Event.Event.Empty => fail(s"Received empty event in $tx")
            })
            .value
          result <- context.transactionClient
            .getTransactionByEventId(eventId, Seq(party))

          notVisibleError <- context.transactionClient
            .getTransactionByEventId(eventId, List("Alice"))
            .failed
        } yield {
          result.transaction should not be empty

          inside(notVisibleError) {
            case sre: StatusRuntimeException =>
              sre.getStatus.getCode shouldEqual Status.INVALID_ARGUMENT.getCode
              sre.getStatus.getDescription shouldEqual "Transaction not found, or not visible."
          }
        }
      }

      "return INVALID_ARGUMENT for invalid event IDs" in allFixtures { context =>
        context.transactionClient
          .getTransactionByEventId("don't worry, be happy", List("party"))
          .failed
          .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return INVALID_ARGUMENT if it does not exist" in allFixtures { context =>
        context.transactionClient
          .getTransactionByEventId(
            "#aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:000",
            List("party"))
          .failed
          .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        newClient(context.transactionService, "not" + config.assertStaticLedgerId)
          .getTransactionByEventId("#42:0", List("party"))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with INVALID_ARGUMENT status if the requesting parties field is empty" in allFixtures {
        context =>
          context.transactionClient
            .getTransactionByEventId(transactionId, Nil)
            .failed
            .map(IsStatusException(Status.INVALID_ARGUMENT))
      }
    }

    "asking for historical flat transactions by event id" should {
      "return the flat transaction if it exists" in allFixtures { context =>
        val beginOffset =
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
        for {
          _ <- insertCommands(
            getTrackerFlow(context),
            "flat-provenance-by-event-id",
            1,
            config.assertStaticLedgerId)
          tx <- context.transactionClient
            .getTransactions(beginOffset, None, transactionFilter)
            .runWith(Sink.head)
          eventId = tx.events.headOption
            .map(_.event match {
              case Archived(v) => v.eventId
              case Created(v) => v.eventId
              case Event.Event.Empty => fail(s"Received empty event in $tx")
            })
            .value
          result <- context.transactionClient
            .getFlatTransactionByEventId(eventId, Seq(party))

          notVisibleError <- context.transactionClient
            .getFlatTransactionByEventId(eventId, List("Alice"))
            .failed
        } yield {
          result.transaction should not be empty

          inside(notVisibleError) {
            case sre: StatusRuntimeException =>
              sre.getStatus.getCode shouldEqual Status.INVALID_ARGUMENT.getCode
              sre.getStatus.getDescription shouldEqual "Transaction not found, or not visible."
          }
        }
      }

      "return INVALID_ARGUMENT for invalid event IDs" in allFixtures { context =>
        context.transactionClient
          .getFlatTransactionByEventId("don't worry, be happy", List("party"))
          .failed
          .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return INVALID_ARGUMENT if it does not exist" in allFixtures { context =>
        context.transactionClient
          .getFlatTransactionByEventId(
            "#aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:000",
            List("party"))
          .failed
          .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        newClient(context.transactionService, "not" + config.assertStaticLedgerId)
          .getFlatTransactionByEventId("#42:0", List("party"))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with INVALID_ARGUMENT status if the requesting parties field is empty" in allFixtures {
        context =>
          context.transactionClient
            .getFlatTransactionByEventId(transactionId, Nil)
            .failed
            .map(IsStatusException(Status.INVALID_ARGUMENT))
      }
    }

    "reading transactions events " should {

      def validateStream(getEvents: () => Future[Seq[Event.Event]]) =
        for {
          events <- getEvents()
        } yield {
          val (creates, archives) = events.zipWithIndex.partition(_._1.isCreated)

          val createIndices = creates.map { case (e, i) => e.contractId -> i }.toMap
          val archiveIndices = archives.map { case (e, i) => e.contractId -> i }.toMap

          createIndices.size shouldEqual creates.size
          archiveIndices.size shouldEqual archives.size

          archiveIndices.map {
            case (cId, archiveIndex) =>
              val createIndex = createIndices(cId)
              createIndex should be < archiveIndex
          }
        }

      val configuredParties = config.parties.list.toList

      "not arrive out of order when using single party subscription " in allFixtures { c =>
        Future
          .sequence(
            configuredParties.map(
              p =>
                validateStream(
                  () =>
                    c.transactionClient
                      .getTransactions(
                        ledgerBegin,
                        Some(ledgerEnd),
                        TransactionFilter(
                          Map(p -> Filters.defaultInstance)
                        ))
                      .runWith(Sink.seq)
                      .map(_.flatMap(_.events.map(_.event)))
              )))
          .map(_ => succeed)
      }

      "not arrive out of order when using a multi party subscription " in allFixtures { c =>
        validateStream(
          () =>
            c.transactionClient
              .getTransactions(
                ledgerBegin,
                Some(ledgerEnd),
                TransactionFilter(configuredParties.map(_ -> Filters.defaultInstance).toMap))
              .runWith(Sink.seq)
              .map(_.flatMap(_.events.map(_.event)))
        ).map(_ => succeed)
      }
    }

    "reading transaction trees with LedgerBegin as end offset" should {

      "serve an empty stream of transactions" in allFixtures { context =>
        context.transactionClient
          .getTransactionTrees(ledgerBegin, Some(ledgerBegin), transactionFilter)
          .runWith(Sink.seq)
          .map(_ shouldBe empty)
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        new TransactionClient("notLedgerId", context.transactionService)
          .getTransactionTrees(ledgerBegin, Some(ledgerEnd), transactionFilter)
          .runWith(Sink.head)
          .failed
          .map(IsStatusException(Status.NOT_FOUND))

      }

      "reading transaction trees without an end offset" should {

        "serve elements until canceled from downstream" in allFixtures { context =>
          val elemsToTake = 10L
          val commandsToSend = 14

          val resultsF = context.transactionClient
            .getTransactionTrees(ledgerBegin, None, transactionFilter)
            .take(elemsToTake)
            .runWith(Sink.seq)

          for {
            _ <- insertCommands(
              getTrackerFlow(context),
              "cancellation-test-tree",
              commandsToSend,
              config.assertStaticLedgerId)
            elems <- resultsF
          } yield (elems should have length elemsToTake)
        }

        "serve the proper content for each party, regardless of single/multi party subscription" in allFixtures {
          c =>
            for {
              mpResults <- c.transactionClient
                .getTransactionTrees(ledgerBegin, Some(ledgerEnd), filterForAllParties)
                .runWith(Sink.seq)
              spResults <- Future.sequence(configuredParties.map { party =>
                c.transactionClient
                  .getTransactionTrees(
                    ledgerBegin,
                    Some(ledgerEnd),
                    TransactionFilter(
                      Map(party -> Filters.defaultInstance)
                    ))
                  .runWith(Sink.seq)
                  .map(party -> _)(DirectExecutionContext)
              })
            } yield {
              val mpTransactionById: Map[String, TransactionTree] =
                mpResults.map(t => t.transactionId -> t)(breakOut)

              val spTreesByParty = spResults.toMap
              for {
                (inspectedParty, transactions) <- spTreesByParty
                transaction <- transactions
              } {
                transaction.rootEventIds.foreach(
                  evId =>
                    transaction
                      .eventsById(evId)
                      .kind
                      .fold(_.witnessParties, _.witnessParties) should not be empty)
                val multiPartyTx = mpTransactionById(transaction.transactionId)
                val filteredMultiPartyTx = removeOtherWitnesses(multiPartyTx, inspectedParty)
                val withoutInvisibleRoots =
                  removeInvisibleRoots(filteredMultiPartyTx, inspectedParty)
                transaction.rootEventIds shouldEqual withoutInvisibleRoots.rootEventIds
                transaction.eventsById shouldEqual withoutInvisibleRoots.eventsById
              }

              succeed
            }
        }
      }

      "reading transaction trees with LedgerEnd as end offset" should {

        "complete the stream by itself as soon as LedgerEnd is hit" in allFixtures { context =>
          val noOfCommands = 10

          for {
            r1 <- context.transactionClient
              .getTransactionTrees(ledgerBegin, Some(ledgerEnd), transactionFilter)
              .runWith(Sink.seq)
            _ <- insertCommands(
              getTrackerFlow(context),
              "complete_test",
              noOfCommands,
              config.assertStaticLedgerId)
            r2 <- context.transactionClient
              .getTransactionTrees(ledgerBegin, Some(ledgerEnd), transactionFilter)
              .runWith(Sink.seq)
          } yield {
            r2.size - r1.size shouldEqual (noOfCommands.toLong)
          }
        }
      }

      "reading flat transactions and trees" should {
        "serve a subset of the tree data in the flat stream" in allFixtures { context =>
          val treesF = context.transactionClient
            .getTransactionTrees(
              ledgerBegin,
              Some(ledgerEnd),
              TransactionFilter(Map("Bob" -> Filters())))
            .runWith(Sink.seq)
          val txsF = context.transactionClient
            .getTransactions(
              ledgerBegin,
              Some(ledgerEnd),
              TransactionFilter(Map("Bob" -> Filters())))
            .runWith(Sink.seq)
          for {
            txs <- txsF
            trees <- treesF
            _ = txs.map(_.transactionId) shouldEqual trees.map(_.transactionId)
          } yield {

            for {
              (tx, tree) <- txs.iterator.zip(trees.iterator)

              treeEventIds: Set[EventId] = Tag.subst(tree.eventsById.keySet)

              txEvent <- tx.events
            } {
              withClue(
                "There should be no event that the flat API serves, but the tree API does not.") {
                treeEventIds should contain(txEvent.eventId)
              }
              withClue(
                "Witnesses on the flat API must be a subset of the witnesses on the tree API") {
                val treeEventWitnesses =
                  tree.eventsById(txEvent.eventId.unwrap).witnessParties.toSet
                txEvent.witnesses.foreach(w => treeEventWitnesses should contain(w))
              }

            }
            succeed
          }
        }
      }

      "reading transactions" should {

        "serve a stream of transactions" in allFixtures { context =>
          val treesF = context.transactionClient
            .getTransactionTrees(
              ledgerBegin,
              Some(ledgerEnd),
              TransactionFilter(Map("Bob" -> Filters())))
            .map(_.eventsById.values)
            .mapConcat(context.testingHelpers.exercisedEventsInNodes(_).toList)
            .map(_.exerciseResult)
            .runWith(Sink.seq)

          treesF.map { results =>
            all(results) should not be empty
          }
        }
      }

    }

  }

  private def getEventsFromTree(
      eventId: String,
      events: Map[String, TreeEvent],
      inheritedWitnesses: Seq[String] = Nil): Seq[Event.Event] = {
    val event = events(eventId).kind
    event match {
      case TreeEvent.Kind.Empty => fail("Unexpected empty event")
      case TreeEvent.Kind.Created(c) => List(Event.Event.Created(c))
      case TreeEvent.Kind.Exercised(e) =>
        val allWitnesses = e.witnessParties ++ inheritedWitnesses
        val childEvents = e.childEventIds.flatMap { e =>
          getEventsFromTree(e, events, allWitnesses)
        }
        childEvents ++ (if (e.consuming)
                          // TODO we can't get proper Archived Event Ids while the old daml core interpreter is in place. ADD JIRA
                          List(
                            Archived(
                              ArchivedEvent("", e.contractId, e.templateId, allWitnesses.distinct)))
                        else Nil)
    }
  }

  private def getTrackerFlow(context: LedgerContext) = {
    Flow.lazyInitAsync(() =>
      context.commandClient().flatMap(_.trackCommands[Int](List(MockMessages.party))))
  }

  private def getBranchingSignatoriesArg(
      whichSign: Boolean,
      signTrue: String,
      signFalse: String) = {
    Vector(
      RecordField("whichSign", Value(Bool(whichSign))),
      RecordField("signTrue", signTrue.asParty),
      RecordField("signFalse", signFalse.asParty)
    )
  }

  private def getBranchingControllerArgs(
      giver: String,
      ctrlTrue: String,
      ctrlFalse: String,
      whichCtrl: Boolean) = {
    Vector(
      RecordField("giver", giver.asParty),
      RecordField("whichCtrl", Value(Bool(whichCtrl))),
      RecordField("ctrlTrue", ctrlTrue.asParty),
      RecordField("ctrlFalse", ctrlFalse.asParty)
    )
  }

  private def exerciseCallChoice(exercisedTemplate: Identifier, factoryContractId: String) = {
    ExerciseCommand(Some(exercisedTemplate), factoryContractId, "DummyFactoryCall", Some(unitArg))
  }

  private def insertCommands(
      prefix: String,
      commandsPerSection: Int,
      context: LedgerContext): Future[Done] = {
    insertCommands(getTrackerFlow(context), prefix, commandsPerSection, config.assertStaticLedgerId)
  }

  private def lastOffsetIn(secondSection: immutable.Seq[Transaction]): Option[LedgerOffset] = {
    secondSection.lastOption.map(tx => LedgerOffset(LedgerOffset.Value.Absolute(tx.offset)))
  }

  private def create(templateId: Identifier, arguments: immutable.Seq[(String, Value)]): Create = {
    Create(CreateCommand(Some(templateId), Some(arguments.asRecordOf(templateId))))
  }

  def getHead[T](elements: Iterable[T]): T = {
    elements should have size 1
    elements.headOption.value
  }

  private def findCreatedEventIn(
      contractCreationTx: Transaction,
      templateToLookFor: Identifier): CreatedEvent = {
    // for helpful scalatest error message
    createdEventsIn(contractCreationTx).flatMap(_.templateId.toList) should contain(
      templateToLookFor)
    createdEventsIn(contractCreationTx).find(_.templateId.contains(templateToLookFor)).value
  }

  private def createdEventsIn(transaction: Transaction): Seq[CreatedEvent] =
    transaction.events
      .map(_.event)
      .collect {
        case Created(createdEvent) => createdEvent
      }

  private def archivedEventsIn(transaction: Transaction): Seq[ArchivedEvent] =
    transaction.events.map(_.event).collect {
      case Archived(archivedEvent) => archivedEvent
    }

  private def mkTriProposalArg(
      operator: String,
      receiver: String,
      giver: String
  ): Vector[RecordField] = {

    Vector(
      RecordField("operator", operator.asParty),
      RecordField("receiver", receiver.asParty),
      RecordField("giver", giver.asParty))
  }

  private def removeOtherWitnesses(t: TransactionTree, party: String): TransactionTree = {
    t.copy(eventsById = t.eventsById.map {
      case (eventId, event) =>
        eventId -> TreeEvent(
          event.kind.fold[TreeEvent.Kind](
            ev =>
              TreeEvent.Kind.Exercised(ev.withWitnessParties(ev.witnessParties.filter(_ == party))),
            ev =>
              TreeEvent.Kind.Created(ev.withWitnessParties(ev.witnessParties.filter(_ == party)))
          ))
    })
  }

  private def removeInvisibleRoots(t: TransactionTree, party: String): TransactionTree = {
    val rootsWithVisibility = t.rootEventIds.map { eventId =>
      val event = t.eventsById(eventId).kind
      val witnessParties = event.fold(_.witnessParties, _.witnessParties)
      val visible = witnessParties.contains(party)
      eventId -> visible
    }
    if (rootsWithVisibility.exists(_._2 == false)) {
      val newRoots = rootsWithVisibility.flatMap {
        case (eventId, visible) =>
          if (visible) List(eventId) else Tag.unsubst(t.eventsById(eventId).children)
      }
      val eventsWithoutInvisibleRoots = rootsWithVisibility.foldLeft(t.eventsById) {
        case (acc, (eventId, visible)) => if (visible) acc else acc - eventId
      }
      removeInvisibleRoots(
        t.copy(rootEventIds = newRoots, eventsById = eventsWithoutInvisibleRoots),
        party)
    } else t
  }

  private def createAgreement(
      c: LedgerContext,
      commandId: String,
      receiver: String,
      giver: String
  ): Future[CreatedEvent] = {
    for {
      agreementFactory <- c.submitCreate(
        commandId + "factory creation",
        templateIds.agreementFactory,
        List(
          "receiver" -> receiver.asParty,
          "giver" -> giver.asParty
        ).asRecordFields,
        giver)
      tx <- c.submitExercise(
        commandId + " acceptance",
        templateIds.agreementFactory,
        unitArg,
        "AgreementFactoryAccept",
        agreementFactory.contractId,
        receiver)
    } yield {
      getHead(c.testingHelpers.createdEventsIn(tx))
    }
  }

  private def failingExercise(
      c: LedgerContext,
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value,
      code: Code,
      pattern: String
  ): Future[Assertion] =
    c.testingHelpers.assertCommandFailsWithCode(
      c.command(
          commandId,
          List(ExerciseCommand(Some(template), contractId, choice, Some(arg)).wrap))
        .update(
          _.commands.party := submitter
        ),
      code,
      pattern
    )

  private def verifyParamShowcaseChoice(
      context: LedgerContext,
      choice: String,
      lbl: String,
      exerciseArg: Value,
      expectedCreateArgs: Seq[RecordField]): Future[Assertion] = {

    for {
      creation <- context.submitCreate(
        s"Creating contract with a multitude of param types for exercising ($choice, $lbl)",
        templateIds.parameterShowcase,
        paramShowcaseArgs(templateIds.testPackageId),
        MockMessages.party
      )
      contractId = creation.contractId
      // first, verify that if we submit with the same inputs they're equal
      exerciseCommand = ExerciseCommand(
        Some(templateIds.parameterShowcase),
        contractId,
        choice,
        exerciseArg).wrap
      exerciseTx <- context.testingHelpers.submitAndListenForSingleResultOfCommand(
        context.command(
          s"Exercising with a multitiude of params ($choice, $lbl)",
          List(exerciseCommand)),
        getAllContracts
      )
    } yield {
      // check that we have the create
      val create = getHead(createdEventsIn(exerciseTx))
      create.getCreateArguments.fields shouldEqual expectedCreateArgs
    }
  }
}
