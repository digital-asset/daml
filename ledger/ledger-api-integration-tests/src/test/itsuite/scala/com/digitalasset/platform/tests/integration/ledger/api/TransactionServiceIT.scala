// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import akka.Done
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.domain.{EventId, LedgerId}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.commands.ExerciseCommand
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.transaction_service.GetLedgerEndResponse
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, Value}
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.api.v1.event.EventOps._
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.LedgerOffsets._
import com.digitalasset.platform.apitesting.TestParties._
import com.digitalasset.platform.apitesting._
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.platform.services.time.TimeProviderType
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest._
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import scalaz.Tag
import scalaz.syntax.tag._

import scala.collection.breakOut
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
    with ParameterShowcaseTesting
    with OptionValues
    with Matchers {

  override protected def config: Config =
    Config.default.withTimeProvider(TimeProviderType.WallClock)

  protected val helpers = new TransactionServiceHelpers(config)
  protected val testTemplateIds = new TestTemplateIds(config)
  protected val templateIds = testTemplateIds.templateIds
  protected val testIdsGenerator = new TestIdsGenerator(config)

  override val timeLimit: Span = scaled(300.seconds)

  private def newClient(stub: TransactionService, ledgerId: LedgerId): TransactionClient =
    new TransactionClient(ledgerId, stub)

  private val configuredParties = config.parties

  private val unitArg = Value(Sum.Record(Record.defaultInstance))

  "Transaction Service" when {

    "submitting and reading transactions" should {

      "expose contract Ids that are results of exercising choices when filtering by template" in allFixtures {
        context =>
          val factoryCreation = testIdsGenerator.testCommandId("Creating_second_factory")
          val exercisingChoice =
            testIdsGenerator.testCommandId("Exercising_choice_on_second_factory")
          val exercisedTemplate = templateIds.dummyFactory
          for {
            creation <- context.submitCreate(
              factoryCreation,
              exercisedTemplate,
              List("operator" -> Alice.asParty).asRecordFields,
              Alice)
            factoryContractId = creation.contractId

            offsetToListenFrom <- context.testingHelpers.submitSuccessfullyAndReturnOffset(
              context
                .command(
                  exercisingChoice,
                  Alice,
                  List(exerciseCallChoice(exercisedTemplate, factoryContractId).wrap))
            )

            txsWithCreate <- context.testingHelpers.listenForResultOfCommand(
              TransactionFilters.templatesByParty(Alice -> List(templateIds.dummyWithParam)),
              Some(exercisingChoice),
              offsetToListenFrom)

            txsWithArchive <- context.testingHelpers.listenForResultOfCommand(
              TransactionFilters.templatesByParty(Alice -> List(templateIds.dummyFactory)),
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

      "serve the proper content for each party, regardless of single/multi party subscription" in allFixtures {
        c =>
          for {
            mpResults <- c.transactionClient
              .getTransactions(
                LedgerBegin,
                Some(LedgerEnd),
                TransactionFilter(configuredParties.map(_ -> Filters.defaultInstance).toMap))
              .runWith(Sink.seq)
            spResults <- Future.sequence(configuredParties.map { party =>
              c.transactionClient
                .getTransactions(
                  LedgerBegin,
                  Some(LedgerEnd),
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

      "allow fetching a contract that has been created in the same transaction" in allFixtures {
        context =>
          val createAndFetchTid = templateIds.createAndFetch
          for {
            createdEvent <- context.submitCreate(
              testIdsGenerator.testCommandId("CreateAndFetch_Create"),
              createAndFetchTid,
              List("p" -> Alice.asParty).asRecordFields,
              Alice)
            cid = createdEvent.contractId
            exerciseTx <- context.submitExercise(
              testIdsGenerator.testCommandId("CreateAndFetch_Run"),
              createAndFetchTid,
              Value(Value.Sum.Record(Record())),
              "CreateAndFetch_Run",
              cid,
              Alice
            )
          } yield {
            val events = exerciseTx.events.map(_.event)
            val (created, archived) = events.partition(_.isCreated)
            created should have length 1
            getHead(archived).archived.value.contractId shouldEqual cid
          }

      }

    }

    "ledger Ids don't match" should {

      "fail with the expected status" in allFixtures { context =>
        newClient(context.transactionService, LedgerId("notLedgerId"))
          .getTransactions(LedgerBegin, Some(LedgerEnd), TransactionFilters.allForParties(Alice))
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
        newClient(context.transactionService, LedgerId(s"not-${context.ledgerId.unwrap}")).getLedgerEnd.failed
          .map(IsStatusException(Status.NOT_FOUND))

      }
    }

    "asking for historical transaction trees by id" should {

      "return the transaction tree if it exists, and the party can see it" in allFixtures {
        context =>
          for {
            GetLedgerEndResponse(Some(beginOffset)) <- context.transactionClient.getLedgerEnd
            _ <- insertCommandsUnique("tree-provenance-by-id", 1, context)
            firstTransaction <- context.transactionClient
              .getTransactions(beginOffset, None, TransactionFilters.allForParties(Alice))
              .runWith(Sink.head)
            transactionId = firstTransaction.transactionId
            response <- context.transactionClient
              .getTransactionById(transactionId, List(Alice))
            notVisibleError <- context.transactionClient
              .getTransactionById(transactionId, List(Bob))
              .failed
          } yield {
            response.transaction should not be empty
            inside(notVisibleError) {
              case sre: StatusRuntimeException =>
                sre.getStatus.getCode shouldEqual Status.NOT_FOUND.getCode
                sre.getStatus.getDescription shouldEqual "Transaction not found, or not visible."
            }
          }
      }

      "return NOT_FOUND if it does not exist" in allFixtures { context =>
        context.transactionClient
          .getTransactionById(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            List(Alice))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        newClient(context.transactionService, LedgerId(s"not-${context.ledgerId.unwrap}"))
          .getTransactionById("invalid", List(Alice))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with INVALID_ARGUMENT status if the requesting parties field is empty" in allFixtures {
        context =>
          context.transactionClient
            .getTransactionById("invalid", Nil)
            .failed
            .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return the same events for each tx as the transaction stream itself" in allFixtures {
        context =>
          val requestingParties = TransactionFilters.allForParties(Alice).filtersByParty.keySet

          Source
            .fromFuture(context.transactionClient.getLedgerEnd)
            .map(resp => resp.getOffset)
            .flatMapConcat(
              beginOffset =>
                context.transactionClient
                  .getTransactions(
                    beginOffset,
                    Some(LedgerEnd),
                    TransactionFilters.allForParties(Alice),
                    true))
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
          for {
            GetLedgerEndResponse(Some(beginOffset)) <- context.transactionClient.getLedgerEnd
            _ <- insertCommandsUnique("flat-provenance-by-id", 1, context)
            firstTransaction <- context.transactionClient
              .getTransactions(beginOffset, None, TransactionFilters.allForParties(Alice))
              .runWith(Sink.head)
            transactionId = firstTransaction.transactionId
            response <- context.transactionClient
              .getFlatTransactionById(transactionId, List(Alice))
            notVisibleError <- context.transactionClient
              .getFlatTransactionById(transactionId, List(Bob))
              .failed
          } yield {
            response.transaction should not be empty
            inside(notVisibleError) {
              case sre: StatusRuntimeException =>
                sre.getStatus.getCode shouldEqual Status.NOT_FOUND.getCode
                sre.getStatus.getDescription shouldEqual "Transaction not found, or not visible."
            }
          }
      }

      "return NOT_FOUND if it does not exist" in allFixtures { context =>
        context.transactionClient
          .getFlatTransactionById(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            List(Alice))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        newClient(context.transactionService, LedgerId(s"not-${context.ledgerId.unwrap}"))
          .getFlatTransactionById("invalid", List(Alice))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with INVALID_ARGUMENT status if the requesting parties field is empty" in allFixtures {
        context =>
          context.transactionClient
            .getFlatTransactionById("invalid", Nil)
            .failed
            .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return the same events for each tx as the transaction stream itself" in allFixtures {
        context =>
          val requestingParties = TransactionFilters.allForParties(Alice).filtersByParty.keySet
          Source
            .fromFuture(context.transactionClient.getLedgerEnd)
            .map(resp => resp.getOffset)
            .flatMapConcat(
              beginOffset =>
                context.transactionClient
                  .getTransactions(
                    beginOffset,
                    Some(LedgerEnd),
                    TransactionFilters.allForParties(Alice),
                    true))
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
        for {
          GetLedgerEndResponse(Some(beginOffset)) <- context.transactionClient.getLedgerEnd
          _ <- insertCommandsUnique("tree-provenance-by-event-id", 1, context)
          tx <- context.transactionClient
            .getTransactions(beginOffset, None, TransactionFilters.allForParties(Alice))
            .runWith(Sink.head)
          eventId = tx.events.headOption
            .map(_.event match {
              case Archived(v) => v.eventId
              case Created(v) => v.eventId
              case Event.Event.Empty => fail(s"Received empty event in $tx")
            })
            .value
          result <- context.transactionClient
            .getTransactionByEventId(eventId, List(Alice))

          notVisibleError <- context.transactionClient
            .getTransactionByEventId(eventId, List(Bob))
            .failed
        } yield {
          result.transaction should not be empty

          inside(notVisibleError) {
            case sre: StatusRuntimeException =>
              sre.getStatus.getCode shouldEqual Status.NOT_FOUND.getCode
              sre.getStatus.getDescription shouldEqual "Transaction not found, or not visible."
          }
        }
      }

      "return INVALID_ARGUMENT for invalid event IDs" in allFixtures { context =>
        context.transactionClient
          .getTransactionByEventId("don't worry, be happy", List(Alice))
          .failed
          .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return NOT_FOUND if it does not exist" in allFixtures { context =>
        context.transactionClient
          .getTransactionByEventId(
            "#aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:000",
            List(Alice))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        newClient(context.transactionService, LedgerId(s"not-${context.ledgerId.unwrap}"))
          .getTransactionByEventId("#42:0", List(Alice))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with INVALID_ARGUMENT status if the requesting parties field is empty" in allFixtures {
        context =>
          context.transactionClient
            .getTransactionByEventId("invalid", Nil)
            .failed
            .map(IsStatusException(Status.INVALID_ARGUMENT))
      }
    }

    "asking for historical flat transactions by event id" should {
      "return the flat transaction if it exists" in allFixtures { context =>
        for {
          GetLedgerEndResponse(Some(beginOffset)) <- context.transactionClient.getLedgerEnd
          _ <- insertCommandsUnique("flat-provenance-by-event-id", 1, context)
          tx <- context.transactionClient
            .getTransactions(beginOffset, None, TransactionFilters.allForParties(Alice))
            .runWith(Sink.head)
          eventId = tx.events.headOption
            .map(_.event match {
              case Archived(v) => v.eventId
              case Created(v) => v.eventId
              case Event.Event.Empty => fail(s"Received empty event in $tx")
            })
            .value
          result <- context.transactionClient
            .getFlatTransactionByEventId(eventId, Seq(Alice))

          notVisibleError <- context.transactionClient
            .getFlatTransactionByEventId(eventId, List(Bob))
            .failed
        } yield {
          result.transaction should not be empty

          inside(notVisibleError) {
            case sre: StatusRuntimeException =>
              sre.getStatus.getCode shouldEqual Status.NOT_FOUND.getCode
              sre.getStatus.getDescription shouldEqual "Transaction not found, or not visible."
          }
        }
      }

      "return INVALID_ARGUMENT for invalid event IDs" in allFixtures { context =>
        context.transactionClient
          .getFlatTransactionByEventId("don't worry, be happy", List(Alice))
          .failed
          .map(IsStatusException(Status.INVALID_ARGUMENT))
      }

      "return NOT_FOUND if it does not exist" in allFixtures { context =>
        context.transactionClient
          .getFlatTransactionByEventId(
            "#aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:000",
            List(Alice))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        newClient(context.transactionService, LedgerId(s"not-${context.ledgerId.unwrap}"))
          .getFlatTransactionByEventId("#42:0", List(Alice))
          .failed
          .map(IsStatusException(Status.NOT_FOUND))
      }

      "fail with INVALID_ARGUMENT status if the requesting parties field is empty" in allFixtures {
        context =>
          context.transactionClient
            .getFlatTransactionByEventId("invalid", Nil)
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

      "not arrive out of order when using single party subscription " in allFixtures { c =>
        Future
          .sequence(
            configuredParties.map(
              p =>
                validateStream(
                  () =>
                    c.transactionClient
                      .getTransactions(
                        LedgerBegin,
                        Some(LedgerEnd),
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
                LedgerBegin,
                Some(LedgerEnd),
                TransactionFilter(configuredParties.map(_ -> Filters.defaultInstance).toMap))
              .runWith(Sink.seq)
              .map(_.flatMap(_.events.map(_.event)))
        ).map(_ => succeed)
      }
    }

    "reading transaction trees with LedgerBegin as end offset" should {

      "serve an empty stream of transactions" in allFixtures { context =>
        context.transactionClient
          .getTransactionTrees(
            LedgerBegin,
            Some(LedgerBegin),
            TransactionFilters.allForParties(Alice))
          .runWith(Sink.seq)
          .map(_ shouldBe empty)
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        new TransactionClient(LedgerId("notLedgerId"), context.transactionService)
          .getTransactionTrees(
            LedgerBegin,
            Some(LedgerEnd),
            TransactionFilters.allForParties(Alice))
          .runWith(Sink.head)
          .failed
          .map(IsStatusException(Status.NOT_FOUND))

      }

      "reading transaction trees without an end offset" should {

        "serve elements until canceled from downstream" in allFixtures { context =>
          val elemsToTake = 10L
          val commandsToSend = 14

          val resultsF = context.transactionClient
            .getTransactionTrees(LedgerBegin, None, TransactionFilters.allForParties(Alice))
            .take(elemsToTake)
            .runWith(Sink.seq)

          for {
            _ <- insertCommandsUnique("cancellation-test-tree", commandsToSend, context)
            elems <- resultsF
          } yield (elems should have length elemsToTake)
        }

        "serve the proper content for each party, regardless of single/multi party subscription" in allFixtures {
          c =>
            for {
              mpResults <- c.transactionClient
                .getTransactionTrees(
                  LedgerBegin,
                  Some(LedgerEnd),
                  TransactionFilters.allForParties(configuredParties: _*))
                .runWith(Sink.seq)
              spResults <- Future.sequence(configuredParties.map { party =>
                c.transactionClient
                  .getTransactionTrees(
                    LedgerBegin,
                    Some(LedgerEnd),
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
              .getTransactionTrees(
                LedgerBegin,
                Some(LedgerEnd),
                TransactionFilters.allForParties(Alice))
              .runWith(Sink.seq)
            _ <- insertCommandsUnique("complete_test", noOfCommands, context)
            r2 <- context.transactionClient
              .getTransactionTrees(
                LedgerBegin,
                Some(LedgerEnd),
                TransactionFilters.allForParties(Alice))
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
              LedgerBegin,
              Some(LedgerEnd),
              TransactionFilter(Map("Bob" -> Filters())))
            .runWith(Sink.seq)
          val txsF = context.transactionClient
            .getTransactions(
              LedgerBegin,
              Some(LedgerEnd),
              TransactionFilter(Map("Bob" -> Filters())))
            .runWith(Sink.seq)
          for {
            txs <- txsF
            trees <- treesF
            _ = txs.map(_.transactionId) shouldEqual trees.map(_.transactionId)
          } yield {

            for {
              (tx, tree) <- txs.iterator.zip(trees.iterator)

              treeEventIds: Set[EventId] = Tag.subst(
                tree.eventsById.keySet.map(Ref.LedgerString.assertFromString))

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
              LedgerBegin,
              Some(LedgerEnd),
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

  private def exerciseCallChoice(exercisedTemplate: Identifier, factoryContractId: String) = {
    ExerciseCommand(Some(exercisedTemplate), factoryContractId, "DummyFactoryCall", Some(unitArg))
  }

  private def insertCommands(
      prefix: String,
      commandsPerSection: Int,
      context: LedgerContext): Future[Done] = {
    helpers.insertCommands(
      request =>
        helpers
          .applyTime(request, context)
          .flatMap(context.commandService.submitAndWaitForTransactionId),
      prefix,
      commandsPerSection,
      context.ledgerId)
  }

  private def insertCommandsUnique(
      prefix: String,
      commandsPerSection: Int,
      context: LedgerContext): Future[Done] = {
    insertCommands(testIdsGenerator.testCommandId(prefix), commandsPerSection, context)
  }

  def getHead[T](elements: Iterable[T]): T = {
    elements should have size 1
    elements.headOption.value
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
}
