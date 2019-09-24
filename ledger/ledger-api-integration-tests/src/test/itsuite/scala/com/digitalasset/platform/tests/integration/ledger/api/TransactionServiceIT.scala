// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import akka.Done
import akka.stream.scaladsl.Sink
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.domain.{EventId, LedgerId}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.api.v1.event.EventOps._
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.LedgerOffsets._
import com.digitalasset.platform.apitesting.TestParties._
import com.digitalasset.platform.apitesting._
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import com.digitalasset.platform.services.time.TimeProviderType
import io.grpc.Status
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
    with OptionValues
    with Matchers {

  override protected def config: Config =
    Config.default.withTimeProvider(TimeProviderType.WallClock)

  protected val helpers = new TransactionServiceHelpers(config)
  protected val testTemplateIds = new TestTemplateIds(config)
  protected val templateIds = testTemplateIds.templateIds
  protected val testIdsGenerator = new TestIdsGenerator(config)

  override val timeLimit: Span = scaled(300.seconds)

  private val configuredParties = config.parties

  "Transaction Service" when {

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
