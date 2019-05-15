// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils._
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Absolute
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.commands.SynchronousCommandClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture, TestCommands}
import com.digitalasset.platform.ledger.acs.data.{AcsFutures, AcsTestUtil}
import io.grpc.Status
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.SpanSugar._
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Assertion, AsyncWordSpec, Matchers, OptionValues}

/**
  * There are not many tests here, because restarting the fixtures is very expensive.
  * This will likely remain the case in the near future.
  * If you need to add to this, consider putting your test case in one of the ACS tests instead.
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ActiveContractsServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with AcsTestUtil
    with AcsFutures
    with SuiteResourceManagementAroundEach
    with ScalaFutures
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues
    with TestCommands {

  override def timeLimit: Span = 60.seconds

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(30000, Millis)), scaled(Span(500, Millis)))

  private def client(
      ctx: LedgerContext,
      ledgerId: String = config.assertStaticLedgerId): ActiveContractSetClient =
    new ActiveContractSetClient(ledgerId, ctx.acsService)

  private def commandClient(
      ctx: LedgerContext,
      ledgerId: String = config.assertStaticLedgerId): SynchronousCommandClient =
    new SynchronousCommandClient(ctx.commandService)

  private def transactionClient(
      ctx: LedgerContext,
      ledgerId: String = config.assertStaticLedgerId): TransactionClient =
    new TransactionClient(ledgerId, ctx.transactionService)

  private def submitRequest(ctx: LedgerContext, request: SubmitAndWaitRequest) =
    commandClient(ctx).submitAndWait(request)

  private def extractEvents(response: GetActiveContractsResponse): Set[CreatedEvent] =
    response.activeContracts.toSet

  private def extractEvents(event: Event) = event.event.created.toList

  private def validateResponses(
      responses: Seq[GetActiveContractsResponse],
      parties: List[String] = List("Alice")): Assertion = {
    responses.dropRight(1) foreach { response =>
      response.activeContracts should not be empty
      response.workflowId shouldEqual MockMessages.workflowId
      response.offset shouldEqual ""
      response.activeContracts.foreach(_.witnessParties should equal(parties))
    }

    responses.last.workflowId shouldEqual ""
    responses.last.activeContracts should contain theSameElementsAs Seq()
    // We only test that the head cursor exists, since we can't predict what it will be.
    // Some tests do more detailed tests with the head cursor, indirectly validating its behavior.
    responses.last.offset should not equal ""
  }

  private def lookForContract(
      events: Seq[CreatedEvent],
      template: Identifier,
      occurrence: Int = 1): Assertion =
    events.collect {
      case ce @ CreatedEvent(_, _, Some(`template`), _, _, _) => ce
    }.size should equal(occurrence)

  def threeCommands(ledgerId: String, commandId: String): SubmitAndWaitRequest =
    super.dummyCommands(ledgerId, commandId, "Alice").toWait

  private def filter = TransactionFilter(Map(config.parties.head -> Filters()))

  "Active Contract Set Service" when {
    "asked for active contracts" should {
      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        client(context, UUID.randomUUID().toString)
          .getActiveContracts(filter)
          .runWith(Sink.head)(materializer)
          .failed map { ex =>
          IsStatusException(Status.NOT_FOUND.getCode)(ex)
        }
      }

      "succeed with empty response" in allFixtures { context =>
        context.acsClient.getActiveContracts(filter).runWith(Sink.seq)(materializer) map {
          // The sandbox sends an empty response, but the ACS app sends a response containing genesis predecessor.
          _.flatMap(_.activeContracts) should be(empty)
        }
      }
    }

    "contracts have been created" should {
      "return them in an ACS snapshot" in allFixtures { ctx =>
        val resultsF = for {
          _ <- submitRequest(ctx, threeCommands(ctx.ledgerId, "created"))
          responses <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Alice" -> Filters()),
            3)
        } yield responses

        resultsF map { responses =>
          validateResponses(responses)

          val events = responses.flatMap(extractEvents)
          events.size shouldEqual 3

          lookForContract(events, templateIds.dummy)
          lookForContract(events, templateIds.dummyWithParam)
          lookForContract(events, templateIds.dummyFactory)
        }
      }
    }

    "contracts have been created and filtered by template" should {
      "return them in an ACS snapshot" in allFixtures { ctx =>
        val resultsF = for {
          _ <- submitRequest(ctx, threeCommands(ctx.ledgerId, "template-test-created"))
          responses <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Alice" -> Filters(Some(InclusiveFilters(List(templateIds.dummy))))),
            1)
        } yield responses

        resultsF map { responses =>
          validateResponses(responses)

          val events = responses.flatMap(extractEvents)
          events.size shouldEqual 1

          lookForContract(events, templateIds.dummy)
          lookForContract(events, templateIds.dummyWithParam, 0)
          lookForContract(events, templateIds.dummyFactory, 0)
        }
      }
    }

    "contracts have been created and one from them has been exercised" should {
      "return them in an ACS snapshot" in allFixtures { ctx =>
        def extractContractId(acsResponse: Seq[GetActiveContractsResponse]) = {
          val events = acsResponse.flatMap(_.activeContracts).toSet
          events.collect {
            case CreatedEvent(contractId, _, Some(tid), _, _, _) if tid == templateIds.dummy =>
              contractId
          }.head
        }

        val resultsF = for {
          _ <- submitRequest(ctx, threeCommands(ctx.ledgerId, "exercise-test-created"))
          responses1 <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Alice" -> Filters(None)),
            3)
          contractId = extractContractId(responses1)
          _ <- submitRequest(
            ctx,
            buildRequest(
              ctx.ledgerId,
              "exercise-test-exercised",
              Seq(exerciseWithUnit(templateIds.dummy, contractId, "DummyChoice1")),
              "Alice").toWait)
          responses2 <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Alice" -> Filters(None)),
            2)
        } yield responses2

        resultsF map { responses =>
          validateResponses(responses)

          val events = responses.flatMap(extractEvents)
          events.size shouldEqual 2

          lookForContract(events, templateIds.dummy, 0)
          lookForContract(events, templateIds.dummyWithParam)
          lookForContract(events, templateIds.dummyFactory)
        }
      }
    }

    "a snapshot given with an offset" should {
      "be a valid offset for submitting new transactions" in allFixtures { ctx =>
        def extractOffset(snapshot: Seq[GetActiveContractsResponse]) =
          snapshot.last.offset

        val resultsF = for {
          _ <- submitRequest(
            ctx,
            buildRequest(
              ctx.ledgerId,
              "commandId1",
              Seq(createWithOperator(templateIds.dummy, "Alice")),
              "Alice").toWait)
          responses1 <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Alice" -> Filters(None)),
            1)
          offset = extractOffset(responses1)
          _ <- submitRequest(
            ctx,
            buildRequest(
              ctx.ledgerId,
              "commandId2",
              Seq(createWithOperator(templateIds.dummyWithParam, "Alice")),
              "Alice").toWait
          )
          responses2 <- transactionClient(ctx)
            .getTransactions(
              LedgerOffset(Absolute(offset)),
              None,
              TransactionFilter(Map("Alice" -> Filters(None))))
            .take(1)
            .runWith(Sink.seq)
        } yield responses2

        resultsF map { responses =>
          responses.head.commandId should equal("commandId2")

          val events = responses.head.events.flatMap(extractEvents)

          lookForContract(events, templateIds.dummy, 0)
          lookForContract(events, templateIds.dummyWithParam)
        }
      }
    }

    "verbosity flag is true" should {
      "disclose field names" in forAllFixtures { ctx =>
        for {
          _ <- submitRequest(ctx.context(), threeCommands(ctx.context().ledgerId, "created"))
          responses <- waitForActiveContracts(
            ctx.context().acsService,
            ctx.context().ledgerId,
            Map("Alice" -> Filters()),
            3,
            verbose = true)
        } yield {
          val fieldLabels =
            responses.flatMap(_.activeContracts.flatMap(_.getCreateArguments.fields.map(_.label)))
          all(fieldLabels) should not be empty
        }
      }
    }

    "multi-party request comes" should {
      "return the correct set of related contracts" in allFixtures { ctx =>
        val resultsF = for {
          _ <- submitRequest(ctx, dummyCommands(ctx.ledgerId, "acsCommand-1", "Alice").toWait)
          _ <- submitRequest(ctx, dummyCommands(ctx.ledgerId, "acsCommand-2", "Bob").toWait)
          allContractsForAlice <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Alice" -> Filters(None)),
            3)
          allContractsForBob <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Bob" -> Filters(None)),
            3)
          allContractsForAliceAndBob <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Alice" -> Filters(None), "Bob" -> Filters(None)),
            6)
          dummyContractsForAlice <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map("Alice" -> Filters(Some(InclusiveFilters(List(templateIds.dummy))))),
            1)

          dummyContractsForAliceAndBob <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Map(
              "Alice" -> Filters(Some(InclusiveFilters(List(templateIds.dummy)))),
              "Bob" -> Filters(Some(InclusiveFilters(List(templateIds.dummy))))),
            2
          )

        } yield
          (
            allContractsForAlice,
            allContractsForBob,
            allContractsForAliceAndBob,
            dummyContractsForAlice,
            dummyContractsForAliceAndBob)

        resultsF map {
          case (
              allContractsForAlice,
              allContractsForBob,
              allContractsForAliceAndBob,
              dummyContractsForAlice,
              dummyContractsForAliceAndBob) =>
            val eventsForAlice = allContractsForAlice.flatMap(extractEvents)
            eventsForAlice.size shouldEqual 3
            lookForContract(eventsForAlice, templateIds.dummy)
            lookForContract(eventsForAlice, templateIds.dummyWithParam)
            lookForContract(eventsForAlice, templateIds.dummyFactory)

            val eventsForBob = allContractsForBob.flatMap(extractEvents)
            eventsForBob.size shouldEqual 3
            lookForContract(eventsForBob, templateIds.dummy)
            lookForContract(eventsForBob, templateIds.dummyWithParam)
            lookForContract(eventsForBob, templateIds.dummyFactory)

            val eventsForAliceAndBob = allContractsForAliceAndBob.flatMap(extractEvents)
            eventsForAliceAndBob.size shouldEqual 6
            lookForContract(eventsForAliceAndBob, templateIds.dummy, 2)
            lookForContract(eventsForAliceAndBob, templateIds.dummyWithParam, 2)
            lookForContract(eventsForAliceAndBob, templateIds.dummyFactory, 2)

            val dummyEventsForAlice = dummyContractsForAlice.flatMap(extractEvents)
            dummyEventsForAlice.size shouldEqual 1
            lookForContract(dummyEventsForAlice, templateIds.dummy)

            val dummyEventsForAliceAndBob = dummyContractsForAliceAndBob.flatMap(extractEvents)
            dummyEventsForAliceAndBob.size shouldEqual 2
            lookForContract(dummyEventsForAliceAndBob, templateIds.dummy, 2)
            lookForContract(dummyEventsForAliceAndBob, templateIds.dummyWithParam, 0)
            lookForContract(dummyEventsForAliceAndBob, templateIds.dummyFactory, 0)
        }
      }
    }
  }

  override protected def config: Config = Config.default

}
