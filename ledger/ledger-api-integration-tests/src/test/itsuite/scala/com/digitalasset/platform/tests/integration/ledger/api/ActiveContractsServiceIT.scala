// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils._
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
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
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.apitesting.{
  LedgerContext,
  MultiLedgerFixture,
  TestCommands,
  TestIdsGenerator
}
import com.digitalasset.platform.ledger.acs.data.{AcsFutures, AcsTestUtil}
import io.grpc.Status
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.SpanSugar._
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Assertion, AsyncWordSpec, Matchers, OptionValues}
import scalaz.syntax.tag._

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
    with SuiteResourceManagementAroundAll
    with ScalaFutures
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues {

  override def timeLimit: Span = scaled(60.seconds)

  protected val testCommands = new TestCommands(config)
  protected val templateIds = testCommands.templateIds
  private val testIds = new TestIdsGenerator(config)

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(scaled(Span(30000, Millis)), scaled(Span(500, Millis)))

  private def transactionClient(ctx: LedgerContext): TransactionClient =
    new TransactionClient(ctx.ledgerId, ctx.transactionService)

  private def submitRequest(ctx: LedgerContext, request: SubmitRequest) =
    ctx.commandClient().flatMap(_.trackSingleCommand(request))

  private def extractEvents(response: GetActiveContractsResponse): Set[CreatedEvent] =
    response.activeContracts.toSet

  private def extractEvents(event: Event) = event.event.created.toList

  private def validateResponses(
      responses: Seq[GetActiveContractsResponse],
      workflowId: String,
      parties: List[String] = List("Alice")): Assertion = {
    responses.dropRight(1) foreach { response =>
      response.activeContracts should not be empty
      response.workflowId shouldEqual workflowId
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
      case ce @ CreatedEvent(_, _, Some(`template`), _, _, _, _, _, _) => ce
    }.size should equal(occurrence)

  def threeCommands(ledgerId: domain.LedgerId, workflowId: String): SubmitRequest =
    testCommands.dummyCommands(ledgerId, uniqueCmdId, "Alice", workflowId)

  private def uniqueCmdId = testIds.testCommandId(UUID.randomUUID().toString)

  "Active Contract Set Service" when {
    "asked for active contracts" should {
      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        new ActiveContractSetClient(
          domain.LedgerId(s"not-${context.ledgerId.unwrap}"),
          context.acsService)
          .getActiveContracts(TransactionFilter(Map("not-relevant" -> Filters())))
          .runWith(Sink.head)(materializer)
          .failed map { ex =>
          IsStatusException(Status.NOT_FOUND.getCode)(ex)
        }
      }

      "succeed with empty response" in allFixtures { context =>
        // using a UUID as party name to make it very likely that the this party doesn't see any
        // contracts yet. in the future we need to allocate a new party before each run to make this
        // test useful
        val filter = TransactionFilter(Map(UUID.randomUUID().toString -> Filters()))
        context.acsClient
          .getActiveContracts(filter)
          .runWith(Sink.seq)(materializer) map {
          // The sandbox sends an empty response, but the ACS app sends a response containing genesis predecessor.
          _.flatMap(_.activeContracts) should be(empty)
        }
      }
    }

    "contracts have been created" should {
      "return them in an ACS snapshot" in allFixtures { ctx =>
        val wfid = testIds.testWorkflowId("created")
        val resultsF = for {
          _ <- submitRequest(ctx, threeCommands(ctx.ledgerId, wfid))
          responses <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfid),
            Map("Alice" -> Filters()),
            3)
        } yield responses

        resultsF map { responses =>
          validateResponses(responses, wfid)

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
        val wfid = testIds.testWorkflowId("template-test-created")
        val resultsF = for {
          _ <- submitRequest(ctx, threeCommands(ctx.ledgerId, wfid))
          responses <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfid),
            Map("Alice" -> Filters(Some(InclusiveFilters(List(templateIds.dummy))))),
            1)
        } yield responses

        resultsF map { responses =>
          validateResponses(responses, wfid)

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
            case CreatedEvent(contractId, _, Some(tid), _, _, _, _, _, _)
                if tid == templateIds.dummy =>
              contractId
          }.head
        }

        val wfid = testIds.testWorkflowId("acs-exercise-test")

        val resultsF = for {
          _ <- submitRequest(ctx, threeCommands(ctx.ledgerId, wfid))
          responses1 <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfid),
            Map("Alice" -> Filters(None)),
            3)
          contractId = extractContractId(responses1)
          _ <- submitRequest(
            ctx,
            testCommands.buildRequest(
              ctx.ledgerId,
              commandId = uniqueCmdId,
              Seq(testCommands.exerciseWithUnit(templateIds.dummy, contractId, "DummyChoice1")),
              "Alice",
              workflowId = wfid)
          )
          responses2 <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfid),
            Map("Alice" -> Filters(None)),
            2)
        } yield responses2

        resultsF map { responses =>
          validateResponses(responses, wfid)

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

        val wfid1 = testIds.testWorkflowId("workflow1")
        val wfid2 = testIds.testWorkflowId("workflow2")
        val resultsF = for {
          _ <- submitRequest(
            ctx,
            testCommands.buildRequest(
              ctx.ledgerId,
              commandId = uniqueCmdId,
              Seq(testCommands.createWithOperator(templateIds.dummy, "Alice")),
              "Alice",
              workflowId = wfid1)
          )
          responses1 <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfid1),
            Map("Alice" -> Filters(None)),
            1)
          offset = extractOffset(responses1)
          _ <- submitRequest(
            ctx,
            testCommands.buildRequest(
              ctx.ledgerId,
              commandId = uniqueCmdId,
              Seq(testCommands.createWithOperator(templateIds.dummyWithParam, "Alice")),
              "Alice",
              workflowId = wfid2)
          )
          responses2 <- transactionClient(ctx)
            .getTransactions(
              LedgerOffset(Absolute(offset)),
              None,
              TransactionFilter(Map("Alice" -> Filters(None))))
            .filter(_.workflowId == wfid2)
            .take(1)
            .runWith(Sink.seq)
        } yield responses2

        resultsF map { responses =>
          responses.head.workflowId shouldEqual wfid2

          val events = responses.head.events.flatMap(extractEvents)

          lookForContract(events, templateIds.dummy, 0)
          lookForContract(events, templateIds.dummyWithParam)
        }
      }
    }

    "verbosity flag is true" should {
      "disclose field names" in forAllFixtures { ctx =>
        val wfid = testIds.testWorkflowId("verbose-created")
        for {
          _ <- submitRequest(ctx.context(), threeCommands(ctx.context().ledgerId, wfid))
          responses <- waitForActiveContracts(
            ctx.context().acsService,
            ctx.context().ledgerId,
            Set(wfid),
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
        val wfidAlice = testIds.testWorkflowId("alice-acsCommand-1")
        val wfidBob = testIds.testWorkflowId("bob-acsCommand-2")
        val resultsF = for {
          _ <- submitRequest(
            ctx,
            testCommands.dummyCommands(ctx.ledgerId, uniqueCmdId, "Alice", wfidAlice))
          _ <- submitRequest(
            ctx,
            testCommands.dummyCommands(ctx.ledgerId, uniqueCmdId, "Bob", wfidBob))
          allContractsForAlice <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfidAlice),
            Map("Alice" -> Filters(None)),
            3)
          allContractsForBob <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfidBob),
            Map("Bob" -> Filters(None)),
            3)
          allContractsForAliceAndBob <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfidAlice, wfidBob),
            Map("Alice" -> Filters(None), "Bob" -> Filters(None)),
            6)
          dummyContractsForAlice <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfidAlice),
            Map("Alice" -> Filters(Some(InclusiveFilters(List(templateIds.dummy))))),
            1)

          dummyContractsForAliceAndBob <- waitForActiveContracts(
            ctx.acsService,
            ctx.ledgerId,
            Set(wfidAlice, wfidBob),
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
