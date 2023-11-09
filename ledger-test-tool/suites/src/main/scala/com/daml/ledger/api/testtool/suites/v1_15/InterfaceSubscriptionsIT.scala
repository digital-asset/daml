// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_15

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.refinements.ApiTypes.TemplateId
import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  Parties,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{Dars, LedgerTestSuite}
import com.daml.ledger.api.testtool.infrastructure.FutureAssertions._
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.v1.event.Event.Event
import com.daml.ledger.api.v1.event.{CreatedEvent, InterfaceView}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.ledger.test.semantic.InterfaceViews._
import com.daml.ledger.test.{
  Carbonv1TestDar,
  Carbonv2TestDar,
  Carbonv3TestDar,
  carbonv1,
  carbonv2,
  carbonv3,
}
import com.daml.logging.LoggingContext
import scalaz.Tag

import java.util.regex.Pattern
import scala.concurrent.duration._

class InterfaceSubscriptionsIT extends InterfaceSubscriptionsITBase("IS", true)
class InterfaceSubscriptionsWithEventBlobsIT extends InterfaceSubscriptionsITBase("ISWP", false)

// Allows using deprecated Protobuf fields for testing
abstract class InterfaceSubscriptionsITBase(prefix: String, useTemplateIdBasedLegacyFormat: Boolean)
    extends LedgerTestSuite {

  test(
    s"${prefix}TransactionsBasic",
    "Basic functionality for interface subscriptions on transaction streams",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      c1 <- create(party, T1(party, 1))
      c2 <- create(party, T2(party, 2))
      c3 <- create(party, T3(party, 3))
      _ <- create(party, T4(party, 4))
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq(T1.id),
            Seq((I.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
      events = transactions.flatMap(createdEvents)
    } yield basicAssertions(c1.toString, c2.toString, c3.toString, events)
  })

  test(
    s"${prefix}AcsBasic",
    "Basic functionality for interface subscriptions on ACS streams",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      c1 <- create(party, T1(party, 1))
      c2 <- create(party, T2(party, 2))
      c3 <- create(party, T3(party, 3))
      _ <- create(party, T4(party, 4))
      (_, createdEvents) <- activeContracts(
        activeContractsRequest(
          Seq(party),
          Seq(T1.id),
          Seq((I.id, true)),
          "",
          useTemplateIdBasedLegacyFormat,
        )
      )
    } yield basicAssertions(c1.toString, c2.toString, c3.toString, createdEvents)
  })

  private def basicAssertions(
      c1: String,
      c2: String,
      c3: String,
      createdEvents: Vector[CreatedEvent],
  ): Unit = {
    assertLength("3 transactions found", 3, createdEvents)

    // T1
    val createdEvent1 = createdEvents(0)
    assertLength("Create event 1 has a view", 1, createdEvent1.interfaceViews)
    assertEquals(
      "Create event 1 template ID",
      createdEvent1.templateId.get.toString,
      Tag.unwrap(T1.id).toString,
    )
    assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1)
    assertViewEquals(createdEvent1.interfaceViews, Tag.unwrap(I.id)) { value =>
      assertLength("View1 has 2 fields", 2, value.fields)
      assertEquals("View1.a", value.fields(0).getValue.getInt64, 1)
      assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      assert(
        value.fields.forall(_.label.nonEmpty),
        "Expected a view with labels (verbose)",
      )
    }
    assertEquals(
      "Create event 1 createArguments must NOT be empty",
      createdEvent1.createArguments.isEmpty,
      false,
    )
    assert(
      createdEvent1.getCreateArguments.fields.forall(_.label.nonEmpty),
      "Expected a contract with labels (verbose)",
    )
    assertEquals(
      "Create event 1 should have a contract key defined",
      createdEvent1.contractKey.isDefined,
      true,
    )

    // T2
    val createdEvent2 = createdEvents(1)
    assertLength("Create event 2 has a view", 1, createdEvent2.interfaceViews)
    assertEquals(
      "Create event 2 template ID",
      createdEvent2.templateId.get.toString,
      Tag.unwrap(T2.id).toString,
    )
    assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2)
    assertViewEquals(createdEvent2.interfaceViews, Tag.unwrap(I.id)) { value =>
      assertLength("View2 has 2 fields", 2, value.fields)
      assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
      assertEquals("View2.b", value.fields(1).getValue.getBool, false)
    }
    assertEquals(
      "Create event 2 createArguments must be empty",
      createdEvent2.createArguments.isEmpty,
      true,
    )
    assertEquals(
      "Create event 2 should have a contract key empty, as no match by template_id",
      createdEvent2.contractKey.isEmpty,
      true,
    )

    // T3
    val createdEvent3 = createdEvents(2)
    assertLength("Create event 3 has a view", 1, createdEvent3.interfaceViews)
    assertEquals(
      "Create event 3 template ID",
      createdEvent3.templateId.get.toString,
      Tag.unwrap(T3.id).toString,
    )
    assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3)
    assertViewFailed(createdEvent3.interfaceViews, Tag.unwrap(I.id))
    assertEquals(
      "Create event 3 createArguments must be empty",
      createdEvent3.createArguments.isEmpty,
      true,
    )
  }

  test(
    s"${prefix}MultipleWitness",
    "Multiple witness",
    allocate(Parties(2)),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party1, party2)) =>
    import ledger._
    for {
      c <- create(party1, T6(party1, party2))
      mergedTransactions <- flatTransactions(
        getTransactionsRequest(
          new TransactionFilter(
            Map(
              party1.toString -> filters(
                Seq.empty,
                Seq((I.id, true)),
                useTemplateIdBasedLegacyFormat,
              ),
              party2.toString -> filters(
                Seq.empty,
                Seq((I2.id, true)),
                useTemplateIdBasedLegacyFormat,
              ),
            )
          )
        )
      )

      party1Transactions <- flatTransactions(
        getTransactionsRequest(
          new TransactionFilter(
            Map(
              party1.toString -> filters(
                Seq.empty,
                Seq((I.id, true)),
                useTemplateIdBasedLegacyFormat,
              )
            )
          )
        )
      )
    } yield {
      assertLength("single transaction found", 1, mergedTransactions)
      val createdEvent1 = createdEvents(mergedTransactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c.toString)
      assertViewEquals(createdEvent1.interfaceViews, Tag.unwrap(I.id)) { value =>
        assertLength("View1 has 2 fields", 2, value.fields)
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 6)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }
      assertViewEquals(createdEvent1.interfaceViews, Tag.unwrap(I2.id)) { value =>
        assertLength("View2 has 1 field", 1, value.fields)
        assertEquals("View2.c", value.fields(0).getValue.getInt64, 7)
      }

      assertLength("single transaction found", 1, party1Transactions)
      val createdEvent2 = createdEvents(party1Transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent2.contractId, c.toString)
      assertLength("single view found", 1, createdEvent2.interfaceViews)
      assertViewEquals(createdEvent2.interfaceViews, Tag.unwrap(I.id)) { value =>
        assertLength("View1 has 2 fields", 2, value.fields)
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 6)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }
    }
  })

  test(
    s"${prefix}MultipleViews",
    "Multiple interface views populated for one event",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      c <- create(party, T5(party, 31337))
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq.empty,
            Seq((I.id, true), (I2.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
    } yield {
      assertLength("single transaction found", 1, transactions)
      val createdEvent = createdEvents(transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent.contractId, c.toString)
      assertViewEquals(createdEvent.interfaceViews, Tag.unwrap(I.id)) { value =>
        assertLength("View1 has 2 fields", 2, value.fields)
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 31337)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }
      assertViewEquals(createdEvent.interfaceViews, Tag.unwrap(I2.id)) { value =>
        assertLength("View2 has 1 field", 1, value.fields)
        assertEquals("View2.c", value.fields(0).getValue.getInt64, 1)
      }
    }
  })

  test(
    s"${prefix}TransactionsIrrelevantTransactions",
    "Subscribing on transaction stream by interface with no relevant transactions",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      _ <- create(party, T4(party, 4))
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq.empty,
            Seq((INoTemplate.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
    } yield {
      assertLength("0 transactions should be found", 0, transactions)
      ()
    }
  })

  test(
    s"${prefix}TransactionsDuplicateInterfaceFilters",
    "Subscribing on transaction stream by interface with duplicate filters and not verbose",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      c1 <- create(party, T1(party, 1))
      c2 <- create(party, T2(party, 2))
      c3 <- create(party, T3(party, 3))
      _ <- create(party, T4(party, 4))
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq(T1.id),
            Seq((I.id, false), (I.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
          .update(_.verbose := false)
      )
    } yield {
      val createdEvent1 = createdEvents(transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      val createdEvent2 = createdEvents(transactions(1)).head
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
      // Expect view to be delivered even though there is an ambiguous
      // includeInterfaceView flag set to true and false at the same time (true wins)
      assertViewEquals(createdEvent2.interfaceViews, Tag.unwrap(I.id)) { value =>
        assertLength("View2 has 2 fields", 2, value.fields)
        assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
        assertEquals("View2.b", value.fields(1).getValue.getBool, false)
        assert(
          value.fields.forall(_.label.isEmpty),
          s"Expected a view with no labels (verbose = false)",
        )
      }
      val createdEvent3 = createdEvents(transactions(2)).head
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.toString)
    }
  })

  test(
    s"${prefix}TransactionsDuplicateTemplateFilters",
    "Subscribing on transaction stream by template with duplicate filters",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      c1 <- create(party, T1(party, 1))
      c2 <- create(party, T2(party, 2))
      c3 <- create(party, T3(party, 3))
      _ <- create(party, T4(party, 4))
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq(T1.id, T1.id),
            Seq((I.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
    } yield {
      val createdEvent1 = createdEvents(transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      assertEquals(
        "Create event 1 createArguments must NOT be empty",
        createdEvent1.createArguments.isEmpty,
        false,
      )
      val createdEvent2 = createdEvents(transactions(1)).head
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
      // Expect view to be delivered even though there is an ambiguous
      // includeInterfaceView flag set to true and false at the same time.
      assertViewEquals(createdEvent2.interfaceViews, Tag.unwrap(I.id)) { value =>
        assertLength("View2 has 2 fields", 2, value.fields)
        assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
        assertEquals("View2.b", value.fields(1).getValue.getBool, false)
      }
      val createdEvent3 = createdEvents(transactions(2)).head
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.toString)
    }
  })

  test(
    s"${prefix}TransactionsNoIncludedView",
    "Subscribing on transaction stream by interface or template without included views",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      c1 <- create(party, T1(party, 1))
      _ <- create(party, T2(party, 2))
      _ <- create(party, T3(party, 3))
      _ <- create(party, T4(party, 4))
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq(T1.id),
            Seq((I.id, false)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
    } yield {
      assertLength("3 transactions found", 3, transactions)
      val interfaceViewCount: Int =
        transactions.flatMap(createdEvents).map(_.interfaceViews.size).sum
      assertEquals("No views have been computed and produced", 0, interfaceViewCount)
      val createArgumentsCount: Int =
        transactions.flatMap(createdEvents).map(_.createArguments.isDefined).count(_ == true)
      assertEquals("Only single create arguments must be delivered", 1, createArgumentsCount)

      // T1
      val createdEvent1 = createdEvents(transactions(0)).head
      assertEquals(
        "Create event 1 template ID",
        createdEvent1.templateId.get.toString,
        Tag.unwrap(T1.id).toString,
      )
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      assertEquals(
        "Create event 1 createArguments must NOT be empty",
        createdEvent1.createArguments.isEmpty,
        false,
      )
    }
  })

  test(
    s"${prefix}TransactionsEquivalentFilters",
    "Subscribing by interface or all implementing templates gives the same result",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    val allImplementations = Seq(T1.id, T2.id, T3.id)
    for {
      _ <- create(party, T1(party, 1))
      _ <- create(party, T2(party, 2))
      _ <- create(party, T3(party, 3))
      _ <- create(party, T4(party, 4))
      // 1. Subscribe by the interface
      transactions1 <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq.empty,
            Seq((I.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
      // 2. Subscribe by all implementing templates
      transactions2 <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            allImplementations,
            Seq.empty,
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
      // 3. Subscribe by both the interface and all templates (redundant filters)
      transactions3 <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            allImplementations,
            Seq((I.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
    } yield {
      assertLength("3 transactions found", 3, transactions1)
      assertEquals(
        "1 and 2 find the same transactions (but not the same views)",
        transactions1.map(_.transactionId),
        transactions2.map(_.transactionId),
      )
      assertEquals(
        "2 and 3 find the same contract_arguments (but not the same views)",
        transactions2.map(updateTransaction()),
        transactions3.map(updateTransaction(emptyView = true)),
      )
      assertEquals(
        "1 and 3 produce the same views (but not the same create arguments)",
        // do not check on details since tid is contained and it is expected to be different
        transactions1
          .map(updateTransaction(emptyDetails = true))
          .map(hideTraceIdFromStatusMessages),
        transactions3
          .map(
            updateTransaction(
              emptyContractKey = true,
              emptyCreateArguments = true,
              emptyDetails = true,
            )
          )
          .map(hideTraceIdFromStatusMessages),
      )
    }
  })

  test(
    s"${prefix}TransactionsUnknownTemplateOrInterface",
    "Subscribing on transaction stream by an unknown template fails",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val unknownTemplate = TemplateId(Tag.unwrap(I.id).copy(entityName = "TemplateDoesNotExist"))
    val unknownInterface = TemplateId(Tag.unwrap(I.id).copy(entityName = "InterfaceDoesNotExist"))
    import ledger._
    for {
      _ <- create(party, T1(party, 1))
      failure <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq(unknownTemplate),
            Seq.empty,
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
        .mustFail("subscribing with an unknown template")
      failure2 <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq.empty,
            Seq((unknownInterface, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
        .mustFail("subscribing with an unknown interface")
    } yield {
      assertGrpcErrorRegex(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.TemplateOrInterfaceIdsNotFound,
        Some(Pattern.compile("Templates do not exist.*TemplateDoesNotExist]")),
      )
      assertGrpcErrorRegex(
        failure2,
        LedgerApiErrors.RequestValidation.NotFound.TemplateOrInterfaceIdsNotFound,
        Some(Pattern.compile("Interfaces do not exist.*InterfaceDoesNotExist]")),
      )
    }
  })

  test(
    s"${prefix}TransactionsMultipleParties",
    "Subscribing on transaction stream by multiple parties",
    allocate(Parties(2)),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party1, party2)) =>
    import ledger._
    for {
      _ <- create(party1, T6(party1, party2))
      party1Iface1transactionsWithView <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party1),
            Seq.empty,
            Seq((I.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
      party1Iface1transactionsWithoutView <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party1),
            Seq.empty,
            Seq((I.id, false)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
      party2Iface1transactionsWithView <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party2),
            Seq.empty,
            Seq((I.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
      party2Iface1transactionsWithoutView <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party2),
            Seq.empty,
            Seq((I.id, false)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
    } yield {
      assertEquals(
        party1Iface1transactionsWithView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
        party2Iface1transactionsWithView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
      )

      assertEquals(
        party1Iface1transactionsWithoutView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
        party2Iface1transactionsWithoutView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
      )

      assertEquals(
        party1Iface1transactionsWithView.map(
          updateTransaction(emptyView = true, emptyWitness = true)
        ),
        party2Iface1transactionsWithoutView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
      )
    }
  })

  test(
    s"${prefix}TransactionsSubscribeBeforeTemplateCreated",
    "Subscribing on transaction stream by interface before template is created",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._

    implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

    for {
      _ <- ledger.uploadDarFile(Dars.read(Carbonv1TestDar.path))

      transactionFuture = flatTransactions(
        take = 1,
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq.empty,
            Seq((carbonv1.CarbonV1.I.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
          // endless stream here as we would like to keep it open until
          // template is uploaded and contract with this template is created
          .update(
            _.optionalEnd := None
          ),
      )
      _ = assertEquals(transactionFuture.isCompleted, false)

      _ <- ledger.uploadDarFile(Dars.read(Carbonv2TestDar.path))

      _ = assertEquals(transactionFuture.isCompleted, false)

      contract <- succeedsEventually(
        maxRetryDuration = 10.seconds,
        description = "Topology processing around Dar upload can take a bit of time.",
        delayMechanism = ledger.delayMechanism,
      ) {
        create(party, carbonv2.CarbonV2.T(party, 21))
      }

      transactions <- transactionFuture

    } yield assertSingleContractWithSimpleView(
      transactions = transactions,
      contractIdentifier = Tag.unwrap(carbonv2.CarbonV2.T.id),
      viewIdentifier = Tag.unwrap(carbonv1.CarbonV1.I.id),
      contractId = contract.toString,
      viewValue = 21,
    )
  })

  test(
    s"${prefix}TransactionsRetroactiveInterface",
    "Subscribe to retroactive interface",
    allocate(SingleParty),
    enabled = _.templateFilters || useTemplateIdBasedLegacyFormat,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

    for {
      _ <- ledger.uploadDarFile(Dars.read(Carbonv1TestDar.path))
      _ <- ledger.uploadDarFile(Dars.read(Carbonv2TestDar.path))
      contract <- succeedsEventually(
        maxRetryDuration = 10.seconds,
        description = "Topology processing around Dar upload can take a bit of time.",
        delayMechanism = ledger.delayMechanism,
      ) {
        create(party, carbonv2.CarbonV2.T(party, 77))
      }
      _ <- ledger.uploadDarFile(Dars.read(Carbonv3TestDar.path))
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq.empty,
            Seq((carbonv3.CarbonV3.RetroI.id, true)),
            useTemplateIdBasedLegacyFormat,
          )
        )
      )
    } yield assertSingleContractWithSimpleView(
      transactions = transactions,
      contractIdentifier = Tag.unwrap(carbonv2.CarbonV2.T.id),
      viewIdentifier = Tag.unwrap(carbonv3.CarbonV3.RetroI.id),
      contractId = contract.toString,
      viewValue = 77,
    )
  })

  private def assertSingleContractWithSimpleView(
      transactions: Vector[Transaction],
      contractIdentifier: Identifier,
      viewIdentifier: Identifier,
      contractId: String,
      viewValue: Long,
  ): Unit = {
    assertLength("transaction should be found", 1, transactions)
    val createdEvent = createdEvents(transactions(0)).head
    assertLength("Create event has a view", 1, createdEvent.interfaceViews)
    assertEquals(
      "Create event template ID",
      createdEvent.templateId.get.toString,
      contractIdentifier.toString,
    )
    assertEquals("Create event contract ID", createdEvent.contractId, contractId)
    assertViewEquals(createdEvent.interfaceViews, viewIdentifier) { value =>
      assertLength("View has 1 field", 1, value.fields)
      assertEquals("View.value", value.fields(0).getValue.getInt64, viewValue)
    }
  }

  private def updateTransaction(
      emptyView: Boolean = false,
      emptyWitness: Boolean = false,
      emptyCreateArguments: Boolean = false,
      emptyContractKey: Boolean = false,
      emptyDetails: Boolean = false,
  )(tx: com.daml.ledger.api.v1.transaction.Transaction): Transaction = {
    tx.copy(
      events = tx.events.map { event =>
        event.copy(event = event.event match {
          case created: Event.Created =>
            created.copy(value =
              created.value.copy(
                witnessParties = if (emptyWitness) Seq.empty else created.value.witnessParties,
                interfaceViews =
                  if (emptyView) Seq.empty
                  else if (emptyDetails)
                    created.value.interfaceViews.map(iv =>
                      iv.copy(viewStatus =
                        iv.viewStatus.map(status => status.copy(details = Seq.empty))
                      )
                    )
                  else created.value.interfaceViews,
                contractKey = if (emptyContractKey) None else created.value.contractKey,
                createArguments = if (emptyCreateArguments) None else created.value.createArguments,
              )
            )
          case other => other
        })
      },
      commandId = "",
    )
  }

  private def hideTraceIdFromStatusMessages(
      tx: com.daml.ledger.api.v1.transaction.Transaction
  ): Transaction = {
    tx.copy(
      events = tx.events.map { event =>
        event.copy(event = event.event match {
          case created: Event.Created =>
            created.copy(value =
              created.value.copy(
                interfaceViews = created.value.interfaceViews.map(view =>
                  view.copy(
                    viewStatus = view.viewStatus.map(status =>
                      status.copy(message =
                        status.message.replaceFirst(
                          """UNHANDLED_EXCEPTION\(9,.{8}\)""",
                          "UNHANDLED_EXCEPTION(9,0)",
                        )
                      )
                    )
                  )
                )
              )
            )
          case other => other
        })
      },
      commandId = "",
    )
  }

  private def assertViewFailed(views: Seq[InterfaceView], interfaceId: Identifier): Unit = {
    val viewSearch = views.find(_.interfaceId.contains(interfaceId))

    val view = assertDefined(viewSearch, "View could not be found")

    val actualInterfaceId = assertDefined(view.interfaceId, "Interface ID is not defined")
    assertEquals("View has correct interface ID", interfaceId, actualInterfaceId)

    val status = assertDefined(view.viewStatus, "Status is not defined")
    assertEquals("Status must be invalid argument", status.code, 9)
  }

  private def assertViewEquals(views: Seq[InterfaceView], interfaceId: Identifier)(
      checkValue: Record => Unit
  ): Unit = {
    val viewSearch = views.find(_.interfaceId.contains(interfaceId))

    val view = assertDefined(
      viewSearch,
      s"View could not be found, there are: ${views.map(_.interfaceId).mkString("[", ",", "]")}",
    )

    val viewCount = views.count(_.interfaceId.contains(interfaceId))
    assertEquals(s"Only one view of interfaceId=$interfaceId must be defined", viewCount, 1)

    val actualInterfaceId = assertDefined(view.interfaceId, "Interface ID is not defined")
    assertEquals("View has correct interface ID", actualInterfaceId, interfaceId)

    val status = assertDefined(view.viewStatus, "Status is not defined")
    assertEquals("Status must be successful", status.code, 0)

    val actualValue = assertDefined(view.viewValue, "Value is not defined")
    checkValue(actualValue)
  }
}
