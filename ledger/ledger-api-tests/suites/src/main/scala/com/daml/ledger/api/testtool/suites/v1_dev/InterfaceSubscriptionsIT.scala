// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_dev

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
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.v1.event.Event.Event
import com.daml.ledger.api.v1.event.InterfaceView
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.ledger.test.semantic.InterfaceViews._
import scalaz.Tag

class InterfaceSubscriptionsIT extends LedgerTestSuite {

  test(
    "ISTransactionsBasic",
    "Basic functionality for interface subscriptions on transaction streams",
    allocate(SingleParty),
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
          )
        )
      )
    } yield {
      assertLength("3 transactions found", 3, transactions)

      // T1
      val createdEvent1 = createdEvents(transactions(0)).head
      assertLength("Create event 1 has a view", 1, createdEvent1.interfaceViews)
      assertEquals(
        "Create event 1 template ID",
        createdEvent1.templateId.get.toString,
        Tag.unwrap(T1.id).toString,
      )
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      assertViewEquals(createdEvent1.interfaceViews, Tag.unwrap(I.id)) { value =>
        assertLength("View1 has 2 fields", 2, value.fields)
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 1)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
        assert(
          value.fields.forall(_.label.nonEmpty),
          s"Expected a view with labels (verbose)",
        )
      }
      assertEquals(
        "Create event 1 createArguments must be defined",
        createdEvent1.createArguments.nonEmpty,
        true,
      )
      assertEquals(
        "Create event 1 should have a contract key defined",
        createdEvent1.contractKey.isDefined,
        true,
      )
      assert(
        createdEvent1.getCreateArguments.fields.forall(_.label.nonEmpty),
        s"Expected a contract with labels (verbose)",
      )

      // T2
      val createdEvent2 = createdEvents(transactions(1)).head
      assertLength("Create event 2 has a view", 1, createdEvent2.interfaceViews)
      assertEquals(
        "Create event 2 template ID",
        createdEvent2.templateId.get.toString,
        Tag.unwrap(T2.id).toString,
      )
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
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
        "Create event 1 should have a contract key empty",
        createdEvent2.contractKey.isEmpty,
        true,
      )

      // T3
      val createdEvent3 = createdEvents(transactions(2)).head
      assertLength("Create event 3 has a view", 1, createdEvent3.interfaceViews)
      assertEquals(
        "Create event 3 template ID",
        createdEvent3.templateId.get.toString,
        Tag.unwrap(T3.id).toString,
      )
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.toString)
      assertViewFailed(createdEvent3.interfaceViews, Tag.unwrap(I.id))
      assertEquals(
        "Create event 3 createArguments must be empty",
        createdEvent3.createArguments.isEmpty,
        true,
      )
      assertEquals(
        "Create event 1 should have empty contract key",
        createdEvent3.contractKey.isEmpty,
        true,
      )
    }
  })

  test(
    "ISAcsBasic",
    "Basic functionality for interface subscriptions on ACS streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      c1 <- create(
        party,
        T1(party, 1),
      ) // Implements: I with view (1, true)
      c2 <- create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- create(party, T4(party, 4)) // Does not implement I
      (_, acs) <- activeContracts(
        activeContractsRequest(Seq(party), Seq(T1.id), Seq((I.id, true)))
      )
    } yield {
      assertLength("3 transactions found", 3, acs)

      // T1
      val createdEvent1 = acs(0)
      assertLength("Create event 1 has a view", 1, createdEvent1.interfaceViews)
      assertEquals(
        "Create event 1 template ID",
        createdEvent1.templateId.get.toString,
        Tag.unwrap(T1.id).toString,
      )
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      assertViewEquals(createdEvent1.interfaceViews, Tag.unwrap(I.id)) { value =>
        assertLength("View1 has 2 fields", 2, value.fields)
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 1)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
        assert(
          value.fields.forall(_.label.nonEmpty),
          s"Expected a view with labels (verbose)",
        )
      }
      assertEquals(
        "Create event 1 createArguments must NOT be empty",
        createdEvent1.createArguments.isEmpty,
        false,
      )
      assert(
        createdEvent1.getCreateArguments.fields.forall(_.label.nonEmpty),
        s"Expected a contract with labels (verbose)",
      )
      assertEquals(
        "Create event 1 should have a contract key defined",
        createdEvent1.contractKey.isDefined,
        true,
      )

      // T2
      val createdEvent2 = acs(1)
      assertLength("Create event 2 has a view", 1, createdEvent2.interfaceViews)
      assertEquals(
        "Create event 2 template ID",
        createdEvent2.templateId.get.toString,
        Tag.unwrap(T2.id).toString,
      )
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
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
        "Create event 2 should not have a contract key defined, as no match by template_id",
        createdEvent2.contractKey.isDefined,
        false,
      )

      // T3
      val createdEvent3 = acs(2)
      assertLength("Create event 3 has a view", 1, createdEvent3.interfaceViews)
      assertEquals(
        "Create event 3 template ID",
        createdEvent3.templateId.get.toString,
        Tag.unwrap(T3.id).toString,
      )
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.toString)
      assertViewFailed(createdEvent3.interfaceViews, Tag.unwrap(I.id))
      assertEquals(
        "Create event 3 createArguments must be empty",
        createdEvent3.createArguments.isEmpty,
        true,
      )
    }
  })

  test(
    "ISMultipleWitness",
    "Multiple witness",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, party1, party2)) =>
    import ledger._
    for {
      c <- create(party1, T6(party1, party2))
      mergedTransactions <- flatTransactions(
        getTransactionsRequest(
          new TransactionFilter(
            Map(
              party1.toString -> filters(Seq.empty, Seq((I.id, true))),
              party2.toString -> filters(Seq.empty, Seq((I2.id, true))),
            )
          )
        )
      )

      party1Transactions <- flatTransactions(
        getTransactionsRequest(
          new TransactionFilter(
            Map(
              party1.toString -> filters(Seq.empty, Seq((I.id, true)))
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
    "ISMultipleViews",
    "Multiple interface views populated for one event",
    allocate(SingleParty),
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
    "ISTransactionsIrrelevantTransactions",
    "Subscribing on transaction stream by interface with no relevant transactions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      _ <- create(party, T4(party, 4)) // Does not implement I
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(Seq(party), Seq.empty, Seq((INoTemplate.id, true)))
        )
      )
    } yield {
      assertLength("0 transactions should be found", 0, transactions)
      ()
    }
  })

  test(
    "ISTransactionsDuplicateInterfaceFilters",
    "Subscribing on transaction stream by interface with duplicate filters and not verbose",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      c1 <- create(
        party,
        T1(party, 1),
      )
      c2 <- create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- create(party, T4(party, 4)) // Does not implement I
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            Seq(T1.id),
            Seq((I.id, false), (I.id, true)),
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
    "ISTransactionsDuplicateTemplateFilters",
    "Subscribing on transaction stream by template with duplicate filters",
    allocate(SingleParty),
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
    "ISTransactionsNoIncludedView",
    "Subscribing on transaction stream by interface or template without included views",
    allocate(SingleParty),
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
    "ISTransactionsEquivalentFilters",
    "Subscribing by interface or all implementing templates gives the same result",
    allocate(SingleParty),
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
          transactionFilter(Seq(party), Seq.empty, Seq((I.id, true)))
        )
      )
      // 2. Subscribe by all implementing templates
      transactions2 <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(Seq(party), allImplementations, Seq.empty)
        )
      )
      // 3. Subscribe by both the interface and all templates (redundant filters)
      transactions3 <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(
            Seq(party),
            allImplementations,
            Seq((I.id, true)),
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
        transactions2,
        transactions3.map(tx =>
          tx.copy(events = tx.events.map { event =>
            event.copy(event = event.event match {
              case created: Event.Created =>
                created.copy(value = created.value.copy(interfaceViews = Seq.empty))
              case other => other
            })
          })
        ),
      )
      assertEquals(
        "1 and 3 produce the same views (but not the same create arguments)",
        transactions1,
        transactions3.map(tx =>
          tx.copy(events = tx.events.map { event =>
            event.copy(event = event.event match {
              case created: Event.Created =>
                created.copy(value = created.value.copy(createArguments = None, contractKey = None))
              case other => other
            })
          })
        ),
      )
    }
  })

  test(
    "ISTransactionsUnknownTemplateOrInterface",
    "Subscribing on transaction stream by an unknown template fails",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val unknownTemplate = TemplateId(Tag.unwrap(I.id).copy(entityName = "IDoesNotExist"))
    val unknownInterface = TemplateId(Tag.unwrap(I.id).copy(entityName = "IDoesNotExist"))
    import ledger._
    for {
      _ <- create(party, T1(party, 1))
      failure <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(Seq(party), Seq(unknownTemplate), Seq.empty)
        )
      )
        .mustFail("subscribing with an unknown template")
      failure2 <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(Seq(party), Seq.empty, Seq((unknownInterface, true)))
        )
      )
        .mustFail("subscribing with an unknown interface")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some(s"Templates do not exist"),
      )
      assertGrpcError(
        failure2,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some(s"Interfaces do not exist"),
      )
    }
  })

  test(
    "ISTransactionsMultipleParties",
    "Subscribing on transaction stream by multiple parties",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, party1, party2)) =>
    import ledger._
    for {
      _ <- create(party1, T6(party1, party2))
      party1Iface1transactionsWithView <- flatTransactions(
        getTransactionsRequest(transactionFilter(Seq(party1), Seq.empty, Seq((I.id, true))))
      )
      party1Iface1transactionsWithoutView <- flatTransactions(
        getTransactionsRequest(transactionFilter(Seq(party1), Seq.empty, Seq((I.id, false))))
      )
      party2Iface1transactionsWithView <- flatTransactions(
        getTransactionsRequest(transactionFilter(Seq(party2), Seq.empty, Seq((I.id, true))))
      )
      party2Iface1transactionsWithoutView <- flatTransactions(
        getTransactionsRequest(transactionFilter(Seq(party2), Seq.empty, Seq((I.id, false))))
      )
    } yield {
      def emptyWitnessParty(
          emptyView: Boolean
      )(tx: com.daml.ledger.api.v1.transaction.Transaction) = {
        tx.copy(
          events = tx.events.map { event =>
            event.copy(event = event.event match {
              case created: Event.Created if emptyView =>
                created.copy(value =
                  created.value.copy(witnessParties = Seq.empty, interfaceViews = Seq.empty)
                )
              case created: Event.Created if !emptyView =>
                created.copy(value = created.value.copy(witnessParties = Seq.empty))
              case other => other
            })
          },
          commandId = "",
        )
      }

      assertEquals(
        party1Iface1transactionsWithView.map(emptyWitnessParty(false)),
        party2Iface1transactionsWithView.map(emptyWitnessParty(false)),
      )

      assertEquals(
        party1Iface1transactionsWithoutView.map(emptyWitnessParty(false)),
        party2Iface1transactionsWithoutView.map(emptyWitnessParty(false)),
      )

      assertEquals(
        party1Iface1transactionsWithView.map(emptyWitnessParty(true)),
        party2Iface1transactionsWithoutView.map(emptyWitnessParty(false)),
      )
    }
  })

  test(
    "ISTransactionsSubscribeBeforeTemplateCreated",
    "Subscribing on transaction stream by interface before template is created",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    import ledger._
    for {
      transactions <- flatTransactions(
        getTransactionsRequest(
          transactionFilter(Seq(party), Seq.empty, Seq((INoTemplate.id, true)))
        )
      )
      /* TODO
      assert no template instances before package is uploaded
      assert package is not uploaded before we attempt to upload it
      upload a package with interface only definition
      subscribe to that interface which was just uploaded
      assert no elements in the stream yet
      upload a package with a template referring an interface defined before.
      create a contract of that template
      assert there is a new transaction with just created contract
       */
    } yield {
      assertLength("0 transactions should be found", 0, transactions)
      ()
    }
  })

  private def assertViewFailed(views: Seq[InterfaceView], interfaceId: Identifier): Unit = {
    val viewSearch = views.find(_.interfaceId.contains(interfaceId))

    val view = assertDefined(viewSearch, "View could not be found")

    val actualInterfaceId = assertDefined(view.interfaceId, "Interface ID is not defined")
    assertEquals("View has correct interface ID", interfaceId, actualInterfaceId)

    val status = assertDefined(view.viewStatus, "Status is not defined")
    assertEquals("Status must be failed", status.code, 9)
  }

  private def assertViewEquals(views: Seq[InterfaceView], interfaceId: Identifier)(
      checkValue: Record => Unit
  ): Unit = {
    val viewSearch = views.find(_.interfaceId.contains(interfaceId))

    val view = assertDefined(viewSearch, "View could not be found")

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
