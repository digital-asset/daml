// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_dev

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.suites.v1_dev.InterfaceSubscriptionsIT.IncludeInterfaceView
import com.daml.ledger.api.v1.event.Event.Event
import com.daml.ledger.api.v1.event.InterfaceView
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.semantic.InterfaceViews._
import scalaz.Tag

// TODO DPP-1068: [implementation detail] Move to an appropriate place, maybe merge with InterfaceIT?
class InterfaceSubscriptionsIT extends LedgerTestSuite {

  private[this] val InterfaceId = Tag.unwrap(T1.id).copy(entityName = "I")
  private[this] val InterfaceNoTemplateId = Tag.unwrap(T1.id).copy(entityName = "INoTemplate")
  private[this] val InterfaceWithNoViewId = Tag.unwrap(T1.id).copy(entityName = "INoView")

  test(
    "ISTransactionsBasic",
    "Basic functionality for interface subscriptions on transaction streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      c1 <- ledger.create(
        party,
        T1(party, 1),
      ) // Implements 2 views: I with view (1, true), INoView with no view
      c2 <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      transactions <- ledger.flatTransactions(
        transactionSubscription(
          party,
          Seq(Tag.unwrap(T1.id)),
          Seq((InterfaceId, true), (InterfaceWithNoViewId, true)),
          ledger,
        )
      )
    } yield {
      assertLength("3 transactions found", 3, transactions)

      // T1
      val createdEvent1 = createdEvents(transactions(0)).head
      assertLength("Create event 1 has a view", 2, createdEvent1.interfaceViews)
      assertEquals(
        "Create event 1 template ID",
        createdEvent1.templateId.get.toString,
        Tag.unwrap(T1.id).toString,
      )
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      assertViewEquals(createdEvent1.interfaceViews(0), InterfaceId) { value =>
        assertLength("View1 has 2 fields", 2, value.fields)
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 1)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }
      assertViewFailed(createdEvent1.interfaceViews(1), InterfaceWithNoViewId)
      assertEquals(
        "Create event 1 createArguments must NOT be empty",
        createdEvent1.createArguments.isEmpty,
        false,
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
      assertViewEquals(createdEvent2.interfaceViews.head, InterfaceId) { value =>
        assertLength("View2 has 2 fields", 2, value.fields)
        assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
        assertEquals("View2.b", value.fields(1).getValue.getBool, false)
      }
      assertEquals(
        "Create event 2 createArguments must be empty",
        createdEvent2.createArguments.isEmpty,
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
      assertViewFailed(createdEvent3.interfaceViews.head, InterfaceId)
      assertEquals(
        "Create event 3 createArguments must be empty",
        createdEvent3.createArguments.isEmpty,
        true,
      )
    }
  })

  test(
    "ISTransactionsIrrelevantTransactions",
    "Subscribing on transaction stream by interface with no relevant transactions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      transactions <- ledger.flatTransactions(
        transactionSubscription(
          party,
          Seq.empty,
          Seq((InterfaceNoTemplateId, true)),
          ledger,
        )
      )
    } yield {
      assertLength("0 transactions should be found", 0, transactions)
      ()
    }
  })

  test(
    "ISTransactionsDuplicateInterfaceFilters",
    "Subscribing on transaction stream by interface with duplicate filters",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      c1 <- ledger.create(
        party,
        T1(party, 1),
      ) // Implements 2 views: I with view (1, true), INoView with no view
      c2 <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      transactions <- ledger.flatTransactions(
        transactionSubscription(
          party,
          Seq(Tag.unwrap(T1.id)),
          Seq((InterfaceId, false), (InterfaceId, true), (InterfaceWithNoViewId, true)),
          ledger,
        )
      )
    } yield {
      val createdEvent1 = createdEvents(transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      val createdEvent2 = createdEvents(transactions(1)).head
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
      // Expect view to be delivered even though there is an ambiguous
      // includeInterfaceView flag set to true and false at the same time.
      assertViewEquals(createdEvent2.interfaceViews.head, InterfaceId) { value =>
        assertLength("View2 has 2 fields", 2, value.fields)
        assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
        assertEquals("View2.b", value.fields(1).getValue.getBool, false)
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
    for {
      c1 <- ledger.create(
        party,
        T1(party, 1),
      ) // Implements 2 views: I with view (1, true), INoView with no view
      c2 <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      transactions <- ledger.flatTransactions(
        transactionSubscription(
          party,
          Seq(Tag.unwrap(T1.id), Tag.unwrap(T1.id)),
          Seq((InterfaceId, true), (InterfaceWithNoViewId, true)),
          ledger,
        )
      )
    } yield {
      val createdEvent1 = createdEvents(transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      val createdEvent2 = createdEvents(transactions(1)).head
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
      // Expect view to be delivered even though there is an ambiguous
      // includeInterfaceView flag set to true and false at the same time.
      assertViewEquals(createdEvent2.interfaceViews.head, InterfaceId) { value =>
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
    for {
      c1 <- ledger.create(
        party,
        T1(party, 1),
      ) // Implements 2 views: I with view (1, true), INoView with no view
      c2 <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      transactions <- ledger.flatTransactions(
        transactionSubscription(
          party,
          Seq(Tag.unwrap(T1.id)),
          Seq((InterfaceId, false), (InterfaceWithNoViewId, false)),
          ledger,
        )
      )
    } yield {
      assertLength("3 transactions found", 3, transactions)

      // T1
      val createdEvent1 = createdEvents(transactions(0)).head
      assertLength("Create event 1 has NO view", 0, createdEvent1.interfaceViews)
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

      // T2
      val createdEvent2 = createdEvents(transactions(1)).head
      assertLength("Create event 2 has NO view", 0, createdEvent2.interfaceViews)
      assertEquals(
        "Create event 2 template ID",
        createdEvent2.templateId.get.toString,
        Tag.unwrap(T2.id).toString,
      )
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
      assertEquals(
        "Create event 2 createArguments must be empty",
        createdEvent2.createArguments.isEmpty,
        true,
      )

      // T3
      val createdEvent3 = createdEvents(transactions(2)).head
      assertLength("Create event 3 has no view", 0, createdEvent3.interfaceViews)
      assertEquals(
        "Create event 3 template ID",
        createdEvent3.templateId.get.toString,
        Tag.unwrap(T3.id).toString,
      )
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.toString)
      assertEquals(
        "Create event 3 createArguments must be empty",
        createdEvent3.createArguments.isEmpty,
        true,
      )
    }
  })

  test(
    "ISTransactionsEquivalentFilters",
    "Subscribing by interface or all implementing templates gives the same result",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val allImplementations = Seq(Tag.unwrap(T1.id), Tag.unwrap(T2.id), Tag.unwrap(T3.id))
    for {
      _ <- ledger.create(party, T1(party, 1))
      _ <- ledger.create(party, T2(party, 2))
      _ <- ledger.create(party, T3(party, 3))
      _ <- ledger.create(party, T4(party, 4))
      // 1. Subscribe by the interface
      transactions1 <- ledger.flatTransactions(
        transactionSubscription(party, Seq.empty, Seq((InterfaceId, true)), ledger)
      )
      // 2. Subscribe by all implementing templates
      transactions2 <- ledger.flatTransactions(
        transactionSubscription(party, allImplementations, Seq.empty, ledger)
      )
      // 3. Subscribe by both the interface and all templates (redundant filters)
      transactions3 <- ledger.flatTransactions(
        transactionSubscription(party, allImplementations, Seq((InterfaceId, true)), ledger)
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
    "ISTransactionsUnknownInterface",
    "Subscribing on transaction stream by an unknown interface fails",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val unknownInterface = InterfaceId.copy(entityName = "IDoesNotExist")
    for {
      _ <- ledger.create(party, T1(party, 1))
      failure <- ledger
        .flatTransactions(
          transactionSubscription(party, Seq.empty, Seq((unknownInterface, true)), ledger)
        )
        .mustFail("subscribing with an unknown interface")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some(s"Interfaces do not exist"),
      )
    }
  })

  test(
    "ISTransactionsUnknownTemplate",
    "Subscribing on transaction stream by an unknown template fails",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val unknownTemplate = InterfaceId.copy(entityName = "IDoesNotExist")
    for {
      _ <- ledger.create(party, T1(party, 1))
      failure <- ledger
        .flatTransactions(
          transactionSubscription(party, Seq(unknownTemplate), Seq.empty, ledger)
        )
        .mustFail("subscribing with an unknown template")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some(s"Templates do not exist"),
      )
    }
  })

  test(
    "ISAcsBasic",
    "Basic functionality for interface subscriptions on ACS streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      c1 <- ledger.create(
        party,
        T1(party, 1),
      ) // Implements 2 views: I with view (1, true), INoView with no view
      c2 <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      (_, acs) <- ledger.activeContracts(
        acsSubscription(
          party,
          Seq(Tag.unwrap(T1.id)),
          Seq((InterfaceId, true), (InterfaceWithNoViewId, true)),
          ledger,
        )
      )
    } yield {
      assertLength("3 transactions found", 3, acs)

      // T1
      val createdEvent1 = acs(0)
      assertLength("Create event 1 has a view", 2, createdEvent1.interfaceViews)
      assertEquals(
        "Create event 1 template ID",
        createdEvent1.templateId.get.toString,
        Tag.unwrap(T1.id).toString,
      )
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      assertViewEquals(createdEvent1.interfaceViews(0), InterfaceId) { value =>
        assertLength("View1 has 2 fields", 2, value.fields)
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 1)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }
      assertViewFailed(createdEvent1.interfaceViews(1), InterfaceWithNoViewId)
      assertEquals(
        "Create event 1 createArguments must NOT be empty",
        createdEvent1.createArguments.isEmpty,
        false,
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
      assertViewEquals(createdEvent2.interfaceViews.head, InterfaceId) { value =>
        assertLength("View2 has 2 fields", 2, value.fields)
        assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
        assertEquals("View2.b", value.fields(1).getValue.getBool, false)
      }
      assertEquals(
        "Create event 2 createArguments must be empty",
        createdEvent2.createArguments.isEmpty,
        true,
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
      assertViewFailed(createdEvent3.interfaceViews.head, InterfaceId)
      assertEquals(
        "Create event 3 createArguments must be empty",
        createdEvent3.createArguments.isEmpty,
        true,
      )
    }
  })

  test(
    "ISAcsIrrelevantFilters",
    "Subscribing on ACS stream by interface with no relevant transactions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      (_, acs) <- ledger.activeContracts(
        acsSubscription(
          party,
          Seq(Tag.unwrap(T1.id)),
          Seq((InterfaceNoTemplateId, true)),
          ledger,
        )
      )
    } yield {
      assertLength("0 transactions should be found", 0, acs)
      ()
    }
  })

  test(
    "ISAcsDuplicateInterfaceFilters",
    "Subscribing on ACS stream by interface with duplicate filters",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      c1 <- ledger.create(
        party,
        T1(party, 1),
      ) // Implements 2 views: I with view (1, true), INoView with no view
      c2 <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      (_, acs) <- ledger.activeContracts(
        acsSubscription(
          party,
          Seq.empty,
          Seq((InterfaceId, false), (InterfaceId, true), (InterfaceWithNoViewId, true)),
          ledger,
        )
      )
    } yield {
      val createdEvent1 = acs(0)
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
      val createdEvent2 = acs(1)
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
      val createdEvent3 = acs(2)
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.toString)
    }
  })

  test(
    "ISAcsDuplicateTemplateFilters",
    "Subscribing on ACS stream by template with duplicate filters",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      c1 <- ledger.create(
        party,
        T1(party, 1),
      ) // Implements 2 views: I with view (1, true), INoView with no view
      _ <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      (_, acs) <- ledger.activeContracts(
        acsSubscription(
          party,
          Seq(Tag.unwrap(T1.id), Tag.unwrap(T1.id)),
          Seq.empty,
          ledger,
        )
      )
    } yield {
      assertLength("1 transaction found", 1, acs)
      val createdEvent1 = acs(0)
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.toString)
    }
  })

  test(
    "ISAcsNoIncludedView",
    "Subscribing by interface on ACS streams without included view",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      c1 <- ledger.create(
        party,
        T1(party, 1),
      ) // Implements 2 views: I with view (1, true), INoView with no view
      c2 <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      (_, acs) <- ledger.activeContracts(
        acsSubscription(
          party,
          Seq(Tag.unwrap(T1.id)),
          Seq((InterfaceId, false), (InterfaceWithNoViewId, false)),
          ledger,
        )
      )
    } yield {
      assertLength("3 transactions found", 3, acs)

      // T1
      val createdEvent1 = acs(0)
      assertLength("Create event 1 has no view", 0, createdEvent1.interfaceViews)
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

      // T2
      val createdEvent2 = acs(1)
      assertLength("Create event 2 has no view", 0, createdEvent2.interfaceViews)
      assertEquals(
        "Create event 2 template ID",
        createdEvent2.templateId.get.toString,
        Tag.unwrap(T2.id).toString,
      )
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.toString)
      assertEquals(
        "Create event 2 createArguments must be empty",
        createdEvent2.createArguments.isEmpty,
        true,
      )

      // T3
      val createdEvent3 = acs(2)
      assertLength("Create event 3 has no view", 0, createdEvent3.interfaceViews)
      assertEquals(
        "Create event 3 template ID",
        createdEvent3.templateId.get.toString,
        Tag.unwrap(T3.id).toString,
      )
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.toString)
      assertEquals(
        "Create event 3 createArguments must be empty",
        createdEvent3.createArguments.isEmpty,
        true,
      )
    }
  })

  test(
    "ISAcsEquivalentFilters",
    "Subscribing by interface or all implementing templates gives the same result",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val allImplementations = Seq(Tag.unwrap(T1.id), Tag.unwrap(T2.id), Tag.unwrap(T3.id))
    for {
      _ <- ledger.create(party, T1(party, 1))
      _ <- ledger.create(party, T2(party, 2))
      _ <- ledger.create(party, T3(party, 3))
      _ <- ledger.create(party, T4(party, 4))
      // 1. Subscribe by the interface
      (_, acs1) <- ledger.activeContracts(
        acsSubscription(party, Seq.empty, Seq((InterfaceId, true)), ledger)
      )
      // 2. Subscribe by all implementing templates
      (_, acs2) <- ledger.activeContracts(
        acsSubscription(party, allImplementations, Seq.empty, ledger)
      )
      // 3. Subscribe by both the interface and all templates (redundant filters)
      (_, acs3) <- ledger.activeContracts(
        acsSubscription(party, allImplementations, Seq((InterfaceId, true)), ledger)
      )
    } yield {
      assertLength("3 active contracts found", 3, acs1)
      assertEquals(
        "1 and 2 find the same contracts (but not the same views)",
        acs1.map(_.contractId),
        acs2.map(_.contractId),
      )
      assertEquals(
        "2 and 3 find the same contract_arguments (but not the same views)",
        acs2,
        acs3.map(created => created.copy(interfaceViews = Seq.empty)),
      )
    }
  })

  test(
    "ISAcsUnknownInterface",
    "Subscribing by interface or all implementing templates gives the same result",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val unknownInterface = InterfaceId.copy(entityName = "IDoesNotExist")
    for {
      _ <- ledger.create(party, T1(party, 1))
      failure <- ledger
        .activeContracts(
          acsSubscription(party, Seq.empty, Seq((unknownInterface, true)), ledger)
        )
        .mustFail("subscribing with an unknown interface")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some(s"Interfaces do not exist"),
      )
    }
  })

  test(
    "ISAcsUnknownTemplate",
    "Subscribing by interface or all implementing templates gives the same result",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val unknownTemplate = InterfaceId.copy(entityName = "IDoesNotExist")
    for {
      _ <- ledger.create(party, T1(party, 1))
      failure <- ledger
        .activeContracts(
          acsSubscription(party, Seq(unknownTemplate), Seq.empty, ledger)
        )
        .mustFail("subscribing with an unknown template")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some(s"Templates do not exist"),
      )
    }
  })

  private def assertViewFailed(view: InterfaceView, interfaceId: Identifier): Unit = {
    val actualInterfaceId = assertDefined(view.interfaceId, "Interface ID is not defined")
    assertEquals("View has correct interface ID", interfaceId, actualInterfaceId)

    val status = assertDefined(view.viewStatus, "Status is not defined")
    assertNotEquals("Status must be failed", status.code, 0)
  }

  private def assertViewEquals(view: InterfaceView, interfaceId: Identifier)(
      checkValue: Record => Unit
  ): Unit = {
    val actualInterfaceId = assertDefined(view.interfaceId, "Interface ID is not defined")
    assertEquals("View has correct interface ID", interfaceId, actualInterfaceId)

    val status = assertDefined(view.viewStatus, "Status is not defined")
    assertEquals("Status must be successful", status.code, 0)

    val actualValue = assertDefined(view.viewValue, "Value is not defined")
    checkValue(actualValue)
  }

  // TODO DPP-1068: [implementation detail] Factor out methods for creating interface subscriptions, see ParticipantTestContext.getTransactionsRequest
  private def transactionSubscription(
      party: Party,
      templateIds: Seq[Identifier],
      interfaceIds: Seq[(Identifier, IncludeInterfaceView)],
      ledger: ParticipantTestContext,
  ) = {
    val filter = new TransactionFilter(
      Map(
        party.toString -> new Filters(
          Some(
            new InclusiveFilters(
              templateIds = templateIds,
              interfaceFilters = interfaceIds.map { case (id, includeInterfaceView) =>
                new InterfaceFilter(
                  Some(id),
                  includeInterfaceView = includeInterfaceView,
                )
              },
            )
          )
        )
      )
    )
    ledger
      .getTransactionsRequest(Seq(party))
      .update(_.filter := filter)
  }

  // TODO DPP-1068: [implementation detail] Factor out methods for creating interface subscriptions, see ParticipantTestContext.getTransactionsRequest
  private def acsSubscription(
      party: Party,
      templateIds: Seq[Identifier],
      interfaceIds: Seq[(Identifier, IncludeInterfaceView)],
      ledger: ParticipantTestContext,
  ) = {
    val filter = new TransactionFilter(
      Map(
        party.toString -> new Filters(
          Some(
            new InclusiveFilters(
              templateIds = templateIds,
              interfaceFilters = interfaceIds.map { case (id, includeInterfaceView) =>
                new InterfaceFilter(
                  Some(id),
                  includeInterfaceView = includeInterfaceView,
                )
              },
            )
          )
        )
      )
    )
    ledger
      .activeContractsRequest(Seq(party))
      .update(_.filter := filter)
  }

}

object InterfaceSubscriptionsIT {
  type IncludeInterfaceView = Boolean
}
