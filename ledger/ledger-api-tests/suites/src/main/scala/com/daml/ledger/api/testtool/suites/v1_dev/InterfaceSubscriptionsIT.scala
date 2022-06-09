// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_dev

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

// TODO DPP-1068: Move to an appropriate place, maybe merge with InterfaceIT?
class InterfaceSubscriptionsIT extends LedgerTestSuite {

  private[this] val InterfaceId = Tag.unwrap(T1.id).copy(entityName = "I")

  test(
    "ISTransactionsBasic",
    "Basic functionality for interface subscriptions on transaction streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      c1 <- ledger.create(party, T1(party, 1)) // Implements I with view (1, true)
      c2 <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      c3 <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      transactions <- ledger.flatTransactions(
        interfaceSubscription(party, Seq.empty, Seq(InterfaceId), ledger)
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
      assertViewEquals(createdEvent1.interfaceViews.head, InterfaceId) { value =>
        assertLength("View1 has 2 fields", 2, value.fields)
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 1)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }

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
        interfaceSubscription(party, Seq.empty, Seq(InterfaceId), ledger)
      )
      // 2. Subscribe by all implementing templates
      transactions2 <- ledger.flatTransactions(
        interfaceSubscription(party, allImplementations, Seq.empty, ledger)
      )
      // 3. Subscribe by both the interface and all templates (redundant filters)
      transactions3 <- ledger.flatTransactions(
        interfaceSubscription(party, allImplementations, Seq(InterfaceId), ledger)
      )
    } yield {
      assertLength("3 transactions found", 3, transactions1)
      assertEquals(
        "1 and 2 find the same transactions",
        transactions1.map(_.transactionId),
        transactions2.map(_.transactionId),
      )
      assertEquals("1 and 3 produce the exact same result", transactions1, transactions3)
    }
  })

  test(
    "ISTransactionsUnknownInterface",
    "Subscribing by an unknown interface fails",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val unknownInterface = InterfaceId.copy(entityName = "IDoesNotExist")
    for {
      _ <- ledger.create(party, T1(party, 1))
      _ <- ledger.create(party, T2(party, 2))
      _ <- ledger.create(party, T3(party, 3))
      _ <- ledger.create(party, T4(party, 4))
      _ <- ledger
        .flatTransactions(interfaceSubscription(party, Seq.empty, Seq(unknownInterface), ledger))
        .failed
    } yield {
      // TODO DPP-1068: The error is currently not a self-service error, check here the error code once that is fixed
      ()
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

  // TODO DPP-1068: Factor out methods for creating interface subscriptions, see ParticipantTestContext.getTransactionsRequest
  private def interfaceSubscription(
      party: Party,
      templateIds: Seq[Identifier],
      interfaceIds: Seq[Identifier],
      ledger: ParticipantTestContext,
  ) = {
    val filter = new TransactionFilter(
      Map(
        party.toString -> new Filters(
          Some(
            new InclusiveFilters(
              templateIds = templateIds,
              interfaceFilters = interfaceIds.map(id =>
                new InterfaceFilter(
                  Some(id),
                  includeInterfaceView = true,
                  includeCreateArgumentsBlob = true,
                )
              ),
            )
          )
        )
      )
    )
    ledger
      .getTransactionsRequest(Seq(party))
      .update(_.filter := filter)
  }

}
