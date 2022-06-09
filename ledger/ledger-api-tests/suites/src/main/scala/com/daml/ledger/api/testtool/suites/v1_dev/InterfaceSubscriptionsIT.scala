// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.{Participant, Participants, SingleParty, allocate}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, InterfaceFilter, TransactionFilter}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.semantic.InterfaceViews._
import scalaz.Tag

// TODO DPP-1068: Move to an appropriate place, maybe merge with InterfaceIT?
class InterfaceSubscriptionsIT extends LedgerTestSuite {

  private[this] val T1Id = Tag.unwrap(T1.id)
  private[this] val T2Id = Tag.unwrap(T2.id)
  private[this] val T3Id = Tag.unwrap(T3.id)
  private[this] val T4Id = Tag.unwrap(T4.id)
  private[this] val IId = Tag.unwrap(T1.id).copy(moduleName = "Interface1", entityName = "I")

  test(
    "ISPlaceholder",
    "Placeholder test",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, T1(party, 1)) // Implements I with view (1, true)
      _ <- ledger.create(party, T2(party, 2)) // Implements I with view (2, false)
      _ <- ledger.create(party, T3(party, 3)) // Implements I with a view that crashes
      _ <- ledger.create(party, T4(party, 4)) // Does not implement I
      transactions <- ledger.flatTransactions(interfaceSubscription(party, Seq(IId), ledger))
    } yield {
      assertLength(s"3 transactions found", 2, transactions)
      val firstCreatedEvent = createdEvents(transactions.head).head
      assertLength(s"2 interface views in the first create event", 2, firstCreatedEvent.interfaceViews)
      val firstInterfaceView = firstCreatedEvent.interfaceViews.head
      assertDefined(firstInterfaceView.viewValue, "First interface view has a value")
      assertEquals("Interface view has the right value", firstInterfaceView.viewValue.get.fields.head.value.get.getParty, party)
    }
  })

  // TODO DPP-1068: Factor out methods for creating interface subscriptions, see ParticipantTestContext.getTransactionsRequest
  private def interfaceSubscription(party: Party, interfaceIds: Seq[Identifier], ledger: ParticipantTestContext) = {
    val filter = new TransactionFilter(Map(party.toString -> new Filters(Some(new InclusiveFilters(
      templateIds = Seq.empty,
      interfaceFilters = interfaceIds.map(id =>
        new InterfaceFilter(Some(id), includeInterfaceView = true, includeCreateArgumentsBlob = true),
      ),
    )))))
    ledger.getTransactionsRequest(Seq(party))
      .update(_.filter := filter)
  }

}
