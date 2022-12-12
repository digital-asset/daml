// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.platform.store.dao.events.TransactionConversion.removeTransient
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TransactionConversionSpec extends AnyWordSpec with Matchers {
  inside(List.fill(2)(TransactionBuilder.newCid)) { case List(contractId1, contractId2) =>
    def create(contractId: Value.ContractId): Event =
      Event.of(
        Event.Event.Created(
          CreatedEvent(
            "",
            contractId.coid,
            None,
            None,
            None,
            None,
            Seq.empty,
            Seq.empty,
            Seq.empty,
            Seq.empty,
            None,
          )
        )
      )

    val create1 = create(contractId1)
    val create2 = create(contractId2)
    val archive1 = Event.of(
      Event.Event.Archived(ArchivedEvent("", contractId1.coid, None, Seq.empty))
    )

    "removeTransient" should {

      "remove Created and Archived events for the same contract from the transaction" in {
        removeTransient(Vector(create1, archive1)) shouldEqual Nil
      }

      "do not touch events with different contract identifiers" in {
        val events = Vector(create2, archive1)
        removeTransient(events) shouldBe events
      }

      "do not touch individual Created events" in {
        val events = Vector(create1)
        removeTransient(events) shouldEqual events
      }

      "do not touch individual Archived events" in {
        val events = Vector(archive1)
        removeTransient(events) shouldEqual events
      }
    }
  }
}
