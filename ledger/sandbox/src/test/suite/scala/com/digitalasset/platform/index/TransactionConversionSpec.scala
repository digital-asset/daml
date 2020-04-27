// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.lf.value.Value
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.platform.index.TransactionConversion.removeTransient
import org.scalatest.{Matchers, WordSpec}

final class TransactionConversionSpec extends WordSpec with Matchers {

  private val contractId1 = Value.AbsoluteContractId.assertFromString("#contractId")
  private val contractId2 = Value.AbsoluteContractId.assertFromString("#contractId2")
  private def create(contractId: Value.AbsoluteContractId): Event =
    Event(
      Event.Event.Created(
        CreatedEvent("", contractId.coid, None, None, None, Seq.empty, Seq.empty, Seq.empty, None)))

  private val create1 = create(contractId1)
  private val create2 = create(contractId2)
  private val archive1 = Event(
    Event.Event.Archived(ArchivedEvent("", contractId1.coid, None, Seq.empty)))

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
