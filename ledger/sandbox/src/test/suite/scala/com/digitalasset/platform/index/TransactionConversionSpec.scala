// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.platform.api.v1.event.EventOps.EventOps
import com.digitalasset.platform.index.TransactionConversion.removeTransient
import org.scalatest.{Matchers, WordSpec}

final class TransactionConversionSpec extends WordSpec with Matchers {

  private val contractId = Ref.ContractIdString.assertFromString("contractId")
  private val contractId2 = Ref.ContractIdString.assertFromString("contractId2")
  private val create = Event(
    Event.Event.Created(
      CreatedEvent("", contractId, None, None, None, Seq.empty, Seq.empty, Seq.empty, None)))
  private val archive = Event(Event.Event.Archived(ArchivedEvent("", contractId, None, Seq.empty)))

  "removeTransient" should {

    "remove Created and Archived events for the same contract from the transaction" in {
      removeTransient(Vector(create, archive)) shouldEqual Nil
    }

    "do not touch events with different contract identifiers" in {
      val events = Vector(create.updateContractId(contractId2), archive)
      removeTransient(events) shouldBe events
    }

    "do not touch individual Created events" in {
      val events = Vector(create)
      removeTransient(events) shouldEqual events
    }

    "do not touch individual Archived events" in {
      val events = Vector(archive)
      removeTransient(events) shouldEqual events
    }
  }
}
