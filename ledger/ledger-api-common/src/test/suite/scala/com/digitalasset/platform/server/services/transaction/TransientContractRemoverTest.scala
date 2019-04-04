// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import org.scalatest.{Matchers, WordSpec}

class TransientContractRemoverTest extends WordSpec with Matchers {

  private val sut = TransientContractRemover
  private val contractId = "contractId"
  private val p1 = "a"
  private val p2 = "b"
  private val evId1 = "-1"
  private val evId2 = "-2"

  "Transient contract remover" should {

    "remove Created and Archived events for the same contract from the transaction" in {
      val create =
        Event(Event.Event.Created(CreatedEvent(evId1, contractId, witnessParties = List(p1))))
      val archive = Event(Event.Event.Archived(ArchivedEvent(evId2, contractId, None, List(p1))))
      sut.removeTransients(List(create, archive)) shouldEqual Nil
    }

    "throw IllegalArgumentException if witnesses do not match on the events received" in {
      val create =
        Event(Event.Event.Created(CreatedEvent(evId1, contractId, witnessParties = List(p1))))
      val archive = Event(Event.Event.Archived(ArchivedEvent(evId2, contractId, None, List(p2))))
      assertThrows[IllegalArgumentException](sut.removeTransients(List(create, archive)))
    }

    "do not touch individual Created events" in {
      val create =
        Event(Event.Event.Created(CreatedEvent(evId1, contractId, witnessParties = List(p1))))
      sut.removeTransients(List(create)) shouldEqual List(create)
    }

    "do not touch individual Archived events" in {
      val archive = Event(Event.Event.Archived(ArchivedEvent(evId2, contractId, None, List(p1))))
      sut.removeTransients(List(archive)) shouldEqual List(archive)
    }

    "remove events with no witnesses in the input" in {
      val create = Event(Event.Event.Created(CreatedEvent(evId1, contractId, witnessParties = Nil)))
      val archive = Event(Event.Event.Archived(ArchivedEvent(evId2, contractId + "2", None, Nil)))
      sut.removeTransients(List(create, archive)) shouldEqual Nil
    }
  }
}
