// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record}
import org.scalatest.{Matchers, WordSpec}
import com.digitalasset.platform.api.v1.event.EventOps.EventOps

class TransientContractRemoverTest extends WordSpec with Matchers {

  import TransientContractRemover.removeTransients

  private val contractId = Ref.ContractIdString.assertFromString("contractId")
  private val p1 = Ref.Party.assertFromString("a")
  private val p2 = Ref.Party.assertFromString("b")
  private val evId1 = Ref.LedgerString.assertFromString("-1")
  private val evId2 = Ref.LedgerString.assertFromString("-2")
  private val templateId = Identifier(Ref.PackageId.assertFromString("pkg"), "mod", "ent")
  private val createE = Event(
    Event.Event.Created(
      CreatedEvent(
        evId1,
        contractId,
        Some(templateId),
        None,
        Some(Record(None, Seq.empty)),
        Seq.empty,
        Seq.empty,
        Seq.empty,
        None)))
  private val archiveE =
    Event(
      Event.Event.Archived(
        ArchivedEvent(
          evId2,
          contractId,
          Some(templateId),
          Seq.empty
        )))

  "Transient contract remover" should {

    "remove Created and Archived events for the same contract from the transaction" in {
      val create = createE.updateWitnessParties(Seq(p1))
      val archive = archiveE.updateWitnessParties(Seq(p1))
      removeTransients(List(create, archive)) shouldEqual Nil
    }

    "throw IllegalArgumentException if witnesses do not match on the events received" in {
      val create = createE.updateWitnessParties(Seq(p1))
      val archive = archiveE.updateWitnessParties(Seq(p2))
      assertThrows[IllegalArgumentException](removeTransients(List(create, archive)))
    }

    "do not touch individual Created events" in {
      val create = createE.updateWitnessParties(Seq(p1))
      removeTransients(List(create)) shouldEqual List(create)
    }

    "do not touch individual Archived events" in {
      val archive = archiveE.updateWitnessParties(Seq(p1))
      removeTransients(List(archive)) shouldEqual List(archive)
    }

    "remove events with no witnesses in the input" in {
      val create = createE.updateWitnessParties(Seq.empty)
      val archive = archiveE.updateWitnessParties(Seq.empty)
      removeTransients(List(create, archive)) shouldEqual Nil
    }
  }
}
