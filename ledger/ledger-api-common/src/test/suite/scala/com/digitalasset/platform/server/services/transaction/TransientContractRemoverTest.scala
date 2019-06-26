// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.ledger.api.domain
import org.scalatest.{Matchers, WordSpec}

class TransientContractRemoverTest extends WordSpec with Matchers {

  private val sut = TransientContractRemover
  private val contractId = domain.ContractId(Ref.ContractIdString.assertFromString("contractId"))
  private val p1 = Ref.Party.assertFromString("a")
  private val p2 = Ref.Party.assertFromString("b")
  private val evId1 = domain.EventId(Ref.LedgerString.assertFromString("-1"))
  private val evId2 = domain.EventId(Ref.LedgerString.assertFromString("-2"))
  private val templateId = Ref.Identifier(
    Ref.PackageId.assertFromString("pkg"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("mod"),
      Ref.DottedName.assertFromString("ent")))
  private val createE = domain.Event.CreatedEvent(
    evId1,
    contractId,
    templateId,
    Value.ValueRecord(None, ImmArray.empty),
    Set(),
    Set(),
    Set(),
    "",
    None)
  private val archiveE = domain.Event.ArchivedEvent(evId2, contractId, templateId, Set())

  "Transient contract remover" should {

    "remove Created and Archived events for the same contract from the transaction" in {
      val create = createE.copy(witnessParties = Set(p1))
      val archive = archiveE.copy(witnessParties = Set(p1))
      sut.removeTransients(List(create, archive)) shouldEqual Nil
    }

    "throw IllegalArgumentException if witnesses do not match on the events received" in {
      val create = createE.copy(witnessParties = Set(p1))
      val archive = archiveE.copy(witnessParties = Set(p2))
      assertThrows[IllegalArgumentException](sut.removeTransients(List(create, archive)))
    }

    "do not touch individual Created events" in {
      val create = createE.copy(witnessParties = Set(p1))
      sut.removeTransients(List(create)) shouldEqual List(create)
    }

    "do not touch individual Archived events" in {
      val archive = archiveE.copy(witnessParties = Set(p1))
      sut.removeTransients(List(archive)) shouldEqual List(archive)
    }

    "remove events with no witnesses in the input" in {
      val create = createE.copy(witnessParties = Set())
      val archive = archiveE.copy(witnessParties = Set())
      sut.removeTransients(List(create, archive)) shouldEqual Nil
    }
  }
}
