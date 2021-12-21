// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Ref
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsEvents
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (events)"

  import StorageBackendTestValues._

  it should "find contracts by party" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreate(offset(1), 1L, "#1", signatory = partySignatory, observer = partyObserver1),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver1),
      dtoCreate(offset(2), 2L, "#2", signatory = partySignatory, observer = partyObserver2),
      dtoCreateFilter(2L, someTemplateId, partySignatory),
      dtoCreateFilter(2L, someTemplateId, partyObserver2),
    )

    for {
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(
        updateLedgerEnd(offset(2), 2L)
      )
      resultSignatory <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partySignatory,
          templateIdFilter = None,
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
      resultObserver1 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partyObserver1,
          templateIdFilter = None,
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
      resultObserver2 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partyObserver2,
          templateIdFilter = None,
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
    } yield {
      resultSignatory should contain theSameElementsAs Vector(1L, 2L)
      resultObserver1 should contain theSameElementsAs Vector(1L)
      resultObserver2 should contain theSameElementsAs Vector(2L)
    }
  }

  it should "find contracts by party and template" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreate(offset(1), 1L, "#1", signatory = partySignatory, observer = partyObserver1),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver1),
      dtoCreate(offset(2), 2L, "#2", signatory = partySignatory, observer = partyObserver2),
      dtoCreateFilter(2L, someTemplateId, partySignatory),
      dtoCreateFilter(2L, someTemplateId, partyObserver2),
    )

    for {
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(
        updateLedgerEnd(offset(2), 2L)
      )
      resultSignatory <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partySignatory,
          templateIdFilter = Some(someTemplateId),
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
      resultObserver1 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partyObserver1,
          templateIdFilter = Some(someTemplateId),
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
      resultObserver2 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partyObserver2,
          templateIdFilter = Some(someTemplateId),
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
    } yield {
      resultSignatory should contain theSameElementsAs Vector(1L, 2L)
      resultObserver1 should contain theSameElementsAs Vector(1L)
      resultObserver2 should contain theSameElementsAs Vector(2L)
    }
  }

  it should "not find contracts when the template doesn't match" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")
    val otherTemplate = Ref.Identifier.assertFromString("pkg:Mod:Template2")

    val dtos = Vector(
      dtoCreate(offset(1), 1L, "#1", signatory = partySignatory, observer = partyObserver1),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver1),
      dtoCreate(offset(2), 2L, "#2", signatory = partySignatory, observer = partyObserver2),
      dtoCreateFilter(2L, someTemplateId, partySignatory),
      dtoCreateFilter(2L, someTemplateId, partyObserver2),
    )

    for {
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(
        updateLedgerEnd(offset(2), 2L)
      )
      resultSignatory <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partySignatory,
          templateIdFilter = Some(otherTemplate),
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
      resultObserver1 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partyObserver1,
          templateIdFilter = Some(otherTemplate),
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
      resultObserver2 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partyObserver2,
          templateIdFilter = Some(otherTemplate),
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
    } yield {
      resultSignatory shouldBe empty
      resultObserver1 shouldBe empty
      resultObserver2 shouldBe empty
    }
  }

  it should "not find contracts when unknown names are used" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver = Ref.Party.assertFromString("observer")
    val partyUnknown = Ref.Party.assertFromString("unknown")
    val unknownTemplate = Ref.Identifier.assertFromString("unknown:unknown:unknown")

    val dtos = Vector(
      dtoCreate(offset(1), 1L, "#1", signatory = partySignatory, observer = partyObserver),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver),
    )

    for {
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(
        updateLedgerEnd(offset(1), 1L)
      )
      resultUnknownParty <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partyUnknown,
          templateIdFilter = None,
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
      resultUnknownTemplate <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partySignatory,
          templateIdFilter = Some(unknownTemplate),
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
      resultUnknownPartyAndTemplate <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partyUnknown,
          templateIdFilter = Some(unknownTemplate),
          startExclusive = 0L,
          endInclusive = 10L,
          limit = 10,
        )
      )
    } yield {
      resultUnknownParty shouldBe empty
      resultUnknownTemplate shouldBe empty
      resultUnknownPartyAndTemplate shouldBe empty
    }
  }

  it should "respect bounds and limits" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreate(offset(1), 1L, "#1", signatory = partySignatory, observer = partyObserver1),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver1),
      dtoCreate(offset(2), 2L, "#2", signatory = partySignatory, observer = partyObserver2),
      dtoCreateFilter(2L, someTemplateId, partySignatory),
      dtoCreateFilter(2L, someTemplateId, partyObserver2),
    )

    for {
      _ <- executeSql(backend.parameter.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(
        updateLedgerEnd(offset(2), 2L)
      )
      result01L2 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partySignatory,
          templateIdFilter = None,
          startExclusive = 0L,
          endInclusive = 1L,
          limit = 2,
        )
      )
      result12L2 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partySignatory,
          templateIdFilter = None,
          startExclusive = 1L,
          endInclusive = 2L,
          limit = 2,
        )
      )
      result02L1 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partySignatory,
          templateIdFilter = None,
          startExclusive = 0L,
          endInclusive = 2L,
          limit = 1,
        )
      )
      result02L2 <- executeSql(
        backend.event.activeContractEventIds(
          partyFilter = partySignatory,
          templateIdFilter = None,
          startExclusive = 0L,
          endInclusive = 2L,
          limit = 2,
        )
      )
    } yield {
      result01L2 should contain theSameElementsAs Vector(1L)
      result12L2 should contain theSameElementsAs Vector(2L)
      result02L1 should contain theSameElementsAs Vector(1L)
      result02L2 should contain theSameElementsAs Vector(1L, 2L)
    }
  }
}
