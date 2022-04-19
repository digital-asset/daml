// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.platform.store.appendonlydao.events.Raw
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsEvents
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (events)"

  import StorageBackendTestValues._

  it should "find contracts by party" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver1,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver1),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        signatory = partySignatory,
        observer = partyObserver2,
      ),
      dtoCreateFilter(2L, someTemplateId, partySignatory),
      dtoCreateFilter(2L, someTemplateId, partyObserver2),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partySignatory,
        templateIdFilter = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partyObserver1,
        templateIdFilter = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partyObserver2,
        templateIdFilter = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    resultSignatory should contain theSameElementsAs Vector(1L, 2L)
    resultObserver1 should contain theSameElementsAs Vector(1L)
    resultObserver2 should contain theSameElementsAs Vector(2L)
  }

  it should "find contracts by party and template" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver1,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver1),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        signatory = partySignatory,
        observer = partyObserver2,
      ),
      dtoCreateFilter(2L, someTemplateId, partySignatory),
      dtoCreateFilter(2L, someTemplateId, partyObserver2),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partySignatory,
        templateIdFilter = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partyObserver1,
        templateIdFilter = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partyObserver2,
        templateIdFilter = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    resultSignatory should contain theSameElementsAs Vector(1L, 2L)
    resultObserver1 should contain theSameElementsAs Vector(1L)
    resultObserver2 should contain theSameElementsAs Vector(2L)
  }

  it should "not find contracts when the template doesn't match" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")
    val otherTemplate = Ref.Identifier.assertFromString("pkg:Mod:Template2")

    val dtos = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver1,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver1),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        signatory = partySignatory,
        observer = partyObserver2,
      ),
      dtoCreateFilter(2L, someTemplateId, partySignatory),
      dtoCreateFilter(2L, someTemplateId, partyObserver2),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partySignatory,
        templateIdFilter = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partyObserver1,
        templateIdFilter = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partyObserver2,
        templateIdFilter = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    resultSignatory shouldBe empty
    resultObserver1 shouldBe empty
    resultObserver2 shouldBe empty
  }

  it should "not find contracts when unknown names are used" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver = Ref.Party.assertFromString("observer")
    val partyUnknown = Ref.Party.assertFromString("unknown")
    val unknownTemplate = Ref.Identifier.assertFromString("unknown:unknown:unknown")

    val dtos = Vector(
      dtoCreate(offset(1), 1L, hashCid("#1"), signatory = partySignatory, observer = partyObserver),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(1), 1L))
    val resultUnknownParty = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partyUnknown,
        templateIdFilter = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultUnknownTemplate = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partySignatory,
        templateIdFilter = Some(unknownTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultUnknownPartyAndTemplate = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partyUnknown,
        templateIdFilter = Some(unknownTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    resultUnknownParty shouldBe empty
    resultUnknownTemplate shouldBe empty
    resultUnknownPartyAndTemplate shouldBe empty
  }

  it should "respect bounds and limits" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver1,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory),
      dtoCreateFilter(1L, someTemplateId, partyObserver1),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        signatory = partySignatory,
        observer = partyObserver2,
      ),
      dtoCreateFilter(2L, someTemplateId, partySignatory),
      dtoCreateFilter(2L, someTemplateId, partyObserver2),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val result01L2 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partySignatory,
        templateIdFilter = None,
        startExclusive = 0L,
        endInclusive = 1L,
        limit = 2,
      )
    )
    val result12L2 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partySignatory,
        templateIdFilter = None,
        startExclusive = 1L,
        endInclusive = 2L,
        limit = 2,
      )
    )
    val result02L1 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partySignatory,
        templateIdFilter = None,
        startExclusive = 0L,
        endInclusive = 2L,
        limit = 1,
      )
    )
    val result02L2 = executeSql(
      backend.event.activeContractEventIds(
        partyFilter = partySignatory,
        templateIdFilter = None,
        startExclusive = 0L,
        endInclusive = 2L,
        limit = 2,
      )
    )

    result01L2 should contain theSameElementsAs Vector(1L)
    result12L2 should contain theSameElementsAs Vector(2L)
    result02L1 should contain theSameElementsAs Vector(1L)
    result02L2 should contain theSameElementsAs Vector(1L, 2L)
  }

  it should "return a flat transaction" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver = Ref.Party.assertFromString("observer")

    // One transaction with the following events:
    // 1. create
    // 2. non-consuming exercise on (1)
    // 3. consuming exercise on (1)
    val create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = hashCid("#1"),
      signatory = partySignatory,
      observer = partyObserver,
    )
    val createFilter1 = DbDto.CreateFilter(1L, someTemplateId.toString, partySignatory)
    val createFilter2 = DbDto.CreateFilter(1L, someTemplateId.toString, partyObserver)
    val transactionId = dtoTransactionId(create)
    val exercise = dtoExercise(
      offset = offset(1),
      eventSequentialId = 2L,
      consuming = false,
      contractId = hashCid("#1"),
      signatory = partySignatory,
      actor = partySignatory,
    )
    val archive = dtoExercise(
      offset = offset(1),
      eventSequentialId = 3L,
      consuming = true,
      contractId = hashCid("#1"),
      signatory = partySignatory,
      actor = partySignatory,
    )
    val filter = FilterParams(Set(partySignatory), Set.empty)

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(Vector(create, createFilter1, createFilter2, exercise, archive), _))
    executeSql(updateLedgerEnd(offset(1), 3L))

    val result = executeSql(
      backend.event.flatTransaction(
        transactionId = transactionId,
        filterParams = filter,
      )
    )

    result should have length 2

    inside(result(0)) { case entry =>
      entry.event shouldBe a[Raw.FlatEvent.Created]
      entry.eventSequentialId shouldBe 1L
      entry.transactionId shouldBe transactionId
    }

    inside(result(1)) { case entry =>
      entry.event shouldBe a[Raw.FlatEvent.Archived]
      entry.eventSequentialId shouldBe 3L
      entry.transactionId shouldBe transactionId
    }
  }
}
