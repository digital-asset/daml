// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
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
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partySignatory,
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partyObserver1,
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partyObserver2,
        templateIdO = None,
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
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partySignatory,
        templateIdO = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partyObserver1,
        templateIdO = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partyObserver2,
        templateIdO = Some(someTemplateId),
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
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partySignatory,
        templateIdO = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partyObserver1,
        templateIdO = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partyObserver2,
        templateIdO = Some(otherTemplate),
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
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partyUnknown,
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultUnknownTemplate = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partySignatory,
        templateIdO = Some(unknownTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultUnknownPartyAndTemplate = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partyUnknown,
        templateIdO = Some(unknownTemplate),
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
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partySignatory,
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 1L,
        limit = 2,
      )
    )
    val result12L2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partySignatory,
        templateIdO = None,
        startExclusive = 1L,
        endInclusive = 2L,
        limit = 2,
      )
    )
    val result02L1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partySignatory,
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 2L,
        limit = 1,
      )
    )
    val result02L2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = partySignatory,
        templateIdO = None,
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

  it should "populate correct maxEventSequentialId based on transaction_meta entries" in {
    val dtos = Vector(
      dtoTransactionMeta(offset(10), 1000, 1099),
      dtoTransactionMeta(offset(15), 1100, 1100),
      dtoTransactionMeta(offset(20), 1101, 1110),
      dtoTransactionMeta(offset(21), 1111, 1115),
      dtoTransactionMeta(offset(1000), 1119, 1120),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(25), 1115))
    val maxEventSequentialId: Long => Long =
      longOffset =>
        executeSql(
          backend.event.maxEventSequentialId(offset(longOffset))
        )

    executeSql(backend.event.maxEventSequentialId(Offset.beforeBegin)) shouldBe 999
    maxEventSequentialId(1) shouldBe 999
    maxEventSequentialId(2) shouldBe 999
    maxEventSequentialId(9) shouldBe 999
    maxEventSequentialId(10) shouldBe 1099
    maxEventSequentialId(11) shouldBe 1099
    maxEventSequentialId(14) shouldBe 1099
    maxEventSequentialId(15) shouldBe 1100
    maxEventSequentialId(16) shouldBe 1100
    maxEventSequentialId(19) shouldBe 1100
    maxEventSequentialId(20) shouldBe 1110
    maxEventSequentialId(21) shouldBe 1115
    maxEventSequentialId(22) shouldBe 1115
    maxEventSequentialId(24) shouldBe 1115
    maxEventSequentialId(25) shouldBe 1115
    maxEventSequentialId(26) shouldBe 1115

    executeSql(updateLedgerEnd(offset(20), 1110))
    maxEventSequentialId(20) shouldBe 1110
    maxEventSequentialId(21) shouldBe 1110
  }
}
