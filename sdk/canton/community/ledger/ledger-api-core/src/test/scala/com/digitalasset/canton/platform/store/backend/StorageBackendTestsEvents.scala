// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction.TreeEvent
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.dao.events.Raw
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsEvents
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (events)"

  import StorageBackendTestValues.*
  import DbDtoEq.*

  private val emptyTraceContext =
    SerializableTraceContext(TraceContext.empty).toDamlProto.toByteArray

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

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partySignatory),
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partyObserver1),
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partyObserver2),
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultSuperReader = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = None,
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    resultSignatory should contain theSameElementsAs Vector(1L, 2L)
    resultObserver1 should contain theSameElementsAs Vector(1L)
    resultObserver2 should contain theSameElementsAs Vector(2L)
    resultSuperReader should contain theSameElementsAs Vector(1L, 1L, 2L, 2L)
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

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partySignatory),
        templateIdO = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partyObserver1),
        templateIdO = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partyObserver2),
        templateIdO = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultSuperReader = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = None,
        templateIdO = Some(someTemplateId),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    resultSignatory should contain theSameElementsAs Vector(1L, 2L)
    resultObserver1 should contain theSameElementsAs Vector(1L)
    resultObserver2 should contain theSameElementsAs Vector(2L)
    resultSuperReader should contain theSameElementsAs Vector(1L, 1L, 2L, 2L)
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

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partySignatory),
        templateIdO = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partyObserver1),
        templateIdO = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultObserver2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partyObserver2),
        templateIdO = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultSuperReader = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = None,
        templateIdO = Some(otherTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    resultSignatory shouldBe empty
    resultObserver1 shouldBe empty
    resultObserver2 shouldBe empty
    resultSuperReader shouldBe empty
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

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(1), 1L))
    val resultUnknownParty = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partyUnknown),
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultUnknownTemplate = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partySignatory),
        templateIdO = Some(unknownTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultUnknownPartyAndTemplate = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partyUnknown),
        templateIdO = Some(unknownTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )
    val resultUnknownTemplateSuperReader = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = None,
        templateIdO = Some(unknownTemplate),
        startExclusive = 0L,
        endInclusive = 10L,
        limit = 10,
      )
    )

    resultUnknownParty shouldBe empty
    resultUnknownTemplate shouldBe empty
    resultUnknownPartyAndTemplate shouldBe empty
    resultUnknownTemplateSuperReader shouldBe empty
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

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val result01L2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partySignatory),
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 1L,
        limit = 2,
      )
    )
    val result12L2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partySignatory),
        templateIdO = None,
        startExclusive = 1L,
        endInclusive = 2L,
        limit = 2,
      )
    )
    val result02L1 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partySignatory),
        templateIdO = None,
        startExclusive = 0L,
        endInclusive = 2L,
        limit = 1,
      )
    )
    val result02L2 = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(partySignatory),
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

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
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

  it should "return the correct trace context for create events" in {
    val traceContexts = (1 to 3)
      .flatMap(_ => List(TraceContext.empty, TraceContext.withNewTraceContext(identity)))
      .map(SerializableTraceContext(_).toDamlProto.toByteArray)
    val dbDtos = Vector(
      dtoCreate(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        traceContext = traceContexts(0),
      ),
      dtoCreate(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        traceContext = traceContexts(1),
      ),
      dtoExercise(
        offset = offset(3),
        eventSequentialId = 3L,
        consuming = false,
        contractId = hashCid("#1"),
        traceContext = traceContexts(2),
      ),
      dtoExercise(
        offset = offset(4),
        eventSequentialId = 4L,
        consuming = false,
        contractId = hashCid("#2"),
        traceContext = traceContexts(3),
      ),
      dtoExercise(
        offset = offset(5),
        eventSequentialId = 5L,
        consuming = true,
        contractId = hashCid("#1"),
        traceContext = traceContexts(4),
      ),
      dtoExercise(
        offset = offset(6),
        eventSequentialId = 6L,
        consuming = true,
        contractId = hashCid("#2"),
        commandId = "command id 6",
        traceContext = traceContexts(5),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    val transactionTrees = executeSql(
      backend.event.transactionPointwiseQueries.fetchTreeTransactionEvents(1L, 6L, Set.empty)
    )
    for (i <- traceContexts.indices)
      yield transactionTrees(i).traceContext should equal(Some(traceContexts(i)))

    val flatTransactions = executeSql(
      backend.event.transactionPointwiseQueries.fetchFlatTransactionEvents(1L, 6L, Set.empty)
    )
    val flatContexts = traceContexts.take(2) ++ traceContexts.drop(4)
    for (i <- flatContexts.indices)
      yield flatTransactions(i).traceContext should equal(Some(flatContexts(i)))
  }

  it should "return the correct keys for create events" in {
    val someKey = Some(someSerializedDamlLfValue)
    val someMaintainer = Some("maintainer")
    val someMaintainers = Array("maintainer")
    val dbDtos = Vector(
      dtoCreate(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        createKey = someKey,
        createKeyMaintainer = someMaintainer,
      ),
      dtoCreate(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        createKey = None,
        createKeyMaintainer = None,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    val transactionTrees = executeSql(
      backend.event.transactionPointwiseQueries.fetchTreeTransactionEvents(1L, 6L, Set.empty)
    )

    def checkKeyAndMaintainersInTrees(
        event: Raw.TreeEvent,
        createKey: Option[Array[Byte]],
        createKeyMaintainers: Array[String],
    ) = event match {
      case created: Raw.Created[TreeEvent] =>
        created.createKeyValue should equal(createKey)
        created.createKeyMaintainers should equal(createKeyMaintainers)
      case _ => fail()
    }

    checkKeyAndMaintainersInTrees(transactionTrees(0).event, someKey, someMaintainers)
    checkKeyAndMaintainersInTrees(transactionTrees(1).event, None, Array.empty)

    val flatTransactions = executeSql(
      backend.event.transactionPointwiseQueries.fetchFlatTransactionEvents(1L, 6L, Set.empty)
    )

    def checkKeyAndMaintainersInFlats(
        event: Raw.FlatEvent,
        createKey: Option[Array[Byte]],
        createKeyMaintainers: Array[String],
    ) = event match {
      case created: Raw.Created[Event] =>
        created.createKeyValue should equal(createKey)
        created.createKeyMaintainers should equal(createKeyMaintainers)
      case _ => fail()
    }

    checkKeyAndMaintainersInFlats(flatTransactions(0).event, someKey, someMaintainers)
    checkKeyAndMaintainersInFlats(flatTransactions(1).event, None, Array.empty)
  }
}
