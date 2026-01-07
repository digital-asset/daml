// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.{HasExecutionContext, TestEssentials}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ValueInt64
import org.mockito.MockitoSugar
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicLong

class ContractStateCachesSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with OptionValues
    with TestEssentials
    with HasExecutionContext {
  behavior of classOf[ContractStateCaches].getSimpleName

  "build" should "set the cache index to the initialization index" in {
    val cacheInitializationEventSeqId = 1337L
    @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
    val contractStateCaches = ContractStateCaches.build(
      cacheInitializationEventSeqId,
      maxContractsCacheSize = 1L,
      maxKeyCacheSize = 1L,
      metrics = LedgerApiServerMetrics.ForTesting,
      loggerFactory,
    )

    contractStateCaches.keyState.cacheEventSeqIdIndex shouldBe cacheInitializationEventSeqId
    contractStateCaches.contractState.cacheEventSeqIdIndex shouldBe cacheInitializationEventSeqId
  }

  "push" should "update the caches with a batch of events" in new TestScope {
    val previousCreate = createEvent(withKey = true)

    val create1 = createEvent(withKey = false)
    val create2 = createEvent(withKey = true)
    val archive1 = archiveEvent(create1)
    val archivedPrevious = archiveEvent(previousCreate)

    val batch = NonEmptyVector.of(create1, create2, archive1, archivedPrevious)

    val expectedContractStateUpdates = Map(
      create1.contractId -> ContractStateStatus.Archived,
      create2.contractId -> ContractStateStatus.Active,
      previousCreate.contractId -> ContractStateStatus.Archived,
    )
    val expectedKeyStateUpdates = Map(
      create2.globalKey.value -> keyAssigned(create2),
      previousCreate.globalKey.value -> ContractKeyStateValue.Unassigned,
    )

    contractStateCaches.push(batch, 4)
    verify(contractStateCache).putBatch(4, expectedContractStateUpdates)
    verify(keyStateCache).putBatch(4, expectedKeyStateUpdates)
  }

  "push" should "update the key state cache even if no key updates" in new TestScope {
    val create1 = createEvent(withKey = false)

    val batch = NonEmptyVector.of(create1)
    val expectedContractStateUpdates = Map(create1.contractId -> ContractStateStatus.Active)

    contractStateCaches.push(batch, 2)
    verify(contractStateCache).putBatch(2, expectedContractStateUpdates)
    verify(keyStateCache).putBatch(2, Map.empty)
  }

  "reset" should "reset the caches on `reset`" in new TestScope {
    private val someOffset = Some(
      LedgerEnd(
        lastOffset = Offset.tryFromLong(112243L),
        lastEventSeqId = 125,
        lastStringInterningId = 0,
        lastPublicationTime = CantonTimestamp.MinValue,
      )
    )

    contractStateCaches.reset(someOffset)
    verify(keyStateCache).reset(125)
    verify(contractStateCache).reset(125)
  }

  private trait TestScope {
    private val contractIdx: AtomicLong = new AtomicLong(0)
    private val keyIdx: AtomicLong = new AtomicLong(0)

    val keyStateCache: StateCache[Key, ContractKeyStateValue] =
      mock[StateCache[Key, ContractKeyStateValue]]
    val contractStateCache: StateCache[ContractId, ContractStateStatus] =
      mock[StateCache[ContractId, ContractStateStatus]]

    val contractStateCaches = new ContractStateCaches(
      keyStateCache,
      contractStateCache,
      loggerFactory,
    )

    def createEvent(
        withKey: Boolean
    ): ContractStateEvent.Created = {
      val cId = contractId(contractIdx.incrementAndGet())
      val templateId = Identifier.assertFromString(s"some:template:name")
      val packageName = Ref.PackageName.assertFromString("pkg-name")
      val key = Option.when(withKey)(
        Key.assertBuild(
          templateId,
          ValueInt64(keyIdx.incrementAndGet()),
          packageName,
        )
      )
      ContractStateEvent.Created(cId, key)
    }

    def archiveEvent(
        create: ContractStateEvent.Created
    ): ContractStateEvent.Archived =
      ContractStateEvent.Archived(
        contractId = create.contractId,
        globalKey = create.globalKey,
      )
  }

  private def keyAssigned(create: ContractStateEvent.Created) =
    ContractKeyStateValue.Assigned(create.contractId)

  private def contractId(id: Long): ContractId =
    ContractId.V1(Hash.hashPrivateKey(id.toString))
}
