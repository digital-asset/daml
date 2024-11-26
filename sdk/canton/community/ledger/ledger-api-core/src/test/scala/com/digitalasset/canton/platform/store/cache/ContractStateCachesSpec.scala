// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.digitalasset.canton.data.AbsoluteOffset
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.{HasExecutionContext, TestEssentials}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{GlobalKey, Versioned}
import com.digitalasset.daml.lf.value.Value.{ContractInstance, ValueInt64, ValueRecord}
import org.mockito.MockitoSugar
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger

class ContractStateCachesSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with OptionValues
    with TestEssentials
    with HasExecutionContext {
  behavior of classOf[ContractStateCaches].getSimpleName

  "build" should "set the cache index to the initialization index" in {
    val cacheInitializationOffset = offset(1337)
    @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
    val contractStateCaches = ContractStateCaches.build(
      Some(cacheInitializationOffset),
      maxContractsCacheSize = 1L,
      maxKeyCacheSize = 1L,
      metrics = LedgerApiServerMetrics.ForTesting,
      loggerFactory,
    )

    contractStateCaches.keyState.cacheIndex shouldBe Some(cacheInitializationOffset)
    contractStateCaches.contractState.cacheIndex shouldBe Some(cacheInitializationOffset)
  }

  "push" should "update the caches with a batch of events" in new TestScope {
    val previousCreate = createEvent(offset = offset(1), withKey = true)

    val create1 = createEvent(offset = offset(2), withKey = false)
    val create2 = createEvent(offset = offset(3), withKey = true)
    val archive1 = archiveEvent(create1, offset(3))
    val archivedPrevious = archiveEvent(previousCreate, offset(4))

    val batch = NonEmptyVector.of(create1, create2, archive1, archivedPrevious)

    val expectedContractStateUpdates = Map(
      create1.contractId -> contractArchived(create1),
      create2.contractId -> contractActive(create2),
      previousCreate.contractId -> contractArchived(previousCreate),
    )
    val expectedKeyStateUpdates = Map(
      create2.globalKey.value -> keyAssigned(create2),
      previousCreate.globalKey.value -> ContractKeyStateValue.Unassigned,
    )

    contractStateCaches.push(batch)
    verify(contractStateCache).putBatch(offset(4), expectedContractStateUpdates)
    verify(keyStateCache).putBatch(offset(4), expectedKeyStateUpdates)
  }

  "push" should "not update the key state cache if no key updates" in new TestScope {
    val create1 = createEvent(offset = offset(2), withKey = false)

    val batch = NonEmptyVector.of(create1)
    val expectedContractStateUpdates = Map(create1.contractId -> contractActive(create1))

    contractStateCaches.push(batch)
    verify(contractStateCache).putBatch(offset(2), expectedContractStateUpdates)
    verifyZeroInteractions(keyStateCache)
  }

  "reset" should "reset the caches on `reset`" in new TestScope {
    private val someOffset = Some(AbsoluteOffset.tryFromLong(112233L))

    contractStateCaches.reset(someOffset)
    verify(keyStateCache).reset(someOffset)
    verify(contractStateCache).reset(someOffset)
  }

  private trait TestScope {
    private val contractIdx: AtomicInteger = new AtomicInteger(0)
    private val keyIdx: AtomicInteger = new AtomicInteger(0)

    val keyStateCache: StateCache[Key, ContractKeyStateValue] =
      mock[StateCache[Key, ContractKeyStateValue]]
    val contractStateCache: StateCache[ContractId, ContractStateValue] =
      mock[StateCache[ContractId, ContractStateValue]]

    val contractStateCaches = new ContractStateCaches(
      keyStateCache,
      contractStateCache,
      loggerFactory,
    )

    def createEvent(
        offset: AbsoluteOffset,
        withKey: Boolean,
    ): ContractStateEvent.Created = {
      val cId = contractIdx.incrementAndGet()
      ContractStateEvent.Created(
        contractId = contractId(cId),
        contract = contract(cId),
        globalKey = if (withKey) Some(globalKey(keyIdx.incrementAndGet())) else None,
        ledgerEffectiveTime = Time.Timestamp(cId.toLong),
        stakeholders = Set(Ref.Party.assertFromString(s"party-$cId")),
        eventOffset = offset,
        signatories = Set(Ref.Party.assertFromString(s"party-$cId")),
        keyMaintainers = None,
        driverMetadata = Array.empty,
      )
    }

    def archiveEvent(
        create: ContractStateEvent.Created,
        offset: AbsoluteOffset,
    ): ContractStateEvent.Archived =
      ContractStateEvent.Archived(
        contractId = create.contractId,
        globalKey = create.globalKey,
        stakeholders = create.stakeholders,
        eventOffset = offset,
      )
  }

  private def contractActive(create: ContractStateEvent.Created) =
    ContractStateValue.Active(
      create.contract,
      create.stakeholders,
      create.ledgerEffectiveTime,
      create.signatories,
      create.globalKey,
      create.keyMaintainers,
      create.driverMetadata,
    )

  private def contractArchived(create: ContractStateEvent.Created) =
    ContractStateValue.Archived(create.stakeholders)

  private def keyAssigned(create: ContractStateEvent.Created) =
    ContractKeyStateValue.Assigned(
      create.contractId,
      create.stakeholders,
    )

  private def contractId(id: Int): ContractId =
    ContractId.V1(Hash.hashPrivateKey(id.toString))

  private def globalKey(id: Int): Key =
    GlobalKey.assertBuild(
      Identifier.assertFromString(s"some:template:name"),
      ValueInt64(id.toLong),
      Ref.PackageName.assertFromString("pkg-name"),
    )

  private def contract(id: Int): Contract = {
    val templateId = Identifier.assertFromString(s"some:template:name")
    val packageName = Ref.PackageName.assertFromString("pkg-name")
    val contractArgument = ValueRecord(
      Some(templateId),
      ImmArray(None -> ValueInt64(id.toLong)),
    )
    val contractInstance =
      ContractInstance(packageName = packageName, template = templateId, arg = contractArgument)
    Versioned(LanguageVersion.StableVersions(LanguageVersion.Major.V2).max, contractInstance)
  }

  private def offset(idx: Int) = AbsoluteOffset.tryFromLong(idx.toLong)
}
