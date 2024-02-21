// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.lf.crypto.Hash
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.{GlobalKey, TransactionVersion, Versioned}
import com.daml.lf.value.Value.{ContractInstance, ValueInt64, ValueRecord}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.{HasExecutionContext, TestEssentials}
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
      cacheInitializationOffset,
      maxContractsCacheSize = 1L,
      maxKeyCacheSize = 1L,
      metrics = Metrics.ForTesting,
      loggerFactory,
    )

    contractStateCaches.keyState.cacheIndex shouldBe cacheInitializationOffset
    contractStateCaches.contractState.cacheIndex shouldBe cacheInitializationOffset
  }

  "push" should "update the caches with a batch of events" in new TestScope {
    val previousCreate = createEvent(offset = offset(1), eventSequentialId = 1, withKey = true)

    val create1 = createEvent(offset = offset(2), eventSequentialId = 2, withKey = false)
    val create2 = createEvent(offset = offset(3), eventSequentialId = 3, withKey = true)
    val archive1 = archiveEvent(create1, offset(3), eventSequentialId = 4)
    val archivedPrevious = archiveEvent(previousCreate, offset(4), eventSequentialId = 5)

    val batch = Vector(create1, create2, archive1, archivedPrevious)

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
    val create1 = createEvent(offset = offset(2), eventSequentialId = 2, withKey = false)

    val batch = Vector(create1)
    val expectedContractStateUpdates = Map(create1.contractId -> contractActive(create1))

    contractStateCaches.push(batch)
    verify(contractStateCache).putBatch(offset(2), expectedContractStateUpdates)
    verifyZeroInteractions(keyStateCache)
  }

  "push" should "ignore empty batches" in new TestScope {
    loggerFactory.assertLogs(
      within = contractStateCaches.push(Vector.empty),
      assertions = _.errorMessage should include("push triggered with empty events batch"),
    )

    verifyZeroInteractions(contractStateCache, keyStateCache)
  }

  "reset" should "reset the caches on `reset`" in new TestScope {
    val someOffset = Offset.fromHexString(Ref.HexString.assertFromString("aabbcc"))

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
        offset: Offset,
        eventSequentialId: Long,
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
        eventSequentialId = eventSequentialId,
        agreementText = None,
        signatories = Set(Ref.Party.assertFromString(s"party-$cId")),
        keyMaintainers = None,
        driverMetadata = None,
      )
    }

    def archiveEvent(
        create: ContractStateEvent.Created,
        offset: Offset,
        eventSequentialId: Long,
    ): ContractStateEvent.Archived =
      ContractStateEvent.Archived(
        contractId = create.contractId,
        globalKey = create.globalKey,
        stakeholders = create.stakeholders,
        eventOffset = offset,
        eventSequentialId = eventSequentialId,
      )
  }

  private def contractActive(create: ContractStateEvent.Created) =
    ContractStateValue.Active(
      create.contract,
      create.stakeholders,
      create.ledgerEffectiveTime,
      create.agreementText,
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
    Versioned(TransactionVersion.StableVersions.max, contractInstance)
  }

  private def offset(idx: Int) = Offset.fromByteArray(BigInt(idx.toLong).toByteArray)
}
