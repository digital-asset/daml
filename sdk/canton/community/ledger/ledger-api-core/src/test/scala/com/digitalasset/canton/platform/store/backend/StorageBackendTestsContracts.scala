// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value.{ValueText, ValueUnit}
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsContracts
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  behavior of "StorageBackend (contracts)"

  it should "correctly find key states" in {
    val key1 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      ValueUnit,
      someTemplateId.pkg.name,
    )
    val key2 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      ValueText("value"),
      someTemplateId.pkg.name,
    )
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val internalContractId4 = 423L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId4,
        create_key_hash = Some(key2.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosCreate(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosCreate(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = internalContractId2,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosConsumingExercise(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = Some(internalContractId2),
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosCreate(
        event_offset = 5L,
        event_sequential_id = 5L,
        internal_contract_id = internalContractId3,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(5), 5L)
    )
    val keyStates2 = executeSql(
      backend.contract.keyStatesNew(
        List(
          key1,
          key2,
        ),
        2L,
      )
    )
    val keyStateKey1_2 = executeSql(
      backend.contract.keyStateNew(key1, 2L)
    )
    val keyStateKey2_2 = executeSql(
      backend.contract.keyStateNew(key2, 2L)
    )
    val keyStates3 = executeSql(
      backend.contract.keyStatesNew(
        List(
          key1,
          key2,
        ),
        3L,
      )
    )
    val keyStateKey1_3 = executeSql(
      backend.contract.keyStateNew(key1, 3L)
    )
    val keyStateKey2_3 = executeSql(
      backend.contract.keyStateNew(key2, 3L)
    )
    val keyStates4 = executeSql(
      backend.contract.keyStatesNew(
        List(
          key1,
          key2,
        ),
        4L,
      )
    )
    val keyStateKey1_4 = executeSql(
      backend.contract.keyStateNew(key1, 4L)
    )
    val keyStateKey2_4 = executeSql(
      backend.contract.keyStateNew(key2, 4L)
    )
    val keyStates5 = executeSql(
      backend.contract.keyStatesNew(
        List(
          key1,
          key2,
        ),
        5L,
      )
    )
    val keyStateKey1_5 = executeSql(
      backend.contract.keyStateNew(key1, 5L)
    )
    val keyStateKey2_5 = executeSql(
      backend.contract.keyStateNew(key2, 5L)
    )

    keyStates2 shouldBe Map(
      key1 -> internalContractId,
      key2 -> internalContractId4,
    )
    keyStateKey1_2 shouldBe Some(internalContractId)
    keyStateKey2_2 shouldBe Some(internalContractId4)
    keyStates3 shouldBe Map(
      key1 -> internalContractId2,
      key2 -> internalContractId4,
    )
    keyStateKey1_3 shouldBe Some(internalContractId2)
    keyStateKey2_3 shouldBe Some(internalContractId4)
    keyStates4 shouldBe Map(
      key2 -> internalContractId4
    )
    keyStateKey1_4 shouldBe None
    keyStateKey2_4 shouldBe Some(internalContractId4)
    keyStates5 shouldBe Map(
      key1 -> internalContractId3,
      key2 -> internalContractId4,
    )
    keyStateKey1_5 shouldBe Some(internalContractId3)
    keyStateKey2_5 shouldBe Some(internalContractId4)
  }

  it should "correctly find active contracts" in {
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosAssign(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId2,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosAssign(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = internalContractId3,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosAssign(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = internalContractId3,
        synchronizer_id = someSynchronizerId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(3), 3L)
    )
    val activeContracts2 = executeSql(
      backend.contract.activeContractsNew(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        2L,
      )
    )
    val activeContracts3 = executeSql(
      backend.contract.activeContractsNew(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        3L,
      )
    )
    val activeIds = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIds(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 1000,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 1000L,
        )
      )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivationsNew(
        List(
          someSynchronizerId -> internalContractId,
          someSynchronizerId -> internalContractId3,
          someSynchronizerId2 -> internalContractId2,
          someSynchronizerId2 -> internalContractId3,
        )
      )
    )

    activeContracts2 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
    )
    activeContracts3 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
      internalContractId3 -> true,
    )
    activeIds shouldBe Vector(1L, 2L, 3L, 4L)
    lastActivations shouldBe Map(
      (someSynchronizerId, internalContractId) -> 1L,
      (someSynchronizerId2, internalContractId2) -> 2L,
      (someSynchronizerId2, internalContractId3) -> 3L,
    )
  }

  it should "correctly find deactivated contracts" in {
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosAssign(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId2,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosUnassign(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = Some(internalContractId),
        deactivated_event_sequential_id = Some(1L),
        synchronizer_id = someSynchronizerId,
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
      dtosConsumingExercise(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = Some(internalContractId2),
        deactivated_event_sequential_id = Some(2L),
        synchronizer_id = someSynchronizerId2,
        stakeholders = Set(signatory),
        template_id = someTemplateId.toString(),
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(4), 4L)
    )
    val activeContracts2 = executeSql(
      backend.contract.activeContractsNew(
        List(
          internalContractId,
          internalContractId2,
        ),
        2L,
      )
    )
    val activeContracts4 = executeSql(
      backend.contract.activeContractsNew(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        4L,
      )
    )
    val activeIds2 = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIds(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 2L,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 2L,
        )
      )
    )
    val activeIds4 = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIds(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 4,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 4L,
        )
      )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivationsNew(
        List(
          someSynchronizerId -> internalContractId,
          someSynchronizerId2 -> internalContractId2,
        )
      )
    )

    activeContracts2 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
    )
    activeContracts4 shouldBe Map(
      internalContractId -> true, // although deactivated, this logic only cares about archivals
      internalContractId2 -> false,
    )
    activeIds2 shouldBe Vector(1L, 2L)
    activeIds4 shouldBe Vector.empty
    // lastActivation does not care about deactivations
    lastActivations shouldBe Map(
      (someSynchronizerId, internalContractId) -> 1L,
      (someSynchronizerId2, internalContractId2) -> 2L,
    )
  }

  behavior of "StorageBackend (contracts) legacy"

  it should "correctly find an active contract" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreateLegacy(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.IdFilterCreateStakeholder(
        event_sequential_id = 1L,
        template_id = someTemplateId.toString,
        party_id = signatory,
        first_per_sequential_id = true,
      ),
      dtoCompletion(offset(1)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(1), 1L)
    )
    val createdContracts = executeSql(
      backend.contract.createdContracts(contractId :: Nil, 1)
    )
    val archivedContracts = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, 1)
    )
    val activeCreateIds = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIdsOfCreateEventsForStakeholderLegacy(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 1000,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 1000L,
        )
      )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivations(
        List(
          someSynchronizerId -> contractId
        )
      )
    )

    createdContracts should contain(contractId)
    archivedContracts shouldBe empty
    activeCreateIds shouldBe Vector(1L)
    lastActivations shouldBe Map(
      (someSynchronizerId, contractId) -> 1L
    )
  }

  it should "not find an active contract with empty flat event witnesses" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node with no flat event witnesses
      dtoCreateLegacy(
        offset(1),
        1L,
        contractId = contractId,
        signatory = signatory,
        emptyFlatEventWitnesses = true,
      ),
      DbDto.IdFilterCreateNonStakeholderInformee(
        1L,
        someTemplateId.toString,
        signatory,
        first_per_sequential_id = true,
      ),
      dtoCompletion(offset(1)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(1), 1L)
    )
    val createdContracts = executeSql(
      backend.contract.createdContracts(contractId :: Nil, 1)
    )
    val archivedContracts = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, 1)
    )
    val activeCreateIds = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIdsOfCreateEventsForStakeholderLegacy(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 1000,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 1000L,
        )
      )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivations(
        List(
          someSynchronizerId -> contractId
        )
      )
    )

    createdContracts shouldBe empty
    archivedContracts shouldBe empty
    activeCreateIds shouldBe Vector.empty
    // Last activation for divulged contracts can be looked up with this query, but we ensure in code that we won't do this:
    // only divulged deactivation can have a divulged activation pair.
    lastActivations shouldBe Map(
      (someSynchronizerId, contractId) -> 1L
    )
  }

  it should "correctly find a contract from assigned table" in {
    val contractId1 = hashCid("#1")
    val contractId2 = hashCid("#2")
    val contractId3 = hashCid("#3")
    val observer2 = Ref.Party.assertFromString("observer2")

    val dtos: Vector[DbDto] = Vector(
      dtoAssignLegacy(offset(1), 1L, contractId1),
      dtoAssignLegacy(offset(2), 2L, contractId1, observer = observer2),
      dtoAssignLegacy(offset(3), 3L, contractId2),
      dtoAssignLegacy(offset(4), 4L, contractId2, observer = observer2),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(4), 4L)
    )
    val assignedContracts1 = executeSql(
      backend.contract
        .assignedContracts(Seq(contractId1, contractId2, contractId3), 4)
    )
    val assignedContracts2 = executeSql(
      backend.contract
        .assignedContracts(Seq(contractId1, contractId2, contractId3), 2)
    )
    assignedContracts1.size shouldBe 2
    assignedContracts1 should contain(contractId1)
    assignedContracts1 should contain(contractId2)
    assignedContracts2.size shouldBe 1
    assignedContracts2 should contain(contractId1)
    assignedContracts2.contains(contractId2) shouldBe false
  }

  it should "not find an archived contract" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreateLegacy(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.IdFilterCreateStakeholder(
        1L,
        someTemplateId.toString,
        signatory,
        first_per_sequential_id = true,
      ),
      dtoCompletion(offset(1)),
      // 2: transaction that archives the contract
      dtoExerciseLegacy(
        offset(2),
        2L,
        consuming = true,
        contractId,
        deactivatedEventSeqId = Some(1L),
      ),
      dtoCompletion(offset(2)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(2), 2L)
    )
    val createdContracts1 = executeSql(
      backend.contract.createdContracts(contractId :: Nil, 1)
    )
    val archivedContracts1 = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, 1)
    )
    val createdContracts2 = executeSql(
      backend.contract.createdContracts(contractId :: Nil, 2)
    )
    val archivedContracts2 = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, 2)
    )
    val activeCreateIds = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIdsOfCreateEventsForStakeholderLegacy(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 1000,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 1000L,
        )
      )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivations(
        List(
          someSynchronizerId -> contractId
        )
      )
    )

    createdContracts1 should contain(contractId)
    archivedContracts1 should not contain contractId
    createdContracts2 should contain(contractId)
    archivedContracts2 should contain(contractId)
    activeCreateIds shouldBe Vector.empty
    lastActivations shouldBe Map(
      (someSynchronizerId, contractId) -> 1L
    )
  }

  it should "not find an archived contract with empty flat event witnesses" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreateLegacy(
        offset(1),
        1L,
        contractId = contractId,
        signatory = signatory,
        emptyFlatEventWitnesses = true,
      ),
      DbDto.IdFilterCreateNonStakeholderInformee(
        1L,
        someTemplateId.toString,
        signatory,
        first_per_sequential_id = true,
      ),
      dtoCompletion(offset(1)),
      // 2: transaction that archives the contract
      dtoExerciseLegacy(
        offset(2),
        2L,
        consuming = true,
        contractId,
        emptyFlatEventWitnesses = true,
      ),
      dtoCompletion(offset(2)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(2), 2L)
    )
    val createdContracts1 = executeSql(
      backend.contract.createdContracts(contractId :: Nil, 1)
    )
    val archivedContracts1 = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, 1)
    )
    val createdContracts2 = executeSql(
      backend.contract.createdContracts(contractId :: Nil, 2)
    )
    val archivedContracts2 = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, 2)
    )

    createdContracts1 shouldBe empty
    archivedContracts1 shouldBe empty
    createdContracts2 shouldBe empty
    archivedContracts2 shouldBe empty
  }

  it should "retrieve multiple contracts correctly for batched contract state query" in {
    val contractId1 = hashCid("#1")
    val contractId2 = hashCid("#2")
    val contractId3 = hashCid("#3")
    val contractId4 = hashCid("#4")
    val contractId5 = hashCid("#5")
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create nodes
      dtoCreateLegacy(offset(1), 1L, contractId = contractId1, signatory = signatory),
      dtoCreateLegacy(offset(1), 2L, contractId = contractId2, signatory = signatory),
      dtoCreateLegacy(offset(1), 3L, contractId = contractId3, signatory = signatory),
      dtoCreateLegacy(offset(1), 4L, contractId = contractId4, signatory = signatory),
      // 2: transaction that archives the contract
      dtoExerciseLegacy(offset(2), 5L, consuming = true, contractId1),
      // 3: transaction that creates one more contract
      dtoCreateLegacy(offset(3), 6L, contractId = contractId5, signatory = signatory),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(3), 6L)
    )
    val createdContracts = executeSql(
      backend.contract.createdContracts(
        List(
          contractId1,
          contractId2,
          contractId3,
          contractId4,
          contractId5,
        ),
        5L,
      )
    )
    val archivedContracts = executeSql(
      backend.contract.archivedContracts(
        List(
          contractId1,
          contractId2,
          contractId3,
          contractId4,
          contractId5,
        ),
        5L,
      )
    )

    createdContracts shouldBe Set(
      contractId1,
      contractId2,
      contractId3,
      contractId4,
    )
    archivedContracts shouldBe Set(
      contractId1
    )
  }

  it should "be able to query with 1000 contract ids" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      updateLedgerEnd(offset(3), 6L)
    )
    val createdContracts = executeSql(
      backend.contract.createdContracts(
        1.to(1000).map(n => hashCid(s"#$n")),
        2,
      )
    )
    val archivedContracts = executeSql(
      backend.contract.archivedContracts(
        1.to(1000).map(n => hashCid(s"#$n")),
        2,
      )
    )

    createdContracts shouldBe empty
    archivedContracts shouldBe empty
  }
}
