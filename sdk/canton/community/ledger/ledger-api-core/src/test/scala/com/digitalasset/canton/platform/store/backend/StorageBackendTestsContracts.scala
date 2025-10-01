// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsContracts
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (contracts)"

  import StorageBackendTestValues.*

  it should "correctly find an active contract" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
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
      backend.contract.createdContracts(contractId :: Nil, offset(1))
    )
    val archivedContracts = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, offset(1))
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
      dtoCreate(
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
      backend.contract.createdContracts(contractId :: Nil, offset(1))
    )
    val archivedContracts = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, offset(1))
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
      dtoAssign(offset(1), 1L, contractId1),
      dtoAssign(offset(2), 2L, contractId1, observer = observer2),
      dtoAssign(offset(3), 3L, contractId2),
      dtoAssign(offset(4), 4L, contractId2, observer = observer2),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(4), 4L)
    )
    val assignedContracts1 = executeSql(
      backend.contract
        .assignedContracts(Seq(contractId1, contractId2, contractId3), offset(4))
    )
    val assignedContracts2 = executeSql(
      backend.contract
        .assignedContracts(Seq(contractId1, contractId2, contractId3), offset(2))
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
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.IdFilterCreateStakeholder(
        1L,
        someTemplateId.toString,
        signatory,
        first_per_sequential_id = true,
      ),
      dtoCompletion(offset(1)),
      // 2: transaction that archives the contract
      dtoExercise(offset(2), 2L, consuming = true, contractId, deactivatedEventSeqId = Some(1L)),
      dtoCompletion(offset(2)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(2), 2L)
    )
    val createdContracts1 = executeSql(
      backend.contract.createdContracts(contractId :: Nil, offset(1))
    )
    val archivedContracts1 = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, offset(1))
    )
    val createdContracts2 = executeSql(
      backend.contract.createdContracts(contractId :: Nil, offset(2))
    )
    val archivedContracts2 = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, offset(2))
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
      dtoCreate(
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
      dtoExercise(offset(2), 2L, consuming = true, contractId, emptyFlatEventWitnesses = true),
      dtoCompletion(offset(2)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(2), 2L)
    )
    val createdContracts1 = executeSql(
      backend.contract.createdContracts(contractId :: Nil, offset(1))
    )
    val archivedContracts1 = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, offset(1))
    )
    val createdContracts2 = executeSql(
      backend.contract.createdContracts(contractId :: Nil, offset(2))
    )
    val archivedContracts2 = executeSql(
      backend.contract.archivedContracts(contractId :: Nil, offset(2))
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
      dtoCreate(offset(1), 1L, contractId = contractId1, signatory = signatory),
      dtoCreate(offset(1), 2L, contractId = contractId2, signatory = signatory),
      dtoCreate(offset(1), 3L, contractId = contractId3, signatory = signatory),
      dtoCreate(offset(1), 4L, contractId = contractId4, signatory = signatory),
      // 2: transaction that archives the contract
      dtoExercise(offset(2), 5L, consuming = true, contractId1),
      // 3: transaction that creates one more contract
      dtoCreate(offset(3), 6L, contractId = contractId5, signatory = signatory),
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
        offset(2),
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
        offset(2),
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
        offset(2),
      )
    )
    val archivedContracts = executeSql(
      backend.contract.archivedContracts(
        1.to(1000).map(n => hashCid(s"#$n")),
        offset(2),
      )
    )

    createdContracts shouldBe empty
    archivedContracts shouldBe empty
  }
}
