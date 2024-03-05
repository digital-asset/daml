// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.lf.value.Value.ContractId
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
    val observer = Ref.Party.assertFromString("observer")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatory),
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

    createdContracts.get(contractId).isDefined shouldBe true
    createdContracts.get(contractId).foreach { c =>
      c.templateId shouldBe someTemplateId.toString
      c.createArgumentCompression shouldBe None
      c.flatEventWitnesses shouldBe Set(signatory, observer)
    }
    archivedContracts.isEmpty shouldBe true
  }

  it should "correctly find a contract from assigned table" in {
    val contractId1 = hashCid("#1")
    val contractId2 = hashCid("#2")
    val contractId3 = hashCid("#3")
    val signatory = Ref.Party.assertFromString("signatory")
    val observer = Ref.Party.assertFromString("observer")
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
    val assignedContracts = executeSql(
      backend.contract.assignedContracts(Seq(contractId1, contractId2, contractId3))
    )
    assignedContracts.size shouldBe 2
    assignedContracts.get(contractId1).isDefined shouldBe true
    assignedContracts.get(contractId1).foreach { raw =>
      raw.templateId shouldBe someTemplateId.toString
      raw.createArgumentCompression shouldBe Some(123)
      raw.flatEventWitnesses shouldBe Set(signatory, observer)
      raw.signatories shouldBe Set(signatory)
    }
    assignedContracts.get(contractId2).isDefined shouldBe true
    assignedContracts.get(contractId2).foreach { raw =>
      raw.templateId shouldBe someTemplateId.toString
      raw.createArgumentCompression shouldBe Some(123)
      raw.flatEventWitnesses shouldBe Set(signatory, observer)
      raw.signatories shouldBe Set(signatory)
    }
  }

  it should "not find an archived contract" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")
    val observer = Ref.Party.assertFromString("observer")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatory),
      dtoCompletion(offset(1)),
      // 2: transaction that archives the contract
      dtoExercise(offset(2), 2L, true, contractId),
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

    createdContracts1.get(contractId).isDefined shouldBe true
    createdContracts1.get(contractId).foreach { c =>
      c.templateId shouldBe someTemplateId.toString
      c.createArgumentCompression shouldBe None
      c.flatEventWitnesses shouldBe Set(signatory, observer)
    }
    archivedContracts1.get(contractId) shouldBe None
    createdContracts2.get(contractId).isDefined shouldBe true
    createdContracts2.get(contractId).foreach { c =>
      c.templateId shouldBe someTemplateId.toString
      c.createArgumentCompression shouldBe None
      c.flatEventWitnesses shouldBe Set(signatory, observer)
    }
    archivedContracts2.get(contractId).isDefined shouldBe true
    archivedContracts2.get(contractId).foreach { c =>
      c.flatEventWitnesses shouldBe Set(signatory)
    }
  }

  it should "retrieve multiple contracts correctly for batched contract state query" in {
    val contractId1 = hashCid("#1")
    val contractId2 = hashCid("#2")
    val contractId3 = hashCid("#3")
    val contractId4 = hashCid("#4")
    val contractId5 = hashCid("#5")
    val signatory = Ref.Party.assertFromString("signatory")
    val observer = Ref.Party.assertFromString("observer")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create nodes
      dtoCreate(offset(1), 1L, contractId = contractId1, signatory = signatory),
      dtoCreate(offset(1), 2L, contractId = contractId2, signatory = signatory),
      dtoCreate(offset(1), 3L, contractId = contractId3, signatory = signatory),
      dtoCreate(offset(1), 4L, contractId = contractId4, signatory = signatory),
      // 2: transaction that archives the contract
      dtoExercise(offset(2), 5L, true, contractId1),
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

    createdContracts.keySet shouldBe Set(
      contractId1,
      contractId2,
      contractId3,
      contractId4,
    )
    def assertContract(
        contractId: ContractId,
        witnesses: Set[Ref.Party] = Set(signatory, observer),
    ) = {
      createdContracts(contractId).templateId shouldBe someTemplateId.toString
      createdContracts(contractId).createArgumentCompression shouldBe None
      createdContracts(contractId).flatEventWitnesses shouldBe witnesses
    }
    assertContract(contractId1)
    assertContract(contractId2)
    assertContract(contractId3)
    assertContract(contractId4)
    archivedContracts.keySet shouldBe Set(
      contractId1
    )
    archivedContracts(contractId1).flatEventWitnesses shouldBe Set(signatory)
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

    createdContracts shouldBe Map.empty
    archivedContracts shouldBe Map.empty
  }
}
