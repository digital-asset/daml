// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

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

  import StorageBackendTestValues._

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

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(1), 1L)
    )
    val rawContractO = executeSql(
      backend.contract.activeContractWithArgument(Set(signatory), contractId)
    )
    val templateIdO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(signatory), contractId)
    )
    val contractState = executeSql(
      backend.contract.contractState(contractId, offset(1))
    )
    val contractStates = executeSql(
      backend.contract.contractStates(contractId :: Nil, offset(1))
    )

    templateIdO shouldBe Some(someTemplateId.toString)
    inside(rawContractO) { case Some(rawContract) =>
      rawContract.templateId shouldBe someTemplateId.toString
      rawContract.createArgumentCompression shouldBe None
    }
    contractState.get.templateId shouldBe Some(someTemplateId.toString)
    contractState.get.createArgumentCompression shouldBe None
    contractState.get.flatEventWitnesses shouldBe Set(signatory, observer)
    contractState.get.eventKind shouldBe 10
    contractStates.get(contractId).map(_.copy(createArgument = None)) shouldBe contractState.map(
      _.copy(createArgument = None)
    )
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

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(2), 2L)
    )
    val rawContractO = executeSql(
      backend.contract.activeContractWithArgument(Set(signatory), contractId)
    )
    val templateIdO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(signatory), contractId)
    )
    val contractState1 = executeSql(
      backend.contract.contractState(contractId, offset(1))
    )
    val contractStates1 = executeSql(
      backend.contract.contractStates(contractId :: Nil, offset(1))
    )
    val contractState2 = executeSql(
      backend.contract.contractState(contractId, offset(2))
    )
    val contractStates2 = executeSql(
      backend.contract.contractStates(contractId :: Nil, offset(2))
    )

    templateIdO shouldBe None
    rawContractO shouldBe None
    contractState1.get.templateId shouldBe Some(someTemplateId.toString)
    contractState1.get.createArgumentCompression shouldBe None
    contractState1.get.flatEventWitnesses shouldBe Set(signatory, observer)
    contractState1.get.eventKind shouldBe 10
    contractStates1.get(contractId).map(_.copy(createArgument = None)) shouldBe contractState1.map(
      _.copy(createArgument = None)
    )
    contractState2.get.templateId shouldBe Some(someTemplateId.toString)
    contractState2.get.createArgumentCompression shouldBe None
    contractState2.get.flatEventWitnesses shouldBe Set(signatory)
    contractState2.get.eventKind shouldBe 20
    contractStates2.get(contractId).map(_.copy(createArgument = None)) shouldBe contractState2.map(
      _.copy(createArgument = None)
    )
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

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(3), 6L)
    )
    val contractStates = executeSql(
      backend.contract.contractStates(
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

    contractStates.keySet shouldBe Set(
      contractId1,
      contractId2,
      contractId3,
      contractId4,
    )
    def assertContract(
        contractId: ContractId,
        eventKind: Int = 10,
        witnesses: Set[Ref.Party] = Set(signatory, observer),
    ) = {
      contractStates(contractId).eventKind shouldBe eventKind
      contractStates(contractId).templateId shouldBe Some(someTemplateId.toString)
      contractStates(contractId).createArgumentCompression shouldBe None
      contractStates(contractId).flatEventWitnesses shouldBe witnesses
    }
    assertContract(contractId1, 20, Set(signatory))
    assertContract(contractId2)
    assertContract(contractId3)
    assertContract(contractId4)
  }

  it should "be able to query with 1000 contract ids" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(
      updateLedgerEnd(offset(3), 6L)
    )
    val contractStates = executeSql(
      backend.contract.contractStates(
        1.to(1000).map(n => hashCid(s"#$n")),
        offset(2),
      )
    )
    contractStates shouldBe Map.empty
  }

  it should "correctly find a divulged contract" in {
    val contractId = hashCid("#1")
    val divulgee = Ref.Party.assertFromString("divulgee")

    val dtos: Vector[DbDto] = Vector(
      // 1: divulgence
      dtoDivulgence(Some(offset(1)), 1L, contractId = contractId, divulgee = divulgee),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, divulgee),
      dtoCompletion(offset(1)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(1), 1L)
    )
    val rawContractO = executeSql(
      backend.contract.activeContractWithArgument(Set(divulgee), contractId)
    )
    val templateIdO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(divulgee), contractId)
    )

    templateIdO shouldBe Some(someTemplateId.toString)
    inside(rawContractO) { case Some(rawContract) =>
      rawContract.templateId shouldBe someTemplateId.toString
      rawContract.createArgumentCompression shouldBe None
    }
  }

  it should "correctly find an active contract that is also divulged" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")
    val divulgee = Ref.Party.assertFromString("divulgee")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatory),
      dtoCompletion(offset(1)),
      // 2: divulgence without any optional information
      dtoDivulgence(Some(offset(2)), 2L, contractId = contractId, divulgee = divulgee)
        .copy(template_id = None, create_argument = None, create_argument_compression = None),
      DbDto.IdFilterCreateStakeholder(2L, someTemplateId.toString, divulgee),
      dtoCompletion(offset(2)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(2), 2L)
    )
    val rawContractO = executeSql(
      backend.contract.activeContractWithArgument(Set(divulgee), contractId)
    )
    val templateIdO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(divulgee), contractId)
    )

    templateIdO shouldBe Some(someTemplateId.toString)
    inside(rawContractO) { case Some(rawContract) =>
      rawContract.templateId shouldBe someTemplateId.toString
      rawContract.createArgumentCompression shouldBe None
    }
  }

  it should "not disclose to divulgees that a contract was archived" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")
    val divulgee = Ref.Party.assertFromString("divulgee")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, signatory),
      dtoCompletion(offset(1)),
      // 2: divulgence
      dtoDivulgence(Some(offset(2)), 2L, contractId = contractId, divulgee = divulgee),
      DbDto.IdFilterCreateStakeholder(2L, someTemplateId.toString, divulgee),
      dtoCompletion(offset(2)),
      // 3: transaction that archives the contract
      dtoExercise(offset(3), 3L, true, contractId, signatory = signatory),
      dtoCompletion(offset(3)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(3), 3L)
    )
    val rawContractDivulgeeO = executeSql(
      backend.contract.activeContractWithArgument(Set(divulgee), contractId)
    )
    val templateIdDivulgeeO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(divulgee), contractId)
    )
    val rawContractSignatoryO = executeSql(
      backend.contract.activeContractWithArgument(Set(signatory), contractId)
    )
    val templateIdSignatoryO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(signatory), contractId)
    )

    // The divulgee still sees the contract
    templateIdDivulgeeO shouldBe Some(someTemplateId.toString)
    inside(rawContractDivulgeeO) { case Some(rawContract) =>
      rawContract.templateId shouldBe someTemplateId.toString
      rawContract.createArgumentCompression shouldBe None
    }
    // The signatory knows it's archived
    templateIdSignatoryO shouldBe None
    rawContractSignatoryO shouldBe None
  }

}
