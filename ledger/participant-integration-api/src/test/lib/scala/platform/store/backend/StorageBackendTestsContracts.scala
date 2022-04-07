// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.lf.data.Ref
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

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.CreateFilter(1L, someTemplateId.toString, signatory),
      dtoCompletion(offset(1)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(1), 1L)
    )
    val templateIdO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(signatory), contractId)
    )

    templateIdO shouldBe Some(someTemplateId.toString)
  }

  it should "not find an archived contract" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.CreateFilter(1L, someTemplateId.toString, signatory),
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
    val templateIdO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(signatory), contractId)
    )

    templateIdO shouldBe None
  }

  it should "correctly find a divulged contract" in {
    val contractId = hashCid("#1")
    val divulgee = Ref.Party.assertFromString("divulgee")

    val dtos: Vector[DbDto] = Vector(
      // 1: divulgence
      dtoDivulgence(Some(offset(1)), 1L, contractId = contractId, divulgee = divulgee),
      DbDto.CreateFilter(1L, someTemplateId.toString, divulgee),
      dtoCompletion(offset(1)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(1), 1L)
    )
    val templateIdO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(divulgee), contractId)
    )

    templateIdO shouldBe Some(someTemplateId.toString)
  }

  it should "correctly find an active contract that is also divulged" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")
    val divulgee = Ref.Party.assertFromString("divulgee")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.CreateFilter(1L, someTemplateId.toString, signatory),
      dtoCompletion(offset(1)),
      // 2: divulgence without any optional information
      dtoDivulgence(Some(offset(2)), 2L, contractId = contractId, divulgee = divulgee)
        .copy(template_id = None, create_argument = None, create_argument_compression = None),
      DbDto.CreateFilter(2L, someTemplateId.toString, divulgee),
      dtoCompletion(offset(2)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(2), 2L)
    )
    val templateIdO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(divulgee), contractId)
    )

    templateIdO shouldBe Some(someTemplateId.toString)
  }

  it should "not disclose to divulgees that a contract was archived" in {
    val contractId = hashCid("#1")
    val signatory = Ref.Party.assertFromString("signatory")
    val divulgee = Ref.Party.assertFromString("divulgee")

    val dtos: Vector[DbDto] = Vector(
      // 1: transaction with create node
      dtoCreate(offset(1), 1L, contractId = contractId, signatory = signatory),
      DbDto.CreateFilter(1L, someTemplateId.toString, signatory),
      dtoCompletion(offset(1)),
      // 2: divulgence
      dtoDivulgence(Some(offset(2)), 2L, contractId = contractId, divulgee = divulgee),
      DbDto.CreateFilter(2L, someTemplateId.toString, divulgee),
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
    val templateIdDivulgeeO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(divulgee), contractId)
    )
    val templateIdSignatoryO = executeSql(
      backend.contract.activeContractWithoutArgument(Set(signatory), contractId)
    )

    // The divulgee still sees the contract
    templateIdDivulgeeO shouldBe Some(someTemplateId.toString)
    // The signatory knows it's archived
    templateIdSignatoryO shouldBe None
  }

}
