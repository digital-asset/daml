// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.protocol.{
  ContractMetadata,
  ExampleContractFactory,
  LfTemplateId,
  Stakeholders,
}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPartyId, ReassignmentCounter}
import org.scalatest.EitherValues.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ContractsReassignmentBatchTest extends AnyWordSpec with Matchers {
  private val contract1 = ExampleContractFactory.build()
  private val contract2 = ExampleContractFactory.build()
  private val contract3 = ExampleContractFactory.build()

  private val templateId = contract1.inst.templateId
  private val sourceValidationPackageId = Source(
    LfPackageId.assertFromString("source-validation-package-id")
  )
  private val targetValidationPackageId = Target(
    LfPackageId.assertFromString("target-validation-package-id")
  )
  private val packageName = contract1.inst.packageName
  private val stakeholders = Stakeholders(contract1.metadata)
  private val counter = ReassignmentCounter(1)

  "ContractsReassignmentBatch.apply" in {
    val batch = ContractsReassignmentBatch(
      contract1,
      sourceValidationPackageId,
      targetValidationPackageId,
      counter,
    )
    batch.contractIds.toList shouldBe List(contract1.contractId)
    batch.contracts.map(_.templateId) shouldBe Seq(templateId)
    batch.contracts.map(_.packageName) shouldBe Seq(packageName)
    batch.contracts.map(_.sourceValidationPackageId) shouldBe Seq(sourceValidationPackageId)
    batch.contracts.map(_.targetValidationPackageId) shouldBe Seq(targetValidationPackageId)
    batch.stakeholders shouldBe stakeholders
  }

  "ContractsReassignmentBatch.create" when {

    "no contracts" in {
      ContractsReassignmentBatch.create(Seq.empty) shouldBe Left(
        ContractsReassignmentBatch.EmptyBatch
      )
    }

    "just one contract" in {
      val batch =
        ContractsReassignmentBatch
          .create(
            Seq((contract1, sourceValidationPackageId, targetValidationPackageId, counter))
          )
          .value
      batch.contractIds.toList shouldBe List(contract1.contractId)
      batch.contracts.map(_.templateId) shouldBe Seq(templateId)
      batch.contracts.map(_.packageName) shouldBe Seq(packageName)
      batch.contracts.map(_.sourceValidationPackageId) shouldBe Seq(
        sourceValidationPackageId
      )
      batch.contracts.map(_.targetValidationPackageId) shouldBe Seq(
        targetValidationPackageId
      )
      batch.stakeholders shouldBe stakeholders
    }

    "multiple homogenous contracts" in {
      val batch = ContractsReassignmentBatch
        .create(
          Seq(contract1, contract2, contract3).map(contract =>
            (contract, sourceValidationPackageId, targetValidationPackageId, counter)
          )
        )
        .value
      batch.contractIds.toList shouldBe List(
        contract1.contractId,
        contract2.contractId,
        contract3.contractId,
      )
      batch.stakeholders shouldBe stakeholders
    }

    "contracts with different template ids" in {
      val newTemplateId = LfTemplateId.assertFromString(templateId.toString + "_but_different")
      val batch = ContractsReassignmentBatch
        .create(
          Seq(
            (contract1, sourceValidationPackageId, targetValidationPackageId, counter),
            (
              ExampleContractFactory.build(templateId = newTemplateId),
              sourceValidationPackageId,
              targetValidationPackageId,
              counter,
            ),
          )
        )
        .value

      batch.contracts.map(_.templateId) shouldBe Seq(templateId, newTemplateId)
    }

    "contracts with different package names" in {
      val newPackageName = LfPackageName.assertFromString("new_package_name")
      val batch = ContractsReassignmentBatch
        .create(
          Seq(
            (contract1, sourceValidationPackageId, targetValidationPackageId, counter),
            (
              ExampleContractFactory.build(packageName = newPackageName),
              sourceValidationPackageId,
              targetValidationPackageId,
              counter,
            ),
          )
        )
        .value
      batch.contracts.map(_.packageName) shouldBe Seq(packageName, newPackageName)
    }

    "contracts with different stakeholders" in {
      val newStakeholders = Stakeholders.tryCreate(
        stakeholders = stakeholders.all ++ Set(LfPartyId.assertFromString("extra_party")),
        signatories = stakeholders.signatories,
      )
      ContractsReassignmentBatch.create(
        Seq(
          (contract1, sourceValidationPackageId, targetValidationPackageId, counter),
          (
            ExampleContractFactory
              .modify(contract2, metadata = Some(ContractMetadata(newStakeholders))),
            sourceValidationPackageId,
            targetValidationPackageId,
            counter,
          ),
        )
      ) shouldBe Left(
        ContractsReassignmentBatch.DifferingStakeholders(Seq(newStakeholders, stakeholders))
      )
    }
  }

  "ContractsReassignmentBatch.partition" when {

    "no contracts" in {
      ContractsReassignmentBatch.partition(Seq.empty) shouldBe Seq.empty
    }

    "just one contract" in {
      val Seq(batch) = ContractsReassignmentBatch.partition(
        Seq(
          (contract1, sourceValidationPackageId, targetValidationPackageId, counter)
        )
      ): @unchecked

      batch.contractIds.toList shouldBe List(contract1.contractId)
      batch.stakeholders shouldBe stakeholders
    }

    "multiple homogenous contracts" in {
      val Seq(batch) = ContractsReassignmentBatch
        .partition(
          Seq(
            (contract1, sourceValidationPackageId, targetValidationPackageId, counter),
            (
              contract2,
              Source(contract2.templateId.packageId),
              Target(contract2.templateId.packageId),
              counter,
            ),
            (
              contract3,
              Source(contract3.templateId.packageId),
              Target(contract3.templateId.packageId),
              counter,
            ),
          )
        ): @unchecked

      batch.contractIds.toList shouldBe List(
        contract1.contractId,
        contract2.contractId,
        contract3.contractId,
      )
      batch.stakeholders shouldBe stakeholders
    }

    "contracts with different template ids" in {
      val newTemplateId = LfTemplateId.assertFromString(templateId.toString + "_but_different")
      val Seq(batch) = ContractsReassignmentBatch
        .partition(
          Seq(
            (contract1, sourceValidationPackageId, targetValidationPackageId, counter),
            (
              ExampleContractFactory.modify(contract2, templateId = Some(newTemplateId)),
              sourceValidationPackageId,
              targetValidationPackageId,
              counter,
            ),
          )
        ): @unchecked

      batch.contractIds.toList shouldBe List(contract1.contractId, contract2.contractId)
      batch.stakeholders shouldBe stakeholders
      batch.contracts.map(_.templateId).toSet shouldBe Set(templateId, newTemplateId)
      batch.contracts.map(_.packageName).toSet shouldBe Set(packageName)
    }

    "contracts with different package names" in {
      val newPackageName = LfPackageName.assertFromString("z_new_package_name")
      val Seq(batch) = ContractsReassignmentBatch
        .partition(
          Seq(
            (contract1, sourceValidationPackageId, targetValidationPackageId, counter),
            (
              ExampleContractFactory.modify(contract2, packageName = Some(newPackageName)),
              sourceValidationPackageId,
              targetValidationPackageId,
              counter,
            ),
          )
        ): @unchecked

      batch.contractIds.toList shouldBe List(contract1.contractId, contract2.contractId)
      batch.contracts.map(_.templateId).toSet shouldBe Set(templateId)
      batch.contracts.map(_.packageName).toSet shouldBe Set(packageName, newPackageName)
      batch.stakeholders shouldBe stakeholders
    }

    "contracts with different stakeholders" in {
      val newStakeholders = Stakeholders.tryCreate(
        stakeholders = stakeholders.all ++ Set(LfPartyId.assertFromString("extra_party")),
        signatories = stakeholders.signatories,
      )
      val Seq(batch1, batch2) = ContractsReassignmentBatch
        .partition(
          Seq(
            (contract1, sourceValidationPackageId, targetValidationPackageId, counter),
            (
              ExampleContractFactory
                .modify(contract2, metadata = Some(ContractMetadata(newStakeholders))),
              sourceValidationPackageId,
              targetValidationPackageId,
              counter,
            ),
          )
        )
        .sortBy(_.stakeholders.all.size): @unchecked

      batch1.contractIds.toList shouldBe List(contract1.contractId)
      batch1.stakeholders shouldBe stakeholders

      batch2.contractIds.toList shouldBe List(contract2.contractId)
      batch2.stakeholders shouldBe newStakeholders
    }
  }

}
