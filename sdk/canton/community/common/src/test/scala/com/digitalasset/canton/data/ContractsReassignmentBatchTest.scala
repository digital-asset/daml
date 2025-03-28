// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.protocol.{
  ContractMetadata,
  ExampleTransactionFactory,
  LfTemplateId,
  SerializableContract,
  Stakeholders,
}
import com.digitalasset.canton.{LfPackageName, LfPartyId, ReassignmentCounter}
import org.scalatest.EitherValues.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ContractsReassignmentBatchTest extends AnyWordSpec with Matchers {
  private val contract1 = ExampleTransactionFactory.asSerializable(contractId =
    ExampleTransactionFactory.suffixedId(-1, 0)
  )
  private val contract2 = contract1.copy(contractId = ExampleTransactionFactory.suffixedId(-1, 1))
  private val contract3 = contract1.copy(contractId = ExampleTransactionFactory.suffixedId(-1, 2))

  private val templateId = contract1.rawContractInstance.contractInstance.unversioned.template
  private val packageName = contract1.rawContractInstance.contractInstance.unversioned.packageName
  private val stakeholders = Stakeholders(contract1.metadata)
  private val counter = ReassignmentCounter(1)

  "ContractsReassignmentBatch.apply" in {
    val batch = ContractsReassignmentBatch(contract1, counter)
    batch.contractIds.toList shouldBe List(contract1.contractId)
    batch.contracts.map(_.templateId) shouldBe Seq(templateId)
    batch.contracts.map(_.packageName) shouldBe Seq(packageName)
    batch.stakeholders shouldBe stakeholders
  }

  "ContractsReassignmentBatch.create" when {

    "no contracts" in {
      ContractsReassignmentBatch.create(Seq.empty) shouldBe Left(
        ContractsReassignmentBatch.EmptyBatch
      )
    }

    "just one contract" in {
      val batch = ContractsReassignmentBatch
        .create(
          Seq(
            (contract1, counter)
          )
        )
        .value
      batch.contractIds.toList shouldBe List(contract1.contractId)
      batch.contracts.map(_.templateId) shouldBe Seq(templateId)
      batch.contracts.map(_.packageName) shouldBe Seq(packageName)
      batch.stakeholders shouldBe stakeholders
    }

    "multiple homogenous contracts" in {
      val batch = ContractsReassignmentBatch
        .create(
          Seq(
            (contract1, counter),
            (contract2, counter),
            (contract3, counter),
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
            (contract1, counter),
            (setTemplateId(contract2, newTemplateId), counter),
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
            (contract1, counter),
            (setPackageName(contract2, newPackageName), counter),
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
          (contract1, counter),
          (setStakeholders(contract2, newStakeholders), counter),
        )
      ) shouldBe Left(
        ContractsReassignmentBatch.DifferingStakeholders(Seq(stakeholders, newStakeholders))
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
          (contract1, counter)
        )
      ): @unchecked

      batch.contractIds.toList shouldBe List(contract1.contractId)
      batch.stakeholders shouldBe stakeholders
    }

    "multiple homogenous contracts" in {
      val Seq(batch) = ContractsReassignmentBatch
        .partition(
          Seq(
            (contract1, counter),
            (contract2, counter),
            (contract3, counter),
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
            (contract1, counter),
            (setTemplateId(contract2, newTemplateId), counter),
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
            (contract1, counter),
            (setPackageName(contract2, newPackageName), counter),
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
            (contract1, counter),
            (setStakeholders(contract2, newStakeholders), counter),
          )
        )
        .sortBy(_.stakeholders.all.size): @unchecked

      batch1.contractIds.toList shouldBe List(contract1.contractId)
      batch1.stakeholders shouldBe stakeholders

      batch2.contractIds.toList shouldBe List(contract2.contractId)
      batch2.stakeholders shouldBe newStakeholders
    }
  }

  private def setTemplateId(
      contract: SerializableContract,
      templateId: LfTemplateId,
  ): SerializableContract =
    ExampleTransactionFactory.asSerializable(
      contractId = contract.contractId,
      contractInstance = ExampleTransactionFactory.contractInstance(templateId = templateId),
      metadata = contract.metadata,
    )

  private def setPackageName(
      contract: SerializableContract,
      packageName: LfPackageName,
  ): SerializableContract =
    ExampleTransactionFactory.asSerializable(
      contractId = contract.contractId,
      contractInstance = ExampleTransactionFactory.contractInstance(packageName = packageName),
      metadata = contract.metadata,
    )

  private def setStakeholders(
      contract: SerializableContract,
      stakeholders: Stakeholders,
  ): SerializableContract =
    ExampleTransactionFactory.asSerializable(
      contractId = contract.contractId,
      contractInstance = contract.contractInstance,
      metadata = ContractMetadata.tryCreate(
        stakeholders.signatories,
        stakeholders.all,
        contract.metadata.maybeKeyWithMaintainersVersioned,
      ),
    )
}
