// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, TestHash, TestSalt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.{
  BaseTest,
  LfPackageName,
  LfPartyId,
  LfTimestamp,
  LfValue,
  LfVersioned,
}
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AnyWordSpec

class SerializableContractTest extends AnyWordSpec with BaseTest {

  private val alice = LfPartyId.assertFromString("Alice")
  private val bob = LfPartyId.assertFromString("Bob")

  private val templateId = ExampleTransactionFactory.templateId

  "SerializableContractInstance" should {
    "deserialize correctly" in {
      val someContractSalt = TestSalt.generateSalt(0)
      val contractId = ExampleTransactionFactory.suffixedId(0, 0)

      val metadata = ContractMetadata.tryCreate(
        signatories = Set(alice),
        stakeholders = Set(alice, bob),
        maybeKeyWithMaintainersVersioned = Some(
          ExampleTransactionFactory.globalKeyWithMaintainers(
            LfGlobalKey
              .build(templateId, Value.ValueUnit, LfPackageName.assertFromString("package-name"))
              .value,
            Set(alice),
          )
        ),
      )

      val sci = ExampleTransactionFactory.asSerializable(
        contractId,
        ExampleTransactionFactory.contractInstance(Seq(contractId)),
        metadata,
        CantonTimestamp.now(),
        someContractSalt,
      )
      SerializableContract.fromProtoVersioned(
        sci.toProtoVersioned(testedProtocolVersion)
      ) shouldEqual Right(sci)
    }
  }

  "SerializableContract.fromDisclosedContract" when {
    val transactionVersion = LfLanguageVersion.v2_dev

    val createdAt = LfTimestamp.Epoch
    val contractSalt = TestSalt.generateSalt(0)
    val driverMetadata =
      Bytes.fromByteArray(DriverContractMetadata(contractSalt).toByteArray(testedProtocolVersion))

    val contractIdDiscriminator = ExampleTransactionFactory.lfHash(0)
    val contractIdSuffix =
      Unicum(Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(0).finish())

    val invalidFormatContractId = LfContractId.assertFromString("00" * 34)

    val authenticatedContractId =
      AuthenticatedContractIdVersionV11.fromDiscriminator(contractIdDiscriminator, contractIdSuffix)

    val pkgName = Ref.PackageName.assertFromString("pkgName")
    val pkgVersion = Some(Ref.PackageVersion.assertFromString("0.1.2"))

    val createNode = Node.Create(
      templateId = templateId,
      packageName = pkgName,
      packageVersion = pkgVersion,
      coid = authenticatedContractId,
      arg = LfValue.ValueInt64(123L),
      signatories = Set(alice),
      stakeholders = Set(alice),
      keyOpt = None,
      version = transactionVersion,
    )

    val disclosedContract =
      FatContractInstance.fromCreateNode(createNode, createdAt, driverMetadata)

    "provided a valid disclosed contract" should {
      "succeed" in {
        val actual = SerializableContract
          .fromDisclosedContract(disclosedContract)
          .value

        actual shouldBe SerializableContract(
          contractId = authenticatedContractId,
          rawContractInstance = SerializableRawContractInstance
            .create(
              LfVersioned(
                transactionVersion,
                LfValue.ContractInstance(
                  packageName = pkgName,
                  packageVersion = pkgVersion,
                  template = templateId,
                  arg = LfValue.ValueInt64(123L),
                ),
              )
            )
            .value,
          metadata = ContractMetadata.tryCreate(Set(alice), Set(alice), None),
          ledgerCreateTime = LedgerCreateTime(CantonTimestamp(createdAt)),
          contractSalt = contractSalt,
        )
      }
    }

    "provided a disclosed contract with unknown contract id format" should {
      "fail" in {
        SerializableContract
          .fromDisclosedContract(
            FatContractInstance.fromCreateNode(
              createNode.mapCid(_ => invalidFormatContractId),
              createdAt,
              driverMetadata,
            )
          )
          .left
          .value shouldBe s"Invalid disclosed contract id: malformed contract id '${invalidFormatContractId.toString}'. Suffix 00 is not a supported contract-id prefix"
      }
    }

    "provided a disclosed contract with missing driver contract metadata" should {
      "fail" in {
        SerializableContract
          .fromDisclosedContract(
            FatContractInstance.fromCreateNode(createNode, createdAt, cantonData = Bytes.Empty)
          )
          .left
          .value shouldBe "Missing driver contract metadata in provided disclosed contract"
      }
    }
  }
}
