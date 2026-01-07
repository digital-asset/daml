// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, TestHash, TestSalt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{
  BaseTest,
  LfPackageName,
  LfPartyId,
  LfTimestamp,
  LfValue,
  LfVersioned,
}
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance, Node}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AnyWordSpec

class SerializableContractTest extends AnyWordSpec with BaseTest {

  private val alice = LfPartyId.assertFromString("Alice")
  private val bob = LfPartyId.assertFromString("Bob")

  private val templateId = ExampleTransactionFactory.templateId

  def fromFatContract(inst: LfFatContractInst): Either[String, SerializableContract] =
    ContractInstance.toSerializableContract(inst)

  "SerializableContractInstance" should {

    // TODO(#23971) use CantonContractIdVersion.all
    forEvery(CantonContractIdVersion.allV1) { contractIdVersion =>
      s"deserialize $contractIdVersion correctly" in {
        val someContractSalt = TestSalt.generateSalt(0)

        val contractId = ExampleTransactionFactory.suffixedId(0, 0, contractIdVersion)

        val metadata = ContractMetadata.tryCreate(
          signatories = Set(alice),
          stakeholders = Set(alice, bob),
          maybeKeyWithMaintainersVersioned = Some(
            ExampleTransactionFactory.globalKeyWithMaintainers(
              LfGlobalKey
                .build(
                  templateId,
                  Value.ValueUnit,
                  LfPackageName.assertFromString("package-name"),
                )
                .value,
              Set(alice),
            )
          ),
        )

        val ci = ExampleTransactionFactory.asContractInstance(
          contractId,
          ExampleTransactionFactory.contractInstance(Seq(contractId)),
          metadata,
          CreationTime.CreatedAt(CantonTimestamp.now().toLf),
        )(
          ContractAuthenticationDataV1(someContractSalt)(contractIdVersion)
        )
        val sci = SerializableContract.fromLfFatContractInst(ci.inst).value
        SerializableContract.fromProtoVersioned(
          sci.toProtoVersioned(testedProtocolVersion)
        ) shouldEqual Right(sci)
      }
    }
  }

  "SerializableContract.fromFatContract" when {
    val serializationVersion = LfSerializationVersion.V1

    val createdAt = LfTimestamp.Epoch
    val contractSalt = TestSalt.generateSalt(0)
    val authenticationData =
      ContractAuthenticationDataV1(contractSalt)(CantonContractIdVersion.maxV1)

    val contractIdDiscriminator = ExampleTransactionFactory.lfHash(0)
    val contractIdSuffix =
      Unicum(Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).addInt(0).finish())

    val invalidFormatContractId = LfContractId.assertFromString("00" * 34)

    val authenticatedContractId =
      CantonContractIdVersion.maxV1.fromDiscriminator(contractIdDiscriminator, contractIdSuffix)

    val pkgName = Ref.PackageName.assertFromString("pkgName")

    val createNode = Node.Create(
      templateId = templateId,
      packageName = pkgName,
      coid = authenticatedContractId,
      arg = LfValue.ValueInt64(123L),
      signatories = Set(alice),
      stakeholders = Set(alice),
      keyOpt = None,
      version = serializationVersion,
    )

    val disclosedContract =
      FatContractInstance.fromCreateNode(
        createNode,
        CreationTime.CreatedAt(createdAt),
        authenticationData.toLfBytes,
      )

    "provided a valid disclosed contract" should {
      "succeed" in {
        val actual = fromFatContract(disclosedContract).value

        actual shouldBe SerializableContract(
          contractId = authenticatedContractId,
          rawContractInstance = SerializableRawContractInstance
            .create(
              LfVersioned(
                serializationVersion,
                LfValue.ThinContractInstance(
                  packageName = pkgName,
                  template = templateId,
                  arg = LfValue.ValueInt64(123L),
                ),
              )
            )
            .value,
          metadata = ContractMetadata.tryCreate(Set(alice), Set(alice), None),
          ledgerCreateTime = CreationTime.CreatedAt(createdAt),
          authenticationData = authenticationData,
        )
      }
    }

    "provided a disclosed contract with unknown contract id format" should {
      "fail" in {
        fromFatContract(
          FatContractInstance.fromCreateNode(
            createNode.mapCid(_ => invalidFormatContractId),
            CreationTime.CreatedAt(createdAt),
            authenticationData.toLfBytes,
          )
        ).left.value shouldBe s"Invalid disclosed contract id: Malformed contract ID: Suffix '00' is not a supported contract-id V1 prefix"
      }
    }

    "provided a disclosed contract with missing contract authentication data" should {
      "fail" in {
        fromFatContract(
          FatContractInstance.fromCreateNode(
            createNode,
            CreationTime.CreatedAt(createdAt),
            authenticationData = Bytes.Empty,
          )
        ).left.value shouldBe "Missing authentication data in provided disclosed contract"
      }
    }
  }
}
