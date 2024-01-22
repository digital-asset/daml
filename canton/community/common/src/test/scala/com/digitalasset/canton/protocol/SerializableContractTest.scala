// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.data.Bytes
import com.daml.lf.transaction.Util
import com.daml.lf.value.Value
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, TestHash, TestSalt}
import com.digitalasset.canton.data.{CantonTimestamp, ProcessedDisclosedContract}
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.{BaseTest, LfPartyId, LfTimestamp, LfValue, LfVersioned}
import org.scalatest.wordspec.AnyWordSpec

class SerializableContractTest extends AnyWordSpec with BaseTest {

  val alice = LfPartyId.assertFromString("Alice")
  val bob = LfPartyId.assertFromString("Bob")

  val languageVersion = ExampleTransactionFactory.languageVersion
  val templateId = ExampleTransactionFactory.templateId

  "SerializableContractInstance" should {
    "deserialize correctly" in {
      val someContractSalt = TestSalt.generateSalt(0)
      val contractId = ExampleTransactionFactory.suffixedId(0, 0)

      val metadata = ContractMetadata.tryCreate(
        signatories = Set(alice),
        stakeholders = Set(alice, bob),
        maybeKeyWithMaintainers = Some(
          ExampleTransactionFactory.globalKeyWithMaintainers(
            LfGlobalKey.build(templateId, Value.ValueUnit, Util.sharedKey(languageVersion)).value,
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
    val transactionVersion = LfTransactionVersion.maxVersion

    val createdAt = LfTimestamp.Epoch
    val contractSalt = TestSalt.generateSalt(0)
    val driverMetadata =
      Bytes.fromByteArray(DriverContractMetadata(contractSalt).toByteArray(testedProtocolVersion))

    val contractIdDiscriminator = ExampleTransactionFactory.lfHash(0)
    val contractIdSuffix =
      Unicum(Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(0).finish())

    val invalidFormatContractId = LfContractId.assertFromString("00" * 34)

    val authenticatedContractId =
      AuthenticatedContractIdVersionV2.fromDiscriminator(contractIdDiscriminator, contractIdSuffix)

    val agreementText = "agreement"
    val disclosedContract = ProcessedDisclosedContract(
      templateId = templateId,
      contractId = authenticatedContractId,
      argument = LfValue.ValueNil,
      createdAt = createdAt,
      driverMetadata = driverMetadata,
      signatories = Set(alice),
      stakeholders = Set(alice),
      keyOpt = None,
      agreementText = agreementText,
      version = transactionVersion,
    )

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
                  template = templateId,
                  arg = LfValue.ValueNil,
                ),
              ),
              AgreementText(agreementText),
            )
            .value,
          metadata = ContractMetadata.tryCreate(Set(alice), Set(alice), None),
          ledgerCreateTime = LedgerCreateTime(CantonTimestamp(createdAt)),
          contractSalt = Some(contractSalt),
        )
      }
    }

    "provided a disclosed contract with unknown contract id format" should {
      "fail" in {
        SerializableContract
          .fromDisclosedContract(
            disclosedContract.copy(create =
              disclosedContract.create.copy(coid = invalidFormatContractId)
            )
          )
          .left
          .value shouldBe s"Invalid disclosed contract id: malformed contract id '${invalidFormatContractId.toString}'. Suffix 00 does not start with one of the supported prefixes: Bytes(ca02)"
      }
    }

    "provided a disclosed contract with missing driver contract metadata" should {
      "fail" in {
        SerializableContract
          .fromDisclosedContract(disclosedContract.copy(driverMetadata = Bytes.Empty))
          .left
          .value shouldBe "Missing driver contract metadata in provided disclosed contract"
      }
    }
  }
}
