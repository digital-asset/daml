// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v1.commands.DisclosedContract.Arguments as ProtoArguments
import com.daml.ledger.api.v1.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.daml.ledger.api.v1.contract_metadata.ContractMetadata as ProtoContractMetadata
import com.daml.ledger.api.v1.value.{
  Identifier as ProtoIdentifier,
  Record as ProtoRecord,
  RecordField as ProtoRecordField,
  Value as ProtoValue,
}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.value.Value.ValueRecord
import com.daml.lf.value.Value as Lf
import com.digitalasset.canton.ledger.api.domain.DisclosedContract
import com.digitalasset.canton.ledger.api.validation.ValidateDisclosedContractsTest.{
  api,
  disabledValidateDisclosedContracts,
  lf,
  validateDisclosedContracts,
}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import io.grpc.Status
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ValidateDisclosedContractsTest extends AnyFlatSpec with Matchers with ValidatorTestUtils {
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging

  behavior of classOf[ValidateDisclosedContracts].getSimpleName

  it should "validate the disclosed contracts when enabled" in {
    validateDisclosedContracts(api.protoCommands) shouldBe Right(
      lf.expectedDisclosedContracts
    )
  }

  it should "pass validation if feature disabled on empty disclosed contracts" in {
    val input = ProtoCommands(disclosedContracts = scala.Seq.empty)
    disabledValidateDisclosedContracts(input) shouldBe Right(ImmArray.empty)
  }

  it should "fail validation if feature disabled on provided disclosed contracts" in {
    requestMustFailWith(
      request = disabledValidateDisclosedContracts(api.protoCommands),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field disclosed_contracts: feature in development: disclosed_contracts should not be set",
      metadata = Map.empty,
    )
  }

  it should "fail validation on missing templateId" in {
    val withMissingTemplateId =
      ProtoCommands(disclosedContracts = scala.Seq(api.protoDisclosedContract.clearTemplateId))

    requestMustFailWith(
      request = validateDisclosedContracts(withMissingTemplateId),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.template_id",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid templateId" in {
    val withInvalidTemplateId =
      ProtoCommands(disclosedContracts =
        scala.Seq(
          api.protoDisclosedContract.update(
            _.templateId.modify(_.copy(packageId = "packageId:with:many:colons"))
          )
        )
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withInvalidTemplateId),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field package_id: non expected character 0x3a in Daml-LF Package ID \"packageId:with:many:colons\"",
      metadata = Map.empty,
    )
  }

  it should "fail validation on missing contractId" in {
    val withMissingContractId =
      ProtoCommands(disclosedContracts = scala.Seq(api.protoDisclosedContract.withContractId("")))

    requestMustFailWith(
      request = validateDisclosedContracts(withMissingContractId),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.contract_id",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid contractId" in {
    val withInvalidContractId =
      ProtoCommands(disclosedContracts =
        scala.Seq(api.protoDisclosedContract.withContractId("badContractId"))
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withInvalidContractId),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field DisclosedContract.contract_id: cannot parse ContractId \"badContractId\"",
      metadata = Map.empty,
    )
  }

  it should "fail validation on missing create arguments" in {
    val withMissingCreateArguments =
      ProtoCommands(disclosedContracts = scala.Seq(api.protoDisclosedContract.clearArguments))

    requestMustFailWith(
      request = validateDisclosedContracts(withMissingCreateArguments),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.arguments",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid create arguments record field" in {
    def invalidArguments: ProtoArguments =
      ProtoArguments.CreateArguments(
        api.contractArgumentsRecord.update(
          _.fields.set(scala.Seq(ProtoRecordField("something", None)))
        )
      )
    val withInvalidRecordField =
      ProtoCommands(disclosedContracts =
        scala.Seq(
          api.protoDisclosedContract.update(
            _.arguments.set(invalidArguments)
          )
        )
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withInvalidRecordField),
      code = Status.Code.INVALID_ARGUMENT,
      description = "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: value",
      metadata = Map.empty,
    )
  }

  it should "fail validation on missing metadata" in {
    val withMissingMetadata =
      ProtoCommands(disclosedContracts = scala.Seq(api.protoDisclosedContract.clearMetadata))

    requestMustFailWith(
      request = validateDisclosedContracts(withMissingMetadata),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.metadata",
      metadata = Map.empty,
    )
  }

  it should "fail validation on missing createdAt metadata" in {
    val withMissingCreatedAt =
      ProtoCommands(disclosedContracts =
        scala.Seq(api.protoDisclosedContract.update(_.metadata.modify(_.clearCreatedAt)))
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withMissingCreatedAt),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.metadata.created_at",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid createdAt metadata" in {
    val withInvalidCreatedAt =
      ProtoCommands(disclosedContracts =
        scala.Seq(api.protoDisclosedContract.update(_.metadata.createdAt.nanos.set(133)))
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withInvalidCreatedAt),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Can not represent DisclosedContract.metadata.created_at (Timestamp(1337,133,UnknownFieldSet(Map()))) as a Daml timestamp: Conversion of 1970-01-01T00:22:17.000000133Z to microsecond granularity would result in loss of precision.",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid contract key hash in metadata" in {
    val withInvalidKeyHashInMetadata =
      ProtoCommands(disclosedContracts =
        scala.Seq(
          api.protoDisclosedContract.update(
            _.metadata.contractKeyHash.set(ByteString.copyFromUtf8("BadKeyHash"))
          )
        )
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withInvalidKeyHashInMetadata),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field DisclosedContract.metadata.contract_key_hash: hash should have 32 bytes, got 10",
      metadata = Map.empty,
    )
  }

  it should "fail validation on unexpected create arguments blob" in {
    val withCrappyArguments =
      ProtoCommands(disclosedContracts =
        scala.Seq(
          api.protoDisclosedContract.update(
            _.arguments := ProtoArguments.CreateArgumentsBlob(
              com.google.protobuf.any.Any("foo ", ByteString.EMPTY)
            )
          )
        )
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withCrappyArguments),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field blob: Type of the Any message does not match the given class.",
      metadata = Map.empty,
    )
  }
}

object ValidateDisclosedContractsTest {
  private val validateDisclosedContracts = new ValidateDisclosedContracts(
    explicitDisclosureFeatureEnabled = true
  )
  private val disabledValidateDisclosedContracts = new ValidateDisclosedContracts(
    explicitDisclosureFeatureEnabled = false
  )

  private object api {
    val templateId: ProtoIdentifier =
      ProtoIdentifier("package", moduleName = "module", entityName = "entity")
    val contractId: String = "00" + "00" * 31 + "ef"
    val keyHash: Hash = Hash.assertFromString("00" * 31 + "ff")
    val contractArgumentsRecord: ProtoRecord = ProtoRecord(
      Some(templateId),
      scala.Seq(ProtoRecordField("something", Some(ProtoValue(ProtoValue.Sum.Bool(true))))),
    )
    val contractMetadata: ProtoContractMetadata = ProtoContractMetadata(
      createdAt = Some(ProtoTimestamp(seconds = 1337L)),
      contractKeyHash = ByteString.copyFrom(keyHash.bytes.toByteArray),
      driverMetadata = ByteString.copyFromUtf8("SomeDriverMetadata"),
    )
    val protoDisclosedContract: ProtoDisclosedContract = ProtoDisclosedContract(
      templateId = Some(templateId),
      contractId = contractId,
      arguments = ProtoArguments.CreateArguments(contractArgumentsRecord),
      metadata = Some(contractMetadata),
    )
    val protoCommands: ProtoCommands =
      ProtoCommands(disclosedContracts = scala.Seq(api.protoDisclosedContract))
  }

  private object lf {
    val templateId: Ref.Identifier = Ref.Identifier(
      Ref.PackageId.assertFromString("package"),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("module"),
        Ref.DottedName.assertFromString("entity"),
      ),
    )

    val lfDisclosedContract: DisclosedContract = DisclosedContract(
      templateId,
      Lf.ContractId.V1.assertFromString(api.contractId),
      argument = ValueRecord(
        tycon = Some(templateId),
        fields = ImmArray(Some(Ref.Name.assertFromString("something")) -> Lf.ValueTrue),
      ),
      createdAt = Time.Timestamp.assertFromLong(1337000000L),
      keyHash = Some(Hash.assertFromString("00" * 31 + "ff")),
      driverMetadata = Bytes.fromByteString(ByteString.copyFromUtf8("SomeDriverMetadata")),
    )

    val expectedDisclosedContracts: ImmArray[DisclosedContract] = ImmArray(lfDisclosedContract)
  }
}
