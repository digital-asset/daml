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
import com.daml.lf.transaction.*
import com.daml.lf.value.Value.{ContractId, ValueRecord}
import com.daml.lf.value.Value as Lf
import com.digitalasset.canton.LfValue
import com.digitalasset.canton.ledger.api.domain.{
  NonUpgradableDisclosedContract,
  UpgradableDisclosedContract,
}
import com.digitalasset.canton.ledger.api.validation.ValidateDisclosedContractsTest.{
  api,
  disabledValidateDisclosedContracts,
  lf,
  validateDisclosedContracts,
}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import io.grpc.Status
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ValidateDisclosedContractsTest
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with ValidatorTestUtils {
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
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field disclosed_contracts: feature disabled: disclosed_contracts should not be set",
      metadata = Map.empty,
    )
  }

  it should "fail validation on missing templateId" in {
    val withMissingTemplateId =
      ProtoCommands(disclosedContracts =
        scala.Seq(api.deprecatedProtoDisclosedContract.clearTemplateId)
      )

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
          api.deprecatedProtoDisclosedContract.update(
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
      ProtoCommands(disclosedContracts =
        scala.Seq(api.deprecatedProtoDisclosedContract.withContractId(""))
      )

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
        scala.Seq(api.deprecatedProtoDisclosedContract.withContractId("badContractId"))
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
      ProtoCommands(disclosedContracts =
        scala.Seq(api.deprecatedProtoDisclosedContract.clearArguments)
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withMissingCreateArguments),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.arguments",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid create arguments record field" in {
    // Allow using deprecated Protobuf fields for backwards compatibility
    @annotation.nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.commands\\.DisclosedContract.*"
    )
    def invalidArguments: ProtoArguments =
      ProtoArguments.CreateArguments(
        api.contractArgumentsRecord.update(
          _.fields.set(scala.Seq(ProtoRecordField("something", None)))
        )
      )
    val withInvalidRecordField =
      ProtoCommands(disclosedContracts =
        scala.Seq(
          api.deprecatedProtoDisclosedContract.update(
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
      ProtoCommands(disclosedContracts =
        scala.Seq(api.deprecatedProtoDisclosedContract.clearMetadata)
      )

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
        scala.Seq(api.deprecatedProtoDisclosedContract.update(_.metadata.modify(_.clearCreatedAt)))
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
        scala.Seq(api.deprecatedProtoDisclosedContract.update(_.metadata.createdAt.nanos.set(133)))
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
          api.deprecatedProtoDisclosedContract.update(
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
    // Allow using deprecated Protobuf fields for backwards compatibility
    @annotation.nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.commands\\.DisclosedContract.*"
    )
    val withCrappyArguments =
      ProtoCommands(disclosedContracts =
        scala.Seq(
          api.deprecatedProtoDisclosedContract.update(
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

  it should "validate the disclosed contract when provided as the created_event_blob" in {
    validateDisclosedContracts(
      api.protoCommands.copy(
        disclosedContracts = scala.Seq(
          api.deprecatedProtoDisclosedContract
            .copy(
              createdEventBlob =
                TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
            )
            // arguments and metadata cannot be set at the same time with created_event_blob
            .clearMetadata
            .clearArguments
        )
      )
    ) shouldBe Right(lf.expectedUpgradableDisclosedContracts)
  }

  it should "fail validation when provided contract_id mismatches the one decoded from the created_event_blob" in {
    val otherContractId = "otherContractId"
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.deprecatedProtoDisclosedContract
              .copy(
                createdEventBlob =
                  TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
              )
              // arguments and metadata cannot be set at the same time with created_event_blob
              .clearMetadata
              .clearArguments
              .copy(contractId = otherContractId)
          )
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Mismatch between DisclosedContract.contract_id ($otherContractId) and contract_id from decoded DisclosedContract.created_event_blob (${lf.lfContractId.coid})",
      metadata = Map.empty,
    )
  }

  it should "fail validation if decoding the created_event_blob fails" in {
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.deprecatedProtoDisclosedContract
              .copy(
                createdEventBlob = Bytes.assertFromString("00abcd").toByteString
              )
              // arguments and metadata cannot be set at the same time with created_event_blob
              .clearMetadata
              .clearArguments
          )
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unable to decode disclosed contract event payload: DecodeError(exception com.google.protobuf.InvalidProtocolBufferException: Protocol message contained an invalid tag (zero). while decoding the versioned object)",
      metadata = Map.empty,
    )
  }

  it should "fail validation if both DisclosedContract formats are set" in {
    val disclosedContractWithAllFieldsSet = api.deprecatedProtoDisclosedContract
      .copy(createdEventBlob =
        TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
      )
    val disclosedContractWithMetadataAndPayloadSet =
      disclosedContractWithAllFieldsSet.clearArguments
    val disclosedContractWithCreateArgsAndPayloadSet =
      disclosedContractWithAllFieldsSet.clearMetadata

    def test(disclosedContract: ProtoDisclosedContract) =
      requestMustFailWith(
        request = validateDisclosedContracts(
          api.protoCommands.copy(disclosedContracts = scala.Seq(disclosedContract))
        ),
        code = Status.Code.INVALID_ARGUMENT,
        description =
          "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: DisclosedContract.arguments or DisclosedContract.metadata cannot be set together with DisclosedContract.created_event_blob",
        metadata = Map.empty,
      )

    test(disclosedContractWithAllFieldsSet)
    test(disclosedContractWithMetadataAndPayloadSet)
    test(disclosedContractWithCreateArgsAndPayloadSet)
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
    val alice: Ref.Party = Ref.Party.assertFromString("alice")
    private val bob: Ref.Party = Ref.Party.assertFromString("bob")
    private val charlie: Ref.Party = Ref.Party.assertFromString("charlie")
    val stakeholders: Set[Ref.Party] = Set(alice, bob, charlie)
    val signatories: Set[Ref.Party] = Set(alice, bob)
    val keyMaintainers: Set[Ref.Party] = Set(bob)
    val contractArgumentsRecord: ProtoRecord = ProtoRecord(
      Some(templateId),
      scala.Seq(ProtoRecordField("something", Some(ProtoValue(ProtoValue.Sum.Bool(true))))),
    )
    val createdAtSeconds = 1337L
    val someDriverMetadataStr = "SomeDriverMetadata"
    val contractMetadata: ProtoContractMetadata = ProtoContractMetadata(
      createdAt = Some(ProtoTimestamp(seconds = createdAtSeconds)),
      contractKeyHash = ByteString.copyFrom(keyHash.bytes.toByteArray),
      driverMetadata = ByteString.copyFromUtf8(someDriverMetadataStr),
    )
    // Allow using deprecated Protobuf fields for backwards compatibility
    @annotation.nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.commands\\.DisclosedContract.*"
    )
    val deprecatedProtoDisclosedContract: ProtoDisclosedContract = ProtoDisclosedContract(
      templateId = Some(templateId),
      contractId = contractId,
      arguments = ProtoArguments.CreateArguments(contractArgumentsRecord),
      metadata = Some(contractMetadata),
      createdEventBlob = ByteString.EMPTY,
    )

    val protoCommands: ProtoCommands =
      ProtoCommands(disclosedContracts = scala.Seq(api.deprecatedProtoDisclosedContract))
  }

  private object lf {
    val templateId: Ref.Identifier = Ref.Identifier(
      Ref.PackageId.assertFromString("package"),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("module"),
        Ref.DottedName.assertFromString("entity"),
      ),
    )
    val createArg: ValueRecord = ValueRecord(
      tycon = Some(templateId),
      fields = ImmArray(Some(Ref.Name.assertFromString("something")) -> Lf.ValueTrue),
    )
    val createArgWithoutLabels: ValueRecord = ValueRecord(
      tycon = None,
      fields = ImmArray(None -> Lf.ValueTrue),
    )
    val lfContractId: ContractId.V1 = Lf.ContractId.V1.assertFromString(api.contractId)

    val driverMetadataBytes: Bytes =
      Bytes.fromByteString(ByteString.copyFromUtf8(api.someDriverMetadataStr))
    val keyWithMaintainers: GlobalKeyWithMaintainers = GlobalKeyWithMaintainers.assertBuild(
      lf.templateId,
      LfValue.ValueRecord(
        None,
        ImmArray(
          None -> LfValue.ValueParty(api.alice),
          None -> LfValue.ValueText("some key"),
        ),
      ),
      api.keyMaintainers,
    )

    private val keyHash: Hash = keyWithMaintainers.globalKey.hash
    private val lfDisclosedContract: NonUpgradableDisclosedContract =
      NonUpgradableDisclosedContract(
        templateId,
        lfContractId,
        argument = createArg,
        createdAt = Time.Timestamp.assertFromLong(api.createdAtSeconds * 1000000L),
        keyHash = Some(Hash.assertFromByteArray(api.contractMetadata.contractKeyHash.toByteArray)),
        driverMetadata = driverMetadataBytes,
      )

    val fatContractInstance: FatContractInstance = FatContractInstance.fromCreateNode(
      create = Node.Create(
        coid = lf.lfContractId,
        templateId = lf.templateId,
        arg = lf.createArg,
        agreementText = "",
        signatories = api.signatories,
        stakeholders = api.stakeholders,
        keyOpt = Some(lf.keyWithMaintainers),
        version = TransactionVersion.maxVersion,
      ),
      createTime = Time.Timestamp.assertFromLong(api.createdAtSeconds * 1000000L),
      cantonData = lf.driverMetadataBytes,
    )

    val expectedDisclosedContracts: ImmArray[NonUpgradableDisclosedContract] = ImmArray(
      lfDisclosedContract
    )
    val expectedUpgradableDisclosedContracts: ImmArray[UpgradableDisclosedContract] = ImmArray(
      UpgradableDisclosedContract(
        templateId,
        lfContractId,
        argument = createArgWithoutLabels,
        createdAt = Time.Timestamp.assertFromLong(api.createdAtSeconds * 1000000L),
        keyHash = Some(keyHash),
        keyMaintainers = Some(api.keyMaintainers),
        driverMetadata = driverMetadataBytes,
        keyValue = Some(keyWithMaintainers.value),
        signatories = api.signatories,
        stakeholders = api.stakeholders,
      )
    )
  }
}
