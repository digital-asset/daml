// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v2.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.daml.ledger.api.v2.value.Identifier as ProtoIdentifier
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.transaction.*
import com.daml.lf.value.Value.{ContractId, ValueRecord}
import com.daml.lf.value.Value as Lf
import com.digitalasset.canton.LfValue
import com.digitalasset.canton.ledger.api.domain.UpgradableDisclosedContract
import com.digitalasset.canton.ledger.api.validation.ValidateDisclosedContractsTest.{
  api,
  lf,
  validateDisclosedContracts,
}
import com.google.protobuf.ByteString
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
    validateDisclosedContracts(api.protoCommands) shouldBe Right(lf.expectedDisclosedContracts)
  }

  it should "fail validation on missing created event blob" in {
    val withMissingBlob =
      ProtoCommands(disclosedContracts =
        scala.Seq(
          api.protoDisclosedContract.copy(
            createdEventBlob = ByteString.EMPTY
          )
        )
      )

    requestMustFailWith(
      request = validateDisclosedContracts(withMissingBlob),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.createdEventBlob",
      metadata = Map.empty,
    )
  }

  it should "fail validation on absent contract_id" in {
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.protoDisclosedContract
              .copy(
                createdEventBlob =
                  TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
              )
              .copy(contractId = "")
          )
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.contract_id",
      metadata = Map.empty,
    )
  }

  it should "fail validation on absent template_id" in {
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.protoDisclosedContract
              .copy(
                createdEventBlob =
                  TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
              )
              .copy(templateId = None)
          )
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.template_id",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid contract_id" in {
    val invalidContractId = "invalidContractId"
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.protoDisclosedContract
              .copy(
                createdEventBlob =
                  TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
              )
              .copy(contractId = invalidContractId)
          )
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        s"""INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field DisclosedContract.contract_id: cannot parse ContractId "$invalidContractId"""",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid template_id" in {
    val invalidTemplateId = ProtoIdentifier("pkgId", "", "entity")
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.protoDisclosedContract
              .copy(
                createdEventBlob =
                  TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
              )
              .copy(templateId = Some(invalidTemplateId))
          )
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field module_name: Expected a non-empty string",
      metadata = Map.empty,
    )
  }

  it should "fail validation when provided contract_id mismatches the one decoded from the created_event_blob" in {
    val otherContractId = "00" + "00" * 31 + "ff"
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.protoDisclosedContract
              .copy(
                createdEventBlob =
                  TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
              )
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

  it should "fail validation when provided template_id mismatches the one decoded from the created_event_blob" in {
    val otherTemplateId = ProtoIdentifier("otherPkgId", "otherModule", "otherEntity")
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.protoDisclosedContract
              .copy(
                createdEventBlob =
                  TransactionCoder.encodeFatContractInstance(lf.fatContractInstance).value
              )
              .copy(templateId = Some(otherTemplateId))
          )
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Mismatch between DisclosedContract.template_id (otherPkgId:otherModule:otherEntity) and template_id from decoded DisclosedContract.created_event_blob (package:module:entity)",
      metadata = Map.empty,
    )
  }

  it should "fail validation if decoding the created_event_blob fails" in {
    requestMustFailWith(
      request = validateDisclosedContracts(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.protoDisclosedContract
              .copy(
                createdEventBlob = Bytes.assertFromString("00abcd").toByteString
              )
          )
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unable to decode disclosed contract event payload: DecodeError(exception com.google.protobuf.InvalidProtocolBufferException: Protocol message contained an invalid tag (zero). while decoding the versioned object)",
      metadata = Map.empty,
    )
  }
}

object ValidateDisclosedContractsTest {

  private val testTxVersion = TransactionVersion.maxVersion

  private val validateDisclosedContracts = new ValidateDisclosedContracts

  private object api {
    val templateId: ProtoIdentifier =
      ProtoIdentifier("package", moduleName = "module", entityName = "entity")
    val packageName: String = "pkg-name"
    val contractId: String = "00" + "00" * 31 + "ef"
    val alice: Ref.Party = Ref.Party.assertFromString("alice")
    private val bob: Ref.Party = Ref.Party.assertFromString("bob")
    private val charlie: Ref.Party = Ref.Party.assertFromString("charlie")
    val stakeholders: Set[Ref.Party] = Set(alice, bob, charlie)
    val signatories: Set[Ref.Party] = Set(alice, bob)
    val keyMaintainers: Set[Ref.Party] = Set(bob)
    val createdAtSeconds = 1337L
    val someDriverMetadataStr = "SomeDriverMetadata"
    val protoDisclosedContract: ProtoDisclosedContract = ProtoDisclosedContract(
      templateId = Some(templateId),
      contractId = contractId,
      createdEventBlob = TransactionCoder
        .encodeFatContractInstance(lf.fatContractInstance)
        .fold(
          err =>
            throw new RuntimeException(s"Cannot serialize createdEventBlob: ${err.errorMessage}"),
          identity,
        ),
    )

    val protoCommands: ProtoCommands =
      ProtoCommands(disclosedContracts = scala.Seq(api.protoDisclosedContract))
  }

  private object lf {
    private val templateId: Ref.Identifier = Ref.Identifier(
      Ref.PackageId.assertFromString(api.templateId.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(api.templateId.moduleName),
        Ref.DottedName.assertFromString(api.templateId.entityName),
      ),
    )
    private val packageName: Ref.PackageName = Ref.PackageName.assertFromString(api.packageName)
    private val createArg: ValueRecord = ValueRecord(
      tycon = Some(templateId),
      fields = ImmArray(Some(Ref.Name.assertFromString("something")) -> Lf.ValueTrue),
    )
    private val createArgWithoutLabels: ValueRecord = ValueRecord(
      tycon = None,
      fields = ImmArray(None -> Lf.ValueTrue),
    )
    val lfContractId: ContractId.V1 = Lf.ContractId.V1.assertFromString(api.contractId)

    private val driverMetadataBytes: Bytes =
      Bytes.fromByteString(ByteString.copyFromUtf8(api.someDriverMetadataStr))
    private val keyWithMaintainers: GlobalKeyWithMaintainers = GlobalKeyWithMaintainers.assertBuild(
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

    val fatContractInstance: FatContractInstance = FatContractInstance.fromCreateNode(
      create = Node.Create(
        coid = lf.lfContractId,
        templateId = lf.templateId,
        packageName = lf.packageName,
        arg = lf.createArg,
        signatories = api.signatories,
        stakeholders = api.stakeholders,
        keyOpt = Some(lf.keyWithMaintainers),
        version = testTxVersion,
      ),
      createTime = Time.Timestamp.assertFromLong(api.createdAtSeconds * 1000000L),
      cantonData = lf.driverMetadataBytes,
    )

    val expectedDisclosedContracts: ImmArray[UpgradableDisclosedContract] = ImmArray(
      UpgradableDisclosedContract(
        templateId,
        packageName,
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
