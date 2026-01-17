// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.daml.ledger.api.v2.value.Identifier as ProtoIdentifier
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Salt, SaltSeed, TestHash}
import com.digitalasset.canton.ledger.api.DisclosedContract
import com.digitalasset.canton.ledger.api.validation.ValidateDisclosedContractsTest.{
  api,
  lf,
  lfContractId,
  underTest,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.{DefaultDamlValues, LfValue}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value as Lf
import com.digitalasset.daml.lf.value.Value.{ContractId, ValueRecord}
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
  private implicit val errorLoggingContext: ErrorLoggingContext = NoLogging

  behavior of classOf[ValidateDisclosedContracts].getSimpleName

  it should "validate the disclosed contracts when enabled" in {
    underTest.validateCommands(api.protoCommands) shouldBe Right(
      lf.expectedDisclosedContracts
    )
  }

  it should "fail validation on missing created event blob" in {
    val withMissingBlob =
      ProtoCommands.defaultInstance.withDisclosedContracts(
        scala.Seq(
          api.protoDisclosedContract.copy(
            createdEventBlob = ByteString.EMPTY
          )
        )
      )

    requestMustFailWith(
      request = underTest.validateCommands(withMissingBlob),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: DisclosedContract.createdEventBlob",
      metadata = Map.empty,
    )
  }

  it should "support absent contract_id" in {
    underTest
      .validateCommands(
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
      )
      .value shouldBe lf.expectedDisclosedContracts
  }

  it should "support absent template_id" in {
    underTest
      .validateCommands(
        api.protoCommands.copy(
          disclosedContracts = scala.Seq(
            api.protoDisclosedContract
              .copy(templateId = None)
          )
        )
      )
      .value shouldBe lf.expectedDisclosedContracts
  }

  it should "fail validation on invalid contract_id" in {
    val invalidContractId = "invalidContractId"
    requestMustFailWith(
      request = underTest.validateCommands(
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
      request = underTest.validateCommands(
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
      request = underTest.validateCommands(
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
        s"INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Mismatch between DisclosedContract.contract_id ($otherContractId) and contract_id from decoded DisclosedContract.created_event_blob (${lfContractId.coid})",
      metadata = Map.empty,
    )
  }

  it should "fail validation when provided template_id mismatches the one decoded from the created_event_blob" in {
    val otherTemplateId = ProtoIdentifier("otherPkgId", "otherModule", "otherEntity")
    requestMustFailWith(
      request = underTest.validateCommands(
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
        "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Mismatch between DisclosedContract.template_id (otherPkgId:otherModule:otherEntity) and template_id from decoded DisclosedContract.created_event_blob (package:module:entity)",
      metadata = Map.empty,
    )
  }

  it should "fail validation if decoding the created_event_blob fails" in {
    requestMustFailWith(
      request = underTest.validateCommands(
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
        "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Unable to decode disclosed contract event payload: DecodeError(exception com.google.protobuf.InvalidProtocolBufferException: Protocol message contained an invalid tag (zero). while decoding the versioned object)",
      metadata = Map.empty,
    )
  }

  it should "fail validation on invalid synchronizer_id" in {
    requestMustFailWith(
      request = underTest.validateCommands(
        ProtoCommands.defaultInstance.copy(disclosedContracts =
          scala.Seq(api.protoDisclosedContract.copy(synchronizerId = "cantBe!"))
        )
      ),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field DisclosedContract.synchronizer_id: Invalid unique identifier `cantBe!` with missing namespace.",
      metadata = Map.empty,
    )
  }

  it should "succeed on duplicate contract ids with same payload" in {
    val commandsWithDuplicateDisclosedContracts =
      ProtoCommands.defaultInstance.copy(disclosedContracts =
        scala.Seq(
          api.protoDisclosedContract,
          api.protoDisclosedContract,
        )
      )
    underTest.validateCommands(commandsWithDuplicateDisclosedContracts) shouldBe Right(
      lf.expectedDuplicateDisclosedContracts
    )
  }

  it should "fail validation on duplicate contract ids with different payloads" in {
    val commandsWithDuplicateDisclosedContracts =
      ProtoCommands.defaultInstance.copy(disclosedContracts =
        scala.Seq(
          api.protoDisclosedContract,
          api.protoConflictingDisclosedContractDuplicate,
        )
      )
    requestMustFailWith(
      request = underTest.validateCommands(commandsWithDuplicateDisclosedContracts),
      code = Status.Code.INVALID_ARGUMENT,
      description =
        s"An error occurred. Please contact the operator and inquire about the request <no-correlation-id> with tid <no-tid>",
      metadata = Map.empty,
    )
  }

}

object ValidateDisclosedContractsTest {

  private val underTest = ValidateDisclosedContracts

  val lfContractId: ContractId.V1 = CantonContractIdVersion.maxV1.fromDiscriminator(
    DefaultDamlValues.lfhash(3),
    Unicum(TestHash.digest(4)),
  )

  private object api {
    val templateId: ProtoIdentifier =
      ProtoIdentifier("package", moduleName = "module", entityName = "entity")
    val packageName: Ref.PackageName = Ref.PackageName.assertFromString("pkg-name")
    val contractId: String = lfContractId.coid
    val alice: Ref.Party = Ref.Party.assertFromString("alice")
    private val bob: Ref.Party = Ref.Party.assertFromString("bob")
    private val charlie: Ref.Party = Ref.Party.assertFromString("charlie")
    val stakeholders: Set[Ref.Party] = Set(alice, bob, charlie)
    val signatories: Set[Ref.Party] = Set(alice, bob)
    val keyMaintainers: Set[Ref.Party] = Set(bob)
    val createdAtSeconds = 1337L
    val createdAtSeconds2 = 1338L
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
      synchronizerId = "",
    )
    val protoConflictingDisclosedContractDuplicate: ProtoDisclosedContract = ProtoDisclosedContract(
      templateId = Some(templateId),
      contractId = contractId,
      createdEventBlob = TransactionCoder
        .encodeFatContractInstance(lf.fatContractInstance2)
        .fold(
          err =>
            throw new RuntimeException(s"Cannot serialize createdEventBlob: ${err.errorMessage}"),
          identity,
        ),
      synchronizerId = "",
    )

    val dupKeyProtoDisclosedContract: ProtoDisclosedContract = protoDisclosedContract.copy(
      contractId = lf.dupKeyFatContractInstance.contractId.coid,
      createdEventBlob = TransactionCoder
        .encodeFatContractInstance(lf.dupKeyFatContractInstance)
        .fold(
          err =>
            throw new RuntimeException(s"Cannot serialize createdEventBlob: ${err.errorMessage}"),
          identity,
        ),
    )

    val protoCommands: ProtoCommands =
      ProtoCommands.defaultInstance.copy(disclosedContracts = scala.Seq(api.protoDisclosedContract))
  }

  private object lf {
    private val templateId: Ref.Identifier = Ref.Identifier(
      Ref.PackageId.assertFromString(api.templateId.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(api.templateId.moduleName),
        Ref.DottedName.assertFromString(api.templateId.entityName),
      ),
    )
    private val createArg: ValueRecord =
      ValueRecord(tycon = None, fields = ImmArray(None -> Lf.ValueTrue))

    private val seedSalt: SaltSeed = SaltSeed.generate()(new SymbolicPureCrypto())
    private val salt = Salt.tryDeriveSalt(seedSalt, 0, new SymbolicPureCrypto())

    private val authenticationDataBytes: Bytes =
      ContractAuthenticationDataV1(salt)(CantonContractIdVersion.maxV1).toLfBytes

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
      api.packageName,
    )

    private val createNode: Node.Create = Node.Create(
      coid = lfContractId,
      templateId = lf.templateId,
      packageName = api.packageName,
      arg = lf.createArg,
      signatories = api.signatories,
      stakeholders = api.stakeholders,
      keyOpt = Some(lf.keyWithMaintainers),
      version = LfSerializationVersion.StableVersions.max,
    )

    private val dupKeyCreateNode = createNode.copy(ExampleContractFactory.buildContractId())

    def fatContractInstance: LfFatContractInst = FatContractInstance.fromCreateNode(
      create = createNode,
      createTime =
        CreationTime.CreatedAt(Time.Timestamp.assertFromLong(api.createdAtSeconds * 1000000L)),
      authenticationData = lf.authenticationDataBytes,
    )

    def fatContractInstance2: LfFatContractInst = FatContractInstance.fromCreateNode(
      create = createNode,
      createTime =
        CreationTime.CreatedAt(Time.Timestamp.assertFromLong(api.createdAtSeconds2 * 1000000L)),
      authenticationData = lf.authenticationDataBytes,
    )

    def dupKeyFatContractInstance: LfFatContractInst = FatContractInstance.fromCreateNode(
      create = dupKeyCreateNode,
      createTime = fatContractInstance.createdAt,
      authenticationData = fatContractInstance.authenticationData,
    )

    val expectedDisclosedContracts: ImmArray[DisclosedContract] = ImmArray(
      DisclosedContract(fatContractInstance, None)
    )

    val expectedDuplicateDisclosedContracts: ImmArray[DisclosedContract] = ImmArray(
      DisclosedContract(fatContractInstance, None),
      DisclosedContract(fatContractInstance, None),
    )

  }
}
