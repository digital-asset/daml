// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v1.commands.DisclosedContract
import com.daml.ledger.api.v1.value.Identifier
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.transaction.{
  FatContractInstance,
  GlobalKeyWithMaintainers,
  Node,
  TransactionCoder,
  TransactionVersion,
}
import com.daml.lf.value.Value.{ContractId, ValueRecord, ValueTrue}
import com.digitalasset.canton.LfValue
import com.google.protobuf.ByteString

object DisclosedContractCreator {

  private val testTxVersion = TransactionVersion.maxVersion

  private object api {
    val templateId: Identifier =
      Identifier("package", moduleName = "module", entityName = "entity")
    val contractId: String = "00" + "00" * 31 + "ef"
    val alice: Ref.Party = Ref.Party.assertFromString("alice")
    val bob: Ref.Party = Ref.Party.assertFromString("bob")
    val charlie: Ref.Party = Ref.Party.assertFromString("charlie")
    val stakeholders: Set[Ref.Party] = Set(alice, bob, charlie)
    val signatories: Set[Ref.Party] = Set(alice, bob)
    val keyMaintainers: Set[Ref.Party] = Set(bob)
    val createdAtSeconds = 1337L
    val someDriverMetadataStr = "SomeDriverMetadata"
  }

  private object lf {
    private val templateId: Ref.Identifier = Ref.Identifier(
      Ref.PackageId.assertFromString(api.templateId.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(api.templateId.moduleName),
        Ref.DottedName.assertFromString(api.templateId.entityName),
      ),
    )
    private val createArg: ValueRecord = ValueRecord(
      tycon = Some(templateId),
      fields = ImmArray(Some(Ref.Name.assertFromString("something")) -> ValueTrue),
    )

    val lfContractId: ContractId.V1 = ContractId.V1.assertFromString(api.contractId)

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

    val fatContractInstance: FatContractInstance = FatContractInstance.fromCreateNode(
      create = Node.Create(
        coid = lf.lfContractId,
        templateId = lf.templateId,
        arg = lf.createArg,
        agreementText = "",
        signatories = api.signatories,
        stakeholders = api.stakeholders,
        keyOpt = Some(lf.keyWithMaintainers),
        version = testTxVersion,
      ),
      createTime = Time.Timestamp.assertFromLong(api.createdAtSeconds * 1000000L),
      cantonData = lf.driverMetadataBytes,
    )
  }

  val disclosedContract: DisclosedContract = DisclosedContract(
    templateId = Some(api.templateId),
    contractId = api.contractId,
    createdEventBlob = TransactionCoder
      .encodeFatContractInstance(lf.fatContractInstance)
      .fold(
        err =>
          throw new RuntimeException(s"Cannot serialize createdEventBlob: ${err.errorMessage}"),
        identity,
      ),
  )

}
