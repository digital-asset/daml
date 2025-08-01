// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Salt, TestHash, TestSalt}
import com.digitalasset.canton.util.LfTransactionBuilder
import com.digitalasset.daml.lf.data.Ref.PackageName
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  GlobalKeyWithMaintainers,
  Node,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ContractId, ValueInt64}
import org.scalatest.EitherValues

import scala.util.Random

object ExampleContractFactory extends EitherValues {

  private val random = new Random(0)
  private val unicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())

  def lfHash(index: Int = random.nextInt()): LfHash =
    LfHash.assertFromBytes(
      Bytes.assertFromString(f"$index%04x".padTo(LfHash.underlyingHashLength * 2, '0'))
    )

  val signatory: LfPartyId = LfPartyId.assertFromString("signatory::default")
  val observer: LfPartyId = LfPartyId.assertFromString("observer::default")
  val extra: LfPartyId = LfPartyId.assertFromString("extra::default")

  val templateId: LfTemplateId = LfTransactionBuilder.defaultTemplateId
  val packageName: PackageName = LfTransactionBuilder.defaultPackageName

  def build[Time <: CreationTime](
      templateId: Ref.Identifier = templateId,
      packageName: Ref.PackageName = packageName,
      argument: Value = ValueInt64(random.nextLong()),
      createdAt: Time = CreationTime.CreatedAt(Time.Timestamp.now()),
      salt: Salt = TestSalt.generateSalt(random.nextInt()),
      signatories: Set[Ref.Party] = Set(signatory),
      stakeholders: Set[Ref.Party] = Set(signatory, observer, extra),
      keyOpt: Option[GlobalKeyWithMaintainers] = None,
      version: LanguageVersion = LanguageVersion.default,
      cantonContractIdVersion: CantonContractIdV1Version = AuthenticatedContractIdVersionV11,
      overrideContractId: Option[ContractId] = None,
  ): GenContractInstance { type InstCreatedAtTime <: Time } = {

    val discriminator = lfHash()

    // Template ID must be common across contract and key
    val contractTemplateId = keyOpt.map(_.globalKey.templateId).getOrElse(templateId)

    val create = Node.Create(
      coid = LfContractId.V1(discriminator),
      templateId = contractTemplateId,
      packageName = packageName,
      arg = argument,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = keyOpt,
      version = version,
    )
    val unsuffixed = FatContractInstance.fromCreateNode(
      create,
      createdAt,
      ContractAuthenticationDataV1(salt)(cantonContractIdVersion).toLfBytes,
    )

    val unicum = unicumGenerator.recomputeUnicum(unsuffixed, cantonContractIdVersion).value

    val contractId =
      overrideContractId.getOrElse(cantonContractIdVersion.fromDiscriminator(discriminator, unicum))

    val inst = FatContractInstance.fromCreateNode(
      create.copy(coid = contractId),
      createdAt,
      ContractAuthenticationDataV1(salt)(cantonContractIdVersion).toLfBytes,
    )

    ContractInstance.create(inst).value
  }

  def buildContractId(
      index: Int = random.nextInt(),
      cantonContractIdVersion: CantonContractIdV1Version = AuthenticatedContractIdVersionV11,
  ): ContractId =
    cantonContractIdVersion.fromDiscriminator(lfHash(index), Unicum(TestHash.digest(index)))

  def modify[Time <: CreationTime](
      base: GenContractInstance { type InstCreatedAtTime <: Time },
      contractId: Option[ContractId] = None,
      metadata: Option[ContractMetadata] = None,
      arg: Option[Value] = None,
      templateId: Option[LfTemplateId] = None,
      packageName: Option[PackageName] = None,
  ): GenContractInstance { type InstCreatedAtTime <: Time } = {

    val create = base.toLf
    val inst = FatContractInstance.fromCreateNode(
      base.toLf.copy(
        coid = contractId.getOrElse(create.coid),
        templateId = templateId.getOrElse(create.templateId),
        arg = arg.getOrElse(create.arg),
        signatories = metadata.map(_.signatories).getOrElse(create.signatories),
        stakeholders = metadata.map(_.stakeholders).getOrElse(create.stakeholders),
        keyOpt = metadata.map(_.maybeKeyWithMaintainers).getOrElse(create.keyOpt),
        packageName = packageName.getOrElse(create.packageName),
      ),
      base.inst.createdAt,
      base.inst.cantonData,
    )

    ContractInstance.create(inst).value
  }

}
