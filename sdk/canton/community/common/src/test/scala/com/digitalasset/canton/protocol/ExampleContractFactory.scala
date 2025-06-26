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

  def build(
      templateId: Ref.Identifier = templateId,
      packageName: Ref.PackageName = packageName,
      argument: Value = ValueInt64(random.nextLong()),
      createdAt: Time.Timestamp = Time.Timestamp.now(),
      salt: Salt = TestSalt.generateSalt(random.nextInt()),
      signatories: Set[Ref.Party] = Set(signatory),
      stakeholders: Set[Ref.Party] = Set(signatory, observer, extra),
      keyOpt: Option[GlobalKeyWithMaintainers] = None,
      version: LanguageVersion = LanguageVersion.default,
      cantonContractIdVersion: CantonContractIdVersion = AuthenticatedContractIdVersionV11,
      overrideContractId: Option[ContractId] = None,
  ): ContractInstance = {

    val discriminator = lfHash()

    val create = Node.Create(
      coid = LfContractId.V1(discriminator),
      templateId = templateId,
      packageName = packageName,
      arg = argument,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = keyOpt,
      version = version,
    )
    val unsuffixed = FatContractInstance.fromCreateNode(
      create,
      CreationTime.CreatedAt(createdAt),
      DriverContractMetadata(salt).toLfBytes(cantonContractIdVersion),
    )

    val unicum = unicumGenerator.recomputeUnicum(unsuffixed, cantonContractIdVersion).value

    val contractId =
      overrideContractId.getOrElse(cantonContractIdVersion.fromDiscriminator(discriminator, unicum))

    val inst = FatContractInstance.fromCreateNode(
      create.copy(coid = contractId),
      CreationTime.CreatedAt(createdAt),
      DriverContractMetadata(salt).toLfBytes(cantonContractIdVersion),
    )

    ContractInstance(inst).value

  }

  def buildContractId(
      index: Int = random.nextInt(),
      cantonContractIdVersion: CantonContractIdVersion = AuthenticatedContractIdVersionV11,
  ): ContractId =
    cantonContractIdVersion.fromDiscriminator(lfHash(index), Unicum(TestHash.digest(index)))

}
