// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.syntax.either.*
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ContractIdAbsolutizer.ContractIdAbsolutizationDataV2
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV10,
  AuthenticatedContractIdVersionV11,
  CantonContractIdV1Version,
  CantonContractIdV2Version,
  CantonContractIdV2Version0,
  CantonContractIdVersion,
  ContractIdAbsolutizer,
  ExampleTransactionFactory,
  LfContractId,
  LfGlobalKey,
  LfHash,
  LfLanguageVersion,
  LfTemplateId,
  RelativeContractIdSuffixV2,
  TransactionId,
  Unicum,
}
import com.digitalasset.canton.topology.{GeneratorsTopology, PartyId}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.Versioned
import com.digitalasset.daml.lf.value.Value.ValueInt64
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsLf(val generatorsTopology: GeneratorsTopology) {
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import generatorsTopology.*

  implicit val lfPartyIdArb: Arbitrary[LfPartyId] = Arbitrary(
    Arbitrary.arbitrary[PartyId].map(_.toLf)
  )

  implicit val lfTimestampArb: Arbitrary[LfTimestamp] = Arbitrary(
    Arbitrary.arbitrary[CantonTimestamp].map(_.underlying)
  )

  implicit val LedgerUserIdArb: Arbitrary[LedgerUserId] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LedgerUserId.assertFromString)
  )

  implicit val lfCommandIdArb: Arbitrary[LfCommandId] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LfCommandId.assertFromString)
  )

  implicit val lfSubmissionIdArb: Arbitrary[LfSubmissionId] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LfSubmissionId.assertFromString)
  )

  implicit val lfWorkflowIdArb: Arbitrary[LfWorkflowId] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LfWorkflowId.assertFromString)
  )

  locally {
    // If this pattern match is not exhaustive anymore, update the generators for CantonContractIdVersions below
    (_: CantonContractIdVersion) match {
      case AuthenticatedContractIdVersionV10 => ()
      case AuthenticatedContractIdVersionV11 => ()
      case CantonContractIdV2Version0 => ()
    }
  }

  implicit val cantonContractIdV1VersionArb: Arbitrary[CantonContractIdV1Version] =
    Arbitrary(Gen.oneOf(AuthenticatedContractIdVersionV10, AuthenticatedContractIdVersionV11))

  implicit val cantonContractIdV2VersionArb: Arbitrary[CantonContractIdV2Version] =
    Arbitrary(Gen.const(CantonContractIdV2Version0))

  implicit val cantonContractIdVersionArb: Arbitrary[CantonContractIdVersion] =
    Arbitrary(
      Gen.oneOf(cantonContractIdV1VersionArb.arbitrary, cantonContractIdV2VersionArb.arbitrary)
    )

  val unsuffixedLfContractIdV1Arb: Arbitrary[LfContractId.V1] = Arbitrary(
    for {
      index <- Gen.posNum[Int]
    } yield LfContractId.V1(ExampleTransactionFactory.lfHash(index), Bytes.Empty)
  )

  val suffixedLfContractIdV1Arb: Arbitrary[LfContractId.V1] = Arbitrary(
    for {
      version <- Arbitrary.arbitrary[CantonContractIdV1Version]
      unsuffixed <- Arbitrary.arbitrary(unsuffixedLfContractIdV1Arb)
      suffix <- Gen.posNum[Int]
    } yield {
      val unicum = Unicum(TestHash.digest(suffix))
      version.fromDiscriminator(unsuffixed.discriminator, unicum)
    }
  )

  implicit val lfContractIdV1Arb: Arbitrary[LfContractId.V1] = Arbitrary(
    Gen.oneOf(unsuffixedLfContractIdV1Arb.arbitrary, suffixedLfContractIdV1Arb.arbitrary)
  )

  val unsuffixedLfContractIdV2Arb: Arbitrary[LfContractId.V2] = Arbitrary(
    for {
      index <- Gen.posNum[Int]
      time <- Arbitrary.arbitrary[CantonTimestamp]
    } yield {
      val discriminator = ExampleTransactionFactory.lfHash(index)
      LfContractId.V2.unsuffixed(time.toLf, discriminator)
    }
  )

  val relativeLfContractIdV2Arb: Arbitrary[LfContractId.V2] = Arbitrary(
    for {
      version <- Arbitrary.arbitrary[CantonContractIdV2Version]
      unsuffixed <- Arbitrary.arbitrary(unsuffixedLfContractIdV2Arb)
      suffix <- Gen.posNum[Int]
    } yield {
      val relativeSuffix =
        RelativeContractIdSuffixV2(version, ExampleTransactionFactory.lfHash(suffix).bytes)
      LfContractId.V2.assertBuild(unsuffixed.local, relativeSuffix.toBytes)
    }
  )

  val absoluteLfContractIdV2Arb: Arbitrary[LfContractId.V2] = Arbitrary(
    for {
      relative <- Arbitrary.arbitrary(relativeLfContractIdV2Arb)
      index <- Gen.posNum[Int]
      ledgerTime <- Arbitrary.arbitrary[LfTimestamp]
    } yield {
      val creatingTransactionId = TransactionId(TestHash.digest(index))
      val absolutizationData =
        ContractIdAbsolutizationDataV2(creatingTransactionId, CantonTimestamp(ledgerTime))
      val (_, absoluteCid) = ContractIdAbsolutizer
        .absoluteSuffixV2(TestHash)(
          relative,
          absolutizationData,
        )
        .valueOr(err => throw new IllegalArgumentException(s"Cannot absolutize contract ID: $err"))
      absoluteCid
    }
  )

  implicit val lfContractIdV2Arb: Arbitrary[LfContractId.V2] = Arbitrary(
    Gen.oneOf(
      unsuffixedLfContractIdV2Arb.arbitrary,
      relativeLfContractIdV2Arb.arbitrary,
      absoluteLfContractIdV2Arb.arbitrary,
    )
  )

  implicit val lfContractIdArb: Arbitrary[LfContractId] = Arbitrary(
    Gen.oneOf(Arbitrary.arbitrary[LfContractId.V1], Arbitrary.arbitrary[LfContractId.V2])
  )

  val relativeLfContractIdArb: Arbitrary[LfContractId] = Arbitrary(
    Gen.oneOf(suffixedLfContractIdV1Arb.arbitrary, relativeLfContractIdV2Arb.arbitrary)
  )

  val suffixedLfContractIdArb: Arbitrary[LfContractId] = Arbitrary(
    Gen.oneOf(
      suffixedLfContractIdV1Arb.arbitrary,
      relativeLfContractIdV2Arb.arbitrary,
      absoluteLfContractIdV2Arb.arbitrary,
    )
  )

  implicit val lfHashArb: Arbitrary[LfHash] = Arbitrary(
    Gen.posNum[Int].map(ExampleTransactionFactory.lfHash)
  )

  implicit val lfChoiceNameArb: Arbitrary[LfChoiceName] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LfChoiceName.assertFromString)
  )

  implicit val lfPackageId: Arbitrary[LfPackageId] = Arbitrary(
    Gen.stringOfN(64, Gen.alphaChar).map(LfPackageId.assertFromString)
  )

  implicit val lfTemplateIdArb: Arbitrary[LfTemplateId] = Arbitrary(for {
    packageName <- Gen.stringOfN(8, Gen.alphaChar)
    moduleName <- Gen.stringOfN(8, Gen.alphaChar)
    scriptName <- Gen.stringOfN(8, Gen.alphaChar)
  } yield LfTemplateId.assertFromString(s"$packageName:$moduleName:$scriptName"))

  private val lfVersionedGlobalKeyGen: Gen[Versioned[LfGlobalKey]] = for {
    templateId <- Arbitrary.arbitrary[LfTemplateId]
    // We consider only this specific value because the goal is not exhaustive testing of LF (de)serialization
    value <- Gen.long.map(ValueInt64.apply)
  } yield ExampleTransactionFactory.globalKey(templateId, value)

  implicit val lfGlobalKeyArb: Arbitrary[LfGlobalKey] = Arbitrary(
    lfVersionedGlobalKeyGen.map(_.unversioned)
  )

  implicit val lfVersionedGlobalKeyArb: Arbitrary[Versioned[LfGlobalKey]] = Arbitrary(
    lfVersionedGlobalKeyGen
  )

  implicit val LfLanguageVersionArb: Arbitrary[LfLanguageVersion] =
    Arbitrary(Gen.oneOf(LfLanguageVersion.AllV2))

}
