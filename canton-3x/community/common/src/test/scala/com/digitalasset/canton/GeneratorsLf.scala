// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.lf.value.Value.ValueInt64
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, TestHash}
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV2,
  ExampleTransactionFactory,
  LfContractId,
  LfGlobalKey,
  LfHash,
  LfTemplateId,
  LfTransactionVersion,
  Unicum,
}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsLf {
  implicit val lfPartyIdArb: Arbitrary[LfPartyId] = Arbitrary(
    com.digitalasset.canton.topology.GeneratorsTopology.partyIdArb.arbitrary.map(_.toLf)
  )

  implicit val lfContractIdArb: Arbitrary[LfContractId] = Arbitrary(
    for {
      index <- Gen.posNum[Int]
      contractIdDiscriminator = ExampleTransactionFactory.lfHash(index)

      suffix <- Gen.posNum[Int]
      contractIdSuffix = Unicum(
        Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(suffix).finish()
      )
    } yield AuthenticatedContractIdVersionV2.fromDiscriminator(
      contractIdDiscriminator,
      contractIdSuffix,
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

  implicit val lfGlobalKeyArb: Arbitrary[LfGlobalKey] = Arbitrary(for {
    templateId <- Arbitrary.arbitrary[LfTemplateId]

    // We consider only this specific value because the goal is not exhaustive testing of LF (de)serialization
    value <- Gen.long.map(ValueInt64)
  } yield LfGlobalKey.assertBuild(templateId, value))

  implicit val lfTransactionVersionArb: Arbitrary[LfTransactionVersion] = genArbitrary
}
