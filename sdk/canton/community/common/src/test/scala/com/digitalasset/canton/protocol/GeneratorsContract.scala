// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsContract(version: CantonContractIdVersion) {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*

  def contractAuthenticationDataV1Arb(
      version: CantonContractIdV1Version
  ): Arbitrary[ContractAuthenticationDataV1] =
    Arbitrary(
      for {
        salt <- Arbitrary.arbitrary[Salt]
      } yield ContractAuthenticationDataV1(salt)(version)
    )

  def contractAuthenticationDataV2Arb(
      version: CantonContractIdV2Version
  ): Arbitrary[ContractAuthenticationDataV2] =
    Arbitrary(
      for {
        salt <- Arbitrary.arbitrary[Salt]
        transactionId <- Gen.option(Arbitrary.arbitrary[TransactionId])
        relativeArgumentSuffixes <- boundedListGen[ByteString]
      } yield ContractAuthenticationDataV2(
        Bytes.fromByteString(salt.forHashing),
        transactionId,
        relativeArgumentSuffixes.sorted(ByteStringUtil.orderingByteString).map(Bytes.fromByteString),
      )(version)
    )

  implicit val contractAuthenticationDataArb: Arbitrary[ContractAuthenticationData] =
    version match {
      case v1: CantonContractIdV1Version => Arbitrary(contractAuthenticationDataV1Arb(v1).arbitrary)
      case v2: CantonContractIdV2Version => Arbitrary(contractAuthenticationDataV2Arb(v2).arbitrary)
    }
}
