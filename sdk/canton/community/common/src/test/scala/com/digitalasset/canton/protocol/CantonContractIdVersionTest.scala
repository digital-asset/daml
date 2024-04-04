// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

class CantonContractIdVersionTest extends AnyWordSpec with BaseTest {

  "AuthenticatedContractIdVersionV2" when {
    val discriminator = ExampleTransactionFactory.lfHash(1)
    val hash =
      Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()

    val unicum = Unicum(hash)
    val cid = AuthenticatedContractIdV2.fromDiscriminator(discriminator, unicum)

    "creating a contract ID from discriminator and unicum" should {
      "succeed" in {
        cid.coid shouldBe (
          LfContractId.V1.prefix.toHexString +
            discriminator.bytes.toHexString +
            AuthenticatedContractIdV2.versionPrefixBytes.toHexString +
            unicum.unwrap.toHexString
        )
      }
    }

    s"ensuring canton contract id of AuthenticatedContractIdVersionV2" should {
      s"return a AuthenticatedContractIdVersionV2" in {
        CantonContractIdVersion.ensureCantonContractId(cid) shouldBe Right(
          AuthenticatedContractIdV2
        )
      }
    }

    "converting between API and LF types" should {
      import ContractIdSyntax.*
      "work both ways" in {
        val discriminator = ExampleTransactionFactory.lfHash(1)
        val hash =
          Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()
        val unicum = Unicum(hash)
        val lfCid = AuthenticatedContractIdV2.fromDiscriminator(discriminator, unicum)

        val apiCid = lfCid.toContractIdUnchecked[Iou]
        val lfCid2 = apiCid.toLf

        lfCid2 shouldBe lfCid
      }
    }
  }

  CantonContractIdVersion.getClass.getSimpleName when {
    "fromProtocolVersion" should {
      "return the correct canton contract id version" in {
        CantonContractIdVersion.fromProtocolVersion(
          ProtocolVersion.v3
        ) shouldBe NonAuthenticatedContractId

        CantonContractIdVersion.fromProtocolVersion(
          ProtocolVersion.v4
        ) shouldBe AuthenticatedContractIdV1

        CantonContractIdVersion.fromProtocolVersion(
          ProtocolVersion.v5
        ) shouldBe AuthenticatedContractIdV2

        forAll(ProtocolVersion.stableAndSupported.filter(_ >= ProtocolVersion.v6))(
          CantonContractIdVersion.fromProtocolVersion(_) shouldBe AuthenticatedContractIdV3
        )
      }
    }
  }
}
