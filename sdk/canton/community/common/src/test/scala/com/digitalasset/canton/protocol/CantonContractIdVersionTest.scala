// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

class CantonContractIdVersionTest extends AnyWordSpec with BaseTest {
  private val cantonContractIdVersions =
    Seq(
      AuthenticatedContractIdVersionV2,
      AuthenticatedContractIdVersion,
      NonAuthenticatedContractIdVersion,
    )

  cantonContractIdVersions.foreach { cantonContractIdVersion =>
    s"${cantonContractIdVersion}" when {
      val discriminator = ExampleTransactionFactory.lfHash(1)
      val hash =
        Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()

      val unicum = Unicum(hash)
      val cid = cantonContractIdVersion.fromDiscriminator(discriminator, unicum)

      "creating a contract ID from discriminator and unicum" should {
        "succeed" in {
          cid.coid shouldBe (
            LfContractId.V1.prefix.toHexString +
              discriminator.bytes.toHexString +
              cantonContractIdVersion.versionPrefixBytes.toHexString +
              unicum.unwrap.toHexString
          )
        }
      }

      s"ensuring canton contract id of ${cantonContractIdVersion.getClass.getSimpleName}" should {
        s"return a ${cantonContractIdVersion}" in {
          CantonContractIdVersion.ensureCantonContractId(cid) shouldBe Right(
            cantonContractIdVersion
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
          val lfCid = cantonContractIdVersion.fromDiscriminator(discriminator, unicum)

          val apiCid = lfCid.toContractIdUnchecked[Iou]
          val lfCid2 = apiCid.toLf

          lfCid2 shouldBe lfCid
        }
      }
    }
  }

  CantonContractIdVersion.getClass.getSimpleName when {
    "fromProtocolVersion" should {
      "return the correct canton contract id version" in {
        val nonAuthenticatedContractIdVersion = Seq(ProtocolVersion.v3)
        val authenticatedContractIdVersion = Seq(ProtocolVersion.v4)
        val authenticatedContractIdVersionV2 =
          Seq(ProtocolVersion.v5, ProtocolVersion.dev)

        forAll(nonAuthenticatedContractIdVersion) { pv =>
          CantonContractIdVersion.fromProtocolVersion(pv) shouldBe NonAuthenticatedContractIdVersion
        }

        forAll(authenticatedContractIdVersion) { pv =>
          CantonContractIdVersion.fromProtocolVersion(pv) shouldBe AuthenticatedContractIdVersion
        }

        forAll(authenticatedContractIdVersionV2) { pv =>
          CantonContractIdVersion.fromProtocolVersion(pv) shouldBe AuthenticatedContractIdVersionV2
        }
      }
    }
  }
}
