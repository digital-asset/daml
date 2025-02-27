// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.*
import org.scalatest.wordspec.AnyWordSpec

class CantonContractIdVersionTest extends AnyWordSpec with BaseTest {

  forEvery(Seq(AuthenticatedContractIdVersionV10, AuthenticatedContractIdVersionV11)) { underTest =>
    s"$underTest" when {
      val discriminator = ExampleTransactionFactory.lfHash(1)
      val hash =
        Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()

      val unicum = Unicum(hash)

      val cid = underTest.fromDiscriminator(discriminator, unicum)

      "creating a contract ID from discriminator and unicum" should {
        "succeed" in {
          cid.coid shouldBe (
            LfContractId.V1.prefix.toHexString +
              discriminator.bytes.toHexString +
              underTest.versionPrefixBytes.toHexString +
              unicum.unwrap.toHexString
          )
        }
      }

      "extracting a canton-contract-id-version" should {
        "succeed" in {
          CantonContractIdVersion.extractCantonContractIdVersion(cid) shouldBe Right(
            underTest
          )
        }
      }
    }
  }
}
