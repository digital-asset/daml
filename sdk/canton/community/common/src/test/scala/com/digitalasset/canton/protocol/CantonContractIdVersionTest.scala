// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.{BaseTest, LfTimestamp}
import com.digitalasset.daml.lf.data.Bytes
import org.scalatest.wordspec.AnyWordSpec

class CantonContractIdVersionTest extends AnyWordSpec with BaseTest {

  forEvery(CantonContractIdVersion.allV1) { underTest =>
    s"$underTest" when {
      val discriminator = ExampleTransactionFactory.lfHash(1)
      val unicum = Unicum(TestHash.digest(1))
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
          CantonContractIdVersion.extractCantonContractIdVersion(cid) shouldBe
            Right(underTest)
        }
      }
    }
  }

  forEvery(Seq(CantonContractIdV2Version0)) { underTest =>
    s"$underTest" when {
      val timestamp = LfTimestamp.Epoch.addMicros(12345678)
      val discriminator = ExampleTransactionFactory.lfHash(1)
      val suffix = Bytes.fromByteString(TestHash.digest(1).unwrap)
      val cidLocal = LfContractId.V2.unsuffixed(timestamp, discriminator)

      "extracting a canton contract id version" should {
        "work for relative suffixes" in {
          val cidRelative =
            LfContractId.V2.assertBuild(
              cidLocal.local,
              underTest.versionPrefixBytesRelative ++ suffix,
            )
          CantonContractIdVersion.extractCantonContractIdVersion(cidRelative) shouldBe
            Right(underTest)
        }

        "work for absolute suffixes" in {
          val cidAbsolute =
            LfContractId.V2.assertBuild(
              cidLocal.local,
              underTest.versionPrefixBytesAbsolute ++ suffix,
            )
          CantonContractIdVersion.extractCantonContractIdVersion(cidAbsolute) shouldBe
            Right(underTest)
        }
      }
    }
  }
}
