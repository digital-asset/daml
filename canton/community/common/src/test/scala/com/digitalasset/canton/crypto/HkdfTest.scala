// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.HexString
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait HkdfTest {
  this: AsyncWordSpec with BaseTest =>

  private case class TestCase(
      ikm: ByteString,
      salt: ByteString,
      info: ByteString,
      length: Int,
      prk: ByteString,
      okm: ByteString,
  )

  private object TestCase {
    def apply(
        ikmS: String,
        saltS: String,
        infoS: String,
        length: Int,
        prkS: String,
        okmS: String,
    ): TestCase = {
      def parse(input: String, desc: String): ByteString = {
        HexString
          .parseToByteString(input.stripMargin.filter(_ != '\n'))
          .valueOrFail(s"Invalid $desc string: $input")
      }

      val ikm = parse(ikmS, "input key material")
      val salt = parse(saltS, "salt")
      val info = parse(infoS, "info")
      val prk = parse(prkS, "pseudo-random key")
      val okm = parse(okmS, "output key material")

      TestCase(ikm = ikm, salt = salt, info = info, length = length, prk = prk, okm = okm)
    }
  }

  private lazy val testCases = List(
    TestCase(
      ikmS = "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b",
      saltS = "000102030405060708090a0b0c",
      infoS = "f0f1f2f3f4f5f6f7f8f9",
      length = 42,
      prkS = """077709362c2e32df0ddc3f0dc47bba63
          |90b6c73bb50f9c3122ec844ad7c2b3e5""",
      okmS = """3cb25f25faacd57a90434f64d0362f2a
          |2d2d0a90cf1a5a4c5db02d56ecc4c5bf
          |34007208d5b887185865""",
    ),
    TestCase(
      ikmS = """000102030405060708090a0b0c0d0e0f
          |101112131415161718191a1b1c1d1e1f
          |202122232425262728292a2b2c2d2e2f
          |303132333435363738393a3b3c3d3e3f
          |404142434445464748494a4b4c4d4e4f""",
      saltS = """606162636465666768696a6b6c6d6e6f
          |707172737475767778797a7b7c7d7e7f
          |808182838485868788898a8b8c8d8e8f
          |909192939495969798999a9b9c9d9e9f
          |a0a1a2a3a4a5a6a7a8a9aaabacadaeaf""",
      infoS = """b0b1b2b3b4b5b6b7b8b9babbbcbdbebf
          |c0c1c2c3c4c5c6c7c8c9cacbcccdcecf
          |d0d1d2d3d4d5d6d7d8d9dadbdcdddedf
          |e0e1e2e3e4e5e6e7e8e9eaebecedeeef
          |f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff""",
      length = 82,
      prkS = """06a6b88c5853361a06104c9ceb35b45c
          |ef760014904671014a193f40c15fc244""",
      okmS = """b11e398dc80327a1c8e7f78c596a4934
        |4f012eda2d4efad8a050cc4c19afa97c
        |59045a99cac7827271cb41c65e590e09
        |da3275600c2f09b8367793a9aca3db71
        |cc30c58179ec3e87c14c01d5c1f3434f
        |1d87""",
    ),
    TestCase(
      ikmS = "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b",
      saltS = "",
      infoS = "",
      length = 42,
      prkS = """19ef24a32c717b167f33a91d6f648bdf
               |96596776afdb6377ac434c1c293ccb04""",
      okmS = """8da4e775a563c18f715f802a063c5a31
               |b8a11f5c5ee1879ec3454e5f3c738d2d
               |9d201395faa4b61a96c8""",
    ),
  )

  def hkdfProvider(providerF: => Future[HkdfOps with RandomOps]): Unit = {
    "HKDF provider" should {
      "pass golden tests from RFC 5869 for extract-and-expand" in {
        val algo = HmacAlgorithm.HmacSha256
        providerF.map { provider =>
          forAll(testCases) { testCase =>
            val expanded =
              provider
                .computeHkdf(
                  testCase.ikm,
                  testCase.length,
                  HkdfInfo.testOnly(testCase.info),
                  testCase.salt,
                  algo,
                )
                .valueOrFail("Could not compute the HMAC for test vector")
            expanded.unwrap shouldBe testCase.okm
          }
        }
      }
    }
  }
}
