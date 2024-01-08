// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class HashTest extends AnyWordSpec with BaseTest {

  private val longString =
    (('a' to 'z').mkString + ('A' to 'Z').mkString + ('0' to '9').mkString + ":") * 10

  forAll(HashAlgorithm.algorithms.values) { algorithm =>
    s"${algorithm.name}" should {
      val hash =
        Hash.digest(TestHash.testHashPurpose, ByteString.copyFromUtf8(longString), algorithm)

      "serializing and deserializing via bytestring" in {
        Hash.fromByteString(hash.getCryptographicEvidence) === Right(hash)
      }

      "serializing and deserializing via hexstring" in {
        Hash.fromHexString(hash.toHexString) === Right(hash)
      }

    }

  }

}
