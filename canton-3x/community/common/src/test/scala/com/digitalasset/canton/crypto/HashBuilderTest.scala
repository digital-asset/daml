// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class HashBuilderTest extends AnyWordSpec with BaseTest {

  def testHashBuilder(sutName: String, sut: HashPurpose => HashBuilder): Unit = {
    def defaultBuilder: HashBuilder = sut(HashPurpose.MerkleTreeInnerNode)

    sutName should {
      val builder1 = defaultBuilder
      val hashEmpty = builder1.finish()

      "addWithoutLengthPrefix fail after finish" in {
        assertThrows[IllegalStateException](builder1.addWithoutLengthPrefix(ByteString.EMPTY))
      }
      "finish fail after finish" in {
        assertThrows[IllegalStateException](builder1.finish())
      }

      val hashArrayL1 =
        defaultBuilder.addWithoutLengthPrefix(ByteString.copyFrom(new Array[Byte](1))).finish()
      "yield different hashes for empty and non-empty inputs" in {
        assert(hashArrayL1.getCryptographicEvidence != hashEmpty.getCryptographicEvidence)
      }

      val hashArrayL2 =
        defaultBuilder.addWithoutLengthPrefix(ByteString.copyFrom(new Array[Byte](2))).finish()
      "yield different hashes for longer inputs" in {
        assert(hashArrayL2.getCryptographicEvidence != hashArrayL1.getCryptographicEvidence)
      }

      val hashArrayL11 =
        defaultBuilder
          .addWithoutLengthPrefix(new Array[Byte](1))
          .addWithoutLengthPrefix(new Array[Byte](1))
          .finish()
      "yield the same hash for the same concatenation of chunks" in {
        assert(hashArrayL2.getCryptographicEvidence == hashArrayL11.getCryptographicEvidence)
      }

      val hashEmpty2 = sut(HashPurpose.SequencedEventSignature).finish()
      "yield different hashes for different purposes" in {
        assert(hashEmpty.getCryptographicEvidence != hashEmpty2.getCryptographicEvidence)
      }

      val builder2 = defaultBuilder
      val builder3 = builder2.addWithoutLengthPrefix(new Array[Byte](1))
      "addWithoutLengthPrefix returns the modified builder" in {
        assert(builder2 eq builder3)
      }
    }
  }

  testHashBuilder(
    "HashBuilderFromMessageDigest",
    purpose => new HashBuilderFromMessageDigest(HashAlgorithm.Sha256, purpose),
  )
}
