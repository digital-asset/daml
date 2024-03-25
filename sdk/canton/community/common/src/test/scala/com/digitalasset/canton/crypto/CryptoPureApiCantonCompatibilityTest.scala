// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV10,
  ExampleTransactionFactory,
  Unicum,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

/** Daml lf and daml-on-x impose a maximum limit on contractIds for which Canton uses hashes.
  * Test that (hex) string versions of all hash variants are below the limit.
  */
class CryptoPureApiCantonCompatibilityTest extends AnyWordSpec with BaseTest {

  private val cantonContractIdVersion = AuthenticatedContractIdVersionV10
  private val longString =
    (('a' to 'z').mkString + ('A' to 'Z').mkString + ('0' to '9').mkString + ":") * 10

  forAll(HashAlgorithm.algorithms.values) { algorithm =>
    s"${algorithm.name}" should {
      val hash =
        Hash.digest(TestHash.testHashPurpose, ByteString.copyFromUtf8(longString), algorithm)
      val maxHashSize = 46

      s"produce binary hashes of length less than ${maxHashSize} bytes" in {
        hash.getCryptographicEvidence.size() should be <= maxHashSize
      }

      s"produce string hashes of length ${maxHashSize * 2} chars" in {
        hash.toHexString.length should be <= (maxHashSize * 2)
      }

      "be able to build a ContractId" in {
        cantonContractIdVersion.fromDiscriminator(
          ExampleTransactionFactory.submissionSeed,
          Unicum(hash),
        )
      }
    }
  }
}
