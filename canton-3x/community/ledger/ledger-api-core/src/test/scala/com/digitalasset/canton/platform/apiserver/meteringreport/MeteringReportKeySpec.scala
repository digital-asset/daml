// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.meteringreport

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MeteringReportKeySpec extends AnyWordSpec with Matchers {

  import MeteringReportKey.*

  MeteringReportKey.getClass.getName should {
    "read community key from path" in {
      val expected = "iENTFX4g-fAvOBTXnGjIVfesNzmWFKpo_35zpUnXEsg="
      val actual = communityKey()
      actual.algorithm shouldBe "HmacSHA256"
      actual.encoded.toBase64 shouldBe expected
    }
    "support community keys" in {
      CommunityKey.key shouldBe communityKey()
    }
    "support enterprise keys" in {
      val expected = HmacSha256.generateKey("test-enterprise")
      EnterpriseKey(expected).key shouldBe expected
    }
    "read test key from test classpath" in {
      val key = MeteringReportKey.assertReadSystemResourceAsKey(
        getClass.getClassLoader.getResource("test-metering-key.json")
      )
      key.scheme shouldBe "test"
    }
    "throw exception for invalid resource" in {
      val badResource = getClass.getClassLoader.getResource("no-such-file")
      intercept[NullPointerException] {
        MeteringReportKey.assertReadSystemResourceAsKey(badResource)
      }
    }

  }

}
