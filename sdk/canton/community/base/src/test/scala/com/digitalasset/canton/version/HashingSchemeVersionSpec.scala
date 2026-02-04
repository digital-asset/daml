// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.version.HashingSchemeVersion.{V2, V3}
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.SortedSet

class HashingSchemeVersionSpec extends AnyWordSpec with Matchers with OptionValues {

  "return the minimal protocol version" in {
    HashingSchemeVersion.minProtocolVersionForHSV(HashingSchemeVersion.V2) shouldBe Some(
      ProtocolVersion.v34
    )
    // TODO(#30463): Enable once V3 is supported in v35
    // HashingSchemeVersion.minProtocolVersionForHSV(HashingSchemeVersion.V3) shouldBe Some(
    //   ProtocolVersion.v35
    // )
    HashingSchemeVersion.minProtocolVersionForHSV(HashingSchemeVersion.V3) shouldBe Some(
      ProtocolVersion.dev
    )
  }
  "return the protocol hashing version" in {

    HashingSchemeVersion
      .getHashingSchemeVersionsForProtocolVersion(
        ProtocolVersion.v34
      )
      .forgetNE shouldBe SortedSet[HashingSchemeVersion](V2)

    // TODO(#30463): Enable once V3 is supported in v35
    //    HashingSchemeVersion
    //      .getHashingSchemeVersionsForProtocolVersion(
    //        ProtocolVersion.v35
    //      )
    //      .forgetNE shouldBe SortedSet[HashingSchemeVersion](V2, V3)

    HashingSchemeVersion
      .getHashingSchemeVersionsForProtocolVersion(
        ProtocolVersion.dev
      )
      .forgetNE shouldBe SortedSet[HashingSchemeVersion](V2, V3)
  }

}
