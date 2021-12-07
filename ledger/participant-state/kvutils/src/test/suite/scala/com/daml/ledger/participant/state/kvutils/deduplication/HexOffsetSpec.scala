// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import com.daml.lf.data.Ref
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.wordspec.AnyWordSpec

class HexOffsetSpec extends AnyWordSpec with Matchers {

  "building lexicographical string" should {

    "return None if the lexicographically previous string does not exist" in {
      HexOffset.firstBefore(
        Ref.HexString.assertFromString("000000000000")
      ) shouldBe None
    }

    "return expected lower offsets" in {
      forAll(
        Table(
          "offset" -> "expected offset",
          "00000001" -> "00000000",
          "0000000a" -> "00000009",
          "00000100" -> "000000ff",
          "a0000000" -> "9fffffff",
          "00000ab0" -> "00000aaf",
          "00007a90" -> "00007a8f",
          "00007a94" -> "00007a93",
        )
      ) { case (offset, expectedOffset) =>
        HexOffset.firstBefore(
          Ref.HexString.assertFromString(offset)
        ) shouldBe Some(Ref.HexString.assertFromString(expectedOffset))
      }
    }

  }
}
