// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package support.crypto.format

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

class RLPTest extends AnyFreeSpec with Matchers {

  import RLP.{RLPBlock, RLPList}

  "correctly decode valid RLP hex strings" in {
    val testCases = Table(
      ("Hex String", "RLP Tree"),
      (
        Ref.HexString.assertFromString("80"),
        RLPBlock(Bytes.Empty),
      ),
      (
        Ref.HexString.assertFromString("84deadbeef"),
        RLPBlock(Bytes.assertFromString("deadbeef")),
      ),
      (
        Ref.HexString.assertFromString("c0"),
        RLPList(),
      ),
      (
        Ref.HexString.assertFromString("c88363617483646f67"),
        RLPList(
          RLPBlock(Bytes.assertFromString("636174")),
          RLPBlock(Bytes.assertFromString("646f67")),
        ),
      ),
      (
        Ref.HexString.assertFromString("c7c0c1c0c3c0c1c0"),
        RLPList(RLPList(), RLPList(RLPList()), RLPList(RLPList(), RLPList(RLPList()))),
      ),
    )

    forAll(testCases) { case (hexStr, rlpTree) =>
      RLP.decode(Bytes.fromHexString(hexStr)) shouldBe rlpTree
    }
  }

  "correctly encode RLP trees" in {
    val testCases = Table(
      ("RLP Tree", "Hex String"),
      (
        RLPBlock(Bytes.Empty),
        Ref.HexString.assertFromString("80"),
      ),
      (
        RLPBlock(Bytes.assertFromString("deadbeef")),
        Ref.HexString.assertFromString("84deadbeef"),
      ),
      (
        RLPList(),
        Ref.HexString.assertFromString("c0"),
      ),
      (
        RLPList(
          RLPBlock(Bytes.assertFromString("636174")),
          RLPBlock(Bytes.assertFromString("646f67")),
        ),
        Ref.HexString.assertFromString("c88363617483646f67"),
      ),
      (
        RLPList(RLPList(), RLPList(RLPList()), RLPList(RLPList(), RLPList(RLPList()))),
        Ref.HexString.assertFromString("c7c0c1c0c3c0c1c0"),
      ),
    )

    forAll(testCases) { case (rlpTree, hexStr) =>
      RLP.encode(rlpTree) shouldBe Bytes.fromHexString(hexStr)
    }
  }
}
