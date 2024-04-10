// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import io.circe.parser.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MeteringTest extends AnyWordSpec with Matchers {

  private val sample =
    """
      |{
      |  "s": "abc",
      |  "b": true,
      |  "n": null,
      |  "d": 2.3,
      |  "a": [1,2,3],
      |  "o": { "x": 1, "y": 2 }
      |}
      |""".stripMargin

  "StructEncoderDecoder" should {
    "serialize/deserialize without loss" in {
      val expected = parse(sample)
      val struct = expected.flatMap(j => StructEncoderDecoder(j.hcursor))
      val actual = struct.map(s => StructEncoderDecoder(s))
      actual shouldBe expected
    }
  }

}
