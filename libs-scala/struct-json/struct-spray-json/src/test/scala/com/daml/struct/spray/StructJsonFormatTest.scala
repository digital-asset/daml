// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.struct.spray

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._

class StructJsonFormatTest extends AnyWordSpec with Matchers {

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

  StructJsonFormat.getClass.getName should {
    "serialize/deserialize without loss" in {
      val expected = sample.parseJson
      val struct = StructJsonFormat.read(expected)
      val actual = StructJsonFormat.write(struct)
      actual shouldBe expected
    }
  }
}
