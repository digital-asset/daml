// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.json

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.value.{Value => V}

import org.scalatest.{Matchers, WordSpec}
import io.circe.parser.parse
import io.circe.syntax._

class JsonConvertersSpec extends WordSpec with Matchers {
  import JsonConverters._
  import Ref.Name.{assertFromString => id}

  "records" should {
    val sampleRecord = V.ValueRecord(
      tycon = None,
      fields = ImmArray(
        (Some(id("foo")), V.ValueText("bar")),
        (Some(id("baz")), V.ValueInt64(1253049))
      ))
    val oneMissingLabel = sampleRecord.copy(fields = sampleRecord.fields map {
      case (Some("foo"), v) => (None, v)
      case o => o
    })

    "encode to JsonObject if all labels present" in {
      Right(sampleRecord.asJson) shouldBe parse("""{"foo": "bar", "baz": 1253049}""")
    }

    "encode to list of values if some label absent" in {
      Right(oneMissingLabel.asJson) shouldBe parse("""["bar", 1253049]""")
    }
  }
}
