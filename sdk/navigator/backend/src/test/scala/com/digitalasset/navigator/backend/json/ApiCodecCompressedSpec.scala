// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.json

import com.daml.navigator.model
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class ApiCodecCompressedSpec extends AnyWordSpec with Matchers {
  import com.daml.navigator.{DamlConstants => C}

  /** Serializes the API value to JSON, then parses it back to an API value */
  private def serializeAndParse(
      value: model.ApiValue,
      typ: model.DamlLfType,
  ): Try[model.ApiValue] = {
    import com.daml.lf.value.json.ApiCodecCompressed
    import ApiCodecCompressed.JsonImplicits._
    import spray.json._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(ApiCodecCompressed.jsValueToApiValue(json, typ, C.allTypes.get _))
    } yield parsed
  }

  "API verbose JSON codec" when {

    "serializing and parsing a value" should {
      "work for SimpleRecord" in {
        serializeAndParse(C.simpleRecordV, C.simpleRecordTC) shouldBe Success(C.simpleRecordV)
      }
      "work for SimpleVariant" in {
        serializeAndParse(C.simpleVariantV, C.simpleVariantTC) shouldBe Success(C.simpleVariantV)
      }
      "work for ComplexRecord" in {
        serializeAndParse(C.complexRecordV, C.complexRecordTC) shouldBe Success(C.complexRecordV)
      }
      "work for Tree" in {
        serializeAndParse(C.treeV, C.treeTC) shouldBe Success(C.treeV)
      }
      "work for Enum" in {
        serializeAndParse(C.redV, C.redTC) shouldBe Success(C.redV)
      }
    }
  }
}
