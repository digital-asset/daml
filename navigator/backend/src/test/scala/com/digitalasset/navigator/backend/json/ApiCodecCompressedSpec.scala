// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.json

import com.digitalasset.navigator.model
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Success, Try}

class ApiCodecCompressedSpec extends WordSpec with Matchers {
  import com.digitalasset.navigator.{DamlConstants => C}

  /** Serializes the API value to JSON, then parses it back to an API value */
  private def serializeAndParse(
      value: model.ApiValue,
      typ: model.DamlLfType): Try[model.ApiValue] = {
    import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
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
      "work for EmptyRecord" in {
        serializeAndParse(C.emptyRecordV, C.emptyRecordTC) shouldBe Success(C.emptyRecordV)
      }
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
