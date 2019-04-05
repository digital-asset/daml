// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.json

import com.digitalasset.navigator.model
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Success, Try}

class ApiCodecVerboseSpec extends WordSpec with Matchers {
  import com.digitalasset.navigator.{DamlConstants => C}

  /** Serializes the API value to JSON, then parses it back to an API value */
  private def serializeAndParse(
      value: model.ApiValue,
      typ: model.DamlLfType): Try[model.ApiValue] = {
    import com.digitalasset.navigator.json.ApiCodecVerbose.JsonImplicits._
    import spray.json._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[model.ApiValue])
    } yield parsed
  }

  "API verbose JSON codec" when {

    "serializing and parsing a value" should {

      "work for Text" in {
        serializeAndParse(C.simpleTextV, C.simpleTextT) shouldBe Success(C.simpleTextV)
      }
      "work for Int64" in {
        serializeAndParse(C.simpleInt64V, C.simpleInt64T) shouldBe Success(C.simpleInt64V)
      }
      "work for Decimal" in {
        serializeAndParse(C.simpleDecimalV, C.simpleDecimalT) shouldBe Success(C.simpleDecimalV)
      }
      "work for Unit" in {
        serializeAndParse(C.simpleUnitV, C.simpleUnitT) shouldBe Success(C.simpleUnitV)
      }
      "work for Date" in {
        serializeAndParse(C.simpleDateV, C.simpleDateT) shouldBe Success(C.simpleDateV)
      }
      "work for Timestamp" in {
        serializeAndParse(C.simpleTimestampV, C.simpleTimestampT) shouldBe Success(
          C.simpleTimestampV)
      }
      "work for Optional" in {
        serializeAndParse(C.simpleOptionalV, C.simpleOptionalT(C.simpleTextT)) shouldBe Success(
          C.simpleOptionalV)
      }
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
      "work for Map" in {
        serializeAndParse(C.simpleMapV, C.simpleMapT(C.simpleTextT)) shouldBe Success(C.simpleMapV)
      }
    }
  }
}
