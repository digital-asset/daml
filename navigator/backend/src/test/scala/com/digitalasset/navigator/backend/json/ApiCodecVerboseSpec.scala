// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.json

import com.daml.navigator.model
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class ApiCodecVerboseSpec extends AnyWordSpec with Matchers {
  import com.daml.navigator.{DamlConstants => C}

  /** Serializes the API value to JSON, then parses it back to an API value */
  private def serializeAndParse(value: model.ApiValue): Try[model.ApiValue] = {
    import com.daml.navigator.json.ApiCodecVerbose.JsonImplicits._
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
        serializeAndParse(C.simpleTextV) shouldBe Success(C.simpleTextV)
      }
      "work for Int64" in {
        serializeAndParse(C.simpleInt64V) shouldBe Success(C.simpleInt64V)
      }
      "work for Decimal" in {
        serializeAndParse(C.simpleDecimalV) shouldBe Success(C.simpleDecimalV)
      }
      "work for Unit" in {
        serializeAndParse(C.simpleUnitV) shouldBe Success(C.simpleUnitV)
      }
      "work for Date" in {
        serializeAndParse(C.simpleDateV) shouldBe Success(C.simpleDateV)
      }
      "work for Timestamp" in {
        serializeAndParse(C.simpleTimestampV) shouldBe Success(C.simpleTimestampV)
      }
      "work for Optional" in {
        serializeAndParse(C.simpleOptionalV) shouldBe Success(C.simpleOptionalV)
      }
      "work for EmptyRecord" in {
        serializeAndParse(C.emptyRecordV) shouldBe Success(C.emptyRecordV)
      }
      "work for SimpleRecord" in {
        serializeAndParse(C.simpleRecordV) shouldBe Success(C.simpleRecordV)
      }
      "work for SimpleVariant" in {
        serializeAndParse(C.simpleVariantV) shouldBe Success(C.simpleVariantV)
      }
      "work for ComplexRecord" in {
        serializeAndParse(C.complexRecordV) shouldBe Success(C.complexRecordV)
      }
      "work for Tree" in {
        serializeAndParse(C.treeV) shouldBe Success(C.treeV)
      }
      "work for TextMap" in {
        serializeAndParse(C.simpleTextMapV) shouldBe Success(C.simpleTextMapV)
      }
      "work for GenMap" in {
        serializeAndParse(C.complexGenMapV) shouldBe Success(C.complexGenMapV)
      }
    }
  }
}
