// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.json

import com.daml.navigator.json.DamlLfCodec.JsonImplicits._
import com.daml.navigator.model
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Success, Try}

class DamlLfCodecSpec extends WordSpec with Matchers {
  import com.daml.navigator.{DamlConstants => C}

  /** Serializes the API value to JSON, then parses it back to an API value */
  private def serializeAndParse[T](value: T)(implicit fmt: spray.json.JsonFormat[T]): Try[T] = {
    import spray.json._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[T])
    } yield parsed
  }

  "DAML-LF JSON codec" when {

    "serializing and parsing a DAML-LF object" should {

      "work for DamlLFIdentifier" in {
        serializeAndParse(C.ref0) shouldBe Success(C.ref0)
      }
      "work for DamlLfTypePrim(Text)" in {
        serializeAndParse[model.DamlLfType](C.simpleTextT) shouldBe Success(C.simpleTextT)
      }
      "work for DamlLfTypeCon(SimpleRecord)" in {
        serializeAndParse[model.DamlLfType](C.simpleRecordTC) shouldBe Success(C.simpleRecordTC)
      }
      "work for DamlLfTypeCon(Tree)" in {
        serializeAndParse[model.DamlLfType](C.treeTC) shouldBe Success(C.treeTC)
      }
      "work for DamlLfDefDataType(SimpleRecord)" in {
        serializeAndParse[model.DamlLfDefDataType](C.simpleRecordGC) shouldBe Success(
          C.simpleRecordGC)
      }
      "work for DamlLfDefDataType(Tree)" in {
        serializeAndParse[model.DamlLfDefDataType](C.treeGC) shouldBe Success(C.treeGC)
      }
      "work for DamlLfDefDataType(ComplexRecord)" in {
        serializeAndParse[model.DamlLfDefDataType](C.complexRecordGC) shouldBe Success(
          C.complexRecordGC)
      }
    }
  }
}
