// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import io.circe.Codec
import io.circe.generic.extras.Configuration
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class CirceRelaxedParserTest
    extends AnyFreeSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {
  implicit val defConfig: Configuration = io.circe.generic.extras.Configuration.default

  implicit val relaxedCodecB: Codec[CaseB] = deriveRelaxedCodec
  implicit val relaxedCodecA: Codec[CaseA] = deriveRelaxedCodec

  val minimalExample = """{
        "a": "string1",
         "ab" : {
            "b" :  "string2"
          }
    }""".stripMargin

  val maximalExample = """{
        "a": "string1",
         "ab" : {
            "b" :  "string2",
            "nested": ["a", "b", "c"]
          },
          "bo" : {
            "b" :  "string3",
            "nested": ["a", "b", "c"]
          },
          "boList" : [{
            "b" :  "string4",
            "nested": ["a", "b", "c"]
          }],
          "boMap" : { "key" : {
            "b" :  "string5",
            "nested": ["a", "b", "c"]
          } }
    }""".stripMargin

  "circle relaxed decoder " - {
    "minimalExample" in {
      io.circe.parser.decode[CaseA](minimalExample) should be(
        Right(
          CaseA(
            a = "string1",
            ab = CaseB(b = "string2", nested = Seq.empty),
            bo = None,
            boList = Seq.empty,
            boMap = Map.empty,
          )
        )
      )
    }
    "maximalExample" in {
      io.circe.parser.decode[CaseA](maximalExample) should be(
        Right(
          CaseA(
            a = "string1",
            ab = CaseB(b = "string2", nested = Seq("a", "b", "c")),
            bo = Some(CaseB(b = "string3", nested = Seq("a", "b", "c"))),
            boList = Seq(CaseB(b = "string4", nested = Seq("a", "b", "c"))),
            boMap = Map("key" -> CaseB(b = "string5", nested = Seq("a", "b", "c"))),
          )
        )
      )
    }
  }
}

final case class CaseB(b: String, nested: Seq[String])
final case class CaseA(
    a: String,
    ab: CaseB,
    bo: Option[CaseB],
    boList: Seq[CaseB],
    boMap: scala.collection.immutable.Map[String, CaseB],
)
