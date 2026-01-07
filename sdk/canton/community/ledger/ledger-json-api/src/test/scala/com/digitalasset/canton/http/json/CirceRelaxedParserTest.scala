// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.testproto.all_types_message.AllTypesMessage
import io.circe.generic.extras.Configuration
import io.circe.syntax.EncoderOps
import io.circe.{Codec, Decoder, Encoder}
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

  "custom example" in {
    implicit val nestedMessageCodec: Codec[AllTypesMessage.NestedMessage] = deriveRelaxedCodec
    implicit val sampleEnumCodec: Codec[AllTypesMessage.SampleEnum] = deriveRelaxedCodec
    implicit val byteStringCodec: Codec[com.google.protobuf.ByteString] =
      Codec.from(
        Decoder.decodeString.map(com.google.protobuf.ByteString.copyFromUtf8),
        Encoder.encodeString.contramap(_.toStringUtf8),
      )
    implicit val oneOfNestedCodec: Codec[AllTypesMessage.OneofField.OneofNested] =
      deriveRelaxedCodec
    implicit val oneOfStringCodec: Codec[AllTypesMessage.OneofField.OneofString] =
      deriveRelaxedCodec
    implicit val oneOfEmptyCodec: Codec[AllTypesMessage.OneofField.Empty.type] =
      deriveRelaxedCodec
    implicit val oneOfFieldCodec: Codec[AllTypesMessage.OneofField] = deriveRelaxedCodec
    implicit val relaxedCodecAllTypesMessage: Codec[AllTypesMessage] = deriveRelaxedCodec

    val expected = AllTypesMessage(
      doubleField = 0d,
      floatField = 0f,
      boolField = false,
      stringField = "",
      bytesField = com.google.protobuf.ByteString.EMPTY,
      enumField = AllTypesMessage.SampleEnum.Unrecognized(0),
      nestedMessageField = None,
      oneofField = AllTypesMessage.OneofField.Empty,
      mapField = Map.empty,
      int32Field = 0,
      int64Field = 0L,
      uint32Field = 0,
      uint64Field = 0L,
      sint32Field = 0,
      sint64Field = 0L,
      fixed32Field = 0,
      fixed64Field = 0L,
      sfixed32Field = 0,
      sfixed64Field = 0L,
    )

    val jsonRendering = expected.asJson.spaces4
    io.circe.parser.decode[AllTypesMessage](jsonRendering) should be(Right(expected))

    inside(io.circe.parser.decode[AllTypesMessage]("""{ }""".stripMargin)) {
      case Right(value) => value shouldBe expected
      case Left(err) => fail(s"decoding failed: $err")
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
