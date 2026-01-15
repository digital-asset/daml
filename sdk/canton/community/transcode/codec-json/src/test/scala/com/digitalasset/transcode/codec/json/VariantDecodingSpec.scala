// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.json

import com.digitalasset.transcode.schema.{DynamicValue, Identifier, VariantConName, *}
import com.digitalasset.transcode.utils.codecspec.CodecCommonSpec
import com.digitalasset.transcode.{Codec, IncorrectVariantRepresentationException}
import ujson.Value
import ujson.Value.InvalidData
import zio.*
import zio.test.*
import zio.test.Assertion.*

object VariantDecodingSpec extends CodecCommonSpec[Value]:
  override val schemaProcessor = JsonCodec()
  val codec = schemaProcessor.constructor(
    Identifier.fromString("aa:aa:aa"),
    Seq(),
    schemaProcessor.variant(
      Seq(
        VariantConName("case1") -> schemaProcessor.text,
        VariantConName("case2") -> schemaProcessor.int64,
      )
    ),
  )

  override def spec =
    suite("Variant decoding in JsonCodec")(
      test("correct example") {
        val value = ujson.Obj("tag" -> "case1", "value" -> ujson.Str("value1"))
        assert(
          decode(
            value,
            codec,
          )
        )(equalTo(DynamicValue.Variant(0, DynamicValue.Text("value1"))))
      },
      test("incorrect tag field name in variant") {
        val value = ujson.Obj("tagi" -> "case1", "value" -> ujson.Str("value1"))
        assertZIO(
          ZIO
            .attempt(
              decode(
                value,
                codec,
              )
            )
            .exit
        )(
          fails(
            isSubtype[IncorrectVariantRepresentationException](hasMessage(containsString("tag")))
          )
        )
      },
      test("missing tag field name in variant") {
        val value = ujson.Obj("value" -> ujson.Str("value1"))
        assertZIO(
          ZIO
            .attempt(
              decode(
                value,
                codec,
              )
            )
            .exit
        )(
          fails(
            isSubtype[IncorrectVariantRepresentationException](hasMessage(containsString("value")))
          )
        )
      },
      test("wrong tag field value in variant") {
        val value = ujson.Obj("tag" -> "casa", "value" -> ujson.Str("value1"))
        assertZIO(
          ZIO
            .attempt(
              decode(
                value,
                codec,
              )
            )
            .exit
        )(
          fails(
            isSubtype[IncorrectVariantRepresentationException](hasMessage(containsString("casa")))
          )
        )
      },
      test("missing value field in variant") {
        val value = ujson.Obj("tag" -> "case1")
        assertZIO(
          ZIO
            .attempt(
              decode(
                value,
                codec,
              )
            )
            .exit
        )(
          fails(
            isSubtype[IncorrectVariantRepresentationException](hasMessage(containsString("value")))
          )
        )
      },
      // This case where which is not handled specifically by JsonCodec,
      // It fails with ujson.InvalidData exception
      test("incorrect value field name in variant") {
        val value = ujson.Obj("tag" -> "case1", "value" -> ujson.Num(15.5))
        assertZIO(
          ZIO
            .attempt(
              decode(
                value,
                codec,
              )
            )
            .exit
        )(fails(isSubtype[InvalidData](anything)))
      },
    )

  def decode[T](
      value: Value,
      codec0: T,
  )(using conv: Conversion[T, Codec[Value]]): DynamicValue =
    val codec = conv(codec0)
    val decodedValue = codec.toDynamicValue(value)
    decodedValue
