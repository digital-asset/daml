// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.struct.circe

import com.google.protobuf.struct
import com.google.protobuf.struct.ListValue
import com.google.protobuf.struct.Value.Kind
import io.circe.Decoder.Result
import io.circe.Json._
import io.circe._

object StructEncoderDecoder extends Encoder[struct.Struct] with Decoder[struct.Struct] {

  override def apply(s: struct.Struct): Json = {
    write(struct.Value.of(Kind.StructValue(s)))
  }

  override def apply(c: HCursor): Result[struct.Struct] = {
    val value = read(c.value)
    if (value.kind.isStructValue) Right(value.getStructValue)
    else Left(DecodingFailure(s"Expected struct, not $value", Nil))
  }

  private def write(value: struct.Value): Json = {
    value.kind match {
      case Kind.BoolValue(v) => Json.fromBoolean(v)
      case Kind.ListValue(v) => Json.fromValues(v.values.map(write))
      case Kind.NumberValue(v) => Json.fromDoubleOrNull(v)
      case Kind.StringValue(v) => Json.fromString(v)
      case Kind.StructValue(v) => Json.fromFields(v.fields.view.mapValues(write))
      case Kind.Empty | Kind.NullValue(_) => Json.Null
    }
  }

  private object StructFolder extends Folder[Kind] {
    def onNull: Kind.NullValue = Kind.NullValue(struct.NullValue.NULL_VALUE)

    def onBoolean(value: Boolean): Kind.BoolValue = Kind.BoolValue(value)

    def onNumber(value: JsonNumber): Kind.NumberValue = Kind.NumberValue(value.toDouble)

    def onString(value: String): Kind.StringValue = Kind.StringValue(value)

    def onArray(value: Vector[Json]): Kind.ListValue = Kind.ListValue(ListValue(value.map(read)))

    def onObject(value: JsonObject): Kind.StructValue =
      Kind.StructValue(struct.Struct.of(value.toMap.view.mapValues(read).toMap))
  }

  private def read(c: Json): struct.Value = struct.Value.of(c.foldWith(StructFolder))

}
