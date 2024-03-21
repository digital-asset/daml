// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.daml.ledger.api.v1.admin.metering_report_service.GetMeteringReportResponse
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.struct
import com.google.protobuf.struct.Value.Kind
import com.google.protobuf.struct.{ListValue, Struct}
import io.circe.Decoder.Result
import io.circe.Json.*
import io.circe.*

object LedgerMeteringReport {

  def fromProtoV0(
      value: GetMeteringReportResponse
  ): ParsingResult[String] = {

    for {
      s <- ProtoConverter.required("meteringReportJson", value.meteringReportJson)
    } yield {
      StructEncoderDecoder(s).spaces2
    }

  }
}

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

  object StructFolder extends Folder[Kind] {
    def onNull = Kind.NullValue(struct.NullValue.NULL_VALUE)
    def onBoolean(value: Boolean) = Kind.BoolValue(value)
    def onNumber(value: JsonNumber) = Kind.NumberValue(value.toDouble)
    def onString(value: String) = Kind.StringValue(value)
    def onArray(value: Vector[Json]) = Kind.ListValue(ListValue(value.map(read)))
    def onObject(value: JsonObject) =
      Kind.StructValue(Struct.of(value.toMap.view.mapValues(read).toMap))
  }

  private def read(c: Json): struct.Value = struct.Value.of(c.foldWith(StructFolder))

}
