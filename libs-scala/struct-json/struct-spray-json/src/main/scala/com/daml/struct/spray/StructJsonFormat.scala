// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.struct.spray

import com.google.protobuf.struct.Struct
import spray.json.{
  JsArray,
  JsBoolean,
  JsNull,
  JsNumber,
  JsObject,
  JsString,
  JsValue,
  RootJsonFormat,
}

object StructJsonFormat extends RootJsonFormat[Struct] {

  import com.google.protobuf.struct
  import com.google.protobuf.struct.Value.Kind
  import com.google.protobuf.struct.{ListValue, Struct}
  import spray.json.DefaultJsonProtocol._
  import struct.Value.Kind._

  private val base: RootJsonFormat[JsObject] = implicitly[RootJsonFormat[JsObject]]

  override def write(obj: Struct): JsValue = {
    writeValue(struct.Value.of(Kind.StructValue(obj)))
  }

  private def writeValue(value: struct.Value): JsValue = {
    value.kind match {
      case BoolValue(v) => JsBoolean(v)
      case Kind.ListValue(v) => JsArray(v.values.map(writeValue).toVector)
      case NumberValue(v) => JsNumber(v)
      case StringValue(v) => JsString(v)
      case StructValue(v) => JsObject(v.fields.view.mapValues(writeValue).toMap)
      case Empty | NullValue(_) => JsNull
    }
  }

  override def read(json: JsValue): Struct = {
    Struct.of(base.read(json).fields.view.mapValues(readValue).toMap)
  }

  private def readValue(value: JsValue): struct.Value = {
    val kind = value match {
      case v: JsBoolean => Kind.BoolValue(v.value)
      case JsNumber(v) => Kind.NumberValue(v.doubleValue)
      case JsString(v) => Kind.StringValue(v)
      case JsArray(v) => Kind.ListValue(ListValue(v.map(readValue)))
      case JsObject(v) => Kind.StructValue(Struct.of(v.view.mapValues(readValue).toMap))
      case JsNull => Kind.NullValue(struct.NullValue.NULL_VALUE)
    }
    struct.Value.of(kind)
  }
}
