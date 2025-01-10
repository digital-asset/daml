// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.json

import com.daml.ledger.javaapi.data.Value
import com.daml.ledger.javaapi.data.codegen.DefinedDataType
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed.JsonImplicits._
import spray.json.JsValue

final class JsonCodec private (encodeDecimalAsString: Boolean, encodeInt64AsString: Boolean) {
  private val apiCodec = new ApiCodecCompressed(encodeDecimalAsString, encodeInt64AsString)

  /** Converts `javaapi.data.Value` to `JsValue`. */
  def toJsValue(value: Value): JsValue =
    apiCodec.apiValueToJsValue(ValueConversion.toLfValue(value))

  /** Converts `javaapi.data.codegen.DefinedDataType` to `JsValue`. */
  def toJsValue[T](definedDataType: DefinedDataType[T]): JsValue =
    apiCodec.apiValueToJsValue(ValueConversion.toLfValue(definedDataType.toValue))
}

object JsonCodec {
  val encodeAsNumbers = new JsonCodec(false, false)
  def apply(encodeDecimalAsString: Boolean, encodeInt64AsString: Boolean) =
    new JsonCodec(encodeDecimalAsString, encodeInt64AsString)
}
