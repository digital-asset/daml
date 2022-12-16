// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.ledger.javaapi.data.Value
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.lf.value.json.ApiCodecCompressed.JsonImplicits._
import spray.json.JsValue

final class JsonCodec(encodeDecimalAsString: Boolean, encodeInt64AsString: Boolean) {
  val apiCodec = new ApiCodecCompressed(encodeDecimalAsString, encodeInt64AsString)

  /** Invoke `toValue()` method of generated java classes to get the `javaapi.data.Value`.
    *  Then use this method to get Json value.
    */
  def toJsValue(value: Value): JsValue =
    apiCodec.apiValueToJsValue(ValueConversion.toLfValue(value))
}

object JsonCodec {
  def apply() = new JsonCodec(false, false)
  def apply(encodeDecimalAsString: Boolean, encodeInt64AsString: Boolean) =
    new JsonCodec(encodeDecimalAsString, encodeInt64AsString)
}
