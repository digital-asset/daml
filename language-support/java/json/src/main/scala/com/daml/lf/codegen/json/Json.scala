// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.ledger.javaapi.data.Value
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.lf.value.json.ApiCodecCompressed.JsonImplicits._
import spray.json.JsValue

object Json {
  object LfValueCodec extends ApiCodecCompressed(false, false)
  def toJsValue(value: Value): JsValue =
    LfValueCodec.apiValueToJsValue(ValueConversion.toLfValue(value))
}
