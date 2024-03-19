// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import JsonProtocol.LfValueCodec
import com.daml.ledger.api.{v2 => lav2}
import com.digitalasset.canton.http.util.ApiValueToLfValueConverter
import scalaz.std.list._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{\/, \/-}
import spray.json.{JsObject, JsValue}

class ApiValueToJsValueConverter(apiToLf: ApiValueToLfValueConverter.ApiValueToLfValue) {

  def apiValueToJsValue(a: lav2.value.Value): JsonError \/ JsValue =
    apiToLf(a)
      .map(LfValueCodec.apiValueToJsValue)
      .leftMap(x => JsonError(x.shows))

  def apiRecordToJsObject[O >: JsObject](a: lav2.value.Record): JsonError \/ O =
    a.fields.toList.traverse(convertField).map(fs => JsObject(fs.toMap))

  private def convertField(field: lav2.value.RecordField): JsonError \/ (String, JsValue) =
    field.value match {
      case None => \/-(field.label -> JsObject.empty)
      case Some(v) => apiValueToJsValue(v).map(field.label -> _)
    }
}
