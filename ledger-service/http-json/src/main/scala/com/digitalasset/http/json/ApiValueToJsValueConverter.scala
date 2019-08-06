// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import JsonProtocol.AbsoluteContractIdFormat
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.std.list._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{\/, \/-}
import spray.json.{JsObject, JsValue}

class ApiValueToJsValueConverter(apiToLf: ApiValueToLfValueConverter.ApiValueToLfValue) {

  def apiValueToJsValue(a: lav1.value.Value): JsonError \/ JsValue =
    apiToLf(a)
      .map { b: lf.value.Value[lf.value.Value.AbsoluteContractId] =>
        ApiCodecCompressed.apiValueToJsValue(b)
      }
      .leftMap(x => JsonError(x.shows))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def apiRecordToJsObject(a: lav1.value.Record): JsonError \/ JsObject = {
    a.fields.toList.traverse(convertField).map(fs => JsObject(fs.toMap))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def convertRecord(
      record: List[lav1.value.RecordField]): JsonError \/ List[(String, JsValue)] = {
    record.traverse(convertField)
  }

  private def convertField(field: lav1.value.RecordField): JsonError \/ (String, JsValue) =
    field.value match {
      case None => \/-(field.label -> JsObject.empty)
      case Some(v) => apiValueToJsValue(v).map(field.label -> _)
    }
}
