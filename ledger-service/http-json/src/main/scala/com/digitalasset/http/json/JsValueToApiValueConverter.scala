// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.http.json.JsValueToApiValueConverter.LfTypeLookup
import com.digitalasset.http.util.IdentifierConverters
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.platform.participant.util.LfEngineToApi
import scalaz.\/
import scalaz.std.list._
import scalaz.syntax.traverse._
import spray.json.{JsObject, JsValue}

class JsValueToApiValueConverter(lfTypeLookup: LfTypeLookup) {

  def jsValueToApiValue(
      lfId: lf.data.Ref.Identifier,
      jsValue: JsValue): JsonError \/ lav1.value.Value = {

    for {
      lfValue <- \/.fromTryCatchNonFatal(
        ApiCodecCompressed.jsValueToApiValue(jsValue, lfId, lfTypeLookup))
        .leftMap(JsonError.toJsonError)
        .map(toAbsoluteContractId)

      apiValue <- \/.fromEither(LfEngineToApi.lfValueToApiValue(verbose = true, lfValue))
        .leftMap(JsonError.toJsonError)
    } yield apiValue
  }

  def jsObjectToApiRecord(
      lfId: lf.data.Ref.Identifier,
      jsObject: JsObject): JsonError \/ lav1.value.Record =
    for {
      tuples <- convertFields(lfId, jsObject.fields.toList)
      recordFields = tuples.map(t => lav1.value.RecordField(t._1, Some(t._2)))
    } yield lav1.value.Record(Some(IdentifierConverters.apiIdentifier(lfId)), recordFields)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def convertFields(
      lfId: lf.data.Ref.Identifier,
      fields: List[(String, JsValue)]): JsonError \/ List[(String, lav1.value.Value)] = {
    fields.traverse(field => convertField(lfId, field))
  }

  private def convertField(
      lfId: lf.data.Ref.Identifier,
      field: (String, JsValue)): JsonError \/ (String, lav1.value.Value) =
    jsValueToApiValue(lfId, field._2).map((field._1, _))

  private def toAbsoluteContractId(
      a: lf.value.Value[String]): lf.value.Value[lf.value.Value.AbsoluteContractId] =
    a.mapContractId(cid =>
      lf.value.Value.AbsoluteContractId(lf.data.Ref.ContractIdString.assertFromString(cid)))
}

object JsValueToApiValueConverter {
  type LfTypeLookup = lf.data.Ref.Identifier => Option[lf.iface.DefDataType.FWT]
}
