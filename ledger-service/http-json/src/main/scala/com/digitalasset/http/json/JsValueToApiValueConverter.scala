// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import JsonProtocol.LfValueCodec
import com.digitalasset.daml.lf
import com.digitalasset.http.json.JsValueToApiValueConverter.LfTypeLookup
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.platform.participant.util.LfEngineToApi
import scalaz.{-\/, \/, \/-}
import spray.json.{JsObject, JsValue}

class JsValueToApiValueConverter(lfTypeLookup: LfTypeLookup) {

  def jsValueToApiValue(
      lfId: lf.data.Ref.Identifier,
      jsValue: JsValue): JsonError \/ lav1.value.Value = {

    for {
      lfValue <- \/.fromTryCatchNonFatal(
        LfValueCodec
          .jsValueToApiValue(jsValue, lfId, lfTypeLookup))
        .leftMap(JsonError.toJsonError)

      apiValue <- \/.fromEither(LfEngineToApi.lfValueToApiValue(verbose = true, lfValue))
        .leftMap(JsonError.toJsonError)
    } yield apiValue
  }

  def jsObjectToApiRecord(
      lfId: lf.data.Ref.Identifier,
      jsObject: JsObject): JsonError \/ lav1.value.Record =
    for {
      a <- jsValueToApiValue(lfId, jsObject)
      b <- mustBeApiRecord(a)
    } yield b

  private def mustBeApiRecord(a: lav1.value.Value): JsonError \/ lav1.value.Record = a.sum match {
    case lav1.value.Value.Sum.Record(b) => \/-(b)
    case _ => -\/(JsonError(s"Expected ${classOf[lav1.value.Value.Sum.Record]}, got: $a"))
  }
}

object JsValueToApiValueConverter {
  type LfTypeLookup = lf.data.Ref.Identifier => Option[lf.iface.DefDataType.FWT]
}
